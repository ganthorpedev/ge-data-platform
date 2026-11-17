"""The ``@step`` decorator and the in-process step registry (ADR-013).

A step is a plain Python function ``fn(ctx: StepContext, params: P) -> dict | None``
whose parameters are a Pydantic model. Registration happens at import time
of a module listed explicitly in ``ge_control_center.steps`` -- there is no
dynamic import by name, no ``eval``, and no way for a pipeline definition to
introduce code. A pipeline can only reference ``(step key, version)`` and
supply parameters that validate against the step's model.

The execution *contract* of a step -- its parameter schema, idempotency mode,
concurrency keys, queue and retry/timeout defaults -- is hashed into
``definition_hash``. Changing any of those without bumping ``version`` is
rejected by ``gecc registry sync``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict

from ge_control_center.registry.context import StepContext
from ge_control_center.registry.errors import (
    RegistryError,
    StepVersionMismatchError,
    UnknownStepError,
)

IdempotencyMode = Literal["read_only", "upsert", "replace_partition", "full_replace", "append_dedup"]
IDEMPOTENCY_MODES: frozenset[str] = frozenset(IdempotencyMode.__args__)  # type: ignore[attr-defined]

STEP_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,32}$")
CONCURRENCY_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")
QUEUE_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")


class StepParams(BaseModel):
    """Base class for step parameter models: unknown fields are rejected, values are immutable."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EmptyParams(StepParams):
    """For steps that take no parameters."""


P = TypeVar("P", bound=StepParams)
StepFunction = Callable[[StepContext, Any], Mapping[str, Any] | None]


@dataclass(frozen=True)
class StepDefinition:
    key: str
    version: str
    display_name: str
    description: str
    params_model: type[StepParams]
    func: StepFunction
    idempotency: str
    concurrency_keys: tuple[str, ...]
    queue: str
    max_attempts: int
    retry_backoff_seconds: int
    retry_backoff_max_seconds: int
    timeout_seconds: int

    def param_schema(self) -> dict[str, Any]:
        return self.params_model.model_json_schema()

    def contract(self) -> dict[str, Any]:
        """The fields that define how this step version behaves. Hashed for drift detection."""
        return {
            "key": self.key,
            "version": self.version,
            "param_schema": self.param_schema(),
            "idempotency": self.idempotency,
            "concurrency_keys": list(self.concurrency_keys),
            "queue": self.queue,
            "max_attempts": self.max_attempts,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "retry_backoff_max_seconds": self.retry_backoff_max_seconds,
            "timeout_seconds": self.timeout_seconds,
        }

    def definition_hash(self) -> str:
        canonical = json.dumps(self.contract(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_definition(definition: StepDefinition) -> None:
    problems: list[str] = []
    if not STEP_KEY_PATTERN.fullmatch(definition.key):
        problems.append(f"key {definition.key!r} must look like 'namespace.name' (lowercase, dotted)")
    if not VERSION_PATTERN.fullmatch(definition.version):
        problems.append(f"version {definition.version!r} must match {VERSION_PATTERN.pattern}")
    if definition.idempotency not in IDEMPOTENCY_MODES:
        problems.append(f"idempotency {definition.idempotency!r} must be one of {sorted(IDEMPOTENCY_MODES)}")
    for key in definition.concurrency_keys:
        if not CONCURRENCY_KEY_PATTERN.fullmatch(key):
            problems.append(f"concurrency key {key!r} must match {CONCURRENCY_KEY_PATTERN.pattern}")
    if len(set(definition.concurrency_keys)) != len(definition.concurrency_keys):
        problems.append("concurrency keys must be unique")
    if not QUEUE_PATTERN.fullmatch(definition.queue):
        problems.append(f"queue {definition.queue!r} must match {QUEUE_PATTERN.pattern}")
    if not 1 <= definition.max_attempts <= 20:
        problems.append("max_attempts must be between 1 and 20")
    if not 0 <= definition.retry_backoff_seconds <= 86400:
        problems.append("retry_backoff_seconds must be between 0 and 86400")
    if not definition.retry_backoff_seconds <= definition.retry_backoff_max_seconds <= 86400:
        problems.append("retry_backoff_max_seconds must be between retry_backoff_seconds and 86400")
    if not 1 <= definition.timeout_seconds <= 172800:
        problems.append("timeout_seconds must be between 1 and 172800")
    if not (isinstance(definition.params_model, type) and issubclass(definition.params_model, StepParams)):
        problems.append("params must be a subclass of StepParams (extra fields forbidden)")
    if not definition.display_name.strip():
        problems.append("display_name is required")
    if problems:
        raise RegistryError(f"invalid step {definition.key!r}: " + "; ".join(problems))


class StepRegistry:
    """Steps registered in this process, keyed by step key (one code version per key)."""

    def __init__(self) -> None:
        self._steps: dict[str, StepDefinition] = {}

    def register(self, definition: StepDefinition) -> None:
        _validate_definition(definition)
        existing = self._steps.get(definition.key)
        if existing is not None and existing.func is not definition.func:
            raise RegistryError(f"step {definition.key!r} is already registered")
        self._steps[definition.key] = definition

    def get(self, key: str, version: str | None = None) -> StepDefinition:
        definition = self._steps.get(key)
        if definition is None:
            raise UnknownStepError(f"unknown step {key!r}")
        if version is not None and version != definition.version:
            raise StepVersionMismatchError(
                f"step {key!r} version {version!r} requested, but this code provides version {definition.version!r}"
            )
        return definition

    def __contains__(self, key: object) -> bool:
        return key in self._steps

    def __iter__(self) -> Iterator[StepDefinition]:
        return iter(sorted(self._steps.values(), key=lambda d: d.key))

    def __len__(self) -> int:
        return len(self._steps)

    def fingerprint(self) -> str:
        """Hash of every registered step contract; identifies a worker's code for compatibility checks."""
        joined = "\n".join(f"{d.key}@{d.version}:{d.definition_hash()}" for d in self)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()


default_registry = StepRegistry()


def step(
    *,
    key: str,
    version: str,
    display_name: str,
    idempotency: IdempotencyMode,
    params: type[StepParams] = EmptyParams,
    description: str = "",
    concurrency_keys: tuple[str, ...] | list[str] = (),
    queue: str = "default",
    max_attempts: int = 3,
    retry_backoff_seconds: int = 30,
    retry_backoff_max_seconds: int = 900,
    timeout_seconds: int = 3600,
    registry: StepRegistry | None = None,
) -> Callable[[StepFunction], StepFunction]:
    """Register ``func`` as a step. Returns the function unchanged."""

    def decorate(func: StepFunction) -> StepFunction:
        definition = StepDefinition(
            key=key,
            version=version,
            display_name=display_name,
            description=description.strip(),
            params_model=params,
            func=func,
            idempotency=idempotency,
            concurrency_keys=tuple(concurrency_keys),
            queue=queue,
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_backoff_max_seconds=retry_backoff_max_seconds,
            timeout_seconds=timeout_seconds,
        )
        (registry if registry is not None else default_registry).register(definition)
        return func

    return decorate


def load_default_registry() -> StepRegistry:
    """Import the explicit list of step modules and return the populated default registry."""
    import ge_control_center.steps  # noqa: F401  (static import: registers built-in steps)

    return default_registry
