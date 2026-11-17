"""Pipeline spec model, validation and canonical hashing (ADR-014).

A pipeline spec is pure data -- JSON that maps one-to-one onto a React Flow
graph (nodes + edges) -- and can only reference registered steps::

    {
      "spec_version": 1,
      "pipeline_key": "demo_dag",
      "name": "Demo",
      "nodes": [
        {"key": "extract", "step": "core.noop", "params": {"message": "hi"}},
        {"key": "load", "step": "core.fail_n_times", "params": {"fail_times": 1},
         "retry": {"max_attempts": 3}, "concurrency_keys": ["provider:demo"]}
      ],
      "edges": [{"from": "extract", "to": "load"}],
      "ui_layout": {"extract": {"x": 0, "y": 0}}
    }

Unknown fields anywhere are rejected (so there is no place to smuggle code
in). ``validate_spec`` collects every problem rather than stopping at the
first, so an editor can show them all at once.

The content hash covers only what affects execution: the pipeline key and
the fully *resolved* nodes (step version, normalised params, retry, timeout,
queue, concurrency keys) and edges, in canonical order. The header (name,
description, owner, max_active_runs) is mutable pipeline configuration, and
``ui_layout`` is cosmetic; neither creates a new version.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ge_control_center.registry.errors import RegistryError
from ge_control_center.registry.step import CONCURRENCY_KEY_PATTERN, QUEUE_PATTERN, StepRegistry

NODE_KEY_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
PIPELINE_KEY_PATTERN = r"^[a-z][a-z0-9_]{2,62}$"
MAX_NODES = 200


class SpecValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("invalid pipeline spec:\n  - " + "\n  - ".join(errors))


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class RetrySpec(_Strict):
    max_attempts: int | None = Field(None, ge=1, le=20)
    backoff_seconds: int | None = Field(None, ge=0, le=86400)
    backoff_max_seconds: int | None = Field(None, ge=0, le=86400)


class NodeSpec(_Strict):
    key: str = Field(pattern=NODE_KEY_PATTERN)
    step: str = Field(min_length=3, max_length=128)
    step_version: str | None = Field(None, max_length=32)
    params: dict[str, Any] = Field(default_factory=dict)
    retry: RetrySpec = Field(default_factory=RetrySpec)
    timeout_seconds: int | None = Field(None, ge=1, le=172800)
    queue: str | None = Field(None, pattern=QUEUE_PATTERN.pattern)
    concurrency_keys: list[str] = Field(default_factory=list, max_length=8)


class EdgeSpec(_Strict):
    upstream: str = Field(alias="from")
    downstream: str = Field(alias="to")


class PipelineSpec(_Strict):
    spec_version: Literal[1] = 1
    pipeline_key: str = Field(pattern=PIPELINE_KEY_PATTERN)
    name: str = Field(min_length=1, max_length=200)
    description: str = Field("", max_length=4000)
    owner: str | None = Field(None, max_length=200)
    max_active_runs: int = Field(1, ge=1, le=50)
    nodes: list[NodeSpec] = Field(min_length=1, max_length=MAX_NODES)
    edges: list[EdgeSpec] = Field(default_factory=list)
    ui_layout: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedNode:
    key: str
    step_key: str
    step_version: str
    params: dict[str, Any]
    max_attempts: int
    retry_backoff_seconds: int
    retry_backoff_max_seconds: int
    timeout_seconds: int
    queue: str
    concurrency_keys: tuple[str, ...]

    def canonical(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "step": self.step_key,
            "step_version": self.step_version,
            "params": self.params,
            "max_attempts": self.max_attempts,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "retry_backoff_max_seconds": self.retry_backoff_max_seconds,
            "timeout_seconds": self.timeout_seconds,
            "queue": self.queue,
            "concurrency_keys": list(self.concurrency_keys),
        }


@dataclass(frozen=True)
class ResolvedPipeline:
    pipeline_key: str
    name: str
    description: str
    owner: str | None
    max_active_runs: int
    nodes: tuple[ResolvedNode, ...]
    edges: tuple[tuple[str, str], ...]
    ui_layout: dict[str, Any]

    def canonical_spec(self) -> dict[str, Any]:
        return {
            "spec_version": 1,
            "pipeline_key": self.pipeline_key,
            "nodes": [node.canonical() for node in self.nodes],
            "edges": [{"from": up, "to": down} for up, down in self.edges],
        }

    @property
    def spec_hash(self) -> str:
        canonical = json.dumps(self.canonical_spec(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _format_pydantic_errors(error: ValidationError, prefix: str = "") -> list[str]:
    messages = []
    for item in error.errors():
        location = ".".join(str(part) for part in item["loc"])
        location = f"{prefix}{location}" if location else prefix.rstrip(".") or "spec"
        messages.append(f"{location}: {item['msg']}")
    return messages


def _find_cycle_nodes(node_keys: list[str], edges: list[tuple[str, str]]) -> list[str]:
    """Kahn's algorithm; returns the nodes that could not be ordered (empty if acyclic)."""
    indegree = {key: 0 for key in node_keys}
    children: dict[str, list[str]] = defaultdict(list)
    for up, down in edges:
        children[up].append(down)
        indegree[down] += 1
    queue = deque(key for key, degree in indegree.items() if degree == 0)
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited == len(node_keys):
        return []
    return sorted(key for key, degree in indegree.items() if degree > 0)


def parse_spec(raw: Any) -> PipelineSpec:
    if not isinstance(raw, dict):
        raise SpecValidationError(["spec must be a JSON object"])
    try:
        return PipelineSpec.model_validate(raw)
    except ValidationError as error:
        raise SpecValidationError(_format_pydantic_errors(error)) from error


def validate_spec(raw: Any, registry: StepRegistry) -> ResolvedPipeline:
    """Validate a raw spec against the registry and return the resolved, hashable pipeline."""
    spec = parse_spec(raw)
    errors: list[str] = []

    node_keys = [node.key for node in spec.nodes]
    duplicates = sorted({key for key in node_keys if node_keys.count(key) > 1})
    if duplicates:
        errors.append(f"duplicate node keys: {', '.join(duplicates)}")
    known = set(node_keys)

    edge_pairs: list[tuple[str, str]] = []
    for index, edge in enumerate(spec.edges):
        where = f"edges[{index}]"
        if edge.upstream not in known:
            errors.append(f"{where}: unknown upstream node {edge.upstream!r}")
        if edge.downstream not in known:
            errors.append(f"{where}: unknown downstream node {edge.downstream!r}")
        if edge.upstream == edge.downstream:
            errors.append(f"{where}: node {edge.upstream!r} cannot depend on itself")
        pair = (edge.upstream, edge.downstream)
        if pair in edge_pairs:
            errors.append(f"{where}: duplicate edge {edge.upstream!r} -> {edge.downstream!r}")
        edge_pairs.append(pair)

    if not errors:
        cycle = _find_cycle_nodes(node_keys, edge_pairs)
        if cycle:
            errors.append(f"the graph has a cycle involving: {', '.join(cycle)}")

    unknown_layout = sorted(set(spec.ui_layout) - known)
    if unknown_layout:
        errors.append(f"ui_layout references unknown nodes: {', '.join(unknown_layout)}")

    resolved: list[ResolvedNode] = []
    for index, node in enumerate(spec.nodes):
        where = f"nodes[{index}] ({node.key})"
        try:
            definition = registry.get(node.step, node.step_version)
        except RegistryError as error:
            errors.append(f"{where}: {error}")
            continue

        try:
            params = definition.params_model.model_validate(node.params).model_dump(mode="json")
        except ValidationError as error:
            errors.extend(_format_pydantic_errors(error, prefix=f"{where}.params."))
            continue

        max_attempts = node.retry.max_attempts or definition.max_attempts
        backoff = definition.retry_backoff_seconds if node.retry.backoff_seconds is None else node.retry.backoff_seconds
        backoff_max = (
            max(definition.retry_backoff_max_seconds, backoff)
            if node.retry.backoff_max_seconds is None
            else node.retry.backoff_max_seconds
        )
        if backoff_max < backoff:
            errors.append(f"{where}: retry.backoff_max_seconds must be >= retry.backoff_seconds")
            continue

        node_keys_extra = []
        for key in node.concurrency_keys:
            if not CONCURRENCY_KEY_PATTERN.fullmatch(key):
                errors.append(f"{where}: concurrency key {key!r} must match {CONCURRENCY_KEY_PATTERN.pattern}")
            node_keys_extra.append(key)

        resolved.append(
            ResolvedNode(
                key=node.key,
                step_key=definition.key,
                step_version=definition.version,
                params=params,
                max_attempts=max_attempts,
                retry_backoff_seconds=backoff,
                retry_backoff_max_seconds=backoff_max,
                timeout_seconds=node.timeout_seconds or definition.timeout_seconds,
                queue=node.queue or definition.queue,
                concurrency_keys=tuple(sorted(set(definition.concurrency_keys) | set(node_keys_extra))),
            )
        )

    if errors:
        raise SpecValidationError(errors)

    return ResolvedPipeline(
        pipeline_key=spec.pipeline_key,
        name=spec.name,
        description=spec.description,
        owner=spec.owner,
        max_active_runs=spec.max_active_runs,
        nodes=tuple(sorted(resolved, key=lambda n: n.key)),
        edges=tuple(sorted(edge_pairs)),
        ui_layout=spec.ui_layout,
    )
