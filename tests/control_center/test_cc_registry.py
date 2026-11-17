from __future__ import annotations

import pytest
from pydantic import Field

from ge_control_center.registry import (
    EmptyParams,
    RegistryError,
    StepParams,
    StepRegistry,
    StepVersionMismatchError,
    UnknownStepError,
    step,
)
from ge_control_center.registry.step import load_default_registry


class _Params(StepParams):
    n: int = Field(1, ge=0)


def _register(registry: StepRegistry, **overrides):
    options = dict(key="test.thing", version="1", display_name="Thing", idempotency="read_only",
                   params=_Params, registry=registry)
    options.update(overrides)

    @step(**options)
    def thing(ctx, params):
        return {"n": params.n}

    return thing


def test_default_registry_contains_the_m1_steps() -> None:
    keys = [d.key for d in load_default_registry()]
    assert keys == [
        "core.fail_n_times",
        "core.noop",
        "core.sleep",
        "warehouse.validate_ge_warehouse_baseline",
    ]


def test_register_and_lookup_by_key_and_version() -> None:
    registry = StepRegistry()
    _register(registry)
    definition = registry.get("test.thing", "1")
    assert definition.version == "1"
    assert definition.param_schema()["properties"]["n"]["minimum"] == 0
    with pytest.raises(StepVersionMismatchError):
        registry.get("test.thing", "2")
    with pytest.raises(UnknownStepError):
        registry.get("test.other")


def test_duplicate_key_is_rejected() -> None:
    registry = StepRegistry()
    _register(registry)
    with pytest.raises(RegistryError, match="already registered"):
        _register(registry)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"key": "NoDots"}, "namespace.name"),
        ({"version": "has space"}, "version"),
        ({"idempotency": "exactly_once"}, "idempotency"),
        ({"concurrency_keys": ("Bad Key",)}, "concurrency key"),
        ({"concurrency_keys": ("a:b", "a:b")}, "unique"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"timeout_seconds": 0}, "timeout_seconds"),
        ({"retry_backoff_seconds": 100, "retry_backoff_max_seconds": 10}, "retry_backoff_max_seconds"),
        ({"params": dict}, "StepParams"),
    ],
)
def test_invalid_definitions_are_rejected(overrides, message) -> None:
    with pytest.raises(RegistryError, match=message):
        _register(StepRegistry(), **overrides)


def test_params_models_reject_unknown_fields() -> None:
    with pytest.raises(Exception, match="Extra inputs"):
        _Params.model_validate({"n": 1, "shell": "rm -rf /"})
    with pytest.raises(Exception, match="Extra inputs"):
        EmptyParams.model_validate({"code": "x"})


def test_definition_hash_tracks_contract_not_cosmetics() -> None:
    a, b, c = StepRegistry(), StepRegistry(), StepRegistry()
    _register(a)
    _register(b, display_name="Renamed", description="new words")
    _register(c, timeout_seconds=5)
    assert a.get("test.thing").definition_hash() == b.get("test.thing").definition_hash()
    assert a.get("test.thing").definition_hash() != c.get("test.thing").definition_hash()
    assert a.fingerprint() == b.fingerprint() != c.fingerprint()
