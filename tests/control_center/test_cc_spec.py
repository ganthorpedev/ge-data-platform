from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from ge_control_center.dag.spec import SpecValidationError, validate_spec
from ge_control_center.registry.step import load_default_registry

EXAMPLES = Path(__file__).resolve().parents[2] / "pipelines" / "examples"
REGISTRY = load_default_registry()


def _spec(**overrides):
    spec = {
        "pipeline_key": "unit_dag",
        "name": "Unit",
        "nodes": [
            {"key": "a", "step": "core.noop"},
            {"key": "b", "step": "core.fail_n_times", "params": {"fail_times": 1}},
            {"key": "c", "step": "core.sleep", "params": {"seconds": 1}, "concurrency_keys": ["provider:x"]},
        ],
        "edges": [{"from": "a", "to": "b"}, {"from": "a", "to": "c"}],
    }
    spec.update(overrides)
    return spec


def _errors(spec) -> list[str]:
    with pytest.raises(SpecValidationError) as info:
        validate_spec(spec, REGISTRY)
    return info.value.errors


@pytest.mark.parametrize("name", ["m1_demo", "m1_retry", "m1_sleep", "m1_timeout"])
def test_example_pipelines_are_valid(name) -> None:
    validate_spec(json.loads((EXAMPLES / f"{name}.json").read_text()), REGISTRY)


def test_resolution_applies_step_defaults_and_normalises_params() -> None:
    resolved = validate_spec(_spec(), REGISTRY)
    nodes = {n.key: n for n in resolved.nodes}
    assert nodes["a"].params == {"message": "noop"}  # default filled in
    assert nodes["b"].max_attempts == 3 and nodes["b"].retry_backoff_seconds == 2
    assert nodes["c"].concurrency_keys == ("provider:x",)
    assert nodes["c"].params == {"seconds": 1.0, "progress_every_seconds": 5.0}
    assert resolved.edges == (("a", "b"), ("a", "c"))


def test_hash_ignores_ordering_layout_and_header() -> None:
    base = validate_spec(_spec(), REGISTRY).spec_hash
    shuffled = _spec(nodes=list(reversed(_spec()["nodes"])), edges=list(reversed(_spec()["edges"])),
                     ui_layout={"a": {"x": 5, "y": 9}}, name="Renamed", description="d", max_active_runs=3)
    assert validate_spec(shuffled, REGISTRY).spec_hash == base


def test_hash_changes_when_execution_changes() -> None:
    base = validate_spec(_spec(), REGISTRY).spec_hash
    changed = copy.deepcopy(_spec())
    changed["nodes"][1]["params"]["fail_times"] = 2
    assert validate_spec(changed, REGISTRY).spec_hash != base
    rewired = _spec(edges=[{"from": "a", "to": "b"}, {"from": "b", "to": "c"}])
    assert validate_spec(rewired, REGISTRY).spec_hash != base


def test_cycle_is_rejected() -> None:
    errors = _errors(_spec(edges=[{"from": "a", "to": "b"}, {"from": "b", "to": "c"}, {"from": "c", "to": "a"}]))
    assert any("cycle" in e and "a, b, c" in e for e in errors)


def test_all_semantic_errors_are_reported_together() -> None:
    spec = _spec(
        nodes=[
            {"key": "a", "step": "core.nope"},
            {"key": "b", "step": "core.sleep", "params": {"seconds": -1, "shell": "x"}},
            {"key": "b", "step": "core.noop"},
        ],
        edges=[{"from": "a", "to": "zzz"}, {"from": "a", "to": "a"}],
        ui_layout={"ghost": {}},
    )
    joined = "\n".join(_errors(spec))
    for fragment in ["duplicate node keys: b", "unknown downstream node 'zzz'", "cannot depend on itself",
                     "unknown step 'core.nope'", "greater than or equal to 0", "shell: Extra inputs",
                     "ui_layout references unknown nodes: ghost"]:
        assert fragment in joined


@pytest.mark.parametrize("field", ["code", "python", "module", "callable"])
def test_no_field_can_carry_code(field) -> None:
    spec = _spec()
    spec["nodes"][0][field] = "import os; os.system('id')"
    assert any("Extra inputs" in e for e in _errors(spec))


def test_step_version_pinning() -> None:
    spec = _spec()
    spec["nodes"][0]["step_version"] = "99"
    assert any("version '99' requested" in e for e in _errors(spec))


def test_retry_override_bounds() -> None:
    spec = _spec()
    spec["nodes"][1]["retry"] = {"backoff_seconds": 50, "backoff_max_seconds": 10}
    assert any("backoff_max_seconds" in e for e in _errors(spec))
    spec["nodes"][1]["retry"] = {"max_attempts": 21}
    assert any("max_attempts" in e for e in _errors(spec))
