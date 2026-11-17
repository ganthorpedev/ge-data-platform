"""The child-process step runner and its protocol, exercised as a real subprocess (no database)."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from ge_control_center.worker.protocol import (
    EXIT_INVOCATION_ERROR,
    EXIT_OK,
    EXIT_PARENT_GONE,
    EXIT_STEP_FAILED,
    encode_invocation,
    encode_message,
    parse_message,
)
from ge_control_center.worker.supervisor import build_child_env

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _invocation(step_key: str, params: dict, *, attempt_no: int = 1, version: str = "1") -> str:
    return encode_invocation(
        attempt_id=str(uuid.uuid4()), attempt_no=attempt_no, max_attempts=3, task_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()), node_key="n", step_key=step_key, step_version=version, params=params,
        run_params={}, window_start=None, window_end=None,
    )


def _run(step_key: str, params: dict, **kwargs) -> tuple[int, list[dict], str]:
    process = subprocess.Popen(
        [sys.executable, "-m", "ge_control_center.worker.step_runner"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=PROJECT_ROOT,
    )
    process.stdin.write(_invocation(step_key, params, **kwargs))
    process.stdin.flush()
    # Keep stdin OPEN until the child exits, exactly like the supervisor: EOF means "parent died".
    output: dict[str, str] = {}
    readers = [threading.Thread(target=lambda name=name, pipe=pipe: output.__setitem__(name, pipe.read()))
               for name, pipe in (("out", process.stdout), ("err", process.stderr))]
    for reader in readers:
        reader.start()
    process.wait(timeout=60)
    for reader in readers:
        reader.join(timeout=10)
    process.stdin.close()
    messages = [m for m in (parse_message(line) for line in output["out"].splitlines()) if m]
    return process.returncode, messages, output["out"] + output["err"]


def test_protocol_roundtrip_and_plain_lines() -> None:
    assert parse_message(encode_message({"type": "result", "result": {"a": 1}})) == {"type": "result", "result": {"a": 1}}
    assert parse_message("INFO ordinary log line") is None
    assert parse_message("@@GECC@@ {not json") is None
    assert parse_message('@@GECC@@ {"type": "other"}') is None


def test_success_reports_result() -> None:
    code, messages, _ = _run("core.noop", {"message": "hello"})
    assert code == EXIT_OK
    assert messages == [{"type": "result", "result": {"message": "hello"}}]


def test_ordinary_exception_is_retryable() -> None:
    code, messages, output = _run("core.fail_n_times", {"fail_times": 1}, attempt_no=1)
    assert code == EXIT_STEP_FAILED
    assert messages[-1]["retryable"] is True and messages[-1]["error_class"] == "RuntimeError"
    assert "Traceback" in output


def test_permanent_error_is_not_retryable() -> None:
    code, messages, _ = _run("core.fail_n_times", {"fail_times": 1, "permanent": True})
    assert code == EXIT_STEP_FAILED
    assert messages[-1]["retryable"] is False and messages[-1]["error_class"] == "PermanentStepError"


def test_attempt_number_reaches_the_step() -> None:
    code, messages, _ = _run("core.fail_n_times", {"fail_times": 1}, attempt_no=2)
    assert code == EXIT_OK and messages[-1]["result"] == {"succeeded_on_attempt": 2}


@pytest.mark.parametrize(
    "step_key, params, version, error_class",
    [
        ("core.unknown", {}, "1", "UnknownStepError"),
        ("core.noop", {}, "2", "StepVersionMismatchError"),
        ("core.sleep", {"seconds": -1}, "1", "InvalidParameters"),
        ("core.noop", {"code": "import os"}, "1", "InvalidParameters"),
    ],
)
def test_invocation_errors_are_permanent(step_key, params, version, error_class) -> None:
    code, messages, _ = _run(step_key, params, version=version)
    assert code == EXIT_INVOCATION_ERROR
    assert messages[-1]["error_class"] == error_class and messages[-1]["retryable"] is False


def test_child_exits_when_supervisor_disappears() -> None:
    """Closing stdin is exactly what the OS does when the worker is kill -9'd."""
    process = subprocess.Popen(
        [sys.executable, "-m", "ge_control_center.worker.step_runner"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=PROJECT_ROOT,
    )
    process.stdin.write(_invocation("core.sleep", {"seconds": 60}))
    process.stdin.flush()
    time.sleep(1.5)
    assert process.poll() is None  # still sleeping
    process.stdin.close()
    assert process.wait(timeout=10) == EXIT_PARENT_GONE


def test_child_environment_excludes_control_plane_credentials() -> None:
    env = build_child_env({
        "GECC_CONTROL_DB_URL": "postgresql://secret", "GECC_API_TOKEN": "t",
        "GECC_WAREHOUSE_DB_URL": "postgresql://wh", "PATH": "/bin",
    })
    assert "GECC_CONTROL_DB_URL" not in env and "GECC_API_TOKEN" not in env
    assert env["GECC_WAREHOUSE_DB_URL"] == "postgresql://wh" and env["PYTHONUNBUFFERED"] == "1"
