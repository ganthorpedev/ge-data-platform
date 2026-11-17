"""End-to-end: a real Worker (in a thread) runs real step subprocesses against a real control DB."""

from __future__ import annotations

import threading
import time
import uuid

import pytest
from sqlalchemy import text

from ge_control_center.controlplane.client import SqlControlPlaneClient
from ge_control_center.registry.step import load_default_registry
from ge_control_center.worker.supervisor import RegistryMismatchError, Worker, WorkerOptions


def _options(queue: str, **overrides) -> WorkerOptions:
    values = dict(name=f"e2e-{uuid.uuid4().hex[:6]}", queues=(queue,), capacity=2, lease_seconds=10,
                  heartbeat_seconds=1, lease_safety_seconds=3, poll_seconds=0.2, reap_seconds=1,
                  drain_seconds=2, terminate_grace_seconds=2)
    values.update(overrides)
    return WorkerOptions(**values)


def _start(engine, options) -> tuple[Worker, threading.Thread]:
    worker = Worker(SqlControlPlaneClient(engine), options, registry=load_default_registry())
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    return worker, thread


def _wait_for(predicate, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("condition not met in time")


def test_worker_runs_a_dag_with_retry_and_ships_logs(engine, make_pipeline, h) -> None:
    pipeline = make_pipeline(
        [{"key": "first", "step": "core.noop", "params": {"message": "hello from e2e"}},
         {"key": "flaky", "step": "core.fail_n_times", "params": {"fail_times": 1},
          "retry": {"max_attempts": 3, "backoff_seconds": 0}},
         {"key": "last", "step": "core.noop"}],
        [("first", "flaky"), ("flaky", "last")],
    )
    run = h.create_run(engine, pipeline["key"], f"e2e-{uuid.uuid4()}")
    worker, thread = _start(engine, _options(pipeline["queue"]))
    try:
        _wait_for(lambda: h.run_state(engine, run["run_id"]) in {"SUCCEEDED", "FAILED"})
    finally:
        worker.request_stop()
        thread.join(timeout=30)

    assert h.run_state(engine, run["run_id"]) == "SUCCEEDED"
    with engine.connect() as conn:
        attempts = conn.execute(text("""
            SELECT t.node_key, a.attempt_no, a.state FROM orchestration.task_attempt a
            JOIN orchestration.task t ON t.task_id = a.task_id
            WHERE a.run_id = CAST(:r AS uuid) ORDER BY a.started_at"""), {"r": run["run_id"]}).all()
        logs = conn.execute(text("""
            SELECT l.line FROM orchestration.task_log l JOIN orchestration.task_attempt a USING (attempt_id)
            WHERE a.run_id = CAST(:r AS uuid)"""), {"r": run["run_id"]}).scalars().all()
        worker_status = conn.execute(text("SELECT status FROM orchestration.worker WHERE worker_id = CAST(:w AS uuid)"),
                                     {"w": worker.worker_id}).scalar_one()
    assert attempts == [("first", 1, "SUCCEEDED"), ("flaky", 1, "FAILED"), ("flaky", 2, "SUCCEEDED"),
                        ("last", 1, "SUCCEEDED")]
    assert any("hello from e2e" in line for line in logs)
    assert any("deliberate failure on attempt 1" in line for line in logs)
    assert worker_status == "STOPPED"


def test_timeout_then_cancel(engine, make_pipeline, h) -> None:
    pipeline = make_pipeline(
        [{"key": "slow", "step": "core.sleep", "params": {"seconds": 60}, "timeout_seconds": 2,
          "retry": {"max_attempts": 2, "backoff_seconds": 0}}],
        max_active_runs=2,
    )
    timed = h.create_run(engine, pipeline["key"], f"timeout-{uuid.uuid4()}")
    worker, thread = _start(engine, _options(pipeline["queue"]))
    try:
        _wait_for(lambda: h.run_state(engine, timed["run_id"]) == "FAILED", timeout=60)

        cancelled = h.create_run(engine, pipeline["key"], f"cancel-{uuid.uuid4()}")
        _wait_for(lambda: h.task_states(engine, cancelled["run_id"]) == {"slow": "RUNNING"})
        with engine.begin() as conn:
            conn.execute(text("SELECT orchestration.request_cancel(CAST(:r AS uuid), 'pytest')"),
                         {"r": cancelled["run_id"]})
        _wait_for(lambda: h.run_state(engine, cancelled["run_id"]) == "CANCELLED", timeout=30)
    finally:
        worker.request_stop()
        thread.join(timeout=30)

    with engine.connect() as conn:
        timed_attempts = conn.execute(text("SELECT state, error_class FROM orchestration.task_attempt "
                                           "WHERE run_id = CAST(:r AS uuid) ORDER BY attempt_no"),
                                      {"r": timed["run_id"]}).all()
    assert timed_attempts == [("TIMED_OUT", "Timeout"), ("TIMED_OUT", "Timeout")]


def test_worker_refuses_to_start_with_mismatched_registry(engine, unique) -> None:
    from ge_control_center.registry import StepRegistry

    worker = Worker(SqlControlPlaneClient(engine), _options(f"q{unique}"), registry=StepRegistry())
    with pytest.raises(RegistryMismatchError, match="not provided by this worker"):
        worker.run()
