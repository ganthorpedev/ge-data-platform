"""Worker self-fencing, exercised with a REAL worker and REAL step subprocess against a fake control plane.

These cover the case a worker crash cannot: the worker is alive, but it can no
longer prove it owns the lease (control DB unreachable / lease reaped). The
worker must kill its child and must NOT report a result.
"""

from __future__ import annotations

import threading
import time
import uuid

from sqlalchemy.exc import OperationalError

from ge_control_center.controlplane.client import ClaimedAttempt, LeaseStatus
from ge_control_center.registry.step import load_default_registry
from ge_control_center.worker.supervisor import Worker, WorkerOptions

REGISTRY = load_default_registry()


class FakeControlPlane:
    def __init__(self, renew_behaviour: str, step_seconds: float = 30) -> None:
        self.renew_behaviour = renew_behaviour  # "unreachable" | "rejected" | "cancel"
        self.step_seconds = step_seconds
        self.claimed = False
        self.finishes: list[dict] = []
        self.renewals = 0
        self.claimed_at: float | None = None

    # registration / heartbeat
    def register_worker(self, *args) -> None: ...
    def worker_heartbeat(self, *args) -> bool: return True
    def stop_worker(self, *args) -> None: ...
    def reap_expired_leases(self, *args) -> int: return 0
    def append_logs(self, attempt_id, token, lines) -> int: return len(lines)

    def active_step_versions(self):
        return {d.key: (d.version, d.definition_hash()) for d in REGISTRY}

    def claim(self, worker_id, queues, lease_seconds):
        if self.claimed:
            return None
        self.claimed, self.claimed_at = True, time.monotonic()
        return ClaimedAttempt(
            attempt_id=str(uuid.uuid4()), lease_token=str(uuid.uuid4()), attempt_no=1, task_id=str(uuid.uuid4()),
            run_id=str(uuid.uuid4()), node_key="nap", step_key="core.sleep", step_version="1",
            params={"seconds": self.step_seconds}, run_params={}, window_start=None, window_end=None,
            max_attempts=3, timeout_seconds=600,
        )

    def renew_lease(self, attempt_id, token, lease_seconds):
        self.renewals += 1
        if self.renew_behaviour == "unreachable":
            raise OperationalError("SELECT renew_lease", {}, Exception("connection refused"))
        if self.renew_behaviour == "rejected":
            return LeaseStatus(ok=False, cancel_requested=False)
        return LeaseStatus(ok=True, cancel_requested=True)

    def finish(self, attempt_id, token, **kwargs):
        self.finishes.append(kwargs)
        return "CANCELLED" if kwargs["outcome"] == "CANCELLED" else "SUCCEEDED"


def _run_worker(fake: FakeControlPlane, *, lease_seconds: int = 6, safety: int = 2) -> tuple[Worker, float]:
    options = WorkerOptions(name="fence-test", queues=("default",), capacity=1, lease_seconds=lease_seconds,
                            heartbeat_seconds=1, lease_safety_seconds=safety, poll_seconds=0.1, reap_seconds=3600,
                            drain_seconds=0, terminate_grace_seconds=2)
    worker = Worker(fake, options, registry=REGISTRY)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(0.2)
        if fake.claimed and not worker._active():
            break
    finished_after = time.monotonic() - (fake.claimed_at or time.monotonic())
    worker.request_stop()
    thread.join(timeout=15)
    return worker, finished_after


def test_worker_self_fences_when_control_plane_is_unreachable() -> None:
    fake = FakeControlPlane("unreachable")
    _, elapsed = _run_worker(fake, lease_seconds=6, safety=2)
    # Must stop the 30 s step at ~ lease - safety = 4 s, well before the lease could expire on the server.
    assert elapsed < 6, f"child not stopped before lease expiry ({elapsed:.1f}s)"
    assert fake.renewals >= 2
    assert fake.finishes == [], "a fenced attempt must not report an outcome"


def test_worker_self_fences_immediately_when_lease_is_rejected() -> None:
    fake = FakeControlPlane("rejected")
    _, elapsed = _run_worker(fake)
    assert elapsed < 4, f"child not stopped promptly after rejection ({elapsed:.1f}s)"
    assert fake.finishes == []


def test_cancel_signal_on_renewal_stops_child_and_reports_cancelled() -> None:
    fake = FakeControlPlane("cancel")
    _, elapsed = _run_worker(fake)
    assert elapsed < 6
    assert [f["outcome"] for f in fake.finishes] == ["CANCELLED"]
    assert fake.finishes[0]["retryable"] is False


class OutageControlPlane(FakeControlPlane):
    """Control DB 'down' for renewals AND log appends until `outage_seconds` after the claim."""

    def __init__(self, outage_seconds: float) -> None:
        super().__init__("unreachable")
        self.outage_seconds = outage_seconds
        self.append_calls = 0
        self.delivered: dict[int, str] = {}

    def _down(self) -> bool:
        return self.claimed_at is not None and time.monotonic() - self.claimed_at < self.outage_seconds

    def append_logs(self, attempt_id, token, lines) -> int:
        self.append_calls += 1
        if self._down():
            raise OperationalError("SELECT append_task_logs", {}, Exception("connection refused"))
        for line in lines:
            self.delivered[line.seq] = line.line
        return len(lines)


def test_fenced_attempt_logs_survive_an_outage_and_shipping_backs_off(caplog) -> None:
    fake = OutageControlPlane(outage_seconds=9)
    with caplog.at_level("WARNING", logger="ge_control_center.worker.supervisor"):
        _run_worker(fake, lease_seconds=6, safety=2)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not any("child killed" in l for l in fake.delivered.values()):
            time.sleep(0.2)

    assert fake.finishes == [], "fenced attempt must not report"
    seqs = sorted(fake.delivered)
    assert seqs == list(range(1, len(seqs) + 1)), f"log lines lost or out of order: {seqs}"
    assert any("child killed; result not reported" in line for line in fake.delivered.values())
    assert any("attempt 1/3 starting" in line for line in fake.delivered.values()), "step output produced during the outage was lost"
    # ~9 s outage: per-second retries would mean ~9+ append calls; backoff keeps it to a handful.
    assert fake.append_calls <= 7, f"log shipping did not back off ({fake.append_calls} calls)"
    ship_warnings = [r for r in caplog.records if "cannot ship log lines" in r.getMessage()]
    assert len(ship_warnings) == 1, f"expected one warning per failure streak, got {len(ship_warnings)}"
