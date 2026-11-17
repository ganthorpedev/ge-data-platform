"""Worker-protocol client (ADR-021).

Workers interact with the control plane ONLY through this interface. The
SQL implementation calls the SECURITY DEFINER functions the ``cc_worker``
role may execute; a future HTTP implementation (for workers without direct
database access) can implement the same protocol without changing worker
or step code.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ClaimedAttempt:
    attempt_id: str
    lease_token: str
    attempt_no: int
    task_id: str
    run_id: str
    node_key: str
    step_key: str
    step_version: str
    params: dict[str, Any]
    run_params: dict[str, Any]
    window_start: datetime | None
    window_end: datetime | None
    max_attempts: int
    timeout_seconds: int

    @property
    def label(self) -> str:
        return f"{self.run_id[:8]}/{self.node_key}#{self.attempt_no}"


@dataclass(frozen=True)
class LeaseStatus:
    ok: bool
    cancel_requested: bool


@dataclass(frozen=True)
class LogLine:
    seq: int
    logged_at: datetime
    stream: str
    line: str


class ControlPlaneClient(Protocol):
    def register_worker(self, worker_id: str, name: str, hostname: str, pid: int,
                        queues: Sequence[str], capacity: int, code_version: str) -> None: ...
    def worker_heartbeat(self, worker_id: str, status: str = "ACTIVE") -> bool: ...
    def stop_worker(self, worker_id: str) -> None: ...
    def claim(self, worker_id: str, queues: Sequence[str], lease_seconds: int) -> ClaimedAttempt | None: ...
    def renew_lease(self, attempt_id: str, lease_token: str, lease_seconds: int) -> LeaseStatus: ...
    def finish(self, attempt_id: str, lease_token: str, *, outcome: str, retryable: bool,
               exit_code: int | None = None, error_class: str | None = None,
               error_message: str | None = None, result: dict[str, Any] | None = None) -> str | None: ...
    def append_logs(self, attempt_id: str, lease_token: str, lines: Sequence[LogLine]) -> int: ...
    def reap_expired_leases(self, limit: int = 100, worker_lost_after_seconds: int = 120) -> int: ...
    def active_step_versions(self) -> dict[str, tuple[str, str]]: ...


class SqlControlPlaneClient:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def register_worker(self, worker_id, name, hostname, pid, queues, capacity, code_version) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("SELECT orchestration.register_worker(CAST(:id AS uuid), :n, :h, :pid, CAST(:q AS text[]), :c, :v)"),
                {"id": worker_id, "n": name, "h": hostname, "pid": pid, "q": list(queues), "c": capacity, "v": code_version},
            )

    def worker_heartbeat(self, worker_id, status="ACTIVE") -> bool:
        with self.engine.begin() as conn:
            return bool(conn.execute(
                text("SELECT orchestration.worker_heartbeat(CAST(:id AS uuid), :s)"), {"id": worker_id, "s": status}
            ).scalar_one())

    def stop_worker(self, worker_id) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("SELECT orchestration.stop_worker(CAST(:id AS uuid))"), {"id": worker_id})

    def claim(self, worker_id, queues, lease_seconds) -> ClaimedAttempt | None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM orchestration.claim_task(CAST(:id AS uuid), CAST(:q AS text[]), :l)"),
                {"id": worker_id, "q": list(queues), "l": lease_seconds},
            ).mappings().first()
        if row is None:
            return None
        return ClaimedAttempt(
            attempt_id=str(row["attempt_id"]),
            lease_token=str(row["lease_token"]),
            attempt_no=row["attempt_no"],
            task_id=str(row["task_id"]),
            run_id=str(row["run_id"]),
            node_key=row["node_key"],
            step_key=row["step_key"],
            step_version=row["step_code_version"],
            params=dict(row["params"] or {}),
            run_params=dict(row["run_params"] or {}),
            window_start=row["window_start"],
            window_end=row["window_end"],
            max_attempts=row["max_attempts"],
            timeout_seconds=row["timeout_seconds"],
        )

    def renew_lease(self, attempt_id, lease_token, lease_seconds) -> LeaseStatus:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT * FROM orchestration.renew_lease(CAST(:a AS uuid), CAST(:t AS uuid), :l)"),
                {"a": attempt_id, "t": lease_token, "l": lease_seconds},
            ).mappings().one()
        return LeaseStatus(ok=bool(row["lease_ok"]), cancel_requested=bool(row["cancel_requested"]))

    def finish(self, attempt_id, lease_token, *, outcome, retryable, exit_code=None, error_class=None,
               error_message=None, result=None) -> str | None:
        with self.engine.begin() as conn:
            return conn.execute(
                text("""
                    SELECT orchestration.finish_attempt(CAST(:a AS uuid), CAST(:t AS uuid), :o, :r, :code,
                                                        :cls, :msg, CAST(:res AS jsonb))
                """),
                {"a": attempt_id, "t": lease_token, "o": outcome, "r": retryable, "code": exit_code,
                 "cls": error_class, "msg": error_message,
                 "res": None if result is None else json.dumps(result, default=str, sort_keys=True)},
            ).scalar_one()

    def append_logs(self, attempt_id, lease_token, lines) -> int:
        if not lines:
            return 0
        with self.engine.begin() as conn:
            return conn.execute(
                text("""
                    SELECT orchestration.append_task_logs(CAST(:a AS uuid), CAST(:t AS uuid), CAST(:seqs AS bigint[]),
                                                          CAST(:ts AS timestamptz[]), CAST(:streams AS text[]),
                                                          CAST(:lines AS text[]))
                """),
                {"a": attempt_id, "t": lease_token, "seqs": [l.seq for l in lines],
                 "ts": [l.logged_at for l in lines], "streams": [l.stream for l in lines],
                 "lines": [l.line for l in lines]},
            ).scalar_one()

    def reap_expired_leases(self, limit=100, worker_lost_after_seconds=120) -> int:
        with self.engine.begin() as conn:
            return conn.execute(
                text("SELECT orchestration.reap_expired_leases(:l, :w)"), {"l": limit, "w": worker_lost_after_seconds}
            ).scalar_one()

    def active_step_versions(self) -> dict[str, tuple[str, str]]:
        """step_key -> (code_version, definition_hash) for every active step in the control DB."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT step_key, code_version, definition_hash FROM registry.step_definition WHERE is_active")
            ).all()
        return {key: (version, digest) for key, version, digest in rows}
