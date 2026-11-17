"""Run operations shared by the API and the CLI.

State changes go through the SQL functions (create_run, request_cancel);
this module only adds audit events and read models.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ge_control_center.controlplane.audit import record_event


@dataclass(frozen=True)
class CreatedRun:
    run_id: str
    created: bool
    state: str


def create_run(
    engine: Engine,
    *,
    pipeline_key: str,
    idempotency_key: str,
    trigger_type: str,
    actor: str,
    actor_type: str,
    params: dict[str, Any] | None = None,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    priority: int = 0,
    request_id: str | None = None,
) -> CreatedRun:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT run_id, created, state FROM orchestration.create_run(
                    :pipeline_key, :trigger_type, :actor, :idempotency_key,
                    CAST(:params AS jsonb), :window_start, :window_end, :priority)
            """),
            {
                "pipeline_key": pipeline_key,
                "trigger_type": trigger_type,
                "actor": actor,
                "idempotency_key": idempotency_key,
                "params": json.dumps(params or {}, sort_keys=True),
                "window_start": window_start,
                "window_end": window_end,
                "priority": priority,
            },
        ).mappings().one()
        result = CreatedRun(str(row["run_id"]), bool(row["created"]), row["state"])
        record_event(
            conn,
            actor_type=actor_type,
            actor_id=actor,
            action="run.created" if result.created else "run.create_deduplicated",
            entity_type="RUN",
            entity_id=result.run_id,
            details={"pipeline_key": pipeline_key, "idempotency_key": idempotency_key, "trigger_type": trigger_type},
            request_id=request_id,
        )
    return result


def request_cancel(
    engine: Engine, run_id: str, *, actor: str, actor_type: str, reason: str | None = None, request_id: str | None = None
) -> str:
    with engine.begin() as conn:
        state = conn.execute(
            text("SELECT orchestration.request_cancel(CAST(:r AS uuid), :a, :reason)"),
            {"r": run_id, "a": actor, "reason": reason},
        ).scalar_one()
        record_event(conn, actor_type=actor_type, actor_id=actor, action="run.cancel_requested",
                     entity_type="RUN", entity_id=run_id, details={"resulting_state": state, "reason": reason},
                     request_id=request_id)
    return state


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif value is not None and not isinstance(value, (str, int, float, bool, dict, list)):
            out[key] = str(value)
        else:
            out[key] = value
    return out


def get_run(engine: Engine, run_id: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        run = conn.execute(
            text("""
                SELECT r.run_id, p.pipeline_key, v.version_no AS pipeline_version, r.state, r.trigger_type,
                       r.triggered_by, r.idempotency_key, r.params, r.window_start, r.window_end, r.priority,
                       r.created_at, r.started_at, r.finished_at, r.cancel_requested_at, r.cancel_requested_by
                FROM orchestration.run r
                JOIN registry.pipeline p ON p.pipeline_id = r.pipeline_id
                JOIN registry.pipeline_version v ON v.pipeline_version_id = r.pipeline_version_id
                WHERE r.run_id = CAST(:r AS uuid)
            """),
            {"r": run_id},
        ).mappings().first()
        if run is None:
            return None
        tasks = conn.execute(
            text("""
                SELECT task_id, node_key, step_key, step_code_version, state, attempt_count, max_attempts,
                       next_eligible_at, started_at, finished_at, concurrency_keys
                FROM orchestration.task WHERE run_id = CAST(:r AS uuid) ORDER BY created_at, node_key
            """),
            {"r": run_id},
        ).mappings().all()
        attempts = conn.execute(
            text("""
                SELECT a.attempt_id, a.task_id, a.attempt_no, a.state, a.worker_id, w.worker_name,
                       a.started_at, a.finished_at, a.lease_expires_at, a.exit_code, a.error_class,
                       a.error_message, a.result
                FROM orchestration.task_attempt a
                JOIN orchestration.worker w ON w.worker_id = a.worker_id
                WHERE a.run_id = CAST(:r AS uuid) ORDER BY a.started_at
            """),
            {"r": run_id},
        ).mappings().all()
        edges = conn.execute(
            text("""
                SELECT e.upstream_node_key AS "from", e.downstream_node_key AS "to"
                FROM orchestration.run r JOIN registry.pipeline_edge e ON e.pipeline_version_id = r.pipeline_version_id
                WHERE r.run_id = CAST(:r AS uuid) ORDER BY 1, 2
            """),
            {"r": run_id},
        ).mappings().all()

    by_task: dict[str, list[dict]] = {}
    for attempt in attempts:
        by_task.setdefault(str(attempt["task_id"]), []).append(_jsonable(dict(attempt)))
    result = _jsonable(dict(run))
    result["tasks"] = []
    for task in tasks:
        item = _jsonable(dict(task))
        item["attempts"] = by_task.get(str(task["task_id"]), [])
        result["tasks"].append(item)
    result["edges"] = [dict(e) for e in edges]
    return result


def list_runs(engine: Engine, *, pipeline_key: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT r.run_id, p.pipeline_key, v.version_no AS pipeline_version, r.state, r.trigger_type,
                       r.triggered_by, r.created_at, r.finished_at
                FROM orchestration.run r
                JOIN registry.pipeline p ON p.pipeline_id = r.pipeline_id
                JOIN registry.pipeline_version v ON v.pipeline_version_id = r.pipeline_version_id
                WHERE (CAST(:k AS text) IS NULL OR p.pipeline_key = :k)
                ORDER BY r.created_at DESC LIMIT :limit
            """),
            {"k": pipeline_key, "limit": max(1, min(limit, 500))},
        ).mappings().all()
    return [_jsonable(dict(row)) for row in rows]


def get_logs(
    engine: Engine, run_id: str, *, node_key: str | None = None, attempt_no: int | None = None,
    after_seq: int = 0, limit: int = 1000,
) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT t.node_key, a.attempt_no, l.seq, l.logged_at, l.stream, l.line
                FROM orchestration.task_log l
                JOIN orchestration.task_attempt a ON a.attempt_id = l.attempt_id
                JOIN orchestration.task t ON t.task_id = a.task_id
                WHERE a.run_id = CAST(:r AS uuid)
                  AND (CAST(:node AS text) IS NULL OR t.node_key = :node)
                  AND (CAST(:attempt AS integer) IS NULL OR a.attempt_no = :attempt)
                  AND l.seq > :after
                ORDER BY a.started_at, l.seq LIMIT :limit
            """),
            {"r": run_id, "node": node_key, "attempt": attempt_no, "after": after_seq,
             "limit": max(1, min(limit, 10000))},
        ).mappings().all()
    return [_jsonable(dict(row)) for row in rows]


def list_steps(engine: Engine, *, include_inactive: bool = False) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT step_key, code_version, display_name, description, param_schema, idempotency_mode,
                       concurrency_keys, default_queue, default_max_attempts, default_timeout_seconds,
                       is_active, registered_at
                FROM registry.step_definition
                WHERE is_active OR :all
                ORDER BY step_key, code_version
            """),
            {"all": include_inactive},
        ).mappings().all()
    return [_jsonable(dict(row)) for row in rows]


def list_pipelines(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT p.pipeline_key, p.name, p.owner, p.max_active_runs, p.is_paused,
                       v.version_no AS latest_version, v.spec_hash, v.published_at, v.published_by
                FROM registry.pipeline p
                LEFT JOIN LATERAL (
                    SELECT version_no, spec_hash, published_at, published_by FROM registry.pipeline_version
                    WHERE pipeline_id = p.pipeline_id AND status = 'PUBLISHED'
                    ORDER BY version_no DESC LIMIT 1
                ) v ON TRUE
                WHERE p.archived_at IS NULL
                ORDER BY p.pipeline_key
            """)
        ).mappings().all()
    return [_jsonable(dict(row)) for row in rows]
