"""Publish a validated pipeline as a new immutable version (or no-op if unchanged)."""

from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ge_control_center.controlplane.audit import record_event
from ge_control_center.dag.spec import ResolvedPipeline


class PublishError(RuntimeError):
    pass


@dataclass(frozen=True)
class PublishResult:
    pipeline_id: str
    pipeline_version_id: str
    version_no: int
    spec_hash: str
    created: bool


def publish_pipeline(
    engine: Engine,
    pipeline: ResolvedPipeline,
    *,
    actor: str,
    actor_type: str = "CLI",
    request_id: str | None = None,
) -> PublishResult:
    with engine.begin() as conn:
        header = {
            "name": pipeline.name,
            "description": pipeline.description,
            "owner": pipeline.owner,
            "max_active_runs": pipeline.max_active_runs,
        }

        row = conn.execute(
            text("""
                SELECT pipeline_id, name, description, owner, max_active_runs, archived_at
                FROM registry.pipeline WHERE pipeline_key = :k FOR NO KEY UPDATE
            """),
            {"k": pipeline.pipeline_key},
        ).mappings().first()

        if row is None:
            pipeline_id = conn.execute(
                text("""
                    INSERT INTO registry.pipeline (pipeline_key, name, description, owner, max_active_runs, created_by)
                    VALUES (:k, :name, :description, :owner, :max_active_runs, :actor)
                    RETURNING pipeline_id
                """),
                {"k": pipeline.pipeline_key, **header, "actor": actor},
            ).scalar_one()
            record_event(conn, actor_type=actor_type, actor_id=actor, action="pipeline.created",
                         entity_type="PIPELINE", entity_id=str(pipeline_id), after={"pipeline_key": pipeline.pipeline_key, **header},
                         request_id=request_id)
        else:
            if row["archived_at"] is not None:
                raise PublishError(f"pipeline {pipeline.pipeline_key!r} is archived")
            pipeline_id = row["pipeline_id"]
            before = {key: row[key] for key in header}
            if before != header:
                conn.execute(
                    text("""
                        UPDATE registry.pipeline
                        SET name = :name, description = :description, owner = :owner,
                            max_active_runs = :max_active_runs, updated_at = now()
                        WHERE pipeline_id = :id
                    """),
                    {**header, "id": pipeline_id},
                )
                record_event(conn, actor_type=actor_type, actor_id=actor, action="pipeline.header_updated",
                             entity_type="PIPELINE", entity_id=str(pipeline_id), before=before, after=header,
                             request_id=request_id)

        # Every referenced step version must be registered AND active in the control DB.
        wanted = {(node.step_key, node.step_version) for node in pipeline.nodes}
        rows = conn.execute(
            text("SELECT step_key, code_version, is_active FROM registry.step_definition WHERE step_key = ANY(:keys)"),
            {"keys": sorted({key for key, _ in wanted})},
        ).mappings().all()
        active = {(r["step_key"], r["code_version"]) for r in rows if r["is_active"]}
        missing = sorted(f"{key}@{version}" for key, version in wanted - active)
        if missing:
            raise PublishError(
                f"step versions not registered/active in the control database: {', '.join(missing)}. "
                "Run `gecc registry sync` with this code first."
            )

        latest = conn.execute(
            text("""
                SELECT pipeline_version_id, version_no, spec_hash FROM registry.pipeline_version
                WHERE pipeline_id = :id ORDER BY version_no DESC LIMIT 1
            """),
            {"id": pipeline_id},
        ).mappings().first()

        layout_json = json.dumps(pipeline.ui_layout, sort_keys=True)
        if latest is not None and latest["spec_hash"] == pipeline.spec_hash:
            conn.execute(
                text("UPDATE registry.pipeline_version SET ui_layout = CAST(:layout AS jsonb) WHERE pipeline_version_id = :v"),
                {"layout": layout_json, "v": latest["pipeline_version_id"]},
            )
            return PublishResult(str(pipeline_id), str(latest["pipeline_version_id"]), latest["version_no"],
                                 pipeline.spec_hash, created=False)

        version_no = 1 if latest is None else latest["version_no"] + 1
        version_id = conn.execute(
            text("""
                INSERT INTO registry.pipeline_version (pipeline_id, version_no, spec, spec_hash, ui_layout, published_by)
                VALUES (:pid, :no, CAST(:spec AS jsonb), :hash, CAST(:layout AS jsonb), :actor)
                RETURNING pipeline_version_id
            """),
            {"pid": pipeline_id, "no": version_no, "spec": json.dumps(pipeline.canonical_spec(), sort_keys=True),
             "hash": pipeline.spec_hash, "layout": layout_json, "actor": actor},
        ).scalar_one()

        for node in pipeline.nodes:
            conn.execute(
                text("""
                    INSERT INTO registry.pipeline_node (
                        pipeline_version_id, node_key, step_key, step_code_version, params, max_attempts,
                        retry_backoff_seconds, retry_backoff_max_seconds, timeout_seconds, queue, concurrency_keys
                    ) VALUES (:v, :key, :step, :ver, CAST(:params AS jsonb), :attempts, :backoff, :backoff_max,
                              :timeout, :queue, :keys)
                """),
                {"v": version_id, "key": node.key, "step": node.step_key, "ver": node.step_version,
                 "params": json.dumps(node.params, sort_keys=True), "attempts": node.max_attempts,
                 "backoff": node.retry_backoff_seconds, "backoff_max": node.retry_backoff_max_seconds,
                 "timeout": node.timeout_seconds, "queue": node.queue, "keys": list(node.concurrency_keys)},
            )
        for upstream, downstream in pipeline.edges:
            conn.execute(
                text("INSERT INTO registry.pipeline_edge VALUES (:v, :up, :down)"),
                {"v": version_id, "up": upstream, "down": downstream},
            )

        record_event(conn, actor_type=actor_type, actor_id=actor, action="pipeline.version_published",
                     entity_type="PIPELINE_VERSION", entity_id=str(version_id),
                     after={"pipeline_key": pipeline.pipeline_key, "version_no": version_no, "spec_hash": pipeline.spec_hash},
                     request_id=request_id)

        return PublishResult(str(pipeline_id), str(version_id), version_no, pipeline.spec_hash, created=True)
