"""Sync the in-code step registry to registry.step_definition (``gecc registry sync``).

Rules:
* a new (key, version) is inserted and becomes the active version of its key;
* an existing (key, version) must have an identical ``definition_hash`` --
  changing a step's contract without bumping its version is an error;
* display name / description are cosmetic and may be updated in place;
* versions of a key other than the one in code, and keys no longer in code,
  are deactivated (kept for history; pipelines pinned to them must be
  republished before they can run);
* every declared concurrency key gets a default limit of 1 if none exists.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ge_control_center.controlplane.audit import record_event
from ge_control_center.registry.errors import RegistryError
from ge_control_center.registry.step import StepRegistry


@dataclass
class SyncReport:
    inserted: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    updated_cosmetic: list[str] = field(default_factory=list)
    deactivated: list[str] = field(default_factory=list)


def sync_registry(engine: Engine, registry: StepRegistry, *, actor: str, actor_type: str = "CLI") -> SyncReport:
    report = SyncReport()
    with engine.begin() as conn:
        conn.execute(text("LOCK TABLE registry.step_definition IN SHARE ROW EXCLUSIVE MODE"))
        rows = conn.execute(text("""
            SELECT step_key, code_version, definition_hash, display_name, description, is_active
            FROM registry.step_definition
        """)).mappings().all()
        existing = {(r["step_key"], r["code_version"]): r for r in rows}

        for definition in registry:
            ident = f"{definition.key}@{definition.version}"
            current = existing.get((definition.key, definition.version))
            if current is not None and current["definition_hash"] != definition.definition_hash():
                raise RegistryError(
                    f"step {ident} is already registered with a different contract "
                    f"(parameters, idempotency, concurrency, queue or retry/timeout defaults changed). "
                    f"Bump the step's version instead of changing version {definition.version!r} in place."
                )

            # Deactivate other versions of this key first (one active version per key).
            deactivated = conn.execute(
                text("""
                    UPDATE registry.step_definition
                    SET is_active = FALSE, deactivated_at = now()
                    WHERE step_key = :k AND code_version <> :v AND is_active
                    RETURNING step_key || '@' || code_version
                """),
                {"k": definition.key, "v": definition.version},
            ).scalars().all()
            report.deactivated.extend(deactivated)

            if current is None:
                conn.execute(
                    text("""
                        INSERT INTO registry.step_definition (
                            step_key, code_version, display_name, description, param_schema, definition_hash,
                            idempotency_mode, concurrency_keys, default_queue, default_max_attempts,
                            default_retry_backoff_seconds, default_retry_backoff_max_seconds,
                            default_timeout_seconds, is_active
                        ) VALUES (
                            :key, :version, :display_name, :description, CAST(:schema AS jsonb), :hash,
                            :idempotency, :concurrency_keys, :queue, :max_attempts,
                            :backoff, :backoff_max, :timeout, TRUE
                        )
                    """),
                    {
                        "key": definition.key,
                        "version": definition.version,
                        "display_name": definition.display_name,
                        "description": definition.description,
                        "schema": json.dumps(definition.param_schema(), sort_keys=True),
                        "hash": definition.definition_hash(),
                        "idempotency": definition.idempotency,
                        "concurrency_keys": list(definition.concurrency_keys),
                        "queue": definition.queue,
                        "max_attempts": definition.max_attempts,
                        "backoff": definition.retry_backoff_seconds,
                        "backoff_max": definition.retry_backoff_max_seconds,
                        "timeout": definition.timeout_seconds,
                    },
                )
                report.inserted.append(ident)
            else:
                cosmetic_changed = (
                    current["display_name"] != definition.display_name
                    or current["description"] != definition.description
                )
                if cosmetic_changed or not current["is_active"]:
                    conn.execute(
                        text("""
                            UPDATE registry.step_definition
                            SET display_name = :d, description = :desc, is_active = TRUE, deactivated_at = NULL
                            WHERE step_key = :k AND code_version = :v
                        """),
                        {"d": definition.display_name, "desc": definition.description,
                         "k": definition.key, "v": definition.version},
                    )
                    report.updated_cosmetic.append(ident)
                else:
                    report.unchanged.append(ident)

            for concurrency_key in definition.concurrency_keys:
                conn.execute(
                    text("""
                        INSERT INTO registry.concurrency_limit (concurrency_key, max_slots, updated_by)
                        VALUES (:k, 1, :actor) ON CONFLICT DO NOTHING
                    """),
                    {"k": concurrency_key, "actor": actor},
                )

        in_code = [d.key for d in registry]
        removed = conn.execute(
            text("""
                UPDATE registry.step_definition
                SET is_active = FALSE, deactivated_at = now()
                WHERE is_active AND NOT (step_key = ANY(:keys))
                RETURNING step_key || '@' || code_version
            """),
            {"keys": in_code},
        ).scalars().all()
        report.deactivated.extend(removed)

        if report.inserted or report.updated_cosmetic or report.deactivated:
            record_event(
                conn,
                actor_type=actor_type,
                actor_id=actor,
                action="registry.synced",
                entity_type="STEP_REGISTRY",
                entity_id=registry.fingerprint(),
                details={
                    "inserted": report.inserted,
                    "updated_cosmetic": report.updated_cosmetic,
                    "deactivated": report.deactivated,
                },
            )
    return report
