"""Append-only audit events (audit.event). State transitions are recorded by database triggers."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

ACTOR_TYPES = frozenset({"USER", "CLI", "API", "WORKER", "SYSTEM"})


def record_event(
    conn: Connection,
    *,
    actor_type: str,
    actor_id: str,
    action: str,
    entity_type: str,
    entity_id: str | None,
    before: Any = None,
    after: Any = None,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> None:
    """Insert one audit event inside the caller's transaction."""
    if actor_type not in ACTOR_TYPES:
        raise ValueError(f"actor_type must be one of {sorted(ACTOR_TYPES)}")
    conn.execute(
        text("""
            INSERT INTO audit.event
                (actor_type, actor_id, action, entity_type, entity_id, before, after, details, request_id)
            VALUES
                (:actor_type, :actor_id, :action, :entity_type, :entity_id,
                 CAST(:before AS jsonb), CAST(:after AS jsonb), CAST(:details AS jsonb), :request_id)
        """),
        {
            "actor_type": actor_type,
            "actor_id": actor_id,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": None if before is None else json.dumps(before, default=str, sort_keys=True),
            "after": None if after is None else json.dumps(after, default=str, sort_keys=True),
            "details": json.dumps(details or {}, default=str, sort_keys=True),
            "request_id": request_id,
        },
    )


def set_actor(conn: Connection, actor: str, reason: str | None = None) -> None:
    """Set transaction-local actor/reason picked up by the state-transition triggers."""
    conn.execute(
        text("SELECT set_config('gecc.actor', :a, true), set_config('gecc.reason', :r, true)"),
        {"a": actor, "r": reason or ""},
    )
