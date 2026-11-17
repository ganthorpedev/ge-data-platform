"""Supervisor <-> step-runner child protocol (ADR-015).

Parent -> child: exactly one JSON line on stdin (the invocation). The parent
then keeps stdin OPEN for the child's lifetime; the child treats EOF on stdin
as "my supervisor died" and exits immediately. This works the same on POSIX
and Windows and prevents orphaned steps from running after a ``kill -9`` of
the worker.

Child -> parent: ordinary stdout/stderr lines are logs. Protocol messages are
stdout lines starting with ``MARKER`` followed by one JSON object:

    {"type": "result", "result": {...}}
    {"type": "error", "error_class": "...", "message": "...", "retryable": true}
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

PROTOCOL_VERSION = 1
MARKER = "@@GECC@@ "

EXIT_OK = 0
EXIT_STEP_FAILED = 1
EXIT_INVOCATION_ERROR = 2
EXIT_PARENT_GONE = 70
EXIT_TERMINATED = 143


def encode_message(message: dict[str, Any]) -> str:
    return MARKER + json.dumps(message, default=str, separators=(",", ":"))


def parse_message(line: str) -> dict[str, Any] | None:
    """Return the protocol message on this line, or None for an ordinary log line."""
    if not line.startswith(MARKER):
        return None
    try:
        message = json.loads(line[len(MARKER):])
    except json.JSONDecodeError:
        return None
    if not isinstance(message, dict) or message.get("type") not in {"result", "error"}:
        return None
    return message


def encode_invocation(**fields: Any) -> str:
    payload = {"protocol": PROTOCOL_VERSION}
    for key, value in fields.items():
        payload[key] = value.isoformat() if isinstance(value, datetime) else value
    return json.dumps(payload, separators=(",", ":")) + "\n"


def decode_invocation(line: str) -> dict[str, Any]:
    payload = json.loads(line)
    if not isinstance(payload, dict) or payload.get("protocol") != PROTOCOL_VERSION:
        raise ValueError(f"unsupported invocation protocol: {payload.get('protocol') if isinstance(payload, dict) else payload!r}")
    for key in ("window_start", "window_end"):
        if payload.get(key):
            payload[key] = datetime.fromisoformat(payload[key])
    return payload
