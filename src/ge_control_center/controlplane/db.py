"""SQLAlchemy engine for the control-plane database (ge_control)."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def create_control_engine(url: str, *, component: str, pool_size: int = 5) -> Engine:
    """Return an engine with conservative timeouts for control-plane calls.

    Every control-plane operation is a short transaction. ``lock_timeout`` and
    ``statement_timeout`` make a stuck lock surface as an error the caller can
    retry instead of an indefinitely hung worker.
    """
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=pool_size,
        connect_args={
            "connect_timeout": 10,
            "application_name": f"gecc-{component}"[:63],
            "options": "-c lock_timeout=10000 -c statement_timeout=30000 -c timezone=UTC",
        },
    )
