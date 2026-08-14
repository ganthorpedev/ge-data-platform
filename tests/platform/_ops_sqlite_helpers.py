"""Shared SQLite ops.pipeline_run/ops.table_load emulation for platform-target tests.

Not a framework -- one small, explicit helper reused by the platform-target
test files for every migrated source (trackunit, sendem, ezytrack,
evolution) plus tests/platform/test_ops_audit.py, so each doesn't
re-implement the same ATTACH DATABASE boilerplate. SQLite (StaticPool, one
shared in-memory connection) is used instead of mocking SQL strings so these
tests prove real INSERT/UPDATE/RETURNING behavior, the same technique
tests/evolution/test_evolution_platform_target.py already uses for
raw_evolution/stg_evolution. SQLite 3.35+'s RETURNING support (available
here -- see the sqlite3 module bundled with the project's Python) makes
ops.table_load's `RETURNING table_load_id` work identically to PostgreSQL.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool


def build_sqlite_ops_engine() -> Engine:
    """Return a fresh in-memory SQLite engine with an `ops` schema attached.

    Column shapes mirror sql/migrations/002_create_ops_metadata.sql's
    ops.pipeline_run/ops.table_load (SQLite is dynamically typed, so exact
    PostgreSQL types like UUID/TIMESTAMPTZ are approximated with TEXT).
    """
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS ops"))
        conn.execute(
            text("""
                CREATE TABLE ops.pipeline_run (
                    pipeline_run_id TEXT PRIMARY KEY,
                    source_system TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    start_date INTEGER,
                    end_date INTEGER,
                    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    rows_fetched INTEGER DEFAULT 0,
                    rows_loaded INTEGER DEFAULT 0,
                    error_message TEXT
                )
            """)
        )
        conn.execute(
            text("""
                CREATE TABLE ops.table_load (
                    table_load_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_run_id TEXT REFERENCES pipeline_run (pipeline_run_id),
                    source_system TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    rows_input INTEGER,
                    rows_loaded INTEGER,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
        )
    return engine
