"""Apply sql/control/migrations/*.sql to the control-plane database.

Reuses the existing ``NNN_description.sql`` discovery from
``ge_data_platform.common.migrations`` (same convention as ge_warehouse),
with two additions suited to the control plane:

* each file runs inside a transaction managed here (files contain no
  BEGIN/COMMIT) and is recorded in ``meta.schema_version`` with a SHA-256
  checksum, in the same transaction;
* a changed checksum for an already-applied file is a hard error -- applied
  migrations are never edited, a new file is added instead.

A session advisory lock serialises concurrent ``gecc migrate`` invocations.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ge_data_platform.common.migrations import discover_migrations

from ge_control_center.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

CONTROL_MIGRATIONS_DIR = PROJECT_ROOT / "sql" / "control" / "migrations"
_MIGRATION_LOCK_KEY = 0x6765_6363_6D69_6772 & 0x7FFF_FFFF_FFFF_FFFF  # "geccmigr"


class MigrationDriftError(RuntimeError):
    """An already-applied migration file has changed on disk."""


@dataclass(frozen=True)
class MigrationOutcome:
    name: str
    applied: bool  # False = already applied (checksum verified)


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_migrations(engine: Engine, directory: Path = CONTROL_MIGRATIONS_DIR) -> list[MigrationOutcome]:
    migrations = discover_migrations(directory)
    outcomes: list[MigrationOutcome] = []

    with engine.connect() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _MIGRATION_LOCK_KEY})
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS meta"))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS meta.schema_version (
                        migration_name TEXT PRIMARY KEY,
                        checksum TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        applied_by TEXT NOT NULL DEFAULT current_user
                    )
                """))

            with engine.connect() as conn:
                applied = dict(
                    conn.execute(text("SELECT migration_name, checksum FROM meta.schema_version")).all()
                )

            for migration in migrations:
                checksum = _checksum(migration.path)
                if migration.name in applied:
                    if applied[migration.name] != checksum:
                        raise MigrationDriftError(
                            f"{migration.name} was already applied with checksum {applied[migration.name]} "
                            f"but the file now has {checksum}. Never edit an applied migration; add a new one."
                        )
                    outcomes.append(MigrationOutcome(migration.name, applied=False))
                    continue

                sql_text = migration.path.read_text(encoding="utf-8")
                with engine.begin() as conn:
                    # Raw DBAPI cursor with NO parameters: psycopg2 must not treat the
                    # '%' placeholders in plpgsql RAISE messages as bind markers.
                    with conn.connection.cursor() as cursor:
                        cursor.execute(sql_text)
                    conn.execute(
                        text("INSERT INTO meta.schema_version (migration_name, checksum) VALUES (:n, :c)"),
                        {"n": migration.name, "c": checksum},
                    )
                logger.info("Applied %s", migration.name)
                outcomes.append(MigrationOutcome(migration.name, applied=True))
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _MIGRATION_LOCK_KEY})
            lock_conn.commit()

    return outcomes


def latest_applied_migration(engine: Engine) -> str | None:
    with engine.connect() as conn:
        return conn.execute(text("SELECT max(migration_name) FROM meta.schema_version")).scalar()
