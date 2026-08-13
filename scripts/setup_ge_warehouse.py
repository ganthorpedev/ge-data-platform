"""Create (if missing) and migrate the ge_warehouse platform database.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.setup_ge_warehouse --all
    python -m scripts.setup_ge_warehouse --create-db
    python -m scripts.setup_ge_warehouse --migrate
    python -m scripts.setup_ge_warehouse --validate

Never touches telemetry_warehouse. `--create-db` connects to the server's
`postgres` maintenance database only long enough to check whether
ge_warehouse exists and, if not, issue `CREATE DATABASE` (which cannot run
inside a migration transaction, hence the separate maintenance-connection
step). `--migrate` applies every sql/migrations/*.sql file, in order, each
inside its own transaction. `--validate` runs
sql/validation/validate_ge_warehouse_baseline.sql and prints every
PASS/FAIL row.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import psycopg2
import psycopg2.extensions

from ge_data_platform.common.migrations import discover_migrations
from ge_data_platform.config.settings import PlatformSettings, get_platform_settings

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = PROJECT_ROOT / "sql" / "migrations"
VALIDATION_FILE = PROJECT_ROOT / "sql" / "validation" / "validate_ge_warehouse_baseline.sql"


def create_database_if_missing(settings: PlatformSettings) -> bool:
    """Create `settings.ge_warehouse_db` if it does not already exist.

    Connects to the server's `postgres` maintenance database (never to
    ge_warehouse or telemetry_warehouse for this step). Returns True if the
    database was created, False if it already existed.
    """
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname="postgres",
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    try:
        conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction block
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (settings.ge_warehouse_db,))
            exists = cur.fetchone() is not None
            if exists:
                print(f"Database {settings.ge_warehouse_db!r} already exists -- no action taken")
                return False

            # Database names cannot be parameterized; validated defensively
            # here even though this is operator-controlled configuration
            # (GE_WAREHOUSE_DB), not user input.
            if not settings.ge_warehouse_db.replace("_", "").isalnum():
                raise ValueError(
                    f"Refusing to CREATE DATABASE with unexpected name {settings.ge_warehouse_db!r}"
                )
            cur.execute(f'CREATE DATABASE "{settings.ge_warehouse_db}"')  # noqa: S608
            print(f"Created database {settings.ge_warehouse_db!r}")
            return True
    finally:
        conn.close()


def apply_migrations(settings: PlatformSettings) -> list[str]:
    """Apply every discovered migration, in order, against ge_warehouse.

    Each file runs inside its own transaction (autocommit is off; the file's
    own BEGIN/COMMIT governs it). A failure rolls back that file only and
    re-raises -- files already applied and committed are unaffected.
    Returns the list of migration filenames applied (or already present).
    """
    migrations = discover_migrations(MIGRATIONS_DIR)
    if not migrations:
        print(f"No migrations found under {MIGRATIONS_DIR}")
        return []

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.ge_warehouse_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    applied: list[str] = []
    try:
        for migration in migrations:
            sql_text = migration.path.read_text(encoding="utf-8")
            with conn.cursor() as cur:
                try:
                    cur.execute(sql_text)
                    conn.commit()
                    print(f"Applied {migration.name}")
                    applied.append(migration.name)
                except Exception:
                    conn.rollback()
                    logger.exception("Migration %s failed; rolled back", migration.name)
                    raise
    finally:
        conn.close()

    return applied


def run_validation(settings: PlatformSettings) -> list[tuple]:
    """Run the baseline validation SQL and print every result row."""
    sql_text = VALIDATION_FILE.read_text(encoding="utf-8")

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.ge_warehouse_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    rows: list[tuple] = []
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Each top-level statement in the file is its own independent
            # SELECT (see the file's own header comment); execute them one
            # at a time so every one of them prints, even if psycopg2's
            # simple-query protocol would otherwise only surface the last
            # statement's result set.
            for statement in _split_sql_statements(sql_text):
                if not statement.strip():
                    continue
                cur.execute(statement)
                if cur.description is None:
                    continue
                columns = [c.name for c in cur.description]
                for row in cur.fetchall():
                    rows.append(row)
                    print(dict(zip(columns, row)))
    finally:
        conn.close()

    return rows


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split on statement-terminating semicolons, stripping full-line comments.

    Good enough for this repository's validation files (no semicolons ever
    appear inside a string literal or a dollar-quoted block in
    validate_ge_warehouse_baseline.sql) -- not a general-purpose SQL parser.
    """
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    cleaned = "\n".join(lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--create-db", action="store_true", help="Create ge_warehouse if it does not exist")
    parser.add_argument("--migrate", action="store_true", help="Apply sql/migrations/*.sql in order")
    parser.add_argument("--validate", action="store_true", help="Run the baseline validation SQL")
    parser.add_argument("--all", action="store_true", help="Equivalent to --create-db --migrate --validate")
    args = parser.parse_args(argv)

    if not (args.create_db or args.migrate or args.validate or args.all):
        parser.print_help()
        return 1

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    settings = get_platform_settings()
    print(
        f"Target: host={settings.postgres_host} port={settings.postgres_port} "
        f"db={settings.ge_warehouse_db} user={settings.postgres_user} (password hidden)"
    )

    if args.create_db or args.all:
        create_database_if_missing(settings)
    if args.migrate or args.all:
        apply_migrations(settings)
    if args.validate or args.all:
        run_validation(settings)

    return 0


if __name__ == "__main__":
    sys.exit(main())
