"""Static checks on the ge_warehouse baseline migration text.

No database connection: these parse sql/migrations/001_create_platform_schemas.sql
directly, so they run anywhere (including CI without Postgres) while still
catching a real regression -- e.g. someone adding a forbidden generic schema
name, or dropping one of the required source/mart schemas.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"
SCHEMAS_FILE = MIGRATIONS_DIR / "001_create_platform_schemas.sql"

EXPECTED_SCHEMAS = {
    "raw_trackunit", "raw_sendem", "raw_ezytrack", "raw_evolution", "raw_fieldops",
    "stg_trackunit", "stg_sendem", "stg_ezytrack", "stg_evolution", "stg_fieldops",
    "core",
    "mart_fleet", "mart_finance", "mart_operations", "mart_maintenance", "mart_procurement", "mart_commercial",
    "ops",
}

# Reserved for telemetry_warehouse; must never appear as a CREATE SCHEMA
# target in the ge_warehouse baseline.
FORBIDDEN_GENERIC_SCHEMAS = {"raw", "staging", "warehouse", "reporting", "etl", "clean"}

_CREATE_SCHEMA_PATTERN = re.compile(r"CREATE SCHEMA IF NOT EXISTS (\w+);")


def _created_schemas() -> set[str]:
    text = SCHEMAS_FILE.read_text(encoding="utf-8")
    return set(_CREATE_SCHEMA_PATTERN.findall(text))


def test_baseline_creates_every_expected_schema() -> None:
    assert EXPECTED_SCHEMAS <= _created_schemas()


def test_baseline_creates_no_unexpected_schema() -> None:
    assert _created_schemas() == EXPECTED_SCHEMAS


def test_baseline_never_creates_a_forbidden_generic_schema_name() -> None:
    assert _created_schemas().isdisjoint(FORBIDDEN_GENERIC_SCHEMAS)


def test_all_schema_names_are_lowercase_snake_case() -> None:
    # Schema-name casing rule only -- singular-vs-plural is a table-naming
    # rule (raw_trackunit.asset, not .assets), not a schema-naming one:
    # mart_operations is intentionally plural, it names a business domain.
    name_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
    for schema in EXPECTED_SCHEMAS:
        assert name_pattern.match(schema), f"{schema} is not lowercase snake_case"
