"""Shared SQLite core.dim_* emulation for bootstrap_core_assets tests.

Not a framework -- one small, explicit helper mirroring
sql/migrations/013_create_core_asset_model.sql and
sql/migrations/014_add_asset_type_and_asset_attributes.sql, in the same
spirit as _ops_sqlite_helpers.build_sqlite_ops_engine (see its docstring). SQLite
(StaticPool, one shared in-memory connection) is used instead of mocking SQL
strings so these tests prove real INSERT ... ON CONFLICT ... DO UPDATE
behavior -- including the two partial unique indexes core.dim_model relies
on for its nullable-make_key uniqueness rule (SQLite supports partial
indexes and the matching `ON CONFLICT (...) WHERE ... DO UPDATE` conflict
target the same way PostgreSQL does).

Differences from the real PostgreSQL schema (SQLite has no BIGINT IDENTITY,
TIMESTAMPTZ, or SMALLINT types) are cosmetic only -- every constraint that
scripts/bootstrap_core_assets.py actually relies on (PK, UNIQUE, the two
partial unique indexes, FK columns) is reproduced exactly.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool


def build_sqlite_core_engine() -> Engine:
    """Return a fresh in-memory SQLite engine with a `core` schema attached."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS core"))

        conn.execute(text("""
            CREATE TABLE core.dim_make (
                make_key INTEGER PRIMARY KEY AUTOINCREMENT,
                make_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE core.dim_model (
                model_key INTEGER PRIMARY KEY AUTOINCREMENT,
                make_key INTEGER REFERENCES dim_make (make_key),
                model_name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(
            text("CREATE UNIQUE INDEX core.uq_dim_model_make_key_model_name ON dim_model (make_key, model_name) WHERE make_key IS NOT NULL")
        )
        conn.execute(
            text("CREATE UNIQUE INDEX core.uq_dim_model_model_name_no_make ON dim_model (model_name) WHERE make_key IS NULL")
        )

        conn.execute(text("""
            CREATE TABLE core.dim_owner (
                owner_key INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE core.dim_usage_type (
                usage_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
                usage_type_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE core.dim_asset_category (
                asset_category_key INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_category_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE core.dim_asset_type (
                asset_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_type_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))

        conn.execute(text("""
            CREATE TABLE core.dim_asset (
                asset_key INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_number TEXT NOT NULL UNIQUE,
                partner_code TEXT,
                serial_number TEXT,
                make_key INTEGER REFERENCES dim_make (make_key),
                model_key INTEGER REFERENCES dim_model (model_key),
                owner_key INTEGER REFERENCES dim_owner (owner_key),
                usage_type_key INTEGER REFERENCES dim_usage_type (usage_type_key),
                asset_category_key INTEGER REFERENCES dim_asset_category (asset_category_key),
                asset_type_key INTEGER REFERENCES dim_asset_type (asset_type_key),
                sale_year INTEGER,
                dispersal_date TEXT,
                stage TEXT,
                size REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (sale_year IS NULL OR sale_year BETWEEN 1900 AND 2100)
            )
        """))
    return engine
