from __future__ import annotations

from pathlib import Path

import pytest

from ge_data_platform.common.migrations import discover_migrations

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"


def test_discover_migrations_finds_real_baseline_files_in_order() -> None:
    # Real local catalog validation, not a mock: exercises the actual
    # sql/migrations/ directory shipped in this repository.
    migrations = discover_migrations(REAL_MIGRATIONS_DIR)

    names = [m.name for m in migrations]
    assert names == [
        "001_create_platform_schemas.sql",
        "002_create_ops_metadata.sql",
        "003_create_platform_roles.sql",
        "004_create_core_dim_date.sql",
        "005_create_raw_trackunit.sql",
        "006_create_stg_trackunit.sql",
        "007_create_raw_sendem.sql",
        "008_create_stg_sendem.sql",
    ]


def test_discover_migrations_orders_numerically_not_lexically(tmp_path: Path) -> None:
    (tmp_path / "010_tenth.sql").write_text("-- ten", encoding="utf-8")
    (tmp_path / "002_second.sql").write_text("-- two", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("-- one", encoding="utf-8")

    migrations = discover_migrations(tmp_path)

    assert [m.name for m in migrations] == ["001_first.sql", "002_second.sql", "010_tenth.sql"]


def test_discover_migrations_skips_non_matching_files(tmp_path: Path) -> None:
    (tmp_path / "001_real_migration.sql").write_text("-- real", encoding="utf-8")
    (tmp_path / "README.md").write_text("not a migration", encoding="utf-8")
    (tmp_path / "notes.sql").write_text("no numeric prefix", encoding="utf-8")

    migrations = discover_migrations(tmp_path)

    assert [m.name for m in migrations] == ["001_real_migration.sql"]


def test_discover_migrations_rejects_duplicate_prefix(tmp_path: Path) -> None:
    # The exact collision that motivated renaming (not renumbering) the old
    # 003_validate_sendem_idempotency.sql / 003_validate_sendem_pipeline.sql
    # pair -- this is now caught mechanically instead of by hand.
    (tmp_path / "003_validate_a.sql").write_text("-- a", encoding="utf-8")
    (tmp_path / "003_validate_b.sql").write_text("-- b", encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate migration prefix"):
        discover_migrations(tmp_path)


def test_discover_migrations_rejects_non_three_digit_prefix(tmp_path: Path) -> None:
    (tmp_path / "1_create_x.sql").write_text("-- x", encoding="utf-8")

    with pytest.raises(ValueError, match="3 digits"):
        discover_migrations(tmp_path)


def test_discover_migrations_raises_on_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        discover_migrations(tmp_path / "does_not_exist")


def test_discover_migrations_empty_directory_returns_empty_list(tmp_path: Path) -> None:
    assert discover_migrations(tmp_path) == []
