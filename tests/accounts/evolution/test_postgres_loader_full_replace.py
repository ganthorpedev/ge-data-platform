"""Proves replace_accounts_evolution_project_reports() is a true full
replace: a row absent from a later extract does not remain in the
destination, and any failure before the swap commits leaves the previous
successful load untouched.

Runs against an in-memory SQLite database (via SQLAlchemy's StaticPool, so
every connection shares the same database) rather than mocking SQL
execution -- mocking would only prove we called the mock as intended, not
that TRUNCATE/DELETE + INSERT actually removes stale rows. PostgresLoader is
constructed by bypassing __init__ (which builds a psycopg2 connection
string) and assigning a SQLite engine directly; the method under test never
calls get_table_columns() or anything else Postgres-specific.
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from loaders.postgres_loader import EVOLUTION_PROJECT_REPORTS_COLUMNS, PostgresLoader


def _make_loader() -> PostgresLoader:
    """A PostgresLoader wired to a fresh in-memory SQLite raw/staging pair."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS staging"))
        columns_ddl = ",\n".join(
            f"{column} INTEGER" if column == "id" else f"{column} TEXT" for column in EVOLUTION_PROJECT_REPORTS_COLUMNS
        )
        conn.execute(
            text(
                f"CREATE TABLE raw.evolution_project_reports ({columns_ddl}, loaded_at TEXT, "
                "PRIMARY KEY (company, id))"
            )
        )

    loader = PostgresLoader.__new__(PostgresLoader)
    loader.engine = engine
    return loader


def _combined_row(company: str, row_id: int) -> dict[str, object]:
    row = {column: None for column in EVOLUTION_PROJECT_REPORTS_COLUMNS}
    row.update({"company": company, "id": row_id, "credit": "0", "debit": "0"})
    return row


def _seed_raw(loader: PostgresLoader, rows: list[dict[str, object]]) -> None:
    """Insert rows straight into raw, simulating a previous successful load."""
    df = pd.DataFrame(rows)[EVOLUTION_PROJECT_REPORTS_COLUMNS]
    df["loaded_at"] = "2026-01-01T00:00:00+00:00"
    with loader.engine.begin() as conn:
        df.to_sql("evolution_project_reports", con=conn, schema="raw", if_exists="append", index=False)


def _current_raw_keys(loader: PostgresLoader) -> set[tuple[str, int]]:
    with loader.engine.connect() as conn:
        rows = conn.execute(text("SELECT company, id FROM raw.evolution_project_reports")).all()
    return {(row[0], row[1]) for row in rows}


def _staging_table_names(loader: PostgresLoader) -> list[str]:
    with loader.engine.connect() as conn:
        rows = conn.execute(text("SELECT name FROM staging.sqlite_master WHERE type = 'table'")).all()
    return [row[0] for row in rows]


def test_replace_removes_rows_absent_from_the_new_extract_and_adds_new_ones() -> None:
    loader = _make_loader()
    _seed_raw(
        loader,
        [
            _combined_row("GE", 1),
            _combined_row("GE", 2),  # will be absent from the new extract
            _combined_row("TLS", 1),
        ],
    )

    new_extract = pd.DataFrame(
        [
            _combined_row("GE", 1),
            _combined_row("TLS", 1),
            _combined_row("TLS", 2),  # newly appeared in this run
        ]
    )

    result = loader.replace_accounts_evolution_project_reports(new_extract, sync_run_id=None)

    assert result == {"raw.evolution_project_reports": 3}
    remaining = _current_raw_keys(loader)
    assert remaining == {("GE", 1), ("TLS", 1), ("TLS", 2)}
    assert ("GE", 2) not in remaining  # the whole point: removed rows don't survive
    assert _staging_table_names(loader) == []  # cleaned up after success


def test_replace_is_refused_and_leaves_the_destination_untouched_on_staged_count_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_loader()
    _seed_raw(loader, [_combined_row("GE", 1), _combined_row("TLS", 1)])
    previous_keys = _current_raw_keys(loader)

    new_extract = pd.DataFrame([_combined_row("GE", 1), _combined_row("GE", 2)])

    # Simulate a partial/interrupted staging write: only the first row makes
    # it into the staging table, so the post-stage count check must catch
    # the mismatch before the destination is ever touched.
    original_to_sql = pd.DataFrame.to_sql

    def _drop_last_row_to_sql(self: pd.DataFrame, *args: object, **kwargs: object) -> object:
        return original_to_sql(self.iloc[:-1], *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_sql", _drop_last_row_to_sql)

    with pytest.raises(RuntimeError, match="Staged row count"):
        loader.replace_accounts_evolution_project_reports(new_extract, sync_run_id=None)

    assert _current_raw_keys(loader) == previous_keys  # previous successful load survives
    assert _staging_table_names(loader) == []  # staging table still cleaned up on failure


def test_replace_is_refused_before_touching_the_destination_when_validation_fails() -> None:
    loader = _make_loader()
    _seed_raw(loader, [_combined_row("GE", 1)])
    previous_keys = _current_raw_keys(loader)

    duplicate_key_extract = pd.DataFrame([_combined_row("GE", 2), _combined_row("GE", 2)])

    with pytest.raises(ValueError, match="duplicate"):
        loader.replace_accounts_evolution_project_reports(duplicate_key_extract, sync_run_id=None)

    assert _current_raw_keys(loader) == previous_keys
    assert _staging_table_names(loader) == []  # nothing was even staged
