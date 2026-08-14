"""Unit tests for Evolution's ge_warehouse ("platform") target support.

replace_evolution_project_reports_platform() is exercised against an
in-memory SQLite raw_evolution/stg_evolution pair (StaticPool, so every
connection shares the same database), the same technique
tests/evolution/test_postgres_loader_full_replace.py uses for the legacy
target -- mocking SQL execution would only prove we called the mock as
intended, not that the atomic DELETE + INSERT actually behaves correctly.
run()'s target validation is exercised via monkeypatching, mirroring
tests/sendem/test_sendem_platform_target.py and
tests/ezytrack/test_ezytrack_platform_target.py.
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from ge_data_platform.common.database import (
    EVOLUTION_PROJECT_REPORTS_COLUMNS,
    EVOLUTION_RAW_PROJECT_REPORT_COLUMNS,
    PostgresLoader,
)
from ge_data_platform.sources.evolution.project_reports import run as evolution_run


def _make_loader() -> PostgresLoader:
    """A PostgresLoader wired to a fresh in-memory SQLite raw_evolution/stg_evolution pair."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw_evolution"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS stg_evolution"))
        raw_columns_ddl = ",\n".join(f"{c} TEXT" for c in EVOLUTION_RAW_PROJECT_REPORT_COLUMNS)
        conn.execute(
            text(
                "CREATE TABLE raw_evolution.project_report ("
                "project_report_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"{raw_columns_ddl}, loaded_at TEXT)"
            )
        )
        staging_columns_ddl = ",\n".join(f"{c} TEXT" for c in EVOLUTION_PROJECT_REPORTS_COLUMNS)
        conn.execute(
            text(
                "CREATE TABLE stg_evolution.project_report ("
                "project_report_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"{staging_columns_ddl}, loaded_at TEXT)"
            )
        )

    loader = PostgresLoader.__new__(PostgresLoader)
    loader.engine = engine
    loader.tracking_backend = "platform"
    return loader


def _raw_row(company: str, id_: str, **overrides: object) -> dict[str, object]:
    row = {c: None for c in EVOLUTION_RAW_PROJECT_REPORT_COLUMNS}
    row.update({"company": company, "id": id_, "credit": "0", "debit": "0"})
    row.update(overrides)
    return row


def _staging_row(company: str, id_: str, business_unit: str = "Unclassified", **overrides: object) -> dict[str, object]:
    row = _raw_row(company, id_, **overrides)
    row["business_unit"] = business_unit
    return row


def _row_count(loader: PostgresLoader, schema: str, table: str) -> int:
    with loader.engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}")).scalar_one()


def test_replace_evolution_project_reports_platform_loads_both_tables() -> None:
    loader = _make_loader()
    raw_df = pd.DataFrame([_raw_row("GE", "Inv"), _raw_row("TLS", "JL")])
    staging_df = pd.DataFrame([_staging_row("GE", "Inv", "Hire"), _staging_row("TLS", "JL", "Haulage")])

    result = loader.replace_evolution_project_reports_platform(raw_df, staging_df)

    assert result == {"raw_evolution.project_report": 2, "stg_evolution.project_report": 2}
    assert _row_count(loader, "raw_evolution", "project_report") == 2
    assert _row_count(loader, "stg_evolution", "project_report") == 2


def test_replace_evolution_project_reports_platform_preserves_duplicate_company_id_rows() -> None:
    # The key discovery of this migration: dbo.vwProjectsReports has no
    # reliable natural key -- duplicate (company, id) rows are legitimate
    # and must survive the load, not be silently collapsed or refused.
    loader = _make_loader()
    raw_df = pd.DataFrame([_raw_row("GE", "Inv"), _raw_row("GE", "Inv"), _raw_row("GE", "Inv")])
    staging_df = pd.DataFrame([_staging_row("GE", "Inv") for _ in range(3)])

    result = loader.replace_evolution_project_reports_platform(raw_df, staging_df)

    assert result == {"raw_evolution.project_report": 3, "stg_evolution.project_report": 3}
    assert _row_count(loader, "raw_evolution", "project_report") == 3


def test_replace_evolution_project_reports_platform_is_a_true_full_replace() -> None:
    loader = _make_loader()
    first_raw = pd.DataFrame([_raw_row("GE", "Inv"), _raw_row("GE", "JL")])
    first_staging = pd.DataFrame([_staging_row("GE", "Inv"), _staging_row("GE", "JL")])
    loader.replace_evolution_project_reports_platform(first_raw, first_staging)

    # Second, smaller batch: the first batch's rows must not survive.
    second_raw = pd.DataFrame([_raw_row("TLS", "CB")])
    second_staging = pd.DataFrame([_staging_row("TLS", "CB")])
    result = loader.replace_evolution_project_reports_platform(second_raw, second_staging)

    assert result == {"raw_evolution.project_report": 1, "stg_evolution.project_report": 1}
    assert _row_count(loader, "raw_evolution", "project_report") == 1
    with loader.engine.connect() as conn:
        remaining = conn.execute(text("SELECT company, id FROM raw_evolution.project_report")).all()
    assert remaining == [("TLS", "CB")]


def test_replace_evolution_project_reports_platform_rerun_with_identical_batch_is_stable() -> None:
    # Idempotency for a full-replace/snapshot load: rerunning with an
    # unchanged batch must reproduce the same row count, not accumulate.
    loader = _make_loader()
    raw_df = pd.DataFrame([_raw_row("GE", "Inv"), _raw_row("GE", "Inv"), _raw_row("TLS", "JL")])
    staging_df = pd.DataFrame(
        [_staging_row("GE", "Inv"), _staging_row("GE", "Inv"), _staging_row("TLS", "JL")]
    )

    loader.replace_evolution_project_reports_platform(raw_df, staging_df)
    result = loader.replace_evolution_project_reports_platform(raw_df, staging_df)

    assert result == {"raw_evolution.project_report": 3, "stg_evolution.project_report": 3}
    assert _row_count(loader, "raw_evolution", "project_report") == 3
    assert _row_count(loader, "stg_evolution", "project_report") == 3


def test_replace_evolution_project_reports_platform_assigns_surrogate_keys() -> None:
    loader = _make_loader()
    raw_df = pd.DataFrame([_raw_row("GE", "Inv"), _raw_row("GE", "Inv")])
    staging_df = pd.DataFrame([_staging_row("GE", "Inv"), _staging_row("GE", "Inv")])

    loader.replace_evolution_project_reports_platform(raw_df, staging_df)

    with loader.engine.connect() as conn:
        ids = [row[0] for row in conn.execute(text("SELECT project_report_id FROM raw_evolution.project_report")).all()]
    assert len(ids) == 2
    assert len(set(ids)) == 2  # unique, even though (company, id) is identical for both rows
    assert all(i is not None for i in ids)


def test_replace_evolution_project_reports_platform_refused_before_touching_destination_when_empty() -> None:
    loader = _make_loader()
    seed_raw = pd.DataFrame([_raw_row("GE", "Inv")])
    seed_staging = pd.DataFrame([_staging_row("GE", "Inv")])
    loader.replace_evolution_project_reports_platform(seed_raw, seed_staging)

    with pytest.raises(ValueError, match="unexpectedly empty"):
        loader.replace_evolution_project_reports_platform(
            pd.DataFrame(columns=EVOLUTION_RAW_PROJECT_REPORT_COLUMNS), seed_staging
        )

    assert _row_count(loader, "raw_evolution", "project_report") == 1  # previous load untouched


def test_evolution_run_rejects_unknown_target() -> None:
    with pytest.raises(ValueError, match="target must be 'legacy' or 'platform'"):
        evolution_run(target="bogus")


def test_evolution_run_rejects_unknown_target_before_touching_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # target validation must happen before get_evolution_settings()/
    # get_settings() are called, so a bad --target fails fast without
    # requiring a fully configured .env or a real Evolution connection.
    def _fail_if_called() -> None:
        raise AssertionError("settings must not be loaded for an invalid target")

    monkeypatch.setattr("ge_data_platform.sources.evolution.project_reports.get_evolution_settings", _fail_if_called)
    monkeypatch.setattr("ge_data_platform.sources.evolution.project_reports.get_settings", _fail_if_called)
    monkeypatch.setattr("ge_data_platform.sources.evolution.project_reports.get_platform_settings", _fail_if_called)

    with pytest.raises(ValueError, match="target must be"):
        evolution_run(target="staging")
