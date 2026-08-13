"""Unit tests for Sendem's ge_warehouse ("platform") target support.

None of these tests touch a real database: PostgresLoader's constructor
only builds a (lazy) SQLAlchemy engine, and load_sendem_tables is exercised
by monkeypatching _run_load_plan to capture the load plan it was given,
never by actually running it. Mirrors
tests/trackunit/test_trackunit_platform_target.py.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ge_data_platform.common.database import PostgresLoader
from ge_data_platform.config.settings import PlatformSettings, Settings
from ge_data_platform.sources.sendem.sync import run as sendem_run
from ge_data_platform.sources.sendem.transform import build_all


def _legacy_settings() -> Settings:
    return Settings(
        insights_api_url="https://example.invalid",
        sendem_api_key="key",
        sendem_group_id=1,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="telemetry_warehouse",
        postgres_user="test_user",
        postgres_password="test_password",
        sync_lookback_days=7,
    )


def _platform_settings() -> PlatformSettings:
    return PlatformSettings(
        postgres_host="localhost",
        postgres_port=5432,
        ge_warehouse_db="ge_warehouse",
        postgres_user="test_user",
        postgres_password="test_password",
    )


def _empty_sendem_dataframes() -> dict[str, pd.DataFrame]:
    # Exactly what ge_data_platform.sources.sendem.transform.build_all()
    # returns for a fully empty API payload -- see test_sendem_reliability.py
    # test_empty_sendem_facts_retain_loader_schemas for the equivalent
    # transform-level assertion.
    return build_all(
        {"assets": [], "sites": [], "event_descriptions": [], "trips": [], "events": []}
    )


def test_load_sendem_tables_legacy_target_uses_raw_and_staging_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = PostgresLoader(_legacy_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_sendem_tables(_empty_sendem_dataframes(), target="legacy")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw", "sendem_assets"),
        ("raw", "sendem_sites"),
        ("raw", "sendem_event_descriptions"),
        ("raw", "sendem_trips_assets_daily"),
        ("raw", "sendem_events_assets_daily"),
        ("staging", "sendem_dim_assets"),
        ("staging", "sendem_dim_sites"),
        ("staging", "sendem_dim_event_types"),
        ("staging", "sendem_fact_trips_daily"),
        ("staging", "sendem_fact_events_daily"),
    ]


def test_load_sendem_tables_platform_target_uses_raw_sendem_and_stg_sendem_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_sendem_tables(_empty_sendem_dataframes(), target="platform")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw_sendem", "asset"),
        ("raw_sendem", "site"),
        ("raw_sendem", "event_description"),
        ("raw_sendem", "trip_daily"),
        ("raw_sendem", "event_daily"),
        ("stg_sendem", "asset"),
        ("stg_sendem", "site"),
        ("stg_sendem", "event_type"),
        ("stg_sendem", "trip_daily"),
        ("stg_sendem", "event_daily"),
    ]


def test_load_sendem_tables_rejects_unknown_target() -> None:
    loader = PostgresLoader(_legacy_settings())

    with pytest.raises(ValueError, match="target must be"):
        loader.load_sendem_tables(_empty_sendem_dataframes(), target="staging")


def test_load_sendem_tables_platform_target_survives_empty_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    # Empty-payload safety must hold under the platform target exactly as it
    # does under legacy: build_all() retains loader-expected columns even
    # when every input list is empty, and load_sendem_tables must not raise
    # or drop a table from the plan just because its dataframe is empty.
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_sendem_tables(_empty_sendem_dataframes(), target="platform")

    assert len(captured["plan"]) == 10
    assert all(df.empty for _schema, _table, df, _cc in captured["plan"])


def test_sync_run_rejects_unknown_target_before_touching_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # target validation must happen before get_settings() is called, so a
    # bad --target fails fast without requiring a fully configured .env.
    def _fail_if_called() -> None:
        raise AssertionError("get_settings() must not be called for an invalid target")

    monkeypatch.setattr("ge_data_platform.sources.sendem.sync.get_settings", _fail_if_called)

    with pytest.raises(ValueError, match="target must be"):
        sendem_run(target="bogus")
