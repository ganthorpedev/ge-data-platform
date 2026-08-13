"""Unit tests for Trackunit's ge_warehouse ("platform") target support.

None of these tests touch a real database: PostgresLoader's constructor
only builds a (lazy) SQLAlchemy engine, and load_trackunit_tables/
load_trackunit_location_enrichment are exercised by monkeypatching
_run_load_plan to capture the load plan it was given, never by actually
running it.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ge_data_platform.common.database import PostgresLoader
from ge_data_platform.config.settings import PlatformSettings, Settings


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


def test_from_platform_settings_targets_ge_warehouse_and_disables_sync_tracking() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())

    assert loader.engine.url.database == "ge_warehouse"
    assert loader.engine.url.host == "localhost"
    assert loader.enable_sync_tracking is False


def test_direct_construction_still_targets_settings_postgres_db() -> None:
    # Every existing call site (PostgresLoader(settings)) must be unaffected.
    loader = PostgresLoader(_legacy_settings())

    assert loader.engine.url.database == "telemetry_warehouse"
    assert loader.enable_sync_tracking is True


def test_database_override_takes_precedence_over_settings_postgres_db() -> None:
    loader = PostgresLoader(_legacy_settings(), database="some_other_db")

    assert loader.engine.url.database == "some_other_db"


def test_sync_tracking_disabled_start_sync_run_returns_id_without_db_access() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())

    sync_run_id = loader.start_sync_run(
        source_system="trackunit", job_name="trackunit_daily_activity_sync", start_date=20260101, end_date=20260101
    )

    assert isinstance(sync_run_id, str) and sync_run_id  # generated, just not persisted


def test_sync_tracking_disabled_start_table_load_returns_none() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())

    load_id = loader.start_table_load("some-run-id", "trackunit", "raw_trackunit", "asset", rows_input=1)

    assert load_id is None


def test_sync_tracking_disabled_finish_sync_run_and_finish_table_load_are_noops() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())

    # Must not raise / must not attempt any DB I/O.
    loader.finish_sync_run(sync_run_id="fake", status="SUCCESS")
    loader.finish_table_load(load_id=123, status="SUCCESS")


def _empty_daily_activity_dataframes() -> dict[str, pd.DataFrame]:
    return {
        "raw_assets_df": pd.DataFrame(),
        "raw_operating_hours_df": pd.DataFrame(),
        "raw_moving_hours_df": pd.DataFrame(),
        "raw_distance_df": pd.DataFrame(),
        "dim_assets_df": pd.DataFrame(),
        "daily_activity_df": pd.DataFrame(),
    }


def _empty_location_dataframes() -> dict[str, pd.DataFrame]:
    return {
        "raw_locations_df": pd.DataFrame(),
        "raw_site_history_df": pd.DataFrame(),
        "raw_sites_df": pd.DataFrame(),
        "enrichment_df": pd.DataFrame(),
    }


def test_load_trackunit_tables_legacy_target_uses_raw_and_staging_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = PostgresLoader(_legacy_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_trackunit_tables(_empty_daily_activity_dataframes(), target="legacy")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw", "trackunit_assets"),
        ("raw", "trackunit_aemp_operating_hours"),
        ("raw", "trackunit_aemp_moving_hours"),
        ("raw", "trackunit_aemp_distance"),
        ("staging", "trackunit_dim_assets"),
        ("staging", "trackunit_daily_activity"),
    ]


def test_load_trackunit_tables_platform_target_uses_raw_trackunit_and_stg_trackunit_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_trackunit_tables(_empty_daily_activity_dataframes(), target="platform")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw_trackunit", "asset"),
        ("raw_trackunit", "aemp_operating_hour"),
        ("raw_trackunit", "aemp_moving_hour"),
        ("raw_trackunit", "aemp_distance"),
        ("stg_trackunit", "asset"),
        ("stg_trackunit", "daily_activity"),
    ]


def test_load_trackunit_tables_rejects_unknown_target() -> None:
    loader = PostgresLoader(_legacy_settings())

    with pytest.raises(ValueError, match="target must be"):
        loader.load_trackunit_tables(_empty_daily_activity_dataframes(), target="staging")


def test_load_trackunit_location_enrichment_platform_target_uses_platform_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_trackunit_location_enrichment(_empty_location_dataframes(), target="platform")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw_trackunit", "aemp_location"),
        ("raw_trackunit", "site_history"),
        ("raw_trackunit", "site"),
        ("stg_trackunit", "location_enrichment"),
    ]


def test_load_trackunit_location_enrichment_rejects_unknown_target() -> None:
    loader = PostgresLoader(_legacy_settings())

    with pytest.raises(ValueError, match="target must be"):
        loader.load_trackunit_location_enrichment(_empty_location_dataframes(), target="bogus")
