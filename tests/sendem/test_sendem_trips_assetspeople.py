"""The Sendem driver-grain trip feed (engine time): transform, load plan, backfill windows.

No database: the transform is pure pandas, the loader's plan is captured by
monkeypatching _run_load_plan (as in test_sendem_platform_target.py), and
the backfill's window arithmetic is pure. The asset-day roll-up itself
lives in reporting.vw_sendem_asset_daily_activity
(sql/legacy/telemetry_migrations/030_create_sendem_trips_assetspeople.sql)
and is proven against real data by scripts/validate_sendem_engine_hours.py;
what is pinned here is everything the roll-up relies on: the grain the
transform preserves, the key the loader UPSERTs on, and that an unknown
engine time is never turned into nought on the way in.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from ge_data_platform.common.database import SENDEM_TRIPS_ASSETS_PEOPLE_KEY, PostgresLoader, prepare_dataframe_for_load
from ge_data_platform.config.settings import Settings
from ge_data_platform.sources.sendem.transform import (
    FACT_TRIPS_ASSETS_PEOPLE_COLUMNS,
    TRIP_ASSETS_PEOPLE_COLUMNS,
    build_all,
)
from scripts.backfill_sendem_trips_assetspeople import windows

# Fleet 9308 (Sendem AssetId 1535407637279948800) on 2026-09-26, exactly as
# the API returned it: four drivers, engine seconds 29 + 12058 + 56 + 114 =
# 12257 s = 3.40 h, trips 2 + 7 + 2 + 4 = 15, distance 2.9 km.
ASSET_9308 = 1535407637279948800
SITE = -1045231813400753653
GROUP = 995672767761364170


def driver_row(driver_id: int, engine, trips: int, km: float, driving=0, duration=None) -> dict:
    return {
        "DateKey": 20260926,
        "GroupId": GROUP,
        "SiteId": SITE,
        "AssetId": ASSET_9308,
        "DriverId": driver_id,
        "TotalTripCount": trips,
        "TotalTripDistanceKilometres": km,
        "TotalFuelUsedLitres": None,
        "TotalEnergyUsedKwh": None,
        "TotalIdleTimeSeconds": 0,
        "TotalAssetDrivingTimeSeconds": driving,
        "TotalDriverDrivingTimeSeconds": driving,
        "TotalEngineSecond": engine,
        "TotalTripDuration": duration if duration is not None else engine,
    }


FOUR_DRIVERS = [
    driver_row(1781392708921413632, 114, 4, 0),
    driver_row(1390113607043342336, 12058, 7, 2.9000000000000004, driving=1987),
    driver_row(1263940529595219968, 29, 2, 0),
    driver_row(1781390660722032640, 56, 2, 0),
]


def _raw(**overrides):
    raw = {"assets": [], "sites": [], "event_descriptions": [], "trips": [], "trips_assets_people": [], "events": []}
    raw.update(overrides)
    return raw


def test_driver_rows_keep_their_grain_and_are_never_summed_by_the_transform() -> None:
    frames = build_all(_raw(
        trips_assets_people=FOUR_DRIVERS,
        assets=[{"AssetId": ASSET_9308, "SiteId": SITE, "FleetNumber": "9308", "Description": "Forklift"}],
        sites=[{"SiteId": SITE, "SiteName": "Yard"}],
    ))

    fact = frames["fact_trips_assets_people"]
    assert list(fact.columns) == FACT_TRIPS_ASSETS_PEOPLE_COLUMNS
    assert len(fact) == 4, "one row per driver, exactly as Sendem states it"
    assert fact["DriverId"].nunique() == 4
    assert set(fact["AssetId"]) == {ASSET_9308}
    assert set(fact["date"]) == {pd.Timestamp("2026-09-26")}
    # Enrichment from the dimensions, like the asset-level fact.
    assert set(fact["FleetNumber"]) == {"9308"}
    assert set(fact["SiteName"]) == {"Yard"}
    # What the view will roll up: the sum over the four rows is 9308's day.
    assert fact["TotalEngineSecond"].sum() == 12257
    assert round(fact["TotalEngineSecond"].sum() / 3600, 2) == 3.40
    assert fact["TotalTripCount"].sum() == 15
    assert round(fact["TotalTripDistanceKilometres"].sum(), 1) == 2.9
    # The raw frame is the API's own columns plus the parsed date.
    assert set(TRIP_ASSETS_PEOPLE_COLUMNS) <= set(frames["trips_people_df"].columns)


def test_a_null_engine_time_stays_null_through_transform_and_load_preparation() -> None:
    frames = build_all(_raw(trips_assets_people=[
        driver_row(1, None, 3, 1.0, duration=600),
        driver_row(2, 0, 1, 0.0),
    ]))

    fact = frames["fact_trips_assets_people"]
    unknown = fact[fact["DriverId"] == 1].iloc[0]
    nought = fact[fact["DriverId"] == 2].iloc[0]
    assert pd.isna(unknown["TotalEngineSecond"]), "unknown is not nought"
    assert nought["TotalEngineSecond"] == 0, "a measured nought is nought"

    prepared = prepare_dataframe_for_load(fact)
    assert "total_engine_second" in prepared.columns, "the API name snake-cases to the table column"
    # A float column keeps NaN rather than None; pandas' to_sql writes
    # either as SQL NULL (how total_fuel_used_litres has always loaded).
    assert pd.isna(prepared.loc[prepared["driver_id"] == 1, "total_engine_second"].iloc[0])
    assert prepared.loc[prepared["driver_id"] == 2, "total_engine_second"].iloc[0] == 0


def test_an_empty_or_absent_payload_keeps_the_loader_schema() -> None:
    absent = build_all({"assets": [], "sites": [], "event_descriptions": [], "trips": [], "events": []})
    assert absent["fact_trips_assets_people"].empty
    assert list(absent["fact_trips_assets_people"].columns) == FACT_TRIPS_ASSETS_PEOPLE_COLUMNS

    empty = build_all(_raw(trips_assets_people=[]))
    assert list(empty["trips_people_df"].columns) == [*TRIP_ASSETS_PEOPLE_COLUMNS, "date"]


def test_driver_rows_survive_empty_dimensions_without_key_errors() -> None:
    frames = build_all(_raw(trips_assets_people=FOUR_DRIVERS))
    fact = frames["fact_trips_assets_people"]
    assert len(fact) == 4
    assert fact["FleetNumber"].isna().all()
    assert fact["SiteName"].isna().all()


def test_the_asset_level_feed_is_still_loaded_beside_the_driver_grain_one(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = PostgresLoader(Settings(
        insights_api_url="https://example.invalid", sendem_api_key="key", sendem_group_id=1,
        postgres_host="localhost", postgres_port=5432, postgres_db="telemetry_warehouse",
        postgres_user="u", postgres_password="p", sync_lookback_days=7,
    ))
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_sendem_tables(build_all(_raw(trips_assets_people=FOUR_DRIVERS)))

    by_table = {(schema, table): (df, key) for schema, table, df, key in captured["plan"]}
    assert ("raw", "sendem_trips_assets_daily") in by_table
    assert ("staging", "sendem_fact_trips_daily") in by_table
    raw_df, raw_key = by_table[("raw", "sendem_trips_assetspeople_daily")]
    stg_df, stg_key = by_table[("staging", "sendem_fact_trips_assetspeople_daily")]
    # UPSERT on the full driver grain: a re-run rewrites, never duplicates.
    assert raw_key == stg_key == SENDEM_TRIPS_ASSETS_PEOPLE_KEY
    assert "driver_id" in raw_key and "site_id" in raw_key
    assert len(raw_df) == len(stg_df) == 4
    # Loaded after every long-established table.
    order = [(schema, table) for schema, table, _df, _key in captured["plan"]]
    assert order[-2:] == [
        ("raw", "sendem_trips_assetspeople_daily"),
        ("staging", "sendem_fact_trips_assetspeople_daily"),
    ]


def test_backfill_windows_are_inclusive_contiguous_and_never_overlap() -> None:
    result = windows(date(2026, 7, 9), date(2026, 10, 2), 14)
    assert result[0] == (date(2026, 7, 9), date(2026, 7, 22))
    assert result[-1][1] == date(2026, 10, 2)
    for (_, end), (next_start, _) in zip(result, result[1:]):
        assert next_start == end + pd.Timedelta(days=1).to_pytimedelta()
    covered = sum((end - start).days + 1 for start, end in result)
    assert covered == (date(2026, 10, 2) - date(2026, 7, 9)).days + 1

    assert windows(date(2026, 9, 26), date(2026, 9, 26), 14) == [(date(2026, 9, 26), date(2026, 9, 26))]
    with pytest.raises(ValueError):
        windows(date(2026, 9, 27), date(2026, 9, 26), 14)
