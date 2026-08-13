from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pandas as pd
import pytest

from ge_data_platform.common.database import (
    POST_LOAD_VALIDATION_QUERIES,
    PostgresLoader,
    finish_sync_run_failed_safe,
    prepare_dataframe_for_load,
)
from ge_data_platform.common.http import build_retrying_session
from ge_data_platform.sources.sendem.transform import FACT_EVENTS_COLUMNS, FACT_TRIPS_COLUMNS, build_all


def _raw_payload(**overrides: list[dict]) -> dict[str, list[dict]]:
    raw: dict[str, list[dict]] = {
        "assets": [],
        "sites": [],
        "event_descriptions": [],
        "trips": [],
        "events": [],
    }
    raw.update(overrides)
    return raw


def test_empty_sendem_facts_retain_loader_schemas() -> None:
    result = build_all(
        _raw_payload(
            assets=[{"AssetId": 10, "SiteId": 20, "FleetNumber": "F-10"}],
            sites=[{"SiteId": 20, "SiteName": "Main"}],
        )
    )

    assert result["fact_trips"].empty
    assert list(result["fact_trips"].columns) == FACT_TRIPS_COLUMNS
    assert result["fact_events"].empty
    assert list(result["fact_events"].columns) == FACT_EVENTS_COLUMNS


def test_trips_survive_empty_assets_and_sites_without_key_errors() -> None:
    result = build_all(
        _raw_payload(
            trips=[
                {
                    "DateKey": 20260803,
                    "GroupId": 1,
                    "SiteId": 20,
                    "AssetId": 10,
                    "TotalTripCount": 2,
                    "TotalTripDistanceKilometres": 12.5,
                    "TotalFuelUsedLitres": 3.0,
                    "TotalEnergyUsedKwh": 0.0,
                }
            ]
        )
    )

    fact = result["fact_trips"]
    assert len(fact) == 1
    assert fact.loc[0, "AssetId"] == 10
    assert fact.loc[0, "SiteId"] == 20
    assert pd.isna(fact.loc[0, "FleetNumber"])
    assert pd.isna(fact.loc[0, "SiteName"])


@pytest.mark.parametrize(
    ("assets", "sites", "expected_fleet", "expected_site"),
    [
        ([], [{"SiteId": 20, "SiteName": "Main"}], None, "Main"),
        ([{"AssetId": 10, "SiteId": 20, "FleetNumber": "F-10"}], [], "F-10", None),
    ],
)
def test_trips_survive_each_missing_dimension_independently(
    assets: list[dict],
    sites: list[dict],
    expected_fleet: str | None,
    expected_site: str | None,
) -> None:
    result = build_all(
        _raw_payload(
            assets=assets,
            sites=sites,
            trips=[
                {
                    "DateKey": 20260803,
                    "GroupId": 1,
                    "SiteId": 20,
                    "AssetId": 10,
                    "TotalTripCount": 1,
                }
            ],
        )
    )

    row = result["fact_trips"].iloc[0]
    if expected_fleet is None:
        assert pd.isna(row["FleetNumber"])
    else:
        assert row["FleetNumber"] == expected_fleet
    if expected_site is None:
        assert pd.isna(row["SiteName"])
    else:
        assert row["SiteName"] == expected_site


def test_events_survive_independently_empty_dimensions() -> None:
    result = build_all(
        _raw_payload(
            events=[
                {
                    "DateKey": 20260803,
                    "GroupId": 1,
                    "SiteId": 20,
                    "AssetId": 10,
                    "EventTypeId": 99,
                    "TotalEventOccurrences": 1,
                }
            ]
        )
    )

    fact = result["fact_events"]
    assert len(fact) == 1
    assert fact.loc[0, "EventTypeId"] == 99
    assert pd.isna(fact.loc[0, "EventName"])


def test_failed_bookkeeping_never_replaces_original_exception() -> None:
    class BrokenBookkeepingLoader:
        def finish_sync_run(self, **_: object) -> None:
            raise RuntimeError("database unavailable")

    with pytest.raises(ValueError, match="provider root cause"):
        try:
            raise ValueError("provider root cause")
        except ValueError as original:
            finish_sync_run_failed_safe(BrokenBookkeepingLoader(), "run-id", original)  # type: ignore[arg-type]
            raise


def test_loaded_at_is_timezone_aware_utc() -> None:
    prepared = prepare_dataframe_for_load(pd.DataFrame([{"AssetId": 1}]))

    loaded_at = prepared.loc[0, "loaded_at"]
    assert loaded_at.tzinfo is not None
    assert loaded_at.utcoffset().total_seconds() == 0


def test_shared_http_attempt_limit_includes_initial_request() -> None:
    settings = SimpleNamespace(
        max_retries=3,
        backoff_seconds=2.0,
        connect_timeout_seconds=30.0,
        read_timeout_seconds=120.0,
    )

    session = build_retrying_session(settings)  # type: ignore[arg-type]
    retry = session.get_adapter("https://").max_retries
    assert retry.total == 2
    assert retry.connect == 2
    assert retry.read == 2
    assert retry.status == 2
    assert retry.is_retry("GET", 500, has_retry_after=False)
    assert not retry.is_retry("GET", 429, has_retry_after=True)
    assert not retry.is_retry("GET", 413, has_retry_after=True)


@pytest.mark.parametrize(("critical", "should_raise"), [(False, False), (True, True)])
def test_strict_validation_only_blocks_on_critical_query_errors(
    monkeypatch: pytest.MonkeyPatch,
    critical: bool,
    should_raise: bool,
) -> None:
    class BrokenConnection:
        def execute(self, *_: object, **__: object) -> None:
            raise RuntimeError("validation database error")

    class BrokenEngine:
        @contextmanager
        def connect(self):
            yield BrokenConnection()

    provider = f"test_validation_{critical}"
    monkeypatch.setitem(
        POST_LOAD_VALIDATION_QUERIES,
        provider,
        [("deliberately broken check", "SELECT 1", critical)],
    )
    loader = object.__new__(PostgresLoader)
    loader.engine = BrokenEngine()  # type: ignore[assignment]

    if should_raise:
        with pytest.raises(RuntimeError, match="Post-load validation query failed"):
            loader.run_post_load_validation(provider, mode="strict", lookback_hours=1)
    else:
        result = loader.run_post_load_validation(provider, mode="strict", lookback_hours=1)
        assert result == [
            {
                "check": "deliberately broken check",
                "critical": False,
                "status": "ERROR",
                "issue_count": None,
            }
        ]
