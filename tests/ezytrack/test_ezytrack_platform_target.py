"""Unit tests for EzyTrack's ge_warehouse ("platform") target support.

None of these tests touch a real database: PostgresLoader's constructor
only builds a (lazy) SQLAlchemy engine, and load_ezytrack_tables is
exercised by monkeypatching _run_load_plan to capture the load plan it was
given. Mirrors tests/trackunit/test_trackunit_platform_target.py and
tests/sendem/test_sendem_platform_target.py.

The get_last_successful_run() tests are the important new addition here:
that method is the one piece of shared code this migration had to change
(see docs/sources/ezytrack.md#ge_warehouse-platform-target) to stop a
platform-target run from ever reading legacy telemetry_warehouse's (possibly
very stale) success cursor and computing an unintended multi-day catch-up.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from ge_data_platform.common.database import PostgresLoader
from ge_data_platform.config.settings import EzytrackSettings, PlatformSettings, Settings
from ge_data_platform.sources.ezytrack import sync


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


def _ezytrack_settings(*, max_pages: int = 500) -> EzytrackSettings:
    return EzytrackSettings(
        auth_url="https://example.invalid/auth",
        graphql_url="https://example.invalid/graphql",
        username="test-user",
        password="test-password",
        grant_type="password",
        organisation_id=123,
        lookback_days=7,
        page_size=50,
        lookback_hours=6,
        chunk_hours=1,
        max_catchup_hours=168,
        catchup_overlap_minutes=30,
        max_pages=max_pages,
        reconciliation_lookback_hours=48,
    )


def _empty_ezytrack_dataframes() -> dict[str, pd.DataFrame]:
    return {
        "raw_assets_df": pd.DataFrame(),
        "raw_trips_df": pd.DataFrame(),
        "dim_assets_df": pd.DataFrame(),
        "fact_trips_df": pd.DataFrame(),
    }


# ---------------------------------------------------------------------------
# load_ezytrack_tables target selection
# ---------------------------------------------------------------------------


def test_load_ezytrack_tables_legacy_target_uses_raw_and_staging_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = PostgresLoader(_legacy_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_ezytrack_tables(_empty_ezytrack_dataframes(), target="legacy")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw", "ezytrack_assets"),
        ("raw", "ezytrack_trips"),
        ("staging", "ezytrack_dim_assets"),
        ("staging", "ezytrack_fact_trips"),
    ]


def test_load_ezytrack_tables_platform_target_uses_raw_ezytrack_and_stg_ezytrack_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    captured: dict = {}
    monkeypatch.setattr(
        loader, "_run_load_plan", lambda load_plan, sync_run_id, provider: captured.setdefault("plan", load_plan) or {}
    )

    loader.load_ezytrack_tables(_empty_ezytrack_dataframes(), target="platform")

    schemas_and_tables = [(schema, table) for schema, table, _df, _cc in captured["plan"]]
    assert schemas_and_tables == [
        ("raw_ezytrack", "asset"),
        ("raw_ezytrack", "trip"),
        ("stg_ezytrack", "asset"),
        ("stg_ezytrack", "trip"),
    ]


def test_load_ezytrack_tables_rejects_unknown_target() -> None:
    loader = PostgresLoader(_legacy_settings())

    with pytest.raises(ValueError, match="target must be"):
        loader.load_ezytrack_tables(_empty_ezytrack_dataframes(), target="staging")


# ---------------------------------------------------------------------------
# get_last_successful_run() catch-up safety guard
# ---------------------------------------------------------------------------


def test_get_last_successful_run_returns_none_without_db_access_for_platform_loader() -> None:
    # tracking_backend is always "platform" for a platform-settings loader,
    # and get_last_successful_run must short-circuit BEFORE any query is
    # issued -- not because etl.sync_runs doesn't exist in ge_warehouse
    # (true, but incidental), but because ops.pipeline_run is audit history,
    # never a catch-up cursor. See
    # tests/platform/test_ops_audit.py::test_get_last_successful_run_returns_none_for_platform_even_with_real_success_history
    # for the stronger version of this guarantee (proven against a real ops
    # schema that actually holds a SUCCESS row).
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    assert loader.tracking_backend == "platform"

    class ExplodingEngine:
        def connect(self):
            raise AssertionError("get_last_successful_run must not query the database for a platform loader")

    loader.engine = ExplodingEngine()  # type: ignore[assignment]

    assert loader.get_last_successful_run("ezytrack") is None


def test_get_last_successful_run_still_queries_for_legacy_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression guard: legacy behavior (etl.sync_runs lookup) must be
    # completely unaffected by the platform-target guard added above.
    loader = PostgresLoader(_legacy_settings())
    assert loader.tracking_backend == "legacy"

    calls: list[str] = []

    class FakeResult:
        def mappings(self):
            return self

        def first(self):
            calls.append("queried")
            return {"sync_run_id": "abc", "job_name": "ezytrack_hourly_sync", "started_at": "2026-08-01", "finished_at": None}

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args, **kwargs):
            return FakeResult()

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    loader.engine = FakeEngine()  # type: ignore[assignment]

    result = loader.get_last_successful_run("ezytrack")

    assert calls == ["queried"]
    assert result is not None and result["sync_run_id"] == "abc"


# ---------------------------------------------------------------------------
# sync.run(): target validation, platform target never attempts catch-up
# ---------------------------------------------------------------------------


def test_sync_run_rejects_unknown_target_before_touching_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_if_called() -> None:
        raise AssertionError("get_settings() must not be called for an invalid target")

    monkeypatch.setattr(sync, "get_settings", _fail_if_called)

    with pytest.raises(ValueError, match="target must be"):
        sync.run(target="bogus")


def test_sync_run_rejects_lookback_hours_below_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync, "get_settings", lambda: object())
    monkeypatch.setattr(sync, "get_ezytrack_settings", _ezytrack_settings)

    with pytest.raises(ValueError, match="lookback_hours must be at least 1"):
        sync.run(target="legacy", lookback_hours=0)


def test_sync_run_platform_target_wires_through_without_legacy_bookkeeping(monkeypatch: pytest.MonkeyPatch) -> None:
    # End-to-end (fake loader/client) proof that run() passes target="platform"
    # all the way through to load_ezytrack_tables and completes successfully
    # using only PostgresLoader.from_platform_settings() -- never constructing
    # a legacy Settings-based loader. Combined with
    # test_get_last_successful_run_returns_none_without_db_access_for_platform_loader
    # above (which proves the *real* PostgresLoader short-circuits before any
    # query when sync tracking is disabled), this confirms the full chain
    # never reads legacy telemetry_warehouse's success cursor for a
    # platform-target run.
    get_last_successful_run_calls: list[str] = []

    class FakeLoader:
        def __init__(self, settings: object) -> None:
            self.settings = settings

        def get_last_successful_run(self, source_system: str) -> None:
            get_last_successful_run_calls.append(source_system)
            return None

        def start_sync_run(self, **kwargs: object) -> str:
            return "sync-run-1"

        def load_ezytrack_tables(self, dataframes: object, **kwargs: object) -> dict:
            assert kwargs.get("target") == "platform"
            return {}

        def finish_sync_run(self, **kwargs: object) -> None:
            pass

    class FakeClient:
        def __init__(self, settings: EzytrackSettings) -> None:
            self.settings = settings

        def authenticate(self) -> None:
            pass

        def fetch_assets(self) -> list:
            return []

        def fetch_trips(self, *args: object, **kwargs: object) -> list:
            return []

    monkeypatch.setattr(sync, "get_settings", lambda: object())
    monkeypatch.setattr(sync, "get_platform_settings", lambda: object())
    monkeypatch.setattr(sync, "get_ezytrack_settings", _ezytrack_settings)
    monkeypatch.setattr(sync, "PostgresLoader", type("PL", (), {"from_platform_settings": staticmethod(lambda settings: FakeLoader(settings))}))
    monkeypatch.setattr(sync, "EzytrackClient", FakeClient)

    sync.run(target="platform", lookback_hours=1, now_utc=datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc))

    # get_last_successful_run was called on the fake loader (that part of
    # run()'s control flow is unconditional), but its result must never
    # produce a catch-up window -- proven by the real PostgresLoader test
    # above; this test proves run() correctly passes target="platform"
    # through to load_ezytrack_tables and completes without touching legacy
    # bookkeeping at all (FakeLoader has no etl.sync_runs dependency).
    assert get_last_successful_run_calls == ["ezytrack"]
