"""Tests for the shared ops.pipeline_run/ops.table_load audit mechanism.

Covers ge_data_platform.common.audit directly (against a real, if SQLite,
`ops` schema -- see _ops_sqlite_helpers.build_sqlite_ops_engine) and
PostgresLoader's legacy/platform dispatch (start_sync_run, finish_sync_run,
start_table_load, finish_table_load, get_last_successful_run) using a small
SQL-capturing fake engine, so isolation between etl.* and ops.* can be
asserted on the literal SQL text without needing two real databases.

None of these tests touch a real PostgreSQL server.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from ge_data_platform.common import audit
from ge_data_platform.common.database import (
    PostgresLoader,
    compute_abandoned_cutoff,
    finish_sync_run_failed_safe,
)
from ge_data_platform.config.settings import PlatformSettings, Settings

from tests.platform._ops_sqlite_helpers import build_sqlite_ops_engine


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


def _platform_loader_with_sqlite_ops():
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    loader.engine = build_sqlite_ops_engine()
    return loader


# ---------------------------------------------------------------------------
# database target selection
# ---------------------------------------------------------------------------


def test_from_platform_settings_selects_platform_tracking_backend() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    assert loader.tracking_backend == "platform"
    assert loader.engine.url.database == "ge_warehouse"


def test_direct_construction_selects_legacy_tracking_backend() -> None:
    loader = PostgresLoader(_legacy_settings())
    assert loader.tracking_backend == "legacy"
    assert loader.engine.url.database == "telemetry_warehouse"


def test_constructor_rejects_unknown_tracking_backend() -> None:
    with pytest.raises(ValueError, match="tracking_backend must be"):
        PostgresLoader(_legacy_settings(), tracking_backend="bogus")


# ---------------------------------------------------------------------------
# ge_data_platform.common.audit -- direct, against a real (SQLite) ops schema
# ---------------------------------------------------------------------------


def test_start_pipeline_run_inserts_started_row_with_correct_identity() -> None:
    engine = build_sqlite_ops_engine()

    pipeline_run_id = audit.start_pipeline_run(
        engine, source_system="trackunit", job_name="trackunit_daily_activity_sync", start_date=20260101, end_date=20260102
    )

    assert isinstance(pipeline_run_id, str) and pipeline_run_id
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT source_system, job_name, status, start_date, end_date, started_at, finished_at "
                 "FROM ops.pipeline_run WHERE pipeline_run_id = :id"),
            {"id": pipeline_run_id},
        ).mappings().one()

    assert row["source_system"] == "trackunit"
    assert row["job_name"] == "trackunit_daily_activity_sync"
    assert row["status"] == "STARTED"
    assert row["start_date"] == 20260101
    assert row["end_date"] == 20260102
    assert row["started_at"] is not None
    assert row["finished_at"] is None


def test_complete_pipeline_run_marks_success_with_counts_and_finished_at() -> None:
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="sendem", job_name="sendem_hourly_sync")

    audit.complete_pipeline_run(engine, pipeline_run_id, rows_fetched=120, rows_loaded=97)

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT status, rows_fetched, rows_loaded, finished_at, error_message "
                 "FROM ops.pipeline_run WHERE pipeline_run_id = :id"),
            {"id": pipeline_run_id},
        ).mappings().one()

    assert row["status"] == "SUCCESS"
    assert row["rows_fetched"] == 120
    assert row["rows_loaded"] == 97  # real extracted-vs-loaded discrepancy preserved, not invented precision
    assert row["finished_at"] is not None
    assert row["error_message"] is None


def test_complete_pipeline_run_records_zero_row_success_accurately() -> None:
    # A genuinely empty successful load must be recorded as SUCCESS with
    # rows_loaded=0, never coerced into a failure.
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="sendem", job_name="sendem_hourly_sync")

    audit.complete_pipeline_run(engine, pipeline_run_id, rows_fetched=0, rows_loaded=0)

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT status, rows_loaded FROM ops.pipeline_run WHERE pipeline_run_id = :id"), {"id": pipeline_run_id}
        ).mappings().one()

    assert row["status"] == "SUCCESS"
    assert row["rows_loaded"] == 0


def test_fail_pipeline_run_marks_failed_and_preserves_error_message() -> None:
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="ezytrack", job_name="ezytrack_hourly_sync")

    audit.fail_pipeline_run(engine, pipeline_run_id, error_message="EzyTrack trip chunk failed: rate limited")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT status, error_message, finished_at FROM ops.pipeline_run WHERE pipeline_run_id = :id"),
            {"id": pipeline_run_id},
        ).mappings().one()

    assert row["status"] == "FAILED"
    assert row["error_message"] == "EzyTrack trip chunk failed: rate limited"
    assert row["finished_at"] is not None


def test_fail_pipeline_run_safe_swallows_bookkeeping_error_and_preserves_original() -> None:
    # Mirrors database.finish_sync_run_failed_safe's contract: if marking
    # FAILED itself blows up, the ORIGINAL exception must still be what the
    # caller sees -- the bookkeeping failure is only logged.
    class ExplodingEngine:
        def begin(self):
            raise RuntimeError("database unreachable")

    original_error = ValueError("original ingestion failure")

    # Must not raise -- the bookkeeping error is swallowed, not propagated.
    audit.fail_pipeline_run_safe(ExplodingEngine(), "some-run-id", original_error)


def test_start_and_finish_table_load_round_trip() -> None:
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="trackunit", job_name="trackunit_daily_activity_sync")

    table_load_id = audit.start_table_load(
        engine, pipeline_run_id, source_system="trackunit", schema_name="raw_trackunit", table_name="asset", rows_input=42
    )
    assert isinstance(table_load_id, int)

    audit.finish_table_load(engine, table_load_id, status="SUCCESS", rows_loaded=42)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT pipeline_run_id, source_system, schema_name, table_name, rows_input, rows_loaded, status "
                "FROM ops.table_load WHERE table_load_id = :id"
            ),
            {"id": table_load_id},
        ).mappings().one()

    assert row["pipeline_run_id"] == pipeline_run_id
    assert row["source_system"] == "trackunit"
    assert row["schema_name"] == "raw_trackunit"
    assert row["table_name"] == "asset"
    assert row["rows_input"] == 42
    assert row["rows_loaded"] == 42
    assert row["status"] == "SUCCESS"


def test_table_load_references_a_real_pipeline_run_no_orphans() -> None:
    # FK/orphan integrity: every table_load row's pipeline_run_id must
    # resolve to a real ops.pipeline_run row.
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="evolution_project_reports", job_name="evolution_project_reports_sync")
    table_load_id = audit.start_table_load(
        engine, pipeline_run_id, source_system="evolution_project_reports", schema_name="raw_evolution", table_name="project_report", rows_input=10
    )

    with engine.connect() as conn:
        orphans = conn.execute(
            text(
                "SELECT COUNT(*) FROM ops.table_load t "
                "LEFT JOIN ops.pipeline_run p ON p.pipeline_run_id = t.pipeline_run_id "
                "WHERE p.pipeline_run_id IS NULL"
            )
        ).scalar_one()

    assert orphans == 0
    assert table_load_id is not None


def test_record_table_load_convenience_wrapper_is_start_then_finish() -> None:
    engine = build_sqlite_ops_engine()
    pipeline_run_id = audit.start_pipeline_run(engine, source_system="sendem", job_name="sendem_hourly_sync")

    table_load_id = audit.record_table_load(
        engine,
        pipeline_run_id,
        source_system="sendem",
        schema_name="raw_sendem",
        table_name="asset",
        rows_input=5,
        rows_loaded=5,
    )

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT status, rows_loaded FROM ops.table_load WHERE table_load_id = :id"), {"id": table_load_id}
        ).mappings().one()
    assert row["status"] == "SUCCESS"
    assert row["rows_loaded"] == 5


def test_mark_abandoned_pipeline_runs_marks_only_old_started_rows() -> None:
    engine = build_sqlite_ops_engine()
    now = datetime.now(timezone.utc)
    old_id = audit.start_pipeline_run(engine, source_system="trackunit", job_name="trackunit_daily_activity_sync")
    fresh_id = audit.start_pipeline_run(engine, source_system="sendem", job_name="sendem_hourly_sync")
    # Backdate the "old" run's started_at past the cutoff.
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ops.pipeline_run SET started_at = :started_at WHERE pipeline_run_id = :id"),
            {"started_at": (now - timedelta(hours=48)).isoformat(), "id": old_id},
        )

    cutoff = compute_abandoned_cutoff(24, now)
    marked = audit.mark_abandoned_pipeline_runs(engine, cutoff, threshold_hours=24)

    assert [run["pipeline_run_id"] for run in marked] == [old_id]
    with engine.connect() as conn:
        old_status, fresh_status = (
            conn.execute(text("SELECT status FROM ops.pipeline_run WHERE pipeline_run_id = :id"), {"id": old_id}).scalar_one(),
            conn.execute(text("SELECT status FROM ops.pipeline_run WHERE pipeline_run_id = :id"), {"id": fresh_id}).scalar_one(),
        )
    assert old_status == "ABANDONED"
    assert fresh_status == "STARTED"


def test_postgres_loader_mark_abandoned_pipeline_runs_delegates_to_audit() -> None:
    loader = _platform_loader_with_sqlite_ops()
    now = datetime.now(timezone.utc)
    old_id = audit.start_pipeline_run(loader.engine, source_system="ezytrack", job_name="ezytrack_hourly_sync")
    with loader.engine.begin() as conn:
        conn.execute(
            text("UPDATE ops.pipeline_run SET started_at = :started_at WHERE pipeline_run_id = :id"),
            {"started_at": (now - timedelta(hours=48)).isoformat(), "id": old_id},
        )

    marked = loader.mark_abandoned_pipeline_runs(24, now_utc=now)

    assert [run["pipeline_run_id"] for run in marked] == [old_id]


# ---------------------------------------------------------------------------
# PostgresLoader dispatch: legacy vs platform isolation (SQL-capturing fake engine)
# ---------------------------------------------------------------------------


class _CapturingConnection:
    def __init__(self, calls: list, scalar_return: object = None) -> None:
        self._calls = calls
        self._scalar_return = scalar_return

    def __enter__(self) -> "_CapturingConnection":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, statement: object, params: dict | None = None) -> "_CapturingResult":
        self._calls.append((str(statement), dict(params or {})))
        return _CapturingResult(self._scalar_return)


class _CapturingResult:
    def __init__(self, scalar_return: object) -> None:
        self._scalar_return = scalar_return

    def scalar_one(self) -> object:
        return self._scalar_return


class _CapturingEngine:
    """Fake engine recording every executed statement's SQL text + params."""

    def __init__(self, scalar_return: object = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._scalar_return = scalar_return

    def begin(self) -> _CapturingConnection:
        return _CapturingConnection(self.calls, self._scalar_return)


def test_platform_start_sync_run_writes_only_ops_pipeline_run() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    engine = _CapturingEngine()
    loader.engine = engine

    pipeline_run_id = loader.start_sync_run(
        source_system="trackunit", job_name="trackunit_daily_activity_sync", start_date=20260101, end_date=20260101
    )

    assert isinstance(pipeline_run_id, str) and pipeline_run_id
    assert len(engine.calls) == 1
    sql, params = engine.calls[0]
    assert "ops.pipeline_run" in sql
    assert "etl." not in sql
    assert params["pipeline_run_id"] == pipeline_run_id
    assert params["source_system"] == "trackunit"


def test_platform_start_table_load_writes_only_ops_table_load_and_returns_real_id() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    engine = _CapturingEngine(scalar_return=7)
    loader.engine = engine

    load_id = loader.start_table_load("some-run-id", "trackunit", "raw_trackunit", "asset", rows_input=3)

    assert load_id == 7
    sql, params = engine.calls[0]
    assert "ops.table_load" in sql
    assert "etl." not in sql
    assert params["pipeline_run_id"] == "some-run-id"
    assert params["source_system"] == "trackunit"


def test_platform_finish_sync_run_and_finish_table_load_write_ops_rows() -> None:
    loader = PostgresLoader.from_platform_settings(_platform_settings())
    engine = _CapturingEngine()
    loader.engine = engine

    loader.finish_sync_run(sync_run_id="fake-run", status="SUCCESS", rows_fetched=5, rows_loaded=5)
    loader.finish_table_load(load_id=123, status="SUCCESS", rows_loaded=5)

    assert len(engine.calls) == 2
    run_sql, run_params = engine.calls[0]
    assert "ops.pipeline_run" in run_sql and "etl." not in run_sql
    assert run_params["status"] == "SUCCESS"
    load_sql, load_params = engine.calls[1]
    assert "ops.table_load" in load_sql and "etl." not in load_sql
    assert load_params["rows_loaded"] == 5


def test_legacy_start_sync_run_and_start_table_load_never_touch_ops() -> None:
    loader = PostgresLoader(_legacy_settings())
    engine = _CapturingEngine(scalar_return=1)
    loader.engine = engine

    sync_run_id = loader.start_sync_run(source_system="trackunit", job_name="j", start_date=None, end_date=None)
    loader.start_table_load(sync_run_id, "trackunit", "raw", "trackunit_assets", rows_input=1)

    assert len(engine.calls) == 2
    for sql, _params in engine.calls:
        assert "ops." not in sql
        assert "etl." in sql


def test_finish_sync_run_failed_safe_preserves_original_error_for_platform_loader() -> None:
    # The project-wide rule (finish_sync_run_failed_safe) must hold
    # regardless of which backend the bookkeeping call underneath targets.
    loader = PostgresLoader.from_platform_settings(_platform_settings())

    class ExplodingEngine:
        def begin(self):
            raise RuntimeError("ge_warehouse unreachable")

    loader.engine = ExplodingEngine()  # type: ignore[assignment]
    original_error = ValueError("original platform ingestion failure")

    # Must not raise the bookkeeping RuntimeError -- only logs it.
    finish_sync_run_failed_safe(loader, "some-run-id", original_error)


# ---------------------------------------------------------------------------
# get_last_successful_run: catch-up-cursor safety (audit history != watermark)
# ---------------------------------------------------------------------------


def test_get_last_successful_run_returns_none_for_platform_even_with_real_success_history() -> None:
    # The critical EzyTrack-catch-up-safety regression guard: even though
    # ops.pipeline_run now genuinely contains SUCCESS rows for platform-
    # target runs, get_last_successful_run must still short-circuit to None
    # WITHOUT querying at all -- proven here with a real SQLite ops schema
    # that actually holds a SUCCESS row (so a query, if issued, would find
    # it and return a stale catch-up cursor).
    loader = _platform_loader_with_sqlite_ops()
    pipeline_run_id = audit.start_pipeline_run(loader.engine, source_system="ezytrack", job_name="ezytrack_hourly_sync")
    audit.complete_pipeline_run(loader.engine, pipeline_run_id, rows_fetched=10, rows_loaded=10)

    class ExplodingEngine:
        def connect(self):
            raise AssertionError("get_last_successful_run must not query ops.pipeline_run for a platform loader")

    real_engine = loader.engine
    # Prove the row genuinely exists first (the safety guard would be
    # meaningless if this assertion failed).
    with real_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM ops.pipeline_run WHERE status = 'SUCCESS'")).scalar_one() == 1

    loader.engine = ExplodingEngine()  # type: ignore[assignment]
    assert loader.get_last_successful_run("ezytrack") is None
