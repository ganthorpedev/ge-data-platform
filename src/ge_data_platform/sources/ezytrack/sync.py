"""Entry point for the EzyTrack / Telematics Guru sync job.

Run with:
    python -m ge_data_platform.sources.ezytrack.sync

Daily reconciliation mode (a longer, fixed lookback):
    python -m ge_data_platform.sources.ezytrack.sync --reconcile

Platform target (ge_warehouse, opt-in, manual only -- see below):
    python -m ge_data_platform.sources.ezytrack.sync --target platform --lookback-hours 1

This orchestrates: fetch (ge_data_platform.sources.ezytrack.client) ->
transform (ge_data_platform.sources.ezytrack.transform) -> load
(ge_data_platform.common.database), and records the run in
etl.sync_runs/etl.sync_table_loads (legacy target) or
ops.pipeline_run/ops.table_load (platform target) -- see below.

RELIABLE CATCH-UP MODE:
The first run uses the existing default lookback (6 hours by default).
Subsequent normal runs continue from the latest successful EzyTrack run's
window end, with a small overlap, capped at a configurable maximum catch-up
window. A reconciliation run deliberately ignores that cursor and uses its
longer configured lookback. Every mode still fetches trips in small chunks
(1-hour chunks and page_size 50 by default) instead of one large request. This is strict,
all-or-nothing: if any chunk fails for any reason (rate limit or otherwise),
the whole run is marked FAILED with that chunk's window in error_message,
and the exception is re-raised. There is no partial SUCCESS -- either every
chunk fetches cleanly and the transformed/deduplicated result is loaded, or
nothing is loaded and the run is marked FAILED.

PLATFORM TARGET AND CATCH-UP SAFETY:
`--target platform` writes to raw_ezytrack.*/stg_ezytrack.* in ge_warehouse
via PostgresLoader.from_platform_settings(), with the run and each table
load recorded in ops.pipeline_run/ops.table_load (see
ge_data_platform.common.audit) exactly like the legacy target records
etl.sync_runs/etl.sync_table_loads. Despite ops.pipeline_run now holding
real platform run history, the catch-up cursor lookup
(`get_last_successful_run`) still ALWAYS returns None for a platform-target
loader -- this is deliberate and unchanged: ops.pipeline_run is audit
history, not the reconciliation cursor (`ops.source_watermark`, not yet
populated by any job, is reserved for that), so it is never read back to
compute a fetch window. A platform-target run therefore always computes its
window as a first-run/explicit-window fetch, exactly like `--reconcile`
already does by construction, never a multi-day catch-up inferred from
either a stale legacy cursor or from ops.pipeline_run's own success history.
`--lookback-hours` (below) additionally lets an operator supply an explicit
small window for a manual platform-target test, overriding
`EzytrackSettings.lookback_hours` for that one run only.

AUTHENTICATION:
This job authenticates once, explicitly, right after starting the sync_run
row -- see ge_data_platform.sources.ezytrack.client.EzytrackClient. Telematics Guru access
tokens expire after ~24h, so the token is never read from a static .env
value; it's fetched fresh every run and held in memory only. An auth
failure marks the run FAILED with a safe message (never the username,
password, or token). A mid-run 401/AUTH_NOT_AUTHENTICATED triggers exactly
one re-authenticate + one retry inside the client; if that also fails, it
propagates like any other chunk failure. This is unrelated to
RateLimitError (GraphQL cost limit) -- token refresh is never used to work
around a rate limit, and a rate limit is never retried automatically.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ge_data_platform.common.database import PostgresLoader, finish_sync_run_failed_safe
from ge_data_platform.common.dates import format_utc_iso, to_date_key, utc_now
from ge_data_platform.common.logging import configure_logging
from ge_data_platform.config.settings import (
    EzytrackSettings,
    get_etl_ops_settings,
    get_ezytrack_settings,
    get_platform_settings,
    get_settings,
)
from ge_data_platform.sources.ezytrack.client import EzytrackClient
from ge_data_platform.sources.ezytrack.transform import build_all

SOURCE_SYSTEM = "ezytrack"
JOB_NAME = "ezytrack_hourly_sync"
RECONCILIATION_JOB_NAME = "ezytrack_daily_reconciliation"

logger = logging.getLogger(__name__)


def _as_utc(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime.

    PostgreSQL normally returns TIMESTAMPTZ values as aware datetimes. A
    naive value is nevertheless treated as UTC for compatibility with test
    doubles and older database drivers; it is never interpreted using the
    machine's local timezone.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def calculate_sync_window(
    *,
    end_time: datetime,
    default_lookback_hours: int,
    max_catchup_hours: int,
    overlap_minutes: int,
    last_success_window_end: datetime | None = None,
    reconciliation_lookback_hours: int | None = None,
) -> tuple[datetime, datetime]:
    """Calculate the EzyTrack UTC fetch window.

    Normal first-ever runs preserve ``default_lookback_hours``. Once a
    successful run exists, its window end is used as a cursor with
    ``overlap_minutes`` subtracted, but the result is never allowed to be
    older than ``max_catchup_hours``. Reconciliation mode is selected by
    supplying ``reconciliation_lookback_hours`` and intentionally ignores
    the success cursor.
    """
    if default_lookback_hours < 1:
        raise ValueError("default_lookback_hours must be at least 1")
    if max_catchup_hours < 1:
        raise ValueError("max_catchup_hours must be at least 1")
    if overlap_minutes < 0:
        raise ValueError("overlap_minutes must not be negative")
    if reconciliation_lookback_hours is not None and reconciliation_lookback_hours < 1:
        raise ValueError("reconciliation_lookback_hours must be at least 1")

    window_end = _as_utc(end_time)

    if reconciliation_lookback_hours is not None:
        return window_end - timedelta(hours=reconciliation_lookback_hours), window_end

    if last_success_window_end is None:
        return window_end - timedelta(hours=default_lookback_hours), window_end

    successful_end = _as_utc(last_success_window_end)
    if successful_end > window_end:
        # A small database/application clock difference must not produce an
        # inverted or empty window. Re-fetch the overlap immediately before
        # this process's current UTC time instead.
        logger.warning(
            "Latest successful EzyTrack window end %s is later than current UTC time %s; "
            "clamping it to the current time",
            successful_end.isoformat(),
            window_end.isoformat(),
        )
        successful_end = window_end

    cursor_start = successful_end - timedelta(minutes=overlap_minutes)
    catchup_floor = window_end - timedelta(hours=max_catchup_hours)
    return max(cursor_start, catchup_floor), window_end


def _last_success_window_end(last_success: dict[str, Any] | None) -> datetime | None:
    """Extract the best available UTC window-end cursor from a sync-run row.

    ``etl.sync_runs`` currently stores only integer start/end date keys, not
    exact timestamps. ``started_at`` is therefore the backward-compatible
    cursor: this job chooses ``end_time`` immediately before inserting the
    run row. ``window_end_utc`` is preferred automatically if a future
    migration adds that exact field.
    """
    if not last_success:
        return None

    value = last_success.get("window_end_utc") or last_success.get("started_at")
    if value is None:
        raise ValueError("Latest successful EzyTrack sync has no usable window-end timestamp")
    if not isinstance(value, datetime):
        raise TypeError(f"Latest successful EzyTrack window end must be a datetime, got {type(value).__name__}")
    return _as_utc(value)


def _build_chunk_windows(start_time: datetime, end_time: datetime, chunk_hours: int) -> list[tuple[datetime, datetime]]:
    """Split [start_time, end_time) into consecutive chunk_hours-sized windows."""
    if chunk_hours < 1:
        raise ValueError("chunk_hours must be at least 1")
    if end_time < start_time:
        raise ValueError("end_time must not be before start_time")

    windows = []
    cursor = start_time
    step = timedelta(hours=chunk_hours)

    while cursor < end_time:
        chunk_end = min(cursor + step, end_time)
        windows.append((cursor, chunk_end))
        cursor = chunk_end

    return windows


def _dedupe_trips_by_id(trips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """De-duplicate trip records by tripId, keeping the first occurrence.

    Adjacent chunk windows can both return a trip that starts exactly on
    their shared boundary, so this runs once over the combined chunk results
    before transform/load.
    """
    seen: set = set()
    deduped: list[dict[str, Any]] = []

    for trip in trips:
        trip_id = trip.get("tripId")
        if trip_id in seen:
            continue
        seen.add(trip_id)
        deduped.append(trip)

    return deduped


def _fetch_trips_for_chunk(
    client: EzytrackClient,
    chunk_start: datetime,
    chunk_end: datetime,
    ezytrack_settings: EzytrackSettings,
) -> list[dict[str, Any]]:
    """Fetch trips for one chunk window, or raise with that window attached.

    Any failure (rate limit or otherwise) is re-raised as a RuntimeError
    whose message names this specific chunk's window, so a failure deep in a
    6-chunk run is still traceable to exactly where it happened.
    """
    chunk_start_str = format_utc_iso(chunk_start)
    chunk_end_str = format_utc_iso(chunk_end)
    print(f"Fetching EzyTrack trips chunk: {chunk_start_str} to {chunk_end_str}")

    try:
        return client.fetch_trips(chunk_start_str, chunk_end_str, page_size=ezytrack_settings.page_size)
    except Exception as error:
        raise RuntimeError(
            f"EzyTrack trip chunk failed ({chunk_start_str} to {chunk_end_str}): {error}"
        ) from error


def run(
    *,
    reconcile: bool = False,
    now_utc: datetime | None = None,
    target: str = "legacy",
    lookback_hours: int | None = None,
) -> None:
    """Execute one EzyTrack sync run: fetch, transform, load, and record the result.

    Marks the run SUCCESS only if every chunk in the lookback window was
    fetched successfully. Any failure marks the run FAILED with the failing
    chunk's window in error_message and re-raises.

    `target` selects the destination: "legacy" (default, unchanged
    behavior) writes raw.ezytrack_*/staging.ezytrack_* in
    telemetry_warehouse, with full etl.sync_runs/etl.sync_table_loads
    bookkeeping, catch-up-cursor lookup, and post-load validation.
    "platform" writes raw_ezytrack.*/stg_ezytrack.* in ge_warehouse via
    PostgresLoader.from_platform_settings() -- same fetch/transform/retry/
    pagination/dedup behavior, with the run and each table load recorded in
    ops.pipeline_run/ops.table_load instead. The catch-up-cursor lookup
    still always returns None for platform (deliberate -- see the
    PLATFORM TARGET AND CATCH-UP SAFETY note above), and post-load
    validation is still skipped for platform (its checks are hardcoded to
    legacy schema names), matching the Trackunit/Sendem platform-target
    precedent -- see docs/sources/ezytrack.md#ge_warehouse-platform-target.

    `lookback_hours`, if given, overrides `EzytrackSettings.lookback_hours`
    for this run only -- lets an operator supply an explicit small window
    (e.g. 1 hour) for a manual platform-target test without touching the
    configured default.
    """
    configure_logging()
    if target not in ("legacy", "platform"):
        raise ValueError(f"target must be 'legacy' or 'platform', got {target!r}")

    print("Loading settings...")
    postgres_settings = get_settings()
    ezytrack_settings = get_ezytrack_settings()
    ops_settings = get_etl_ops_settings()
    effective_lookback_hours = (
        ezytrack_settings.lookback_hours if lookback_hours is None else lookback_hours
    )
    if effective_lookback_hours < 1:
        raise ValueError(f"lookback_hours must be at least 1, got {effective_lookback_hours}")

    if target == "platform":
        loader = PostgresLoader.from_platform_settings(get_platform_settings())
    else:
        loader = PostgresLoader(postgres_settings)
    client = EzytrackClient(ezytrack_settings)

    # get_last_successful_run() returns None unconditionally for a
    # "platform" tracking_backend loader (see
    # PostgresLoader.get_last_successful_run) -- ops.pipeline_run is audit
    # history, not a catch-up cursor, so a platform-target run always falls
    # through to first-run/explicit-window behavior below, never a catch-up
    # window computed from either telemetry_warehouse's legacy cursor or
    # ops.pipeline_run's own success history.
    last_success = None if reconcile else loader.get_last_successful_run(SOURCE_SYSTEM)
    success_cursor = _last_success_window_end(last_success)
    # Choose the window end after the lookup so it remains as close as
    # possible to the sync row's database-generated started_at timestamp.
    end_time = _as_utc(now_utc) if now_utc is not None else utc_now()
    start_time, end_time = calculate_sync_window(
        end_time=end_time,
        default_lookback_hours=effective_lookback_hours,
        max_catchup_hours=ezytrack_settings.max_catchup_hours,
        overlap_minutes=ezytrack_settings.catchup_overlap_minutes,
        last_success_window_end=success_cursor,
        reconciliation_lookback_hours=(
            ezytrack_settings.reconciliation_lookback_hours if reconcile else None
        ),
    )
    chunk_windows = _build_chunk_windows(start_time, end_time, ezytrack_settings.chunk_hours)
    window_mode = "reconciliation" if reconcile else ("catch-up" if success_cursor else "first-run")
    print(f"Target: {target}")
    print(
        f"Sync window ({window_mode}, UTC): {format_utc_iso(start_time)} to "
        f"{format_utc_iso(end_time)} "
        f"({len(chunk_windows)} chunk(s) of {ezytrack_settings.chunk_hours}h, page_size={ezytrack_settings.page_size})"
    )

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=RECONCILIATION_JOB_NAME if reconcile else JOB_NAME,
        start_date=to_date_key(start_time),
        end_date=to_date_key(end_time),
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        print("Authenticating with EzyTrack / Telematics Guru...")
        client.authenticate()

        print("Fetching EzyTrack assets...")
        assets = client.fetch_assets()

        all_trips: list[dict[str, Any]] = []
        for chunk_start, chunk_end in chunk_windows:
            chunk_trips = _fetch_trips_for_chunk(client, chunk_start, chunk_end, ezytrack_settings)
            all_trips.extend(chunk_trips)

        deduped_trips = _dedupe_trips_by_id(all_trips)
        print(
            f"Fetched {len(all_trips)} trip(s) across {len(chunk_windows)} chunk(s), "
            f"{len(deduped_trips)} after de-duplication by tripId"
        )

        print("Transforming data...")
        dataframes = build_all(assets, deduped_trips)

        print("Loading data into PostgreSQL...")
        load_counts = loader.load_ezytrack_tables(
            dataframes,
            sync_run_id=sync_run_id,
            provider=SOURCE_SYSTEM,
            target=target,
        )
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

        if target == "legacy":
            loader.run_post_load_validation(
                SOURCE_SYSTEM,
                mode=ops_settings.validation_mode,
                lookback_hours=ops_settings.validation_lookback_hours,
            )
        else:
            print("Skipping post-load validation for platform target (checks are hardcoded to legacy schema names)")

        rows_fetched = len(assets) + len(deduped_trips)
        rows_loaded = sum(load_counts.values())

        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="SUCCESS",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
        )
        print(f"Sync run {sync_run_id} completed: SUCCESS (fetched={rows_fetched}, loaded={rows_loaded})")

    except Exception as error:
        print(f"Sync run {sync_run_id} failed: {error}")
        logger.exception("EzyTrack sync run %s failed", sync_run_id)
        finish_sync_run_failed_safe(loader, sync_run_id, error)
        raise


def main() -> None:
    """Parse CLI arguments and run the selected EzyTrack mode."""
    parser = argparse.ArgumentParser(description="EzyTrack trip sync with gap recovery")
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Use EZYTRACK_RECONCILIATION_LOOKBACK_HOURS instead of the success cursor",
    )
    parser.add_argument(
        "--target",
        choices=["legacy", "platform"],
        default="legacy",
        help="legacy (default): telemetry_warehouse raw/staging. platform: ge_warehouse raw_ezytrack/stg_ezytrack.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Override TELEMATICS_LOOKBACK_HOURS for one run (e.g. a small bounded --target platform test)",
    )
    args = parser.parse_args()
    run(reconcile=args.reconcile, target=args.target, lookback_hours=args.lookback_hours)


if __name__ == "__main__":
    main()
