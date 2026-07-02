"""Entry point for the EzyTrack / Telematics Guru sync job.

Run with:
    python -m jobs.sync_ezytrack

This orchestrates: fetch (connectors.ezytrack_client) -> transform
(transforms.ezytrack_transform) -> load (loaders.postgres_loader), and
records the run in etl.sync_runs / etl.sync_table_loads.

TEMPORARY CONSERVATIVE MODE:
Until EzyTrack/Telematics Guru confirms how their GraphQL cost limit works,
this job fetches trips in small chunks (default: last 6 hours, 1-hour
chunks, page_size 50) instead of one large request. This is strict,
all-or-nothing: if any chunk fails for any reason (rate limit or otherwise),
the whole run is marked FAILED with that chunk's window in error_message,
and the exception is re-raised. There is no partial SUCCESS -- either every
chunk fetches cleanly and the transformed/deduplicated result is loaded, or
nothing is loaded and the run is marked FAILED.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from config.settings import EzytrackSettings, get_ezytrack_settings, get_settings
from connectors.ezytrack_client import fetch_ezytrack_assets, fetch_ezytrack_trips
from loaders.postgres_loader import PostgresLoader
from transforms.ezytrack_transform import build_all

SOURCE_SYSTEM = "ezytrack"
JOB_NAME = "ezytrack_hourly_sync"


def _to_date_key(value: datetime) -> int:
    """Convert a UTC datetime to its YYYYMMDD integer representation.

    etl.sync_runs.start_date/end_date are INTEGER (the same convention
    jobs/sync_sendem.py uses); the EzyTrack API window itself still uses the
    full UTC timestamps below, this is only for that log row.
    """
    return int(value.strftime("%Y%m%d"))


def _build_chunk_windows(start_time: datetime, end_time: datetime, chunk_hours: int) -> list[tuple[datetime, datetime]]:
    """Split [start_time, end_time) into consecutive chunk_hours-sized windows."""
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
    chunk_start: datetime,
    chunk_end: datetime,
    ezytrack_settings: EzytrackSettings,
) -> list[dict[str, Any]]:
    """Fetch trips for one chunk window, or raise with that window attached.

    Any failure (rate limit or otherwise) is re-raised as a RuntimeError
    whose message names this specific chunk's window, so a failure deep in a
    6-chunk run is still traceable to exactly where it happened.
    """
    chunk_start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    chunk_end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Fetching EzyTrack trips chunk: {chunk_start_str} to {chunk_end_str}")

    try:
        return fetch_ezytrack_trips(
            chunk_start_str,
            chunk_end_str,
            page_size=ezytrack_settings.page_size,
            settings=ezytrack_settings,
        )
    except Exception as error:
        raise RuntimeError(
            f"EzyTrack trip chunk failed ({chunk_start_str} to {chunk_end_str}): {error}"
        ) from error


def run() -> None:
    """Execute one EzyTrack sync run: fetch, transform, load, and record the result.

    Marks etl.sync_runs SUCCESS only if every chunk in the lookback window
    was fetched successfully. Any failure marks the run FAILED with the
    failing chunk's window in error_message and re-raises.
    """
    print("Loading settings...")
    postgres_settings = get_settings()
    ezytrack_settings = get_ezytrack_settings()

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=ezytrack_settings.lookback_hours)
    chunk_windows = _build_chunk_windows(start_time, end_time, ezytrack_settings.chunk_hours)
    print(
        f"Sync window (UTC): {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')} to "
        f"{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"({len(chunk_windows)} chunk(s) of {ezytrack_settings.chunk_hours}h, page_size={ezytrack_settings.page_size})"
    )

    loader = PostgresLoader(postgres_settings)

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=_to_date_key(start_time),
        end_date=_to_date_key(end_time),
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        print("Fetching EzyTrack assets...")
        assets = fetch_ezytrack_assets(ezytrack_settings)

        all_trips: list[dict[str, Any]] = []
        for chunk_start, chunk_end in chunk_windows:
            chunk_trips = _fetch_trips_for_chunk(chunk_start, chunk_end, ezytrack_settings)
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
        )
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

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
        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="FAILED",
            error_message=str(error),
        )
        raise


if __name__ == "__main__":
    run()
