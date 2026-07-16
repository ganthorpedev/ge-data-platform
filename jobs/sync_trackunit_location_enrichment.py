"""Trackunit location enrichment V1 -- entry point.

Run with:
    python -m jobs.sync_trackunit_location_enrichment --date 2026-07-05 --machines 5986
    python -m jobs.sync_trackunit_location_enrichment --date 2026-07-05 --limit 20
    python -m jobs.sync_trackunit_location_enrichment --date 2026-07-05

Reads already-loaded staging.trackunit_daily_activity rows for one
report_date (this job does not fetch metrics -- run
jobs/sync_trackunit_daily_activity.py first for the same date), then for
each asset with start/stop boundaries:
  1. Fetches AEMP historical Locations for a 48h lookback window ending at
     each boundary, picks the latest point <= the boundary.
  2. Fetches Site History for the asset across the same overall window,
     resolves which site (zone) was active at each boundary, and resolves
     that site's name (Site History returns an id only).
  3. Writes one row to staging.trackunit_location_enrichment per
     (report_date, asset_id) -- UPSERT, so rerunning the same date/machines
     updates the same rows rather than duplicating them.

This is completely separate from jobs/sync_trackunit_daily_activity.py: it
does not call it, does not touch staging.trackunit_daily_activity, and a
failure here cannot affect the metric ETL's own sync_runs history (this job
records under source_system="trackunit_location", not "trackunit").

Address/zip/city/country are never populated -- see
transforms/trackunit_location_transform.py's module docstring. Do not add
reverse geocoding here.

API safety: one AEMP/Site call at a time, 1-second pause between calls, no
retry loops. A site name is only resolved once per run per distinct site id
(cached), not once per asset. Any failure stops the whole run immediately
and marks etl.sync_runs FAILED.
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from config.settings import get_settings, get_trackunit_settings
from connectors.trackunit_client import TrackunitClient
from loaders.postgres_loader import PostgresLoader
from transforms.trackunit_location_transform import (
    ENRICHMENT_COLUMNS,
    RAW_LOCATION_COLUMNS,
    RAW_SITE_COLUMNS,
    RAW_SITE_HISTORY_COLUMNS,
    build_enrichment_row,
    build_raw_location_rows,
    build_raw_site_history_rows,
    build_raw_site_row,
    extract_location_points,
    find_active_site_id,
    latest_point_at_or_before,
)

SOURCE_SYSTEM = "trackunit_location"
JOB_NAME = "trackunit_location_enrichment_sync"

API_CALL_PAUSE_SECONDS = 1
LOOKBACK_HOURS = 48


def _to_date_key(value: date) -> int:
    """Convert a date to its YYYYMMDD integer representation (etl.sync_runs convention)."""
    return int(value.strftime("%Y%m%d"))


def _default_report_date(timezone_name: str) -> date:
    """Return yesterday's date in `timezone_name` (matches the metric job's own default)."""
    now_local = datetime.now(ZoneInfo(timezone_name))
    return (now_local - timedelta(days=1)).date()


def _fmt_utc(dt) -> str:
    """Format a (possibly non-UTC tz-aware) datetime as an AEMP-style UTC ISO-8601 string."""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_activity_rows(loader: PostgresLoader, report_date: date, machines: list[str] | None, limit: int | None) -> list[dict[str, Any]]:
    """Read the already-loaded daily-activity rows this job will enrich."""
    from sqlalchemy import text

    query = "SELECT asset_id, machine, pin, start_time_utc, stop_time_utc FROM staging.trackunit_daily_activity WHERE report_date = :report_date"
    params: dict[str, Any] = {"report_date": report_date}

    if machines:
        query += " AND machine = ANY(:machines)"
        params["machines"] = machines

    query += " ORDER BY machine"

    if limit is not None:
        query += " LIMIT :limit"
        params["limit"] = limit

    with loader.engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return [dict(row) for row in rows]


def _enrich_one_asset(
    client: TrackunitClient,
    site_cache: dict[str, dict[str, Any]],
    report_date: date,
    timezone_name: str,
    row: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Enrich one activity row. Returns (enrichment_row, raw_location_rows, raw_site_history_rows, raw_site_rows)."""
    asset_id = row["asset_id"]
    pin = row["pin"]
    start_utc = row["start_time_utc"]
    stop_utc = row["stop_time_utc"]

    had_boundaries = start_utc is not None and stop_utc is not None

    if not had_boundaries:
        enrichment_row = build_enrichment_row(
            report_date=report_date,
            asset_id=asset_id,
            timezone_name=timezone_name,
            had_boundaries=False,
            start_point=None,
            stop_point=None,
            start_zone_name=None,
            stop_zone_name=None,
        )
        return enrichment_row, [], [], []

    start_window_start = _fmt_utc(start_utc - timedelta(hours=LOOKBACK_HOURS))
    start_window_end = _fmt_utc(start_utc)
    stop_window_start = _fmt_utc(stop_utc - timedelta(hours=LOOKBACK_HOURS))
    stop_window_end = _fmt_utc(stop_utc)

    print(f"  [{row['machine']}] fetching AEMP Locations (start boundary window)...")
    start_aemp = client.get_aemp_series(pin, "Locations", "location", start_window_start, start_window_end, page=1)
    start_points = extract_location_points(start_aemp)
    time.sleep(API_CALL_PAUSE_SECONDS)

    print(f"  [{row['machine']}] fetching AEMP Locations (stop boundary window)...")
    stop_aemp = client.get_aemp_series(pin, "Locations", "location", stop_window_start, stop_window_end, page=1)
    stop_points = extract_location_points(stop_aemp)
    time.sleep(API_CALL_PAUSE_SECONDS)

    start_point = latest_point_at_or_before(start_points, start_utc)
    stop_point = latest_point_at_or_before(stop_points, stop_utc)

    print(f"  [{row['machine']}] fetching Site History...")
    site_history_response = client.get_site_history(asset_id, start_window_start, stop_window_end)
    site_history_intervals = site_history_response.get("content", [])
    time.sleep(API_CALL_PAUSE_SECONDS)

    start_site_id = find_active_site_id(site_history_intervals, start_utc)
    stop_site_id = find_active_site_id(site_history_intervals, stop_utc)

    raw_site_rows = []
    for site_id in {sid for sid in (start_site_id, stop_site_id) if sid is not None}:
        if site_id not in site_cache:
            print(f"  [{row['machine']}] resolving site name for {site_id}...")
            site_detail = client.get_site(site_id)
            site_cache[site_id] = site_detail
            raw_site_rows.append(build_raw_site_row(site_id, site_detail))
            time.sleep(API_CALL_PAUSE_SECONDS)

    start_zone_name = site_cache[start_site_id].get("name") if start_site_id else None
    stop_zone_name = site_cache[stop_site_id].get("name") if stop_site_id else None

    enrichment_row = build_enrichment_row(
        report_date=report_date,
        asset_id=asset_id,
        timezone_name=timezone_name,
        had_boundaries=True,
        start_point=start_point,
        stop_point=stop_point,
        start_zone_name=start_zone_name,
        stop_zone_name=stop_zone_name,
    )

    raw_location_rows = build_raw_location_rows(asset_id, report_date, "start", start_points) + build_raw_location_rows(
        asset_id, report_date, "stop", stop_points
    )
    raw_site_history_rows = build_raw_site_history_rows(asset_id, site_history_intervals)

    print(
        f"  [{row['machine']}] status={enrichment_row['location_enrichment_status']} "
        f"zone_start={start_zone_name} zone_stop={stop_zone_name}"
    )

    return enrichment_row, raw_location_rows, raw_site_history_rows, raw_site_rows


def run(report_date: date, machines: list[str] | None = None, limit: int | None = None) -> None:
    """Execute one Trackunit location-enrichment run for `report_date`."""
    print("Loading settings...")
    postgres_settings = get_settings()
    trackunit_settings = get_trackunit_settings()

    loader = PostgresLoader(postgres_settings)
    client = TrackunitClient(trackunit_settings)

    print(f"Report date: {report_date} ({trackunit_settings.timezone})")

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=_to_date_key(report_date),
        end_date=_to_date_key(report_date),
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        activity_rows = _fetch_activity_rows(loader, report_date, machines, limit)
        print(f"Found {len(activity_rows)} staging.trackunit_daily_activity row(s) to enrich.")
        if not activity_rows:
            print("Nothing to enrich for this report_date/machine filter -- has jobs.sync_trackunit_daily_activity run for this date?")

        print("Authenticating with Trackunit...")
        client.authenticate()

        site_cache: dict[str, dict[str, Any]] = {}
        enrichment_rows = []
        all_location_rows = []
        all_site_history_rows = []
        all_site_rows = []

        for row in activity_rows:
            enrichment_row, location_rows, site_history_rows, site_rows = _enrich_one_asset(
                client, site_cache, report_date, trackunit_settings.timezone, row
            )
            enrichment_rows.append(enrichment_row)
            all_location_rows.extend(location_rows)
            all_site_history_rows.extend(site_history_rows)
            all_site_rows.extend(site_rows)

        # The start/stop 48h lookback windows commonly overlap (they're both
        # anchored within the same report day), so the same asset_id +
        # location_timestamp_utc point can be fetched for both boundaries.
        # De-duplicate before loading -- the primary key is (asset_id,
        # location_timestamp_utc), not (..., boundary_type), and the values
        # are identical for the same point regardless of which window it
        # came from.
        raw_locations_df = pd.DataFrame(all_location_rows, columns=RAW_LOCATION_COLUMNS)
        if not raw_locations_df.empty:
            raw_locations_df = raw_locations_df.drop_duplicates(
                subset=["asset_id", "location_timestamp_utc"], keep="last"
            )

        dataframes = {
            "raw_locations_df": raw_locations_df,
            "raw_site_history_df": pd.DataFrame(all_site_history_rows, columns=RAW_SITE_HISTORY_COLUMNS),
            "raw_sites_df": pd.DataFrame(all_site_rows, columns=RAW_SITE_COLUMNS),
            "enrichment_df": pd.DataFrame(enrichment_rows, columns=ENRICHMENT_COLUMNS),
        }

        print("Loading data into PostgreSQL...")
        load_counts = loader.load_trackunit_location_enrichment(dataframes, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM)
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

        rows_fetched = len(activity_rows)
        rows_loaded = sum(load_counts.values())

        loader.finish_sync_run(sync_run_id=sync_run_id, status="SUCCESS", rows_fetched=rows_fetched, rows_loaded=rows_loaded)
        print(f"Sync run {sync_run_id} completed: SUCCESS (fetched={rows_fetched}, loaded={rows_loaded})")

    except Exception as error:
        print(f"Sync run {sync_run_id} failed: {error}")
        loader.finish_sync_run(sync_run_id=sync_run_id, status="FAILED", error_message=str(error))
        raise


def main() -> None:
    """Parse CLI args and run the enrichment job."""
    parser = argparse.ArgumentParser(description="Trackunit location enrichment V1 (manual-date)")
    parser.add_argument("--date", type=str, default=None, help="Report date YYYY-MM-DD (local day). Defaults to yesterday.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on the number of activity rows processed.")
    parser.add_argument(
        "--machines",
        type=str,
        default=None,
        help="Optional comma-separated list of machine names to restrict to, e.g. 2277,3846,5986",
    )
    args = parser.parse_args()

    trackunit_settings = get_trackunit_settings()

    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        report_date = _default_report_date(trackunit_settings.timezone)

    machines = [name.strip() for name in args.machines.split(",")] if args.machines else None

    run(report_date=report_date, machines=machines, limit=args.limit)


if __name__ == "__main__":
    main()
