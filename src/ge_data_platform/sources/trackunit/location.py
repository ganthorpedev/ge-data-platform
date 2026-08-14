"""Trackunit location enrichment V1 -- entry point.

Run with:
    python -m ge_data_platform.sources.trackunit.location --date 2026-07-05 --machines 5986
    python -m ge_data_platform.sources.trackunit.location --date 2026-07-05 --limit 20
    python -m ge_data_platform.sources.trackunit.location --date 2026-07-05

Reads already-loaded staging.trackunit_daily_activity rows for one
report_date (this job does not fetch metrics -- run
ge_data_platform.sources.trackunit.daily_activity first for the same date),
then for each asset with start/stop boundaries:
  1. Fetches AEMP historical Locations for a 48h lookback window ending at
     each boundary, picks the latest point <= the boundary.
  2. Fetches Site History for the asset across the same overall window,
     resolves which site (zone) was active at each boundary, and resolves
     that site's name (Site History returns an id only).
  3. Writes one row to staging.trackunit_location_enrichment per
     (report_date, asset_id) -- UPSERT, so rerunning the same date/machines
     updates the same rows rather than duplicating them.

This is completely separate from ge_data_platform.sources.trackunit.
daily_activity: it does not call it, does not touch
staging.trackunit_daily_activity, and a failure here cannot affect the
metric ETL's own sync_runs history (this job records under
source_system="trackunit_location", not "trackunit").

Address/zip/city/country are never populated -- see
ge_data_platform.sources.trackunit.location_transform's module docstring.
Do not add reverse geocoding here.

API safety: one AEMP/Site call at a time, with configurable AEMP pacing and
bounded retries handled by ge_data_platform.sources.trackunit.client. A site
name is only resolved once per run per distinct site id (cached), not once
per asset. A site-detail 403 (this account cannot see that specific site) is
non-fatal: it is logged, the denied site_id is cached so it is never
requested again this run, and the affected asset's row is marked
PARTIAL/SITE_ACCESS_DENIED instead of aborting the sync. All other failures
(auth-wide 401, database errors, invalid responses, exhausted 429/5xx
retries) still stop the whole run and mark the run row FAILED (etl.sync_runs
for legacy target, ops.pipeline_run for platform target).
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from ge_data_platform.common.database import PostgresLoader, finish_sync_run_failed_safe
from ge_data_platform.common.dates import format_utc_iso, local_today, to_date_key
from ge_data_platform.common.logging import configure_logging
from ge_data_platform.common.overlap import TRACKUNIT_OVERLAP_GROUP, provider_job_lock
from ge_data_platform.config.settings import get_platform_settings, get_settings, get_trackunit_settings
from ge_data_platform.sources.trackunit.client import TrackunitClient, TrackunitSiteAccessDeniedError
from ge_data_platform.sources.trackunit.location_transform import (
    ENRICHMENT_COLUMNS,
    RAW_LOCATION_COLUMNS,
    RAW_SITE_COLUMNS,
    RAW_SITE_HISTORY_COLUMNS,
    STATUS_ENRICHED,
    STATUS_NOT_FOUND,
    STATUS_PARTIAL,
    STATUS_SITE_ACCESS_DENIED,
    build_enrichment_row,
    build_raw_location_rows,
    build_raw_site_history_rows,
    build_raw_site_row,
    extract_location_points,
    find_active_site_id,
    latest_point_at_or_before,
)

# Sentinel stored in site_cache for a site_id that returned 403 -- lets us
# skip re-requesting it for the rest of the run without treating it the same
# as an unresolved/never-looked-up site.
_SITE_ACCESS_DENIED = object()

SOURCE_SYSTEM = "trackunit_location"
JOB_NAME = "trackunit_location_enrichment_sync"

LOOKBACK_HOURS = 48

logger = logging.getLogger(__name__)


def _default_report_date(timezone_name: str) -> date:
    """Return yesterday's date in `timezone_name` (matches the metric job's own default)."""
    return local_today(timezone_name) - timedelta(days=1)


def _fmt_utc(dt) -> str:
    """Format a (possibly non-UTC tz-aware) datetime as an AEMP-style UTC ISO-8601 string."""
    return format_utc_iso(dt)


def _fetch_activity_rows(
    loader: PostgresLoader, report_date: date, machines: list[str] | None, limit: int | None, target: str = "legacy"
) -> list[dict[str, Any]]:
    """Read the already-loaded daily-activity rows this job will enrich."""
    from sqlalchemy import text

    daily_activity_table = "stg_trackunit.daily_activity" if target == "platform" else "staging.trackunit_daily_activity"
    query = f"SELECT asset_id, machine, pin, start_time_utc, stop_time_utc FROM {daily_activity_table} WHERE report_date = :report_date"
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

    print(f"  [{row['machine']}] fetching AEMP Locations (stop boundary window)...")
    stop_aemp = client.get_aemp_series(pin, "Locations", "location", stop_window_start, stop_window_end, page=1)
    stop_points = extract_location_points(stop_aemp)

    start_point = latest_point_at_or_before(start_points, start_utc)
    stop_point = latest_point_at_or_before(stop_points, stop_utc)

    print(f"  [{row['machine']}] fetching Site History...")
    site_history_response = client.get_site_history(asset_id, start_window_start, stop_window_end)
    site_history_intervals = site_history_response.get("content", [])

    start_site_id = find_active_site_id(site_history_intervals, start_utc)
    stop_site_id = find_active_site_id(site_history_intervals, stop_utc)

    raw_site_rows = []
    for site_id in {sid for sid in (start_site_id, stop_site_id) if sid is not None}:
        if site_id in site_cache:
            continue
        print(f"  [{row['machine']}] resolving site name for {site_id}...")
        try:
            site_detail = client.get_site(site_id)
        except TrackunitSiteAccessDeniedError:
            print(
                f"  WARNING [{row['machine']}] site access denied (403) for site_id={site_id}; "
                f"continuing with any name already known from Site History."
            )
            site_cache[site_id] = _SITE_ACCESS_DENIED
            continue
        site_cache[site_id] = site_detail
        raw_site_rows.append(build_raw_site_row(site_id, site_detail))

    def _zone_name(site_id: str | None) -> str | None:
        if site_id is None:
            return None
        cached = site_cache.get(site_id)
        if cached is None or cached is _SITE_ACCESS_DENIED:
            return None
        return cached.get("name")

    start_zone_name = _zone_name(start_site_id)
    stop_zone_name = _zone_name(stop_site_id)

    site_access_denied = any(
        site_id is not None and site_cache.get(site_id) is _SITE_ACCESS_DENIED
        for site_id in (start_site_id, stop_site_id)
    )

    enrichment_row = build_enrichment_row(
        report_date=report_date,
        asset_id=asset_id,
        timezone_name=timezone_name,
        had_boundaries=True,
        start_point=start_point,
        stop_point=stop_point,
        start_zone_name=start_zone_name,
        stop_zone_name=stop_zone_name,
        site_access_denied=site_access_denied,
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


@provider_job_lock(TRACKUNIT_OVERLAP_GROUP)
def run(
    report_date: date, machines: list[str] | None = None, limit: int | None = None, target: str = "legacy"
) -> None:
    """Execute one Trackunit location-enrichment run for `report_date`.

    `target="legacy"` (default) reads/writes telemetry_warehouse raw/staging
    schemas -- unchanged. `target="platform"` reads/writes ge_warehouse
    raw_trackunit/stg_trackunit schemas instead -- see
    docs/migration/legacy-to-platform-migration.md. Run bookkeeping is
    recorded in ops.pipeline_run/ops.table_load for platform target (see
    PostgresLoader.from_platform_settings and ge_data_platform.common.audit).
    """
    if target not in ("legacy", "platform"):
        raise ValueError(f"target must be 'legacy' or 'platform', got {target!r}")

    configure_logging()
    print("Loading settings...")
    trackunit_settings = get_trackunit_settings()

    if target == "platform":
        platform_settings = get_platform_settings()
        host = getattr(platform_settings, "postgres_host", "?")
        port = getattr(platform_settings, "postgres_port", "?")
        db = getattr(platform_settings, "ge_warehouse_db", "?")
        print(f"Target: platform (ge_warehouse) -- {host}:{port}/{db} -- schemas raw_trackunit / stg_trackunit")
        loader = PostgresLoader.from_platform_settings(platform_settings)
    else:
        postgres_settings = get_settings()
        host = getattr(postgres_settings, "postgres_host", "?")
        port = getattr(postgres_settings, "postgres_port", "?")
        db = getattr(postgres_settings, "postgres_db", "?")
        print(f"Target: legacy (telemetry_warehouse) -- {host}:{port}/{db} -- schemas raw / staging")
        loader = PostgresLoader(postgres_settings)

    client = TrackunitClient(trackunit_settings)

    print(f"Report date: {report_date} ({trackunit_settings.timezone})")

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=to_date_key(report_date),
        end_date=to_date_key(report_date),
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        activity_rows = _fetch_activity_rows(loader, report_date, machines, limit, target)
        print(f"Found {len(activity_rows)} staging.trackunit_daily_activity row(s) to enrich.")
        if not activity_rows:
            print(
                "Nothing to enrich for this report_date/machine filter -- has "
                "ge_data_platform.sources.trackunit.daily_activity run for this date?"
            )

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

        status_counts = {row["location_enrichment_status"]: 0 for row in enrichment_rows}
        for enrichment_row in enrichment_rows:
            status_counts[enrichment_row["location_enrichment_status"]] += 1
        summary_counts = {
            "enriched": status_counts.get(STATUS_ENRICHED, 0),
            "partial": status_counts.get(STATUS_PARTIAL, 0),
            "site_access_denied": status_counts.get(STATUS_SITE_ACCESS_DENIED, 0),
            "failed": status_counts.get(STATUS_NOT_FOUND, 0),
        }
        print(
            "Enrichment summary: "
            f"enriched={summary_counts['enriched']} "
            f"partial={summary_counts['partial']} "
            f"site_access_denied={summary_counts['site_access_denied']} "
            f"failed={summary_counts['failed']}"
        )

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
        load_counts = loader.load_trackunit_location_enrichment(
            dataframes, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM, target=target
        )
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

        rows_fetched = len(activity_rows)
        rows_loaded = sum(load_counts.values())

        loader.finish_sync_run(sync_run_id=sync_run_id, status="SUCCESS", rows_fetched=rows_fetched, rows_loaded=rows_loaded)
        print(f"Sync run {sync_run_id} completed: SUCCESS (fetched={rows_fetched}, loaded={rows_loaded})")

    except Exception as error:
        logger.exception("Trackunit location sync run %s failed: %s", sync_run_id, error)
        finish_sync_run_failed_safe(loader, sync_run_id, error)
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
    parser.add_argument(
        "--target",
        type=str,
        choices=["legacy", "platform"],
        default="legacy",
        help=(
            "Which database/schemas to read/write: 'legacy' (default) reads/writes telemetry_warehouse "
            "raw.trackunit_*/staging.trackunit_*, unchanged from current behavior. 'platform' reads/writes "
            "ge_warehouse raw_trackunit.*/stg_trackunit.* -- see docs/migration/legacy-to-platform-migration.md."
        ),
    )
    args = parser.parse_args()

    trackunit_settings = get_trackunit_settings()

    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        report_date = _default_report_date(trackunit_settings.timezone)

    machines = [name.strip() for name in args.machines.split(",")] if args.machines else None

    run(report_date=report_date, machines=machines, limit=args.limit, target=args.target)


if __name__ == "__main__":
    main()
