"""Re-runnable historical backfill of the Sendem driver-grain trip feed.

Fetches GET /api/aggregates/trips/assetspeople/{groupId} (engine time per
asset, driver, site and day) for an explicit date range and loads it into
raw.sendem_trips_assetspeople_daily / staging.sendem_fact_trips_assetspeople_daily
(legacy target) or raw_sendem.trip_driver_daily / stg_sendem.trip_driver_daily
(platform target), with the same client, transform, enrichment and UPSERT
the scheduled sync uses. Nothing here is a second implementation: the only
difference from `python -m ge_data_platform.sources.sendem.sync
--lookback-days N` is that this fetches ONLY the driver-grain feed (plus the
asset/site dimensions it is enriched with), walks a long range in bounded
windows, and lets the destination database be named.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02
    python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02 --database telemetry_warehouse_dev
    python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02 --target platform
    python -m scripts.backfill_sendem_trips_assetspeople ... --dry-run   # fetch and report, load nothing

Safety and idempotency:
  * Takes the same cross-process Sendem overlap lock as the scheduled sync
    (ge_data_platform.common.overlap.SENDEM_OVERLAP_GROUP), so it never
    runs beside a scheduled Sendem run.
  * Refuses a non-local POSTGRES_HOST unless --allow-remote-host is given
    (the scheduled sync has no such guard, because it is the production
    job; a manual historical backfill should be deliberate).
  * Every window is UPSERTed on the tables' natural key
    (date_key, group_id, site_id, asset_id, driver_id). Re-running the same
    range, or overlapping ranges, rewrites the same rows and can never
    duplicate a driver row -- so the asset-day roll-up in
    reporting.vw_sendem_asset_daily_activity can never double count.
  * Each run is recorded in etl.sync_runs / ops.pipeline_run as
    job_name = sendem_assetspeople_backfill with the full range, and every
    table load in etl.sync_table_loads / ops.table_load, exactly like the
    scheduled job.
  * Dimensions (assets, sites) are fetched once and UPSERTed too, so a
    driver row's enrichment never dangles on an asset the dims did not
    know yet.

The window size (--window-days, default 14) bounds a single API response;
Sendem returns ~100-150 driver rows per day for this group, so 14 days is
a modest payload. Windows are inclusive on both ends and never overlap.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from ge_data_platform.common.database import (
    SENDEM_TRIPS_ASSETS_PEOPLE_KEY,
    PostgresLoader,
    finish_sync_run_failed_safe,
)
from ge_data_platform.common.dates import to_date_key
from ge_data_platform.common.logging import configure_logging
from ge_data_platform.common.overlap import SENDEM_OVERLAP_GROUP, provider_overlap_guard
from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import get_platform_settings, get_settings
from ge_data_platform.sources.sendem.client import SendemClient
from ge_data_platform.sources.sendem.transform import build_all

logger = logging.getLogger(__name__)

SOURCE_SYSTEM = "sendem"
JOB_NAME = "sendem_assetspeople_backfill"
CONTEXT = "Sendem driver-grain trips backfill"
DEFAULT_WINDOW_DAYS = 14

_TABLES = {
    "legacy": {
        "raw": ("raw", "sendem_trips_assetspeople_daily"),
        "staging": ("staging", "sendem_fact_trips_assetspeople_daily"),
        "dim_assets": ("staging", "sendem_dim_assets"),
        "dim_sites": ("staging", "sendem_dim_sites"),
        "raw_assets": ("raw", "sendem_assets"),
        "raw_sites": ("raw", "sendem_sites"),
    },
    "platform": {
        "raw": ("raw_sendem", "trip_driver_daily"),
        "staging": ("stg_sendem", "trip_driver_daily"),
        "dim_assets": ("stg_sendem", "asset"),
        "dim_sites": ("stg_sendem", "site"),
        "raw_assets": ("raw_sendem", "asset"),
        "raw_sites": ("raw_sendem", "site"),
    },
}


def windows(date_from: date, date_to: date, window_days: int) -> list[tuple[date, date]]:
    """Split an inclusive range into inclusive, non-overlapping windows."""
    if date_to < date_from:
        raise ValueError(f"date_to {date_to} is before date_from {date_from}")
    if window_days < 1:
        raise ValueError(f"window_days must be at least 1, got {window_days}")
    result: list[tuple[date, date]] = []
    start = date_from
    while start <= date_to:
        end = min(start + timedelta(days=window_days - 1), date_to)
        result.append((start, end))
        start = end + timedelta(days=1)
    return result


def run(
    *,
    date_from: date,
    date_to: date,
    target: str = "legacy",
    database: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    dry_run: bool = False,
    allow_remote_host: bool = False,
) -> dict[str, int]:
    """Backfill the driver-grain feed for [date_from, date_to]. Returns rows loaded per table."""
    configure_logging()
    if target not in _TABLES:
        raise ValueError(f"target must be 'legacy' or 'platform', got {target!r}")
    plan_windows = windows(date_from, date_to, window_days)

    settings = get_settings()
    if not allow_remote_host:
        assert_local_host(settings.postgres_host, context=CONTEXT)

    if target == "platform":
        loader = PostgresLoader.from_platform_settings(get_platform_settings())
        if database is not None:
            raise ValueError("--database applies to the legacy target only; the platform database is GE_WAREHOUSE_DB")
    else:
        loader = PostgresLoader(settings, database=database)
    tables = _TABLES[target]
    destination = database or (settings.postgres_db if target == "legacy" else get_platform_settings().ge_warehouse_db)

    client = SendemClient(settings)
    print(f"Range: {date_from} to {date_to} in {len(plan_windows)} window(s) of up to {window_days} day(s)")
    print(f"Target: {target} ({settings.postgres_host}:{settings.postgres_port}/{destination})")
    if dry_run:
        print("DRY RUN: fetching only, nothing will be loaded")

    totals: dict[str, int] = {}
    with provider_overlap_guard(SENDEM_OVERLAP_GROUP, accept_inherited=True):
        sync_run_id = None
        if not dry_run:
            sync_run_id = loader.start_sync_run(
                source_system=SOURCE_SYSTEM,
                job_name=JOB_NAME,
                start_date=to_date_key(date_from),
                end_date=to_date_key(date_to),
            )
            print(f"sync_run_id: {sync_run_id}")
        try:
            print("Fetching Sendem dimensions...")
            assets = client.get_assets()
            sites = client.get_sites()
            rows_fetched = 0
            for start, end in plan_windows:
                start_key, end_key = to_date_key(start), to_date_key(end)
                payload = client.get_trips_assets_people_daily(start_key, end_key)
                rows_fetched += len(payload)
                frames = build_all({
                    "assets": assets,
                    "sites": sites,
                    "trips_assets_people": payload,
                })
                asset_days = (
                    frames["trips_people_df"][["DateKey", "AssetId"]].drop_duplicates().shape[0]
                    if not frames["trips_people_df"].empty else 0
                )
                print(f"  {start}..{end}: {len(payload)} driver rows over {asset_days} asset-days")
                if dry_run:
                    continue
                load_plan = [
                    (*tables["raw_assets"], frames["assets_df"], ["asset_id"]),
                    (*tables["raw_sites"], frames["sites_df"], ["site_id"]),
                    (*tables["dim_assets"], frames["assets_df"], ["asset_id"]),
                    (*tables["dim_sites"], frames["sites_df"], ["site_id"]),
                    (*tables["raw"], frames["trips_people_df"], SENDEM_TRIPS_ASSETS_PEOPLE_KEY),
                    (*tables["staging"], frames["fact_trips_assets_people"], SENDEM_TRIPS_ASSETS_PEOPLE_KEY),
                ]
                counts = loader._run_load_plan(load_plan, sync_run_id, SOURCE_SYSTEM)
                for name, count in counts.items():
                    totals[name] = totals.get(name, 0) + count
            if sync_run_id is not None:
                loader.finish_sync_run(
                    sync_run_id=sync_run_id,
                    status="SUCCESS",
                    rows_fetched=rows_fetched,
                    rows_loaded=sum(totals.values()),
                )
                print(f"Backfill run {sync_run_id} completed: SUCCESS (fetched={rows_fetched}, loaded={sum(totals.values())})")
            else:
                print(f"Dry run complete: {rows_fetched} driver rows would be loaded")
        except Exception as error:
            if sync_run_id is not None:
                logger.exception("Backfill run %s failed", sync_run_id)
                finish_sync_run_failed_safe(loader, sync_run_id, error)
            raise
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date-from", type=date.fromisoformat, required=True, help="First day, inclusive (YYYY-MM-DD)")
    parser.add_argument("--date-to", type=date.fromisoformat, required=True, help="Last day, inclusive (YYYY-MM-DD)")
    parser.add_argument("--target", choices=["legacy", "platform"], default="legacy")
    parser.add_argument(
        "--database", default=None,
        help="Legacy target only: load into this database instead of POSTGRES_DB (e.g. a dev restore)",
    )
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report; load nothing, record no run")
    parser.add_argument(
        "--allow-remote-host", action="store_true",
        help="Permit a non-local POSTGRES_HOST (the production warehouse); off by default",
    )
    args = parser.parse_args()
    run(
        date_from=args.date_from,
        date_to=args.date_to,
        target=args.target,
        database=args.database,
        window_days=args.window_days,
        dry_run=args.dry_run,
        allow_remote_host=args.allow_remote_host,
    )


if __name__ == "__main__":
    main()
