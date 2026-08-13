"""Entry point for the Sendem/MiX sync job.

Run with:
    python -m ge_data_platform.sources.sendem.sync
    python -m ge_data_platform.sources.sendem.sync --target platform

This orchestrates: fetch (ge_data_platform.sources.sendem.client) ->
transform (ge_data_platform.sources.sendem.transform) -> load
(ge_data_platform.common.database), and records the run in etl.sync_runs
(legacy target only -- see below).
"""

from __future__ import annotations

import argparse
import logging

from ge_data_platform.common.database import PostgresLoader, finish_sync_run_failed_safe
from ge_data_platform.common.dates import rolling_window
from ge_data_platform.common.logging import configure_logging
from ge_data_platform.config.settings import get_etl_ops_settings, get_platform_settings, get_settings
from ge_data_platform.sources.sendem.client import SendemClient
from ge_data_platform.sources.sendem.transform import build_all

SOURCE_SYSTEM = "sendem"
JOB_NAME = "sendem_hourly_sync"
logger = logging.getLogger(__name__)


def run(*, lookback_days: int | None = None, target: str = "legacy") -> None:
    """Execute one Sendem sync run: fetch, transform, load, and record the result.

    `target` selects the destination: "legacy" (default, unchanged behavior)
    writes raw.sendem_*/staging.sendem_* in telemetry_warehouse via
    PostgresLoader(settings), with full etl.sync_runs/etl.sync_table_loads
    bookkeeping and post-load validation. "platform" writes
    raw_sendem.*/stg_sendem.* in ge_warehouse via
    PostgresLoader.from_platform_settings() -- same fetch/transform/retry/
    empty-payload behavior, but sync tracking and post-load validation are
    both skipped (ops.pipeline_run/ops.table_load are not yet wired, and the
    post-load checks are hardcoded to legacy schema names), matching the
    Trackunit platform-target precedent -- see
    docs/sources/trackunit.md#ge_warehouse-platform-target.
    """
    configure_logging()
    if target not in ("legacy", "platform"):
        raise ValueError(f"target must be 'legacy' or 'platform', got {target!r}")

    print("Loading settings...")
    settings = get_settings()
    ops_settings = get_etl_ops_settings()

    window_days = settings.sync_lookback_days if lookback_days is None else lookback_days
    if window_days < 1:
        raise ValueError(f"lookback_days must be at least 1, got {window_days}")
    start_date, end_date = rolling_window(window_days)
    print(f"Sync window: {start_date} to {end_date}")
    print(f"Target: {target}")

    client = SendemClient(settings)
    if target == "platform":
        loader = PostgresLoader.from_platform_settings(get_platform_settings())
    else:
        loader = PostgresLoader(settings)

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        print("Fetching Sendem data...")
        trips = client.get_trips_assets_daily(start_date, end_date)
        events = client.get_events_assets_daily(start_date, end_date)
        raw = {
            "assets": client.get_assets(),
            "sites": client.get_sites(),
            "event_descriptions": client.get_event_descriptions(),
            "trips": trips,
            "events": events,
        }

        print("Transforming data...")
        dataframes = build_all(raw)

        print("Loading data into PostgreSQL...")
        load_counts = loader.load_sendem_tables(
            dataframes, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM, target=target
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

        rows_fetched = len(trips) + len(events)
        rows_loaded = sum(load_counts.values())

        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="SUCCESS",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
        )
        print(f"Sync run {sync_run_id} completed: SUCCESS (fetched={rows_fetched}, loaded={rows_loaded})")

    except Exception as error:
        logger.exception("Sync run %s failed", sync_run_id)
        finish_sync_run_failed_safe(loader, sync_run_id, error)
        raise


def main() -> None:
    """Parse the optional manual-recovery lookback/target and run the sync."""
    parser = argparse.ArgumentParser(description="Sendem telemetry warehouse sync")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override SYNC_LOOKBACK_DAYS for a safe overlapping recovery run",
    )
    parser.add_argument(
        "--target",
        choices=["legacy", "platform"],
        default="legacy",
        help="legacy (default): telemetry_warehouse raw/staging. platform: ge_warehouse raw_sendem/stg_sendem.",
    )
    args = parser.parse_args()
    run(lookback_days=args.lookback_days, target=args.target)


if __name__ == "__main__":
    main()
