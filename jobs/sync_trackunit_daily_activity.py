"""Auto-updating entry point for the Trackunit / Manitou daily activity sync.

Run modes:
    # Exact date
    python -m jobs.sync_trackunit_daily_activity --date 2026-07-05

    # Backfill a range (inclusive)
    python -m jobs.sync_trackunit_daily_activity --from-date 2026-07-01 --to-date 2026-07-05

    # Rolling update: last N local (Africa/Harare) days, ending today
    python -m jobs.sync_trackunit_daily_activity --rolling-days 2

    # No date arguments -> defaults to --rolling-days 2
    python -m jobs.sync_trackunit_daily_activity

Other examples:
    # 4-machine validation
    python -m jobs.sync_trackunit_daily_activity --date 2026-07-01 --machines 2277,3846,3849,4955

    # 20-machine controlled run
    python -m jobs.sync_trackunit_daily_activity --date 2026-07-05 --limit 20

    # Normal daily scheduler run (today plus yesterday for late-arriving data)
    python -m jobs.sync_trackunit_daily_activity --rolling-days 2

    # 7-day reconciliation
    python -m jobs.sync_trackunit_daily_activity --rolling-days 7

    # Backfill
    python -m jobs.sync_trackunit_daily_activity --from-date 2026-07-01 --to-date 2026-07-05

This orchestrates: fetch (connectors.trackunit_client) -> transform
(transforms.trackunit_transform) -> load (loaders.postgres_loader), and
records ONE run in etl.sync_runs / etl.sync_table_loads covering every
report_date processed in this invocation.

Every date's UTC window is calculated dynamically from the local
(Africa/Harare, or TRACKUNIT_TIMEZONE) calendar day -- nothing here hardcodes
a UTC window or a specific date; dates only ever come from CLI arguments or
"today" at run time. Every load is UPSERT (see loaders.postgres_loader), so
re-running the same date, range, or rolling window updates existing rows
rather than duplicating them. Nothing here truncates or deletes rows.

V1 API safety: one AEMP call at a time, no parallel requests. Pacing between
calls and 429 backoff/retry are handled inside connectors.trackunit_client
(TrackunitClient.get_aemp_series), configured via TrackunitSettings -- this
job does not sleep or retry itself. Any failure that exhausts those retries
(or any other non-retryable error) stops the whole run immediately and marks
etl.sync_runs FAILED -- there is no partial-success mode, even across a
multi-date backfill/rolling run.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta

from config.settings import get_etl_ops_settings, get_settings, get_trackunit_settings
from connectors.trackunit_client import TrackunitClient
from loaders.postgres_loader import PostgresLoader, finish_sync_run_failed_safe
from transforms.trackunit_transform import build_daily_activity_rows, extract_metric_points, local_report_day_to_utc_window
from utils.dates import local_today, to_date_key
from utils.logging_config import configure_logging
from utils.overlap_lock import TRACKUNIT_OVERLAP_GROUP, provider_job_lock

SOURCE_SYSTEM = "trackunit"
JOB_NAME = "trackunit_daily_activity_sync"

DEFAULT_ROLLING_DAYS = 2

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    """Parse a CLI "YYYY-MM-DD" string into a date."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> list[date]:
    """Return every date from `start` to `end`, inclusive."""
    if end < start:
        raise ValueError(f"--to-date ({end}) must not be before --from-date ({start})")

    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _rolling_dates(rolling_days: int, timezone: str) -> list[date]:
    """Return the last `rolling_days` local calendar days, ending today (inclusive), oldest first."""
    if rolling_days < 1:
        raise ValueError("--rolling-days must be at least 1")

    today_local = local_today(timezone)
    return [today_local - timedelta(days=offset) for offset in range(rolling_days - 1, -1, -1)]


def resolve_report_dates(
    date_arg: str | None,
    from_date_arg: str | None,
    to_date_arg: str | None,
    rolling_days_arg: int | None,
    timezone: str,
) -> list[date]:
    """Resolve the list of report dates to sync from the CLI mode selected.

    Exactly one of: --date, (--from-date and --to-date), --rolling-days may
    be supplied. If none are supplied, defaults to a rolling 2-day window.
    """
    modes_supplied = [
        bool(date_arg),
        bool(from_date_arg or to_date_arg),
        rolling_days_arg is not None,
    ]
    if sum(modes_supplied) > 1:
        raise ValueError("Supply only one of: --date, --from-date/--to-date, or --rolling-days")

    if date_arg:
        return [_parse_date(date_arg)]

    if from_date_arg or to_date_arg:
        if not (from_date_arg and to_date_arg):
            raise ValueError("Both --from-date and --to-date are required for backfill mode")
        return _date_range(_parse_date(from_date_arg), _parse_date(to_date_arg))

    rolling_days = rolling_days_arg if rolling_days_arg is not None else DEFAULT_ROLLING_DAYS
    return _rolling_dates(rolling_days, timezone)


def _sync_one_date(
    client: TrackunitClient,
    assets: list[dict],
    report_date: date,
    timezone: str,
    loader: PostgresLoader,
    sync_run_id: str,
) -> tuple[int, int]:
    """Fetch, transform, and load one report_date. Returns (rows_fetched, rows_loaded).

    Raises with `report_date` embedded in the message on any failure, so a
    multi-date run's error_message always identifies exactly which date
    failed.
    """
    try:
        start_utc, end_utc = local_report_day_to_utc_window(report_date, timezone)
        print(f"\n--- report_date {report_date} | UTC window {start_utc} to {end_utc} ---")

        metric_results_by_asset: dict[str, dict[str, list[dict]]] = {}

        for asset in assets:
            asset_id = asset.get("id")
            machine_name = asset.get("name")
            pin = asset.get("externalReference") or asset.get("serialNumber")

            if not pin:
                print(f"  [{machine_name}] no PIN/serial available -- skipping AEMP fetch (zero row).")
                metric_results_by_asset[asset_id] = {
                    "operating_points": [],
                    "moving_points": [],
                    "distance_points": [],
                }
                continue

            print(f"  [{machine_name}] pin={pin}: fetching CumulativeOperatingHours...")
            operating_response = client.get_aemp_series(
                pin, "CumulativeOperatingHours", "cumulativeOperatingHours", start_utc, end_utc
            )
            operating_points = extract_metric_points(operating_response, "cumulativeOperatingHours")

            print(f"  [{machine_name}] pin={pin}: fetching CumulativeMovingHours...")
            moving_response = client.get_aemp_series(
                pin, "CumulativeMovingHours", "cumulativeMovingHours", start_utc, end_utc
            )
            moving_points = extract_metric_points(moving_response, "cumulativeMovingHours")

            print(f"  [{machine_name}] pin={pin}: fetching Distance...")
            distance_response = client.get_aemp_series(pin, "Distance", "distance", start_utc, end_utc)
            distance_points = extract_metric_points(distance_response, "distance")

            metric_results_by_asset[asset_id] = {
                "operating_points": operating_points,
                "moving_points": moving_points,
                "distance_points": distance_points,
            }
            print(
                f"  [{machine_name}] operating={len(operating_points)} pt, "
                f"moving={len(moving_points)} pt, distance={len(distance_points)} pt"
            )

        print(f"Transforming data for {report_date}...")
        dataframes = build_daily_activity_rows(assets, metric_results_by_asset, report_date, timezone)

        print(f"Loading data for {report_date} into PostgreSQL...")
        load_counts = loader.load_trackunit_tables(dataframes, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM)
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

        return len(assets), sum(load_counts.values())

    except Exception as error:
        raise RuntimeError(f"Trackunit sync failed for report_date {report_date}: {error}") from error


@provider_job_lock(TRACKUNIT_OVERLAP_GROUP)
def run(
    date_arg: str | None = None,
    from_date_arg: str | None = None,
    to_date_arg: str | None = None,
    rolling_days_arg: int | None = None,
    limit: int | None = None,
    machines: list[str] | None = None,
) -> None:
    """Execute one Trackunit daily-activity sync covering every resolved report_date.

    Marks etl.sync_runs SUCCESS only if every report_date's AEMP calls
    succeeded and loaded cleanly. Any failure (any date, any asset, any
    metric -- including a 429 that exhausts its retries) marks the whole run
    FAILED with the error in error_message and re-raises -- there is no
    partial SUCCESS across a multi-date backfill/rolling run.
    """
    configure_logging()
    print("Loading settings...")
    postgres_settings = get_settings()
    trackunit_settings = get_trackunit_settings()
    ops_settings = get_etl_ops_settings()

    report_dates = resolve_report_dates(
        date_arg, from_date_arg, to_date_arg, rolling_days_arg, trackunit_settings.timezone
    )
    print(f"Report dates ({len(report_dates)}): {[d.isoformat() for d in report_dates]}")

    loader = PostgresLoader(postgres_settings)
    client = TrackunitClient(trackunit_settings)

    print("Starting sync run...")
    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=to_date_key(min(report_dates)),
        end_date=to_date_key(max(report_dates)),
    )
    print(f"sync_run_id: {sync_run_id}")

    try:
        print("Authenticating with Trackunit...")
        client.authenticate()

        print("Fetching all Trackunit assets...")
        assets = client.get_all_assets()
        print(f"Fetched {len(assets)} asset(s).")

        if machines:
            machine_set = set(machines)
            assets = [asset for asset in assets if asset.get("name") in machine_set]
            print(f"Filtered to {len(assets)} asset(s) matching --machines {machines}.")

        if limit is not None:
            assets = assets[:limit]
            print(f"Limited to {len(assets)} asset(s) (--limit {limit}).")

        total_rows_fetched = 0
        total_rows_loaded = 0

        for report_date in report_dates:
            rows_fetched, rows_loaded = _sync_one_date(
                client, assets, report_date, trackunit_settings.timezone, loader, sync_run_id
            )
            total_rows_fetched += rows_fetched
            total_rows_loaded += rows_loaded

        loader.run_post_load_validation(
            SOURCE_SYSTEM,
            mode=ops_settings.validation_mode,
            lookback_hours=ops_settings.validation_lookback_hours,
        )

        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="SUCCESS",
            rows_fetched=total_rows_fetched,
            rows_loaded=total_rows_loaded,
        )
        print(
            f"\nSync run {sync_run_id} completed: SUCCESS "
            f"(dates={len(report_dates)}, fetched={total_rows_fetched}, loaded={total_rows_loaded})"
        )

    except Exception as error:
        logger.exception("Trackunit sync run %s failed: %s", sync_run_id, error)
        finish_sync_run_failed_safe(loader, sync_run_id, error)
        raise


def main() -> None:
    """Parse CLI args and run the sync."""
    parser = argparse.ArgumentParser(
        description="Trackunit daily activity sync (exact date / backfill range / rolling update)",
        epilog=(
            "Examples:\n"
            "  4-machine validation:\n"
            "    python -m jobs.sync_trackunit_daily_activity --date 2026-07-01 --machines 2277,3846,3849,4955\n"
            "\n"
            "  20-machine controlled run:\n"
            "    python -m jobs.sync_trackunit_daily_activity --date 2026-07-05 --limit 20\n"
            "\n"
            "  Normal daily scheduler run (today plus yesterday for late-arriving data):\n"
            "    python -m jobs.sync_trackunit_daily_activity --rolling-days 2\n"
            "\n"
            "  7-day reconciliation:\n"
            "    python -m jobs.sync_trackunit_daily_activity --rolling-days 7\n"
            "\n"
            "  Backfill a range:\n"
            "    python -m jobs.sync_trackunit_daily_activity --from-date 2026-07-01 --to-date 2026-07-05\n"
            "\n"
            "  No date argument -> defaults to --rolling-days 2:\n"
            "    python -m jobs.sync_trackunit_daily_activity\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--date", type=str, default=None, help="Exact report date, YYYY-MM-DD (local Africa/Harare day).")
    parser.add_argument("--from-date", type=str, default=None, help="Backfill range start, YYYY-MM-DD (inclusive).")
    parser.add_argument("--to-date", type=str, default=None, help="Backfill range end, YYYY-MM-DD (inclusive).")
    parser.add_argument(
        "--rolling-days",
        type=int,
        default=None,
        help="Sync the last N local days ending today. Default mode when no date argument is given (N=2).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on the number of assets processed.")
    parser.add_argument(
        "--machines",
        type=str,
        default=None,
        help="Optional comma-separated list of machine names to restrict to, e.g. 2277,3846,3849,4955",
    )
    args = parser.parse_args()

    machines = [name.strip() for name in args.machines.split(",")] if args.machines else None

    run(
        date_arg=args.date,
        from_date_arg=args.from_date,
        to_date_arg=args.to_date,
        rolling_days_arg=args.rolling_days,
        limit=args.limit,
        machines=machines,
    )


if __name__ == "__main__":
    main()
