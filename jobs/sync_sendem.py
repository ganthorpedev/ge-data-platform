"""Entry point for the Sendem/MiX sync job.

Run with:
    python -m jobs.sync_sendem

This orchestrates: fetch (connectors.sendem_client) -> transform
(transforms.sendem_transform) -> load (loaders.postgres_loader), and records
the run in etl.sync_runs.
"""

from __future__ import annotations

from datetime import date, timedelta

from config.settings import get_settings
from connectors.sendem_client import SendemClient
from loaders.postgres_loader import PostgresLoader
from transforms.sendem_transform import build_all

SOURCE_SYSTEM = "sendem"
JOB_NAME = "sendem_hourly_sync"


def _to_date_key(value: date) -> int:
    """Convert a `date` to its YYYYMMDD integer representation."""
    return int(value.strftime("%Y%m%d"))


def run() -> None:
    """Execute one Sendem sync run: fetch, transform, load, and record the result."""
    print("Loading settings...")
    settings = get_settings()

    today = date.today()
    end_date = _to_date_key(today)
    start_date = _to_date_key(today - timedelta(days=settings.sync_lookback_days))
    print(f"Sync window: {start_date} to {end_date}")

    client = SendemClient(settings)
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
            "people": client.get_people(),
            "organisations": client.get_organisations(),
            "event_descriptions": client.get_event_descriptions(),
            "trips": trips,
            "events": events,
        }

        print("Transforming data...")
        dataframes = build_all(raw)

        print("Loading data into PostgreSQL...")
        load_counts = loader.load_sendem_tables(dataframes, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM)
        for table_name, row_count in load_counts.items():
            print(f"  {table_name}: {row_count} rows")

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
        print(f"Sync run {sync_run_id} failed: {error}")
        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="FAILED",
            error_message=str(error),
        )
        raise


if __name__ == "__main__":
    run()
