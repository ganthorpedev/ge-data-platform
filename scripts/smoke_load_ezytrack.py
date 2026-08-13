"""EzyTrack loader smoke test.

Run with:
    python -m scripts.smoke_load_ezytrack

Fetches a tiny live window (last 1 hour) of EzyTrack assets/trips,
transforms them, and loads them into PostgreSQL using
ge_data_platform.common.database.PostgresLoader.load_ezytrack_tables().

This is a smoke test only, NOT the production sync job:
- Does not create a sync_run.
- Does not write to etl.sync_runs or etl.sync_table_loads
  (load_ezytrack_tables only logs per-table loads when a sync_run_id is
  passed in, and this script never passes one).
- Does not touch Sendem in any way.
"""

from __future__ import annotations

from datetime import timedelta

from ge_data_platform.common.database import PostgresLoader
from ge_data_platform.common.dates import format_utc_iso, utc_now
from ge_data_platform.config.settings import get_settings
from ge_data_platform.sources.ezytrack.client import fetch_ezytrack_assets, fetch_ezytrack_trips
from ge_data_platform.sources.ezytrack.transform import build_all


def run() -> None:
    """Fetch, transform, and load one tiny EzyTrack window; print results only."""
    end_time = utc_now()
    start_time = end_time - timedelta(hours=1)
    start_str = format_utc_iso(start_time)
    end_str = format_utc_iso(end_time)

    print("Fetching EzyTrack assets...")
    assets = fetch_ezytrack_assets()

    print(f"Fetching EzyTrack trips: {start_str} to {end_str}")
    trips = fetch_ezytrack_trips(start_str, end_str, page_size=50)

    print("\nTransforming data...")
    dataframes = build_all(assets, trips)

    print()
    for name, df in dataframes.items():
        print(f"{name}: shape={df.shape}")
        print(f"  columns={list(df.columns)}")

    print("\nConnecting to PostgreSQL...")
    settings = get_settings()
    loader = PostgresLoader(settings)
    loader.test_connection()

    print("\nLoading EzyTrack tables...")
    load_counts = loader.load_ezytrack_tables(dataframes)

    print("\nLoaded row counts:")
    for table_name, row_count in load_counts.items():
        print(f"  {table_name}: {row_count} rows")


if __name__ == "__main__":
    run()
