"""One-time, re-runnable historical backfill: telemetry_warehouse -> ge_warehouse.

Copies every EzyTrack raw/staging object from the legacy telemetry_warehouse
database into the new raw_ezytrack/stg_ezytrack schemas in ge_warehouse. See
docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed
for the full legacy -> platform object mapping this script implements.

Unlike Sendem, there is no legacy `clean.*` (or any other historical-only)
schema for EzyTrack -- raw.ezytrack_*/staging.ezytrack_* is the complete
legacy object set, confirmed by live catalog inspection. This script is
therefore a straight 1:1 copy, with no overlap/precedence merge needed.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.backfill_ezytrack_historical

Safety:
  * Refuses to run unless both POSTGRES_HOST (legacy) and the ge_warehouse
    platform host resolve to a local Postgres instance
    (localhost/127.0.0.1/::1) -- see ge_data_platform.common.safety.
  * The telemetry_warehouse connection is opened read-only at the session
    level (SET default_transaction_read_only = on); this script never
    executes a write statement against it regardless.
  * Idempotent: every insert is `INSERT ... ON CONFLICT (<primary key>) DO
    NOTHING`, keyed on each destination table's real primary key. Re-running
    this script against already-populated tables changes nothing -- same
    row counts, same values, no duplicates.
"""

from __future__ import annotations

import argparse
import logging

import psycopg2
import psycopg2.extras

from ge_data_platform.common.logging import configure_logging
from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import PlatformSettings, Settings, get_platform_settings, get_settings

logger = logging.getLogger(__name__)

CONTEXT = "EzyTrack historical backfill (telemetry_warehouse -> ge_warehouse)"

BATCH_SIZE = 5000

# (source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)
RAW_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "raw", "ezytrack_assets", "raw_ezytrack", "asset",
        ["asset_id", "asset_code", "asset_name", "asset_description", "department_name", "project_name",
         "allocated_driver_name", "allocated_driver_code", "current_geofence_name", "is_enabled",
         "last_connected_utc", "loaded_at"],
        ["asset_id"],
    ),
    (
        "raw", "ezytrack_trips", "raw_ezytrack", "trip",
        ["trip_id", "asset_id", "start_time_utc", "end_time_utc", "duration_seconds", "distance_meters",
         "stop_time_seconds", "idle_time_seconds", "start_odometer_meters", "start_run_seconds",
         "driver_name", "driver_code", "start_geofence_name", "loaded_at"],
        ["trip_id"],
    ),
]

STAGING_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "staging", "ezytrack_dim_assets", "stg_ezytrack", "asset",
        ["asset_id", "asset_code", "asset_name", "asset_description", "department_name", "project_name",
         "allocated_driver_name", "allocated_driver_code", "current_geofence_name", "is_enabled",
         "last_connected_utc", "loaded_at"],
        ["asset_id"],
    ),
    (
        "staging", "ezytrack_fact_trips", "stg_ezytrack", "trip",
        ["trip_id", "asset_id", "start_time_utc", "end_time_utc", "duration_seconds", "distance_meters",
         "distance_km", "stop_time_seconds", "idle_time_seconds", "time_in_motion_seconds",
         "start_odometer_meters", "start_odometer_km", "end_odometer_meters", "end_odometer_km",
         "start_run_seconds", "end_run_seconds", "runtime_start_hrs", "runtime_end_hrs",
         "driver_name", "driver_code", "start_geofence_name", "loaded_at"],
        ["trip_id"],
    ),
]


def _open_source_connection(settings: Settings):
    """Open a read-only connection to legacy telemetry_warehouse."""
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    conn.commit()
    return conn


def _open_dest_connection(settings: PlatformSettings):
    """Open a read-write connection to ge_warehouse."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.ge_warehouse_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )


def _copy_table(
    source_conn,
    dest_conn,
    source_schema: str,
    source_table: str,
    dest_schema: str,
    dest_table: str,
    columns: list[str],
    conflict_columns: list[str],
) -> tuple[int, int]:
    """Stream-copy one table from source to dest. Returns (rows_read, rows_inserted).

    Uses a named (server-side) source cursor so large tables are never fully
    materialised in memory, and a chunked `execute_values` insert with
    `ON CONFLICT (<primary key>) DO NOTHING` on the destination so
    re-running this function is a no-op against already-copied rows.
    """
    col_list = ", ".join(columns)
    order_by = ", ".join(conflict_columns)
    insert_sql = (
        f"INSERT INTO {dest_schema}.{dest_table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING"
    )

    rows_read = 0
    rows_inserted = 0
    with source_conn.cursor(name=f"backfill_{source_schema}_{source_table}") as source_cur:
        source_cur.itersize = BATCH_SIZE
        source_cur.execute(f"SELECT {col_list} FROM {source_schema}.{source_table} ORDER BY {order_by}")
        with dest_conn.cursor() as dest_cur:
            while True:
                rows = source_cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                rows_read += len(rows)
                psycopg2.extras.execute_values(dest_cur, insert_sql, rows, page_size=BATCH_SIZE)
                rows_inserted += dest_cur.rowcount

    dest_conn.commit()
    print(
        f"  {source_schema}.{source_table} -> {dest_schema}.{dest_table}: "
        f"read {rows_read}, inserted {rows_inserted}"
    )
    return rows_read, rows_inserted


def run() -> None:
    configure_logging()
    legacy_settings = get_settings()
    platform_settings = get_platform_settings()

    assert_local_host(legacy_settings.postgres_host, context=f"{CONTEXT} (legacy telemetry_warehouse)")
    assert_local_host(platform_settings.postgres_host, context=f"{CONTEXT} (ge_warehouse)")

    print(
        f"Source (read-only): {legacy_settings.postgres_host}:{legacy_settings.postgres_port}/{legacy_settings.postgres_db}"
    )
    print(
        f"Destination: {platform_settings.postgres_host}:{platform_settings.postgres_port}/{platform_settings.ge_warehouse_db}"
    )

    source_conn = _open_source_connection(legacy_settings)
    dest_conn = _open_dest_connection(platform_settings)

    try:
        print("\nCopying raw_ezytrack tables...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in RAW_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nCopying stg_ezytrack tables...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in STAGING_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nBackfill complete.")
    finally:
        source_conn.close()
        dest_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
