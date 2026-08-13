"""One-time, re-runnable historical backfill: telemetry_warehouse -> ge_warehouse.

Copies every Trackunit raw/staging object from the legacy telemetry_warehouse
database into the new raw_trackunit/stg_trackunit schemas in ge_warehouse.
See docs/migration/legacy-to-platform-migration.md for the full legacy ->
platform object mapping this script implements.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.backfill_trackunit_historical

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

Counter-reset quality columns: legacy staging.trackunit_daily_activity has
no counter_reset_detected/data_quality_status columns at all (migration 027
was written but never applied to this local telemetry_warehouse -- see
docs/migration/legacy-to-platform-migration.md). This script copies every
other column from the legacy row unchanged, then applies the exact
reset-detection UPDATE logic from
sql/legacy/telemetry_migrations/027_add_trackunit_counter_quality.sql
against the newly-copied raw_trackunit tables, so stg_trackunit.daily_activity
ends up in the state 027 always intended -- corrected in ge_warehouse, not
mutated in the read-only legacy database.
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

CONTEXT = "Trackunit historical backfill (telemetry_warehouse -> ge_warehouse)"

# (source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)
RAW_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "raw", "trackunit_assets", "raw_trackunit", "asset",
        ["asset_id", "name", "external_reference", "serial_number", "asset_type", "type", "brand", "model",
         "production_year", "owner_account_id", "created_at", "telematics_device_id",
         "telematics_device_serial", "loaded_at"],
        ["asset_id"],
    ),
    (
        "raw", "trackunit_aemp_operating_hours", "raw_trackunit", "aemp_operating_hour",
        ["asset_id", "metric_name", "metric_timestamp_utc", "metric_value", "metric_unit", "loaded_at"],
        ["asset_id", "metric_timestamp_utc", "metric_name"],
    ),
    (
        "raw", "trackunit_aemp_moving_hours", "raw_trackunit", "aemp_moving_hour",
        ["asset_id", "metric_name", "metric_timestamp_utc", "metric_value", "metric_unit", "loaded_at"],
        ["asset_id", "metric_timestamp_utc", "metric_name"],
    ),
    (
        "raw", "trackunit_aemp_distance", "raw_trackunit", "aemp_distance",
        ["asset_id", "metric_name", "metric_timestamp_utc", "metric_value", "metric_unit", "loaded_at"],
        ["asset_id", "metric_timestamp_utc", "metric_name"],
    ),
    (
        "raw", "trackunit_aemp_locations", "raw_trackunit", "aemp_location",
        ["asset_id", "location_timestamp_utc", "latitude", "longitude", "altitude", "altitude_units",
         "boundary_type", "report_date", "loaded_at"],
        ["asset_id", "location_timestamp_utc"],
    ),
    (
        "raw", "trackunit_site_history", "raw_trackunit", "site_history",
        ["asset_id", "site_id", "entered_at", "left_at", "loaded_at"],
        ["asset_id", "site_id", "entered_at"],
    ),
    (
        "raw", "trackunit_sites", "raw_trackunit", "site",
        ["site_id", "site_name", "city", "country", "polygon_geojson", "loaded_at"],
        ["site_id"],
    ),
]

STAGING_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "staging", "trackunit_dim_assets", "stg_trackunit", "asset",
        ["asset_id", "name", "external_reference", "serial_number", "asset_type", "type", "brand", "model",
         "production_year", "owner_account_id", "created_at", "telematics_device_id",
         "telematics_device_serial", "loaded_at"],
        ["asset_id"],
    ),
    (
        "staging", "trackunit_location_enrichment", "stg_trackunit", "location_enrichment",
        ["report_date", "asset_id", "zone_name_start", "zone_name_stop",
         "last_known_start_location_timestamp_utc", "last_known_start_location_timestamp_local",
         "start_latitude", "start_longitude", "start_altitude",
         "last_known_stop_location_timestamp_utc", "last_known_stop_location_timestamp_local",
         "stop_latitude", "stop_longitude", "stop_altitude",
         "address_start", "zip_start", "city_start", "country_start",
         "address_stop", "zip_stop", "city_stop", "country_stop",
         "location_enrichment_status", "location_enriched_at"],
        ["report_date", "asset_id"],
    ),
]

# Handled separately from STAGING_COPY_PLAN: counter_reset_detected/
# data_quality_status are not in the legacy source and are backfilled with
# their table defaults here, then corrected by apply_counter_reset_repair().
DAILY_ACTIVITY_COLUMNS = [
    "report_date", "asset_id", "machine", "pin", "machine_serial_number", "brand", "machine_type", "model",
    "start_time_utc", "stop_time_utc", "start_time_local", "stop_time_local",
    "work_day_minutes", "work_day_hhmm", "operating_minutes", "operating_hhmm",
    "active_driving_minutes", "active_driving_hhmm", "distance_km",
    "operating_points", "moving_points", "distance_points", "source_provider", "loaded_at",
]

BATCH_SIZE = 5000


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

    Uses a named (server-side) source cursor so large tables (100k+ rows)
    are never fully materialised in memory, and a chunked `execute_values`
    insert with `ON CONFLICT (<primary key>) DO NOTHING` on the destination
    so re-running this function is a no-op against already-copied rows.
    `rows_inserted` can be less than `rows_read` on a re-run -- that is
    expected and is exactly what makes this idempotent, not a bug.
    """
    col_list = ", ".join(columns)
    order_by = ", ".join(conflict_columns)
    insert_sql = (
        f"INSERT INTO {dest_schema}.{dest_table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING"
    )

    rows_read = 0
    rows_inserted = 0
    with source_conn.cursor(name=f"backfill_{source_table}") as source_cur:
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
    print(f"  {source_schema}.{source_table} -> {dest_schema}.{dest_table}: read {rows_read}, inserted {rows_inserted}")
    return rows_read, rows_inserted


def apply_counter_reset_repair(dest_conn) -> None:
    """Recompute counter_reset_detected/data_quality_status in ge_warehouse.

    Mirrors sql/legacy/telemetry_migrations/027_add_trackunit_counter_quality.sql
    exactly (same LAG-based within-local-day decrease detection), but reads
    from raw_trackunit.* (already copied into ge_warehouse by this script)
    and writes to stg_trackunit.daily_activity -- never touches
    telemetry_warehouse. Deterministic and idempotent: re-running produces
    the same result every time.
    """
    with dest_conn.cursor() as cur:
        cur.execute("""
            WITH ordered AS (
                SELECT
                    asset_id,
                    (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
                    metric_value,
                    LAG(metric_value) OVER (
                        PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
                        ORDER BY metric_timestamp_utc
                    ) AS previous_value
                FROM raw_trackunit.aemp_operating_hour
                WHERE metric_value IS NOT NULL
            ), resets AS (
                SELECT DISTINCT asset_id, report_date FROM ordered WHERE metric_value < previous_value
            )
            UPDATE stg_trackunit.daily_activity AS daily
            SET operating_minutes = NULL,
                operating_hhmm = NULL,
                counter_reset_detected = TRUE,
                data_quality_status = 'COUNTER_RESET'
            FROM resets
            WHERE daily.asset_id = resets.asset_id
              AND daily.report_date = resets.report_date
        """)
        operating_repaired = cur.rowcount

        cur.execute("""
            WITH ordered AS (
                SELECT
                    asset_id,
                    (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
                    metric_value,
                    LAG(metric_value) OVER (
                        PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
                        ORDER BY metric_timestamp_utc
                    ) AS previous_value
                FROM raw_trackunit.aemp_moving_hour
                WHERE metric_value IS NOT NULL
            ), resets AS (
                SELECT DISTINCT asset_id, report_date FROM ordered WHERE metric_value < previous_value
            )
            UPDATE stg_trackunit.daily_activity AS daily
            SET active_driving_minutes = NULL,
                active_driving_hhmm = NULL,
                counter_reset_detected = TRUE,
                data_quality_status = 'COUNTER_RESET'
            FROM resets
            WHERE daily.asset_id = resets.asset_id
              AND daily.report_date = resets.report_date
        """)
        moving_repaired = cur.rowcount

        cur.execute("""
            WITH ordered AS (
                SELECT
                    asset_id,
                    (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
                    metric_value,
                    LAG(metric_value) OVER (
                        PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
                        ORDER BY metric_timestamp_utc
                    ) AS previous_value
                FROM raw_trackunit.aemp_distance
                WHERE metric_value IS NOT NULL
            ), resets AS (
                SELECT DISTINCT asset_id, report_date FROM ordered WHERE metric_value < previous_value
            )
            UPDATE stg_trackunit.daily_activity AS daily
            SET distance_km = NULL,
                counter_reset_detected = TRUE,
                data_quality_status = 'COUNTER_RESET'
            FROM resets
            WHERE daily.asset_id = resets.asset_id
              AND daily.report_date = resets.report_date
        """)
        distance_repaired = cur.rowcount

        # Belt-and-suspenders: repair any historical negative delta whose raw
        # boundary readings are unavailable (mirrors 027's final UPDATE).
        cur.execute("""
            UPDATE stg_trackunit.daily_activity
            SET operating_minutes = CASE WHEN operating_minutes < 0 THEN NULL ELSE operating_minutes END,
                operating_hhmm = CASE WHEN operating_minutes < 0 THEN NULL ELSE operating_hhmm END,
                active_driving_minutes = CASE
                    WHEN active_driving_minutes < 0 THEN NULL ELSE active_driving_minutes
                END,
                active_driving_hhmm = CASE
                    WHEN active_driving_minutes < 0 THEN NULL ELSE active_driving_hhmm
                END,
                distance_km = CASE WHEN distance_km < 0 THEN NULL ELSE distance_km END,
                counter_reset_detected = TRUE,
                data_quality_status = 'COUNTER_RESET'
            WHERE operating_minutes < 0
               OR active_driving_minutes < 0
               OR distance_km < 0
        """)
        negative_repaired = cur.rowcount

    dest_conn.commit()
    print(
        f"  counter-reset repair: operating={operating_repaired} moving={moving_repaired} "
        f"distance={distance_repaired} negative-delta={negative_repaired} rows updated"
    )


def run() -> None:
    configure_logging()
    legacy_settings = get_settings()
    platform_settings = get_platform_settings()

    assert_local_host(legacy_settings.postgres_host, context=f"{CONTEXT} (legacy telemetry_warehouse)")
    assert_local_host(platform_settings.postgres_host, context=f"{CONTEXT} (ge_warehouse)")

    print(f"Source (read-only): {legacy_settings.postgres_host}:{legacy_settings.postgres_port}/{legacy_settings.postgres_db}")
    print(f"Destination: {platform_settings.postgres_host}:{platform_settings.postgres_port}/{platform_settings.ge_warehouse_db}")

    source_conn = _open_source_connection(legacy_settings)
    dest_conn = _open_dest_connection(platform_settings)

    try:
        print("\nCopying raw_trackunit tables...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in RAW_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nCopying stg_trackunit.asset and stg_trackunit.location_enrichment...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in STAGING_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nCopying stg_trackunit.daily_activity (quality columns default to live/false)...")
        _copy_table(
            source_conn, dest_conn,
            "staging", "trackunit_daily_activity",
            "stg_trackunit", "daily_activity",
            DAILY_ACTIVITY_COLUMNS,
            ["report_date", "asset_id"],
        )

        print("\nApplying counter-reset quality repair (mirrors migration 027's logic)...")
        apply_counter_reset_repair(dest_conn)

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
