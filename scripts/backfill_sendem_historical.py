"""One-time, re-runnable historical backfill: telemetry_warehouse -> ge_warehouse.

Copies every Sendem raw/staging object from the legacy telemetry_warehouse
database into the new raw_sendem/stg_sendem schemas in ge_warehouse, AND
folds in the legacy clean.sendem_fact_trips_daily / clean.sendem_fact_events_daily
history (2026-01-01 to 2026-06-30) that exists nowhere else -- see
docs/migration/legacy-to-platform-migration.md#sendem-migration for the full
legacy -> platform object mapping and the clean.* investigation this script
implements.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.backfill_sendem_historical

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

clean.* handling (see the migration doc for the full key-set analysis):
  * clean.sendem_dim_assets/_dim_sites/_dim_event_types are NOT copied --
    confirmed to be proper subsets of the current staging dims (0 exclusive
    asset/site/event-type ids), so there is no unique master-data value to
    preserve there.
  * clean.sendem_fact_trips_daily / clean.sendem_fact_events_daily ARE
    copied into stg_sendem.trip_daily / stg_sendem.event_daily, but only
    AFTER staging's own fact rows are loaded first, and only via
    `ON CONFLICT (<pk>) DO NOTHING`: on the 577 trip / 4,934 event
    (date_key, group_id, site_id, asset_id[, event_type_id]) keys that exist
    in both clean and staging (the 2026-06-24..06-30 overlap), staging's
    live-pipeline value is kept; clean's value is discarded (float precision
    drift between the two independent extraction runs, not a business
    difference). Only clean's ~11,439 / ~106,188 EXCLUSIVE keys (2026-01-01
    to 2026-06-23) actually extend history.
  * clean.sendem_fact_events_daily references 2 event_type_ids
    (41 rows total) present in NO dimension table anywhere (not raw, not
    clean-dim, not staging-dim) -- apply_inferred_event_types() synthesizes
    the same "Unknown Sendem Event Type" placeholder rows
    ge_data_platform.sources.sendem.transform.build_dim_event_types() would
    produce for this situation, so the merged event facts never orphan.
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

CONTEXT = "Sendem historical backfill (telemetry_warehouse -> ge_warehouse)"

BATCH_SIZE = 5000

# (source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)
RAW_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "raw", "sendem_assets", "raw_sendem", "asset",
        ["asset_id", "site_id", "asset_type", "description", "vin_number", "country", "group_id",
         "registration_number", "is_available", "fleet_number", "make", "model", "fuel_type", "year", "loaded_at"],
        ["asset_id"],
    ),
    (
        "raw", "sendem_sites", "raw_sendem", "site",
        ["site_id", "site_name", "loaded_at"],
        ["site_id"],
    ),
    (
        "raw", "sendem_event_descriptions", "raw_sendem", "event_description",
        ["event_type_id", "event_name", "group_id", "metric_type", "unit_type", "event_category", "loaded_at"],
        ["event_type_id"],
    ),
    (
        "raw", "sendem_trips_assets_daily", "raw_sendem", "trip_daily",
        ["date_key", "group_id", "site_id", "asset_id", "total_trip_count", "total_trip_distance_kilometres",
         "total_fuel_used_litres", "total_energy_used_kwh", "date", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id"],
    ),
    (
        "raw", "sendem_events_assets_daily", "raw_sendem", "event_daily",
        ["date_key", "group_id", "site_id", "asset_id", "event_type_id", "total_event_occurrences",
         "min_event_value", "max_event_value", "total_event_value", "min_event_duration", "max_event_duration",
         "total_event_duration", "date", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
    ),
]

STAGING_COPY_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "staging", "sendem_dim_assets", "stg_sendem", "asset",
        ["asset_id", "site_id", "asset_type", "description", "vin_number", "country", "group_id",
         "registration_number", "is_available", "fleet_number", "make", "model", "fuel_type", "year", "loaded_at"],
        ["asset_id"],
    ),
    (
        "staging", "sendem_dim_sites", "stg_sendem", "site",
        ["site_id", "site_name", "loaded_at"],
        ["site_id"],
    ),
    (
        "staging", "sendem_dim_event_types", "stg_sendem", "event_type",
        ["event_type_id", "event_name", "group_id", "metric_type", "unit_type", "event_category", "loaded_at"],
        ["event_type_id"],
    ),
    (
        "staging", "sendem_fact_trips_daily", "stg_sendem", "trip_daily",
        ["date", "date_key", "group_id", "site_id", "site_name", "asset_id", "fleet_number", "registration_number",
         "description", "make", "model", "asset_type", "total_trip_count", "total_trip_distance_kilometres",
         "total_fuel_used_litres", "total_energy_used_kwh", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id"],
    ),
    (
        "staging", "sendem_fact_events_daily", "stg_sendem", "event_daily",
        ["date", "date_key", "group_id", "site_id", "site_name", "asset_id", "fleet_number", "registration_number",
         "description", "make", "model", "asset_type", "event_type_id", "event_name", "event_category",
         "metric_type", "unit_type", "total_event_occurrences", "min_event_value", "max_event_value",
         "total_event_value", "min_event_duration", "max_event_duration", "total_event_duration", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
    ),
]

# clean.* fact tables have the exact same columns as their staging
# counterparts, minus the `source_system` column (constant 'sendem',
# dropped rather than added as a column stg_sendem.trip_daily/event_daily
# don't have -- every row here is Sendem by definition of the schema it
# lives in). Loaded AFTER STAGING_COPY_PLAN's fact tables so ON CONFLICT DO
# NOTHING keeps the live staging value on any overlapping key -- see the
# module docstring.
CLEAN_MERGE_PLAN: list[tuple[str, str, str, str, list[str], list[str]]] = [
    (
        "clean", "sendem_fact_trips_daily", "stg_sendem", "trip_daily",
        ["date", "date_key", "group_id", "site_id", "site_name", "asset_id", "fleet_number", "registration_number",
         "description", "make", "model", "asset_type", "total_trip_count", "total_trip_distance_kilometres",
         "total_fuel_used_litres", "total_energy_used_kwh", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id"],
    ),
    (
        "clean", "sendem_fact_events_daily", "stg_sendem", "event_daily",
        ["date", "date_key", "group_id", "site_id", "site_name", "asset_id", "fleet_number", "registration_number",
         "description", "make", "model", "asset_type", "event_type_id", "event_name", "event_category",
         "metric_type", "unit_type", "total_event_occurrences", "min_event_value", "max_event_value",
         "total_event_value", "min_event_duration", "max_event_duration", "total_event_duration", "loaded_at"],
        ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
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
    `rows_inserted` can be less than `rows_read` on a re-run, or whenever the
    destination already holds the key with a different source (e.g. clean
    rows colliding with already-loaded staging rows) -- that is expected and
    is exactly what makes this idempotent and overlap-safe, not a bug.
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


def apply_inferred_event_types(source_conn, dest_conn) -> int:
    """Insert placeholder stg_sendem.event_type rows for orphaned event types.

    clean.sendem_fact_events_daily references event_type_ids that exist in
    no dimension table anywhere (not raw, not clean's own dim, not the
    current staging dim) -- 2 ids / 41 rows, confirmed by inspection. This
    mirrors ge_data_platform.sources.sendem.transform.build_dim_event_types(),
    which does the same inference for event types discovered live but
    missing from the real dimension. Idempotent: ON CONFLICT DO NOTHING.
    """
    with dest_conn.cursor() as cur:
        cur.execute("SELECT event_type_id FROM stg_sendem.event_type")
        known_ids = {row[0] for row in cur.fetchall()}

    with source_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT event_type_id, group_id FROM clean.sendem_fact_events_daily")
        clean_fact_ids = cur.fetchall()

    missing = [(event_type_id, group_id) for event_type_id, group_id in clean_fact_ids if event_type_id not in known_ids]
    if not missing:
        print("  No orphaned event_type_ids found in legacy clean fact data.")
        return 0

    with dest_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO stg_sendem.event_type
                (event_type_id, event_name, group_id, metric_type, unit_type, event_category)
            VALUES %s
            ON CONFLICT (event_type_id) DO NOTHING
            """,
            [(event_type_id, "Unknown Sendem Event Type", group_id, "", "", "unknown") for event_type_id, group_id in missing],
        )
        inserted = cur.rowcount
    dest_conn.commit()
    print(f"  Inferred {inserted} placeholder event_type row(s) for id(s): {[m[0] for m in missing]}")
    return inserted


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
        print("\nCopying raw_sendem tables...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in RAW_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nCopying stg_sendem dims and facts (staging is the overlap-window authority)...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in STAGING_COPY_PLAN:
            _copy_table(source_conn, dest_conn, source_schema, source_table, dest_schema, dest_table, columns, conflict_columns)

        print("\nResolving legacy clean.* orphaned event types (before merging clean facts)...")
        apply_inferred_event_types(source_conn, dest_conn)

        print("\nMerging legacy clean.* fact history into stg_sendem (exclusive keys only)...")
        for source_schema, source_table, dest_schema, dest_table, columns, conflict_columns in CLEAN_MERGE_PLAN:
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
