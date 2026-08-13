"""Loads telemetry provider dataframes into PostgreSQL.

This module owns all database writes: connection creation, the generic
strict upsert (upsert_dataframe), the per-provider raw/staging table load
plans (load_sendem_tables, load_ezytrack_tables, load_trackunit_tables), and
etl.sync_runs / etl.sync_table_loads tracking.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ge_data_platform.config.settings import Settings
from ge_data_platform.common.dates import utc_now

logger = logging.getLogger(__name__)

# to_sql chunk size for the temp-table stage of every upsert. The widest
# tables here have ~30 columns; 500 rows x 30 columns = 15,000 bound
# parameters per INSERT, comfortably under psycopg2/PostgreSQL's 65,535
# parameter limit while still batching far better than row-by-row inserts.
TO_SQL_CHUNKSIZE = 500

# These checks are deliberately the bounded, post-load subset of the existing
# sql/*validate*.sql packs. The full packs remain available for manual
# reconciliation; running their full-history joins after every small sync
# would be unnecessarily expensive. Every query below only inspects rows
# touched recently via loaded_at.
POST_LOAD_VALIDATION_QUERIES: dict[str, list[tuple[str, str, bool]]] = {
    "sendem": [
        (
            "recent negative trip metrics",
            """
            SELECT COUNT(*)
            FROM staging.sendem_fact_trips_daily
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND (
                    total_trip_count < 0
                 OR total_trip_distance_kilometres < 0
                 OR total_fuel_used_litres < 0
                 OR total_energy_used_kwh < 0
              )
            """,
            True,
        ),
        (
            "recent negative event counts or durations",
            """
            SELECT COUNT(*)
            FROM staging.sendem_fact_events_daily
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND (
                    total_event_occurrences < 0
                 OR min_event_duration < 0
                 OR max_event_duration < 0
                 OR total_event_duration < 0
              )
            """,
            True,
        ),
        (
            "recent facts missing an asset or site dimension",
            """
            SELECT COUNT(*)
            FROM (
                SELECT f.date_key, f.group_id, f.site_id, f.asset_id
                FROM staging.sendem_fact_trips_daily f
                LEFT JOIN staging.sendem_dim_assets a ON a.asset_id = f.asset_id
                LEFT JOIN staging.sendem_dim_sites s ON s.site_id = f.site_id
                WHERE f.loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
                  AND (a.asset_id IS NULL OR s.site_id IS NULL)
                UNION ALL
                SELECT f.date_key, f.group_id, f.site_id, f.asset_id
                FROM staging.sendem_fact_events_daily f
                LEFT JOIN staging.sendem_dim_assets a ON a.asset_id = f.asset_id
                LEFT JOIN staging.sendem_dim_sites s ON s.site_id = f.site_id
                WHERE f.loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
                  AND (a.asset_id IS NULL OR s.site_id IS NULL)
            ) AS missing_dimensions
            """,
            False,
        ),
    ],
    "ezytrack": [
        (
            "recent negative trip metrics",
            """
            SELECT COUNT(*)
            FROM staging.ezytrack_fact_trips
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND (
                    duration_seconds < 0
                 OR distance_meters < 0
                 OR distance_km < 0
                 OR stop_time_seconds < 0
                 OR idle_time_seconds < 0
                 OR time_in_motion_seconds < 0
              )
            """,
            True,
        ),
        (
            "recent trips missing an asset dimension",
            """
            SELECT COUNT(*)
            FROM staging.ezytrack_fact_trips f
            LEFT JOIN staging.ezytrack_dim_assets a ON a.asset_id = f.asset_id
            WHERE f.loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND a.asset_id IS NULL
            """,
            False,
        ),
    ],
    "trackunit": [
        (
            "recent negative daily activity metrics",
            """
            SELECT COUNT(*)
            FROM staging.trackunit_daily_activity
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND (
                    work_day_minutes < 0
                 OR operating_minutes < 0
                 OR active_driving_minutes < 0
                 OR distance_km < 0
              )
            """,
            True,
        ),
        (
            "recent counter-reset quality flag/status mismatches",
            """
            SELECT COUNT(*)
            FROM staging.trackunit_daily_activity
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND counter_reset_detected IS DISTINCT FROM
                  (data_quality_status = 'COUNTER_RESET')
            """,
            True,
        ),
    ],
    "trackunit_location": [],
    "evolution_project_reports": [
        (
            "recent rows missing their (company, id) load key",
            """
            SELECT COUNT(*)
            FROM raw.evolution_project_reports
            WHERE loaded_at >= CURRENT_TIMESTAMP - make_interval(hours => :lookback_hours)
              AND (company IS NULL OR id IS NULL)
            """,
            True,
        ),
    ],
}


def to_snake_case(name: str) -> str:
    """Convert a PascalCase/camelCase identifier to snake_case."""
    step1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    step2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", step1)
    return step2.lower()


def prepare_dataframe_for_load(df: pd.DataFrame) -> pd.DataFrame:
    """Snake-case columns, null-normalise, and stamp a UTC `loaded_at`.

    `loaded_at` is always timezone-aware UTC (never the machine-local naive
    clock) so TIMESTAMPTZ columns store an unambiguous instant regardless of
    the Windows box's timezone setting.
    """
    prepared = df.rename(columns={col: to_snake_case(col) for col in df.columns})
    prepared = prepared.where(pd.notnull(prepared), None)
    if "loaded_at" not in prepared.columns:
        prepared["loaded_at"] = pd.Timestamp.now(tz="UTC")
    return prepared


# The exact raw.evolution_project_reports column set (excluding loaded_at,
# which prepare_dataframe_for_load adds), matching sql/migrations/029_create_
# accounts_evolution_project_reports_schema.sql. Unlike the telemetry providers'
# upsert_dataframe (which introspects the destination via
# information_schema.columns because upstream API payloads can drift), this
# pipeline fully owns its DDL and its transform's output columns are fixed,
# so a plain constant is simpler and needs no live schema round-trip.
EVOLUTION_PROJECT_REPORTS_COLUMNS = [
    "company",
    "id",
    "account_description",
    "account_type_description",
    "cost_type",
    "credit",
    "customer",
    "customer_unique_id",
    "d_date",
    "debit",
    "description",
    "fleet_number",
    "inclusive_amount",
    "master_sub_account",
    "module",
    "project",
    "project_code",
    "project_name",
    "quantity_invoiced",
    "reference",
    "tax_amount",
    "transaction_description",
    "business_unit",
]


def validate_combined_for_full_replace(
    df: pd.DataFrame,
    *,
    dataset_name: str = "raw.evolution_project_reports",
) -> None:
    """Refuse to replace the destination table with unsafe data.

    This runs on the fully combined, transformed, snake_case DataFrame
    (transforms.accounts.evolution.project_reports_transform.build_combined
    output) before any staging or destination table is touched. Raises
    `ValueError` -- never silently proceeds -- if:

    * the extract is unexpectedly empty (a broken extraction returning zero
      rows must never be allowed to wipe a previously loaded table via the
      full replace below);
    * `company` or `id` contain any missing values; or
    * `(company, id)` is not unique (the destination's primary key, and the
      column pair the atomic replace joins staging back into raw on).
    """
    if df.empty:
        raise ValueError(f"Refusing to replace {dataset_name}: the combined extract is unexpectedly empty")

    required_columns = {"company", "id"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Refusing to replace {dataset_name}: missing required column(s) {sorted(missing_columns)}")

    missing_company = df["company"].isna() | (df["company"].astype(str).str.strip() == "")
    if missing_company.any():
        raise ValueError(
            f"Refusing to replace {dataset_name}: {int(missing_company.sum())} row(s) have a missing company"
        )

    missing_id = df["id"].isna()
    if missing_id.any():
        raise ValueError(f"Refusing to replace {dataset_name}: {int(missing_id.sum())} row(s) have a missing id")

    duplicate_mask = df.duplicated(subset=["company", "id"], keep=False)
    if duplicate_mask.any():
        raise ValueError(
            f"Refusing to replace {dataset_name}: {int(duplicate_mask.sum())} row(s) "
            "share a duplicate (company, id) key"
        )


def finish_sync_run_failed_safe(loader: "PostgresLoader", sync_run_id: str, error: BaseException) -> None:
    """Mark a sync run FAILED without ever masking the original job error.

    If the FAILED bookkeeping update itself fails (e.g. the database is the
    thing that broke), the bookkeeping error is logged and swallowed so the
    caller's `raise` re-raises the ORIGINAL exception, and the sync_runs row
    is left in STARTED for the housekeeping job to mark ABANDONED later.
    """
    try:
        loader.finish_sync_run(sync_run_id=sync_run_id, status="FAILED", error_message=str(error))
    except Exception as bookkeeping_error:
        logger.error(
            "Could not mark sync run %s as FAILED (bookkeeping error: %s). "
            "The original job error is preserved and re-raised; this run will "
            "be picked up by stale-run cleanup as ABANDONED.",
            sync_run_id,
            bookkeeping_error,
        )


class PostgresLoader:
    """Loads Sendem dataframes into the telemetry_warehouse PostgreSQL database."""

    def __init__(self, settings: Settings) -> None:
        """Create and store a SQLAlchemy engine from `settings`.

        pool_pre_ping revalidates pooled connections before use (the
        Trackunit job can spend hours fetching before it writes, easily
        outliving an idle connection); pool_recycle=1800 proactively replaces
        connections older than 30 minutes.
        """
        password = quote_plus(settings.postgres_password)
        connection_string = (
            f"postgresql+psycopg2://{settings.postgres_user}:{password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        self.engine: Engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=settings.postgres_pool_timeout_seconds,
        )

    def test_connection(self) -> None:
        """Run a trivial query and print the connected database and user."""
        with self.engine.connect() as conn:
            database, user = conn.execute(text("SELECT current_database(), current_user")).one()

        print(f"Connected to database '{database}' as user '{user}'")

    def get_table_columns(self, schema: str, table: str) -> list[str]:
        """Return `schema.table`'s column names ordered by ordinal position."""
        statement = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(statement, {"schema": schema, "table": table}).all()

        return [row[0] for row in rows]

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        conflict_columns: list[str],
    ) -> int:
        """Upsert `df` into `schema.table` using INSERT ... ON CONFLICT DO UPDATE.

        Column names are converted to snake_case, a `loaded_at` timestamp is
        added if missing, and the dataframe is filtered down to only the
        columns that actually exist on the destination table (extra API
        columns are dropped). The data is then staged in a temporary table
        and merged into the target table. Returns the number of rows loaded.
        """
        if df.empty:
            print(f"No rows to load into {schema}.{table}")
            return 0

        prepared = prepare_dataframe_for_load(df)

        destination_columns = self.get_table_columns(schema, table)
        original_columns = list(prepared.columns)
        filtered_columns = [c for c in original_columns if c in destination_columns]
        dropped_columns = [c for c in original_columns if c not in destination_columns]

        print(f"Target table: {schema}.{table}")
        print(f"Original column count: {len(original_columns)}")
        print(f"Filtered column count: {len(filtered_columns)}")
        if dropped_columns:
            print(f"Dropped columns: {dropped_columns}")

        if not filtered_columns:
            raise ValueError(
                f"No usable columns remain for {schema}.{table} after filtering against the destination schema"
            )

        missing_conflict_columns = [c for c in conflict_columns if c not in filtered_columns]
        if missing_conflict_columns:
            raise ValueError(
                f"Conflict columns {missing_conflict_columns} are not present in the filtered "
                f"dataframe for {schema}.{table}"
            )

        prepared = prepared[filtered_columns]

        column_list_sql = ", ".join(filtered_columns)
        update_columns = [c for c in filtered_columns if c not in conflict_columns]
        update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        conflict_sql = ", ".join(conflict_columns)

        temp_table = f"tmp_{table}_{uuid.uuid4().hex[:8]}"

        insert_statement = text(f"""
            INSERT INTO {schema}.{table} ({column_list_sql})
            SELECT {column_list_sql} FROM {temp_table}
            ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}
        """)

        with self.engine.begin() as conn:
            conn.execute(
                text(f"CREATE TEMP TABLE {temp_table} AS SELECT {column_list_sql} FROM {schema}.{table} WHERE FALSE")
            )
            # method="multi" batches many rows per INSERT statement; see
            # TO_SQL_CHUNKSIZE for why 500. UPSERT semantics are untouched --
            # this only speeds up the temp-table staging step.
            prepared.to_sql(
                temp_table,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=TO_SQL_CHUNKSIZE,
            )
            conn.execute(insert_statement)
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        return len(prepared)

    def start_table_load(
        self,
        sync_run_id: str,
        provider: str,
        schema: str,
        table: str,
        rows_input: int,
    ) -> int:
        """Insert a STARTED row into etl.sync_table_loads and return its id."""
        statement = text("""
            INSERT INTO etl.sync_table_loads
                (sync_run_id, provider, schema_name, table_name, rows_input, started_at, status)
            VALUES
                (:sync_run_id, :provider, :schema_name, :table_name, :rows_input, CURRENT_TIMESTAMP, 'STARTED')
            RETURNING id
        """)

        with self.engine.begin() as conn:
            load_id = conn.execute(
                statement,
                {
                    "sync_run_id": sync_run_id,
                    "provider": provider,
                    "schema_name": schema,
                    "table_name": table,
                    "rows_input": rows_input,
                },
            ).scalar_one()

        return load_id

    def finish_table_load(
        self,
        load_id: int,
        status: str,
        rows_loaded: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Update an etl.sync_table_loads row with its final status and counts."""
        statement = text("""
            UPDATE etl.sync_table_loads
            SET status = :status,
                rows_loaded = :rows_loaded,
                finished_at = CURRENT_TIMESTAMP,
                error_message = :error_message
            WHERE id = :load_id
        """)

        with self.engine.begin() as conn:
            conn.execute(
                statement,
                {
                    "load_id": load_id,
                    "status": status,
                    "rows_loaded": rows_loaded,
                    "error_message": error_message,
                },
            )

    def _run_load_plan(
        self,
        load_plan: list[tuple[str, str, pd.DataFrame, list[str]]],
        sync_run_id: str | None,
        provider: str,
    ) -> dict[str, int]:
        """Run a (schema, table, dataframe, conflict_columns) load plan.

        Shared by load_sendem_tables and load_ezytrack_tables so the
        per-table-logging + upsert loop is defined once. If `sync_run_id` is
        given, each table load is separately recorded in
        etl.sync_table_loads (started before the upsert, finished after), so
        a failure partway through leaves a clear per-table record of what
        succeeded and what did not before the exception propagates to the
        caller.

        Returns a dict of "schema.table" -> rows loaded.
        """
        results: dict[str, int] = {}
        for schema, table, df, conflict_columns in load_plan:
            load_id = None
            if sync_run_id is not None:
                load_id = self.start_table_load(sync_run_id, provider, schema, table, len(df))

            try:
                rows_loaded = self.upsert_dataframe(df, schema, table, conflict_columns)
                results[f"{schema}.{table}"] = rows_loaded
                if load_id is not None:
                    self.finish_table_load(load_id, status="SUCCESS", rows_loaded=rows_loaded)
            except Exception as error:
                if load_id is not None:
                    try:
                        self.finish_table_load(load_id, status="FAILED", error_message=str(error))
                    except Exception as bookkeeping_error:
                        logger.error(
                            "Could not mark table load %s as FAILED after %s.%s failed: %s",
                            load_id,
                            schema,
                            table,
                            bookkeeping_error,
                        )
                raise

        return results

    def load_sendem_tables(
        self,
        dataframes: dict[str, pd.DataFrame],
        sync_run_id: str | None = None,
        provider: str = "sendem",
    ) -> dict[str, int]:
        """Load all Sendem raw and staging tables from the given dataframes.

        `dataframes` is expected to contain: assets_df, sites_df, event_desc_df,
        dim_event_types_df, trips_df, events_df, fact_trips, fact_events.

        `event_desc_df` (raw, exactly what the Sendem API returned) feeds
        raw.sendem_event_descriptions. `dim_event_types_df` (event_desc_df
        plus inferred "unknown" rows for event_type_ids seen in fact data but
        missing from the real descriptions) feeds staging.sendem_dim_event_types,
        so fact rows always join cleanly without raw ever being mutated.

        If `sync_run_id` is given, each table load is separately recorded in
        etl.sync_table_loads. See _run_load_plan for details.

        Returns a dict of "schema.table" -> rows loaded.
        """
        load_plan = [
            ("raw", "sendem_assets", dataframes["assets_df"], ["asset_id"]),
            ("raw", "sendem_sites", dataframes["sites_df"], ["site_id"]),
            ("raw", "sendem_event_descriptions", dataframes["event_desc_df"], ["event_type_id"]),
            (
                "raw",
                "sendem_trips_assets_daily",
                dataframes["trips_df"],
                ["date_key", "group_id", "site_id", "asset_id"],
            ),
            (
                "raw",
                "sendem_events_assets_daily",
                dataframes["events_df"],
                ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
            ),
            ("staging", "sendem_dim_assets", dataframes["assets_df"], ["asset_id"]),
            ("staging", "sendem_dim_sites", dataframes["sites_df"], ["site_id"]),
            ("staging", "sendem_dim_event_types", dataframes["dim_event_types_df"], ["event_type_id"]),
            (
                "staging",
                "sendem_fact_trips_daily",
                dataframes["fact_trips"],
                ["date_key", "group_id", "site_id", "asset_id"],
            ),
            (
                "staging",
                "sendem_fact_events_daily",
                dataframes["fact_events"],
                ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
            ),
        ]

        return self._run_load_plan(load_plan, sync_run_id, provider)

    def load_ezytrack_tables(
        self,
        dataframes: dict[str, pd.DataFrame],
        sync_run_id: str | None = None,
        provider: str = "ezytrack",
    ) -> dict[str, int]:
        """Load all EzyTrack raw and staging tables from the given dataframes.

        `dataframes` is expected to contain exactly: raw_assets_df,
        raw_trips_df, dim_assets_df, fact_trips_df (as produced by
        transforms.ezytrack_transform.build_all()). Raises `ValueError` if
        any of these keys are missing -- it does not guess or substitute.

        If `sync_run_id` is given, each table load is separately recorded in
        etl.sync_table_loads. See _run_load_plan for details.

        Returns a dict of "schema.table" -> rows loaded.
        """
        required_keys = ("raw_assets_df", "raw_trips_df", "dim_assets_df", "fact_trips_df")
        missing_keys = [key for key in required_keys if key not in dataframes]
        if missing_keys:
            raise ValueError(f"Missing required EzyTrack dataframe keys: {missing_keys}")

        load_plan = [
            ("raw", "ezytrack_assets", dataframes["raw_assets_df"], ["asset_id"]),
            ("raw", "ezytrack_trips", dataframes["raw_trips_df"], ["trip_id"]),
            ("staging", "ezytrack_dim_assets", dataframes["dim_assets_df"], ["asset_id"]),
            ("staging", "ezytrack_fact_trips", dataframes["fact_trips_df"], ["trip_id"]),
        ]

        return self._run_load_plan(load_plan, sync_run_id, provider)

    def load_trackunit_tables(
        self,
        dataframes: dict[str, pd.DataFrame],
        sync_run_id: str | None = None,
        provider: str = "trackunit",
    ) -> dict[str, int]:
        """Load all Trackunit raw and staging tables from the given dataframes.

        `dataframes` is expected to contain exactly: raw_assets_df,
        raw_operating_hours_df, raw_moving_hours_df, raw_distance_df,
        dim_assets_df, daily_activity_df (as produced by
        transforms.trackunit_transform.build_daily_activity_rows()). Raises
        `ValueError` if any of these keys are missing.

        If `sync_run_id` is given, each table load is separately recorded in
        etl.sync_table_loads. See _run_load_plan for details.

        Returns a dict of "schema.table" -> rows loaded.
        """
        required_keys = (
            "raw_assets_df",
            "raw_operating_hours_df",
            "raw_moving_hours_df",
            "raw_distance_df",
            "dim_assets_df",
            "daily_activity_df",
        )
        missing_keys = [key for key in required_keys if key not in dataframes]
        if missing_keys:
            raise ValueError(f"Missing required Trackunit dataframe keys: {missing_keys}")

        metric_conflict_columns = ["asset_id", "metric_timestamp_utc", "metric_name"]

        load_plan = [
            ("raw", "trackunit_assets", dataframes["raw_assets_df"], ["asset_id"]),
            ("raw", "trackunit_aemp_operating_hours", dataframes["raw_operating_hours_df"], metric_conflict_columns),
            ("raw", "trackunit_aemp_moving_hours", dataframes["raw_moving_hours_df"], metric_conflict_columns),
            ("raw", "trackunit_aemp_distance", dataframes["raw_distance_df"], metric_conflict_columns),
            ("staging", "trackunit_dim_assets", dataframes["dim_assets_df"], ["asset_id"]),
            (
                "staging",
                "trackunit_daily_activity",
                dataframes["daily_activity_df"],
                ["report_date", "asset_id"],
            ),
        ]

        return self._run_load_plan(load_plan, sync_run_id, provider)

    def load_trackunit_location_enrichment(
        self,
        dataframes: dict[str, pd.DataFrame],
        sync_run_id: str | None = None,
        provider: str = "trackunit_location",
    ) -> dict[str, int]:
        """Load Trackunit location-enrichment raw and staging tables.

        Separate from load_trackunit_tables (the working metric ETL) --
        this only ever touches raw.trackunit_aemp_locations,
        raw.trackunit_site_history, raw.trackunit_sites, and
        staging.trackunit_location_enrichment. `dataframes` is expected to
        contain exactly: raw_locations_df, raw_site_history_df, raw_sites_df,
        enrichment_df (as produced by jobs/sync_trackunit_location_enrichment.py).
        Raises `ValueError` if any of these keys are missing.

        If `sync_run_id` is given, each table load is separately recorded in
        etl.sync_table_loads. See _run_load_plan for details.

        Returns a dict of "schema.table" -> rows loaded.
        """
        required_keys = ("raw_locations_df", "raw_site_history_df", "raw_sites_df", "enrichment_df")
        missing_keys = [key for key in required_keys if key not in dataframes]
        if missing_keys:
            raise ValueError(f"Missing required Trackunit location enrichment dataframe keys: {missing_keys}")

        load_plan = [
            ("raw", "trackunit_aemp_locations", dataframes["raw_locations_df"], ["asset_id", "location_timestamp_utc"]),
            ("raw", "trackunit_site_history", dataframes["raw_site_history_df"], ["asset_id", "site_id", "entered_at"]),
            ("raw", "trackunit_sites", dataframes["raw_sites_df"], ["site_id"]),
            (
                "staging",
                "trackunit_location_enrichment",
                dataframes["enrichment_df"],
                ["report_date", "asset_id"],
            ),
        ]

        return self._run_load_plan(load_plan, sync_run_id, provider)

    def replace_accounts_evolution_project_reports(
        self,
        combined_df: pd.DataFrame,
        sync_run_id: str | None = None,
        provider: str = "evolution_project_reports",
    ) -> dict[str, int]:
        """Atomically replace raw.evolution_project_reports with combined_df.

        `combined_df` is expected to be the output of
        transforms.accounts.evolution.project_reports_transform.build_combined
        (already snake_case, combined, and business-unit classified).

        This is a full-refresh load, not an upsert: dbo.vwProjectsReports is
        re-extracted in full every run (no evidenced incremental key), so a
        row removed from the source must also disappear from PostgreSQL --
        an upsert alone leaves it behind. Steps:

        1. validate_combined_for_full_replace() -- refuse outright if the
           extract looks unsafe (empty, missing keys, duplicate keys).
        2. Stage `combined_df` into a freshly created staging table, typed
           from the destination via `CREATE TABLE ... AS SELECT ... WHERE
           1 = 0` so money columns keep their NUMERIC(20, 4) precision.
        3. Verify the staged row count equals `len(combined_df)`.
        4. Inside one transaction: DELETE every row from the destination,
           INSERT ... SELECT the validated staged rows, then commit.
        5. Drop the staging table, on both the success and failure paths.

        Any failure before step 4's transaction commits -- a failed
        validation, a staged-count mismatch, or a database error during the
        swap itself -- leaves the destination table completely untouched;
        the previous successful load remains the queryable production data.
        A mid-transaction failure is rolled back by `engine.begin()` before
        it can be observed by other sessions.

        If `sync_run_id` is given, the load is recorded in
        etl.sync_table_loads (one row covering the whole replace).

        Returns {"raw.evolution_project_reports": rows_loaded} on success.
        """
        validate_combined_for_full_replace(combined_df)

        prepared = prepare_dataframe_for_load(combined_df)
        missing_columns = [c for c in EVOLUTION_PROJECT_REPORTS_COLUMNS if c not in prepared.columns]
        if missing_columns:
            raise ValueError(
                "Combined extract is missing expected raw.evolution_project_reports "
                f"column(s): {missing_columns}"
            )
        prepared = prepared[EVOLUTION_PROJECT_REPORTS_COLUMNS]

        schema, table = "raw", "evolution_project_reports"
        staging_table = f"evolution_project_reports_stage_{uuid.uuid4().hex[:8]}"
        column_list_sql = ", ".join(prepared.columns)

        load_id = None
        if sync_run_id is not None:
            load_id = self.start_table_load(sync_run_id, provider, schema, table, len(prepared))

        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"CREATE TABLE staging.{staging_table} AS "
                        f"SELECT {column_list_sql} FROM {schema}.{table} WHERE 1 = 0"
                    )
                )
                # method="multi" batches many rows per INSERT; see
                # TO_SQL_CHUNKSIZE for why 500.
                prepared.to_sql(
                    staging_table,
                    con=conn,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=TO_SQL_CHUNKSIZE,
                )

            with self.engine.connect() as conn:
                staged_count = conn.execute(text(f"SELECT COUNT(*) FROM staging.{staging_table}")).scalar_one()

            if staged_count != len(prepared):
                raise RuntimeError(
                    f"Staged row count ({staged_count}) does not match the transformed "
                    f"DataFrame row count ({len(prepared)}) for {schema}.{table}; "
                    "refusing to replace the destination table"
                )

            with self.engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {schema}.{table}"))
                conn.execute(
                    text(
                        f"INSERT INTO {schema}.{table} ({column_list_sql}) "
                        f"SELECT {column_list_sql} FROM staging.{staging_table}"
                    )
                )

            rows_loaded = len(prepared)
            if load_id is not None:
                self.finish_table_load(load_id, status="SUCCESS", rows_loaded=rows_loaded)
            return {f"{schema}.{table}": rows_loaded}

        except Exception as error:
            if load_id is not None:
                try:
                    self.finish_table_load(load_id, status="FAILED", error_message=str(error))
                except Exception as bookkeeping_error:
                    logger.error(
                        "Could not mark table load %s as FAILED after the %s.%s replace failed: %s",
                        load_id,
                        schema,
                        table,
                        bookkeeping_error,
                    )
            raise
        finally:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS staging.{staging_table}"))
            except Exception as cleanup_error:
                logger.error(
                    "Could not drop staging table staging.%s: %s", staging_table, cleanup_error
                )

    def start_sync_run(
        self,
        source_system: str,
        job_name: str,
        start_date: int | None,
        end_date: int | None,
    ) -> str:
        """Insert a STARTED row into etl.sync_runs and return the new sync_run_id."""
        sync_run_id = str(uuid.uuid4())

        statement = text("""
            INSERT INTO etl.sync_runs (sync_run_id, source_system, job_name, start_date, end_date, status)
            VALUES (:sync_run_id, :source_system, :job_name, :start_date, :end_date, 'STARTED')
        """)

        with self.engine.begin() as conn:
            conn.execute(
                statement,
                {
                    "sync_run_id": sync_run_id,
                    "source_system": source_system,
                    "job_name": job_name,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

        return sync_run_id

    def finish_sync_run(
        self,
        sync_run_id: str,
        status: str,
        rows_fetched: int = 0,
        rows_loaded: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Update an etl.sync_runs row with its final status and counts."""
        statement = text("""
            UPDATE etl.sync_runs
            SET status = :status,
                finished_at = CURRENT_TIMESTAMP,
                rows_fetched = :rows_fetched,
                rows_loaded = :rows_loaded,
                error_message = :error_message
            WHERE sync_run_id = :sync_run_id
        """)

        with self.engine.begin() as conn:
            conn.execute(
                statement,
                {
                    "sync_run_id": sync_run_id,
                    "status": status,
                    "rows_fetched": rows_fetched,
                    "rows_loaded": rows_loaded,
                    "error_message": error_message,
                },
            )

    def run_post_load_validation(
        self,
        provider: str,
        *,
        mode: str = "warn",
        lookback_hours: int = 24,
    ) -> list[dict[str, object]]:
        """Run bounded provider checks after a load.

        ``mode`` controls production behavior:

        * ``off`` logs that validation was skipped.
        * ``warn`` logs findings/query errors without failing the sync.
        * ``strict`` raises on a critical finding or a critical-check query
          error. Informational checks remain non-blocking in every mode.

        Results are returned for tests and operational callers. The complete
        full-history validation packs in ``sql/`` remain the manual recovery
        and reconciliation tool; this method intentionally checks only rows
        updated during the configured recent window.
        """
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"off", "warn", "strict"}:
            raise ValueError(f"Unsupported validation mode: {mode!r}")
        if lookback_hours < 1:
            raise ValueError(f"lookback_hours must be at least 1, got {lookback_hours}")
        if normalized_mode == "off":
            logger.info("Post-load validation disabled for provider %s", provider)
            return []

        checks = POST_LOAD_VALIDATION_QUERIES.get(provider)
        if checks is None:
            message = f"No post-load validation checks are registered for provider {provider!r}"
            if normalized_mode == "strict":
                raise ValueError(message)
            logger.warning(message)
            return []

        results: list[dict[str, object]] = []
        for check_name, query, critical in checks:
            try:
                with self.engine.connect() as conn:
                    issue_count = int(
                        conn.execute(text(query), {"lookback_hours": lookback_hours}).scalar_one()
                    )
            except Exception as validation_error:
                message = f"Post-load validation query failed for {provider} ({check_name}): {validation_error}"
                logger.exception(message)
                if normalized_mode == "strict" and critical:
                    raise RuntimeError(message) from validation_error
                results.append(
                    {"check": check_name, "critical": critical, "status": "ERROR", "issue_count": None}
                )
                continue

            status = "PASS" if issue_count == 0 else "FAIL"
            result = {
                "check": check_name,
                "critical": critical,
                "status": status,
                "issue_count": issue_count,
            }
            results.append(result)
            if issue_count:
                message = (
                    f"Post-load validation failed for {provider}: {check_name} "
                    f"found {issue_count} issue(s) in the last {lookback_hours} hour(s)"
                )
                if critical:
                    logger.error(message)
                    if normalized_mode == "strict":
                        raise RuntimeError(message)
                else:
                    logger.warning(message)
            else:
                logger.info("Post-load validation passed for %s: %s", provider, check_name)

        return results

    def get_last_successful_run(self, source_system: str) -> dict | None:
        """Return the most recent SUCCESS sync run for `source_system`, or None.

        The dict has keys sync_run_id, job_name, started_at, finished_at.
        `started_at` is the reliable proxy for that run's fetch-window end
        (jobs anchor their window at "now" immediately before starting the
        run row), which is what EzyTrack gap recovery needs.
        """
        statement = text("""
            SELECT sync_run_id, job_name, started_at, finished_at
            FROM etl.sync_runs
            WHERE source_system = :source_system AND status = 'SUCCESS'
            ORDER BY started_at DESC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            row = conn.execute(statement, {"source_system": source_system}).mappings().first()

        return dict(row) if row else None

    def mark_abandoned_runs(self, threshold_hours: int, now_utc: datetime | None = None) -> list[dict]:
        """Mark STARTED runs older than `threshold_hours` as ABANDONED.

        Returns the runs that were marked (sync_run_id, source_system,
        job_name, started_at) so callers can log/alert on each. Callers are
        responsible for making sure no marked run is still genuinely active.
        The housekeeping sensor/op in orchestration/monitoring.py maps each
        candidate to its provider jobs and defers the eligible batch when a
        corresponding Dagster run is still active.
        """
        cutoff = compute_abandoned_cutoff(threshold_hours, now_utc)

        statement = text("""
            UPDATE etl.sync_runs
            SET status = 'ABANDONED',
                finished_at = CURRENT_TIMESTAMP,
                error_message = COALESCE(error_message, '')
                    || :note
            WHERE status = 'STARTED' AND started_at < :cutoff
            RETURNING sync_run_id, source_system, job_name, started_at
        """)

        note = f" [Marked ABANDONED by housekeeping: STARTED for more than {threshold_hours}h with no finish]"

        with self.engine.begin() as conn:
            rows = conn.execute(statement, {"cutoff": cutoff, "note": note}).mappings().all()

        marked = [dict(row) for row in rows]
        for run in marked:
            logger.warning(
                "Marked sync run %s (%s / %s, started %s) as ABANDONED",
                run["sync_run_id"],
                run["source_system"],
                run["job_name"],
                run["started_at"],
            )
        return marked


def compute_abandoned_cutoff(threshold_hours: int, now_utc: datetime | None = None) -> datetime:
    """Return the UTC cutoff before which a STARTED run counts as abandoned."""
    if threshold_hours < 1:
        raise ValueError(f"threshold_hours must be at least 1, got {threshold_hours}")
    now = now_utc if now_utc is not None else utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now - timedelta(hours=threshold_hours)
