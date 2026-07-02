"""Loads telemetry provider dataframes into PostgreSQL.

This module owns all database writes: connection creation, the generic
strict upsert (upsert_dataframe), the per-provider raw/staging table load
plans (load_sendem_tables, load_ezytrack_tables), and etl.sync_runs /
etl.sync_table_loads tracking.
"""

from __future__ import annotations

import re
import uuid
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import Settings


def to_snake_case(name: str) -> str:
    """Convert a PascalCase/camelCase identifier to snake_case."""
    step1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    step2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", step1)
    return step2.lower()


class PostgresLoader:
    """Loads Sendem dataframes into the telemetry_warehouse PostgreSQL database."""

    def __init__(self, settings: Settings) -> None:
        """Create and store a SQLAlchemy engine from `settings`."""
        password = quote_plus(settings.postgres_password)
        connection_string = (
            f"postgresql+psycopg2://{settings.postgres_user}:{password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        self.engine: Engine = create_engine(connection_string)

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

        prepared = df.rename(columns={col: to_snake_case(col) for col in df.columns})
        prepared = prepared.where(pd.notnull(prepared), None)
        if "loaded_at" not in prepared.columns:
            prepared["loaded_at"] = pd.Timestamp.now()

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
            prepared.to_sql(temp_table, con=conn, if_exists="append", index=False)
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
                    self.finish_table_load(load_id, status="FAILED", error_message=str(error))
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

    def start_sync_run(
        self,
        source_system: str,
        job_name: str,
        start_date: int,
        end_date: int,
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
