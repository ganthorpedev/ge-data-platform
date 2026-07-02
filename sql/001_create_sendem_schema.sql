-- Sendem/MiX schema, derived directly from the current production code:
--   transforms/sendem_transform.py  (exact DataFrame columns produced)
--   loaders/postgres_loader.py      (exact schema.table names and conflict keys)
--
-- Rerunnable: safe to execute against an empty or partially-created database.
--
-- NOTE ON DIVERGENCE FROM THE OLD DISCOVERY-NOTEBOOK SCHEMA:
--   1. sendem_assets / sendem_dim_assets now include site_id.
--      transforms/sendem_transform.py's to_dataframe() does a raw
--      pd.json_normalize() for assets_df with no column selection, so
--      site_id (returned by the Sendem API on the assets dimension) is
--      still present when assets_df reaches the loader. The notebook used
--      to strip it; the current production transform does not.
--   2. sendem_trips_assets_daily / sendem_events_assets_daily now include a
--      "date" column. build_all() calls add_date_column() on trips_df and
--      events_df *before* they are handed to load_sendem_tables for the raw
--      tables, so "date" (not just date_key) is part of what actually gets
--      inserted.
--   3. External IDs (asset_id, site_id, event_type_id, group_id) are BIGINT,
--      not TEXT, even though "TEXT for external IDs" was requested. The
--      Sendem API returns these as large signed integers (observed range
--      roughly -9.0e18 to 9.96e17, well inside BIGINT's +-9.22e18 range),
--      and they arrive in pandas as int64. Loading an int64 column into a
--      TEXT destination via SQLAlchemy/psycopg2 is not guaranteed to work
--      (parameter type inference can raise "column is of type text but
--      expression is of type bigint"), whereas BIGINT is exactly what the
--      original discovery notebook used and proved end-to-end. Flagging
--      this instead of silently applying a type that could break inserts.
--   4. date_key stays INTEGER. The code always produces it as a YYYYMMDD
--      integer (e.g. 20260630) via json normalization, never as an actual
--      SQL date value, so a DATE column here would fail on every insert
--      ("invalid input syntax for type date"). This is documented as a
--      known mismatch rather than silently "fixed" by picking a type that
--      breaks the pipeline.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS etl;

-- ---------------------------------------------------------------------------
-- raw: source-specific, close to the original API shape
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.sendem_assets (
    asset_id BIGINT PRIMARY KEY,
    site_id BIGINT,
    asset_type TEXT,
    description TEXT,
    vin_number TEXT,
    country TEXT,
    group_id BIGINT,
    registration_number TEXT,
    is_available BOOLEAN,
    fleet_number TEXT,
    make TEXT,
    model TEXT,
    fuel_type TEXT,
    year INTEGER,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sendem_sites (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sendem_event_descriptions (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sendem_trips_assets_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    date DATE,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

CREATE TABLE IF NOT EXISTS raw.sendem_events_assets_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    event_type_id BIGINT NOT NULL,
    total_event_occurrences NUMERIC,
    min_event_value NUMERIC,
    max_event_value NUMERIC,
    total_event_value NUMERIC,
    min_event_duration NUMERIC,
    max_event_duration NUMERIC,
    total_event_duration NUMERIC,
    date DATE,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);

-- ---------------------------------------------------------------------------
-- staging: source-specific, cleaned/enriched
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.sendem_dim_assets (
    asset_id BIGINT PRIMARY KEY,
    site_id BIGINT,
    asset_type TEXT,
    description TEXT,
    vin_number TEXT,
    country TEXT,
    group_id BIGINT,
    registration_number TEXT,
    is_available BOOLEAN,
    fleet_number TEXT,
    make TEXT,
    model TEXT,
    fuel_type TEXT,
    year INTEGER,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.sendem_dim_sites (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.sendem_dim_event_types (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.sendem_fact_trips_daily (
    date DATE,
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    site_name TEXT,
    asset_id BIGINT NOT NULL,
    fleet_number TEXT,
    registration_number TEXT,
    description TEXT,
    make TEXT,
    model TEXT,
    asset_type TEXT,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

CREATE TABLE IF NOT EXISTS staging.sendem_fact_events_daily (
    date DATE,
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    site_name TEXT,
    asset_id BIGINT NOT NULL,
    fleet_number TEXT,
    registration_number TEXT,
    description TEXT,
    make TEXT,
    model TEXT,
    asset_type TEXT,
    event_type_id BIGINT NOT NULL,
    event_name TEXT,
    event_category TEXT,
    metric_type TEXT,
    unit_type TEXT,
    total_event_occurrences NUMERIC,
    min_event_value NUMERIC,
    max_event_value NUMERIC,
    total_event_value NUMERIC,
    min_event_duration NUMERIC,
    max_event_duration NUMERIC,
    total_event_duration NUMERIC,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);

-- ---------------------------------------------------------------------------
-- etl: pipeline run tracking (used by jobs/sync_sendem.py via
-- PostgresLoader.start_sync_run / finish_sync_run)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.sync_runs (
    sync_run_id UUID PRIMARY KEY,
    source_system TEXT NOT NULL,
    job_name TEXT NOT NULL,
    start_date INTEGER,
    end_date INTEGER,
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_fetched INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    error_message TEXT
);
