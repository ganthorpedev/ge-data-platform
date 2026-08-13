-- raw_sendem: source-faithful Sendem/MiX raw tables.
--
-- Mirrors the legacy telemetry_warehouse raw.sendem_* tables 1:1 in shape
-- (columns, types, keys) -- this is a schema-location and naming migration,
-- not a redesign. Column types match the CURRENT live raw.sendem_* shape
-- (sql/legacy/telemetry_migrations/001_create_sendem_schema.sql), not the
-- older sql/legacy/telemetry_migrations/sendem_tables.sql shape that
-- clean.* was built from -- see docs/migration/legacy-to-platform-migration.md
-- for the full legacy -> platform object mapping and why clean.* has no
-- raw_sendem counterpart (it is staging-grade, not raw-shaped, data).
--
-- Table names drop the "sendem_" prefix (schema already identifies the
-- source), matching the Trackunit precedent:
--   raw.sendem_assets              -> raw_sendem.asset
--   raw.sendem_sites                -> raw_sendem.site
--   raw.sendem_event_descriptions   -> raw_sendem.event_description
--   raw.sendem_trips_assets_daily   -> raw_sendem.trip_daily
--   raw.sendem_events_assets_daily  -> raw_sendem.event_daily
--
-- asset_id/site_id/event_type_id/group_id stay BIGINT (not TEXT): Sendem's
-- API returns these as large signed integers that arrive as int64 in
-- pandas -- see the identical note in 001_create_sendem_schema.sql, which
-- this migration is a faithful port of. date_key stays INTEGER (YYYYMMDD)
-- for the same documented reason.
--
-- Grants: raw_sendem already has default privileges granting
-- ge_platform_admin/ge_etl access to all current and future tables (set in
-- 003_create_platform_roles.sql) -- no additional GRANT statements needed
-- here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw_sendem;

-- =============================================================================
-- raw_sendem.asset
-- Source: GET /api/dimensions/assets/{group_id}. One row per Sendem asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.asset (
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
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_sendem.asset IS
    'One row per Sendem asset, as returned by GET /api/dimensions/assets/{group_id}. Source-faithful; see ge_data_platform.sources.sendem.transform.to_dataframe / ASSET_COLUMNS.';

-- =============================================================================
-- raw_sendem.site
-- Source: GET /api/dimensions/sites/{group_id}. No group_id column -- the
-- Sendem sites endpoint does not return one (confirmed in transform.py and
-- the live raw.sendem_sites catalog).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.site (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_sendem.site IS
    'One row per Sendem site, as returned by GET /api/dimensions/sites/{group_id}. No group_id -- the API does not return one for this endpoint.';

-- =============================================================================
-- raw_sendem.event_description
-- Source: GET /api/dimensions/event-descriptions/{group_id}.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.event_description (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_sendem.event_description IS
    'One row per Sendem event type, as returned by GET /api/dimensions/event-descriptions/{group_id}.';

-- =============================================================================
-- raw_sendem.trip_daily
-- Source: GET /api/aggregates/trips/assets/{group_id}?startDate=&endDate=.
-- Grain: one row per (date_key, group_id, site_id, asset_id) -- pre-aggregated
-- daily totals returned directly by the API (no per-trip fetch).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.trip_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    date DATE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

COMMENT ON TABLE raw_sendem.trip_daily IS
    'Pre-aggregated daily trip totals per (date_key, group_id, site_id, asset_id), from GET /api/aggregates/trips/assets/{group_id}. Only what the live rolling-window sync has fetched -- historical rows from legacy clean.sendem_fact_trips_daily are staging-grade, not raw-shaped, and live in stg_sendem.trip_daily instead. See docs/migration/legacy-to-platform-migration.md.';

-- =============================================================================
-- raw_sendem.event_daily
-- Source: GET /api/aggregates/events/assets/{group_id}?startDate=&endDate=.
-- Grain: one row per (date_key, group_id, site_id, asset_id, event_type_id).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.event_daily (
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
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);

COMMENT ON TABLE raw_sendem.event_daily IS
    'Pre-aggregated daily event totals per (date_key, group_id, site_id, asset_id, event_type_id), from GET /api/aggregates/events/assets/{group_id}. Only what the live rolling-window sync has fetched -- see raw_sendem.trip_daily comment for why legacy clean.* history is not mirrored here.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('007_create_raw_sendem.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
