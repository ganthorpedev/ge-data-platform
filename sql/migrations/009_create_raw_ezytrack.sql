-- raw_ezytrack: source-faithful EzyTrack/Telematics Guru raw tables.
--
-- Mirrors the legacy telemetry_warehouse raw.ezytrack_* tables 1:1 in shape
-- (columns, types, keys) -- this is a schema-location and naming migration,
-- not a redesign. Phase-1 scope only (assets + trips), matching the legacy
-- table set exactly -- see sql/legacy/telemetry_migrations/010_create_ezytrack_schema.sql
-- and docs/sources/ezytrack.md. No clean/historical-only schema exists for
-- EzyTrack (unlike Sendem's clean.*) -- raw+staging is the complete legacy
-- object set.
--
-- Table names drop the "ezytrack_" prefix (schema already identifies the
-- source), matching the Trackunit/Sendem precedent:
--   raw.ezytrack_assets -> raw_ezytrack.asset
--   raw.ezytrack_trips  -> raw_ezytrack.trip
--
-- Grants: raw_ezytrack already has default privileges granting
-- ge_platform_admin/ge_etl access to all current and future tables (set in
-- 003_create_platform_roles.sql) -- no additional GRANT statements needed
-- here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw_ezytrack;

-- =============================================================================
-- raw_ezytrack.asset
-- Source: EzyTrack GraphQL ASSETS_QUERY. One row per asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_ezytrack.asset (
    asset_id BIGINT PRIMARY KEY,
    asset_code TEXT,
    asset_name TEXT,
    asset_description TEXT,
    department_name TEXT,
    project_name TEXT,
    allocated_driver_name TEXT,
    allocated_driver_code TEXT,
    current_geofence_name TEXT,
    is_enabled BOOLEAN,
    last_connected_utc TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_ezytrack.asset IS
    'One row per EzyTrack asset, as returned by GraphQL ASSETS_QUERY. Source-faithful; see ge_data_platform.sources.ezytrack.transform._extract_raw_asset.';

-- =============================================================================
-- raw_ezytrack.trip
-- Source: EzyTrack GraphQL TRIPS_QUERY. One row per trip.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_ezytrack.trip (
    trip_id BIGINT PRIMARY KEY,
    asset_id BIGINT,
    start_time_utc TIMESTAMPTZ,
    end_time_utc TIMESTAMPTZ,
    duration_seconds INTEGER,
    distance_meters NUMERIC,
    stop_time_seconds INTEGER,
    idle_time_seconds INTEGER,
    start_odometer_meters NUMERIC,
    start_run_seconds INTEGER,
    driver_name TEXT,
    driver_code TEXT,
    start_geofence_name TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_ezytrack_trip_asset
    ON raw_ezytrack.trip (asset_id);
CREATE INDEX IF NOT EXISTS idx_raw_ezytrack_trip_start_time
    ON raw_ezytrack.trip (start_time_utc);

COMMENT ON TABLE raw_ezytrack.trip IS
    'One row per EzyTrack trip, as returned by GraphQL TRIPS_QUERY. Source-faithful; see ge_data_platform.sources.ezytrack.transform._extract_raw_trip. end_time_utc/stop_time_seconds legitimately NULL for still-open trips.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('009_create_raw_ezytrack.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
