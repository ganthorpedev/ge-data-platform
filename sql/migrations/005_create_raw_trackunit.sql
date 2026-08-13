-- raw_trackunit: source-faithful Trackunit/Manitou raw tables.
--
-- Mirrors the legacy telemetry_warehouse raw.trackunit_* tables 1:1 in
-- shape (columns, types, keys) -- this is a schema-location and naming
-- migration, not a redesign. See docs/migration/legacy-to-platform-migration.md
-- for the full legacy -> platform object mapping and the historical backfill
-- procedure. asset_id/pin/serial columns stay TEXT (Trackunit ids are
-- UUID-formatted strings, PINs/serials are alphanumeric) -- no
-- parsing/range assumptions.
--
-- Table names drop the "trackunit_" prefix the legacy raw/staging tables
-- needed (schema was shared across providers there); raw_trackunit already
-- identifies the source, so table names name the entity only:
--   raw.trackunit_assets              -> raw_trackunit.asset
--   raw.trackunit_aemp_operating_hours -> raw_trackunit.aemp_operating_hour
--   raw.trackunit_aemp_moving_hours    -> raw_trackunit.aemp_moving_hour
--   raw.trackunit_aemp_distance        -> raw_trackunit.aemp_distance
--   raw.trackunit_aemp_locations       -> raw_trackunit.aemp_location
--   raw.trackunit_site_history         -> raw_trackunit.site_history
--   raw.trackunit_sites                -> raw_trackunit.site
--
-- Grants: raw_trackunit already has default privileges granting
-- ge_platform_admin/ge_etl access to all current and future tables (set in
-- 003_create_platform_roles.sql, scoped to the role that runs migrations) --
-- no additional GRANT statements are needed here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw_trackunit;

-- =============================================================================
-- raw_trackunit.asset
-- Source: GET /api/asset/v1/assets. One row per Trackunit asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_trackunit.asset (
    asset_id TEXT PRIMARY KEY,
    name TEXT,
    external_reference TEXT,
    serial_number TEXT,
    asset_type TEXT,
    type TEXT,
    brand TEXT,
    model TEXT,
    production_year INTEGER,
    owner_account_id TEXT,
    created_at TIMESTAMPTZ,
    telematics_device_id TEXT,
    telematics_device_serial TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_trackunit.asset IS
    'One row per Trackunit asset, as returned by GET /api/asset/v1/assets. Source-faithful; see ge_data_platform.sources.trackunit.transform._extract_raw_asset.';

-- =============================================================================
-- raw_trackunit.aemp_operating_hour / raw_trackunit.aemp_moving_hour /
-- raw_trackunit.aemp_distance
-- Source: AEMP time-series, {AEMP_BASE_URL}/{account}/Fleet/Equipment/ID/
-- {pin}/{CumulativeOperatingHours|CumulativeMovingHours|Distance}/{start}/{end}/{page}.
-- One row per (asset, timestamp) data point. metric_unit is only populated
-- for Distance (e.g. "KILOMETRE"); NULL for the hours metrics.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_trackunit.aemp_operating_hour (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

CREATE TABLE IF NOT EXISTS raw_trackunit.aemp_moving_hour (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

CREATE TABLE IF NOT EXISTS raw_trackunit.aemp_distance (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_aemp_operating_hour_asset_ts
    ON raw_trackunit.aemp_operating_hour (asset_id, metric_timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_aemp_moving_hour_asset_ts
    ON raw_trackunit.aemp_moving_hour (asset_id, metric_timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_aemp_distance_asset_ts
    ON raw_trackunit.aemp_distance (asset_id, metric_timestamp_utc);

COMMENT ON TABLE raw_trackunit.aemp_operating_hour IS
    'AEMP CumulativeOperatingHours time-series, one row per (asset, timestamp) point actually fetched.';
COMMENT ON TABLE raw_trackunit.aemp_moving_hour IS
    'AEMP CumulativeMovingHours time-series, one row per (asset, timestamp) point actually fetched.';
COMMENT ON TABLE raw_trackunit.aemp_distance IS
    'AEMP Distance (odometer) time-series, one row per (asset, timestamp) point actually fetched.';

-- =============================================================================
-- raw_trackunit.aemp_location
-- Source: AEMP time-series Locations, one row per (asset, timestamp) point
-- actually fetched during location enrichment (a 48h lookback window per
-- boundary, so the same point can be re-fetched across runs -- upserted,
-- not duplicated). boundary_type records which boundary's lookback window
-- this point was fetched for, for traceability.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_trackunit.aemp_location (
    asset_id TEXT NOT NULL,
    location_timestamp_utc TIMESTAMPTZ NOT NULL,
    latitude NUMERIC,
    longitude NUMERIC,
    altitude NUMERIC,
    altitude_units TEXT,
    boundary_type TEXT,
    report_date DATE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, location_timestamp_utc)
);

COMMENT ON TABLE raw_trackunit.aemp_location IS
    'AEMP historical Locations time-series points fetched during location enrichment V1. No address/zip/city/country -- latitude/longitude/altitude/datetime only.';

-- =============================================================================
-- raw_trackunit.site_history
-- Source: GET /api/site/v1/sites/history. One row per (asset, site, entry)
-- site-assignment interval. left_at NULL means still open/current as of
-- when it was fetched.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_trackunit.site_history (
    asset_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    entered_at TIMESTAMPTZ NOT NULL,
    left_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, site_id, entered_at)
);

COMMENT ON TABLE raw_trackunit.site_history IS
    'One row per (asset, site, entry) site-assignment interval, from GET /api/site/v1/sites/history.';

-- =============================================================================
-- raw_trackunit.site
-- Source: GET /api/site/v1/sites/{siteId}. One row per site actually looked
-- up during enrichment (not the full sites master -- resolved on demand and
-- cached per run). polygon_geojson is the raw polygon.geometry as text.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_trackunit.site (
    site_id TEXT PRIMARY KEY,
    site_name TEXT,
    city TEXT,
    country TEXT,
    polygon_geojson TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_trackunit.site IS
    'One row per Trackunit site actually looked up during location enrichment, from GET /api/site/v1/sites/{siteId}.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('005_create_raw_trackunit.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
