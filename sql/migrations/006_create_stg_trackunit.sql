-- stg_trackunit: cleaned, typed Trackunit/Manitou staging tables.
--
-- Mirrors legacy telemetry_warehouse staging.trackunit_* in shape, with one
-- deliberate improvement: stg_trackunit.daily_activity is built with the
-- counter-reset quality columns (counter_reset_detected, data_quality_status)
-- present and enforced from the start, rather than needing a later ALTER
-- TABLE + backfill migration the way legacy migrations 020 and 027 did. The
-- legacy telemetry_warehouse database never actually had 027 applied (see
-- docs/migration/legacy-to-platform-migration.md), so this is the first
-- place these columns exist as real, populated data on this platform.
--
-- Table names drop the "trackunit_" prefix (schema already identifies the
-- source):
--   staging.trackunit_dim_assets          -> stg_trackunit.asset
--   staging.trackunit_daily_activity      -> stg_trackunit.daily_activity
--   staging.trackunit_location_enrichment -> stg_trackunit.location_enrichment
--
-- Grants: stg_trackunit already has default privileges for
-- ge_platform_admin/ge_etl from 003_create_platform_roles.sql -- no
-- additional GRANT statements needed here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS stg_trackunit;

-- =============================================================================
-- stg_trackunit.asset
-- Clean asset master. Column-for-column mirror of raw_trackunit.asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_trackunit.asset (
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

COMMENT ON TABLE stg_trackunit.asset IS
    'Clean Trackunit asset master, one row per asset_id.';

-- =============================================================================
-- stg_trackunit.daily_activity
-- Grain: one row per (report_date, asset_id) -- the proven Activity Report
-- calculation (work day / operating hours / active driving / distance). See
-- ge_data_platform.sources.trackunit.transform.build_daily_activity_rows().
--
-- A machine with no operating-hours and no moving-hours points for the day
-- gets a valid zero row (work_day_minutes/operating_minutes/
-- active_driving_minutes = 0, start/stop_time_utc = NULL) -- not an error.
--
-- counter_reset_detected/data_quality_status: any consecutive decrease in a
-- cumulative operating/moving/distance reading within the report day nulls
-- only that metric's derived value (never clamps to zero) and marks the row
-- data_quality_status='COUNTER_RESET'. Raw readings are never altered --
-- this is purely a derived-metric and quality-label concern. See
-- docs/operations/data-quality.md and docs/sources/trackunit.md.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_trackunit.daily_activity (
    report_date DATE NOT NULL,
    asset_id TEXT NOT NULL,
    machine TEXT,
    pin TEXT,
    machine_serial_number TEXT,
    brand TEXT,
    machine_type TEXT,
    model TEXT,
    start_time_utc TIMESTAMPTZ,
    stop_time_utc TIMESTAMPTZ,
    start_time_local TIMESTAMP,
    stop_time_local TIMESTAMP,
    work_day_minutes INTEGER,
    work_day_hhmm TEXT,
    operating_minutes INTEGER,
    operating_hhmm TEXT,
    active_driving_minutes INTEGER,
    active_driving_hhmm TEXT,
    distance_km NUMERIC,
    operating_points INTEGER,
    moving_points INTEGER,
    distance_points INTEGER,
    source_provider TEXT NOT NULL DEFAULT 'trackunit',
    counter_reset_detected BOOLEAN NOT NULL DEFAULT FALSE,
    data_quality_status TEXT NOT NULL DEFAULT 'live',
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT daily_activity_nonnegative_counters CHECK (
        (operating_minutes IS NULL OR operating_minutes >= 0)
        AND (active_driving_minutes IS NULL OR active_driving_minutes >= 0)
        AND (distance_km IS NULL OR distance_km >= 0)
    ),
    PRIMARY KEY (report_date, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_activity_asset
    ON stg_trackunit.daily_activity (asset_id);

COMMENT ON TABLE stg_trackunit.daily_activity IS
    'One row per Trackunit machine per local report date, with counter-reset-aware derived metrics.';
COMMENT ON COLUMN stg_trackunit.daily_activity.counter_reset_detected IS
    'True when any cumulative operating, moving, or distance counter decreased within the report-day series.';
COMMENT ON COLUMN stg_trackunit.daily_activity.data_quality_status IS
    'live for normal rows; COUNTER_RESET when a derived cumulative metric was nulled after a counter decrease.';

-- =============================================================================
-- stg_trackunit.location_enrichment
-- Grain: one row per (report_date, asset_id) -- same grain and same primary
-- key as stg_trackunit.daily_activity. Address/zip/city/country columns are
-- present but always NULL in V1 (no reverse-geocoding source) -- see
-- docs/sources/trackunit.md.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_trackunit.location_enrichment (
    report_date DATE NOT NULL,
    asset_id TEXT NOT NULL,

    zone_name_start TEXT,
    zone_name_stop TEXT,

    last_known_start_location_timestamp_utc TIMESTAMPTZ,
    last_known_start_location_timestamp_local TIMESTAMP,
    start_latitude NUMERIC,
    start_longitude NUMERIC,
    start_altitude NUMERIC,

    last_known_stop_location_timestamp_utc TIMESTAMPTZ,
    last_known_stop_location_timestamp_local TIMESTAMP,
    stop_latitude NUMERIC,
    stop_longitude NUMERIC,
    stop_altitude NUMERIC,

    -- Always NULL in V1 -- see docs/sources/trackunit.md.
    address_start TEXT,
    zip_start TEXT,
    city_start TEXT,
    country_start TEXT,
    address_stop TEXT,
    zip_stop TEXT,
    city_stop TEXT,
    country_stop TEXT,

    -- One of: NOT_APPLICABLE_ZERO_ACTIVITY, ENRICHED, PARTIAL,
    -- SITE_ACCESS_DENIED, NOT_FOUND. Never silently blank.
    location_enrichment_status TEXT NOT NULL,
    location_enriched_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (report_date, asset_id)
);

COMMENT ON TABLE stg_trackunit.location_enrichment IS
    'One row per (report_date, asset_id): start/stop zone and coordinate enrichment for stg_trackunit.daily_activity rows with detected boundaries.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('006_create_stg_trackunit.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
