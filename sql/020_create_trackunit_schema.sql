-- Trackunit / Manitou schema: assets + daily activity (operating hours,
-- active driving, distance) via the AEMP time-series API.
--
-- Lives in the shared telemetry_warehouse schemas (raw/staging/warehouse/etl),
-- the same schemas Sendem and EzyTrack use. Column shapes are taken directly
-- from the proven exploration notebook
-- (Manitou/manitou_trackunit_exploration.ipynb) and transforms/trackunit_transform.py.
--
-- asset_id/pin/serial columns are TEXT, not BIGINT/UUID: Trackunit asset ids
-- are UUID-formatted strings (e.g. "00000000-0000-0000-0000-000000829017")
-- and PINs/serials are alphanumeric (e.g. "MAN00000V00883846") -- TEXT avoids
-- any parsing/range assumptions.
--
-- Rerunnable: CREATE SCHEMA IF NOT EXISTS / CREATE TABLE IF NOT EXISTS
-- throughout. Never truncates or deletes existing rows.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS etl;

-- =============================================================================
-- raw.trackunit_assets
-- Source: GET /api/asset/v1/assets (id, name, externalReference,
-- serialNumber, assetType, type, brand, model, productionYear,
-- ownerAccountId, createdAt, telematicsDevices[0]{id, serialNumber})
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.trackunit_assets (
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
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- raw.trackunit_aemp_operating_hours / raw.trackunit_aemp_moving_hours /
-- raw.trackunit_aemp_distance
-- Source: AEMP time-series, {AEMP_BASE_URL}/{account}/Fleet/Equipment/ID/
-- {pin}/{CumulativeOperatingHours|CumulativeMovingHours|Distance}/{start}/{end}/{page}
-- One row per (asset, timestamp) data point. metric_unit is only populated
-- for Distance (e.g. "KILOMETRE"); NULL for the hours metrics.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.trackunit_aemp_operating_hours (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

CREATE TABLE IF NOT EXISTS raw.trackunit_aemp_moving_hours (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

CREATE TABLE IF NOT EXISTS raw.trackunit_aemp_distance (
    asset_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_timestamp_utc TIMESTAMPTZ NOT NULL,
    metric_value NUMERIC,
    metric_unit TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (asset_id, metric_timestamp_utc, metric_name)
);

-- =============================================================================
-- staging.trackunit_dim_assets
-- Clean asset master. Mirrors raw.trackunit_assets column-for-column, same
-- pattern as staging.ezytrack_dim_assets / staging.sendem_dim_assets.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.trackunit_dim_assets (
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
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- staging.trackunit_daily_activity
-- One row per (report_date, asset_id): the proven Activity Report
-- calculation (work day / operating hours / active driving / distance).
-- See transforms/trackunit_transform.py:build_daily_activity_rows().
--
-- A machine with no operating-hours and no moving-hours points for the day
-- gets a valid zero row (work_day_minutes/operating_minutes/
-- active_driving_minutes = 0, start/stop_time_utc = NULL) -- this is not an
-- error condition.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.trackunit_daily_activity (
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
    source_provider TEXT DEFAULT 'trackunit',
    counter_reset_detected BOOLEAN NOT NULL DEFAULT FALSE,
    data_quality_status TEXT NOT NULL DEFAULT 'live',
    loaded_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT trackunit_daily_activity_nonnegative_counters CHECK (
        (operating_minutes IS NULL OR operating_minutes >= 0)
        AND (active_driving_minutes IS NULL OR active_driving_minutes >= 0)
        AND (distance_km IS NULL OR distance_km >= 0)
    ),
    PRIMARY KEY (report_date, asset_id)
);

-- CREATE TABLE IF NOT EXISTS does not evolve an already-existing production
-- table. Keep this canonical schema script independently rerunnable so the
-- reporting-view script (022) can safely reference the new quality fields
-- before the historical backfill/constraint migration (027) is applied.
ALTER TABLE staging.trackunit_daily_activity
    ADD COLUMN IF NOT EXISTS counter_reset_detected BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE staging.trackunit_daily_activity
    ADD COLUMN IF NOT EXISTS data_quality_status TEXT NOT NULL DEFAULT 'live';
