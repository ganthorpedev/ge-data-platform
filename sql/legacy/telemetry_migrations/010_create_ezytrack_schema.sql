-- EzyTrack / Telematics Guru schema, phase 1 (assets + trips only).
--
-- Lives in the shared telemetry_warehouse schemas (raw/staging/warehouse/etl),
-- the same schemas Sendem uses. This intentionally does NOT use or create the
-- old prototype's "telematics" schema (see EzyTrack/telematics_data.ipynb,
-- POSTGRES_SCHEMA=telematics) -- that prototype schema is retired.
--
-- Field list and shapes are taken directly from the working prototype
-- notebook's ASSETS_QUERY / TRIPS_QUERY and its format_asset_for_db() /
-- format_trip_for_db() functions, restricted to the phase-1 fields called
-- out below. Fields the prototype fetches but that fall under "richer
-- data" (device/tracking metadata on assets; lat/long, trip type, harsh
-- driving counts, speeding, milli rates on trips -- the latter are never
-- actually queried by TRIPS_QUERY today, only present as None placeholders
-- in the prototype's Python dict) are intentionally omitted. See the
-- explanation accompanying this file for the full list of omissions.
--
-- Rerunnable: CREATE SCHEMA IF NOT EXISTS / CREATE TABLE IF NOT EXISTS
-- throughout. Never truncates or deletes existing rows.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS etl;

-- =============================================================================
-- raw.ezytrack_assets
-- Source: ASSETS_QUERY (assetId, assetCode, name, description, isEnabled,
-- lastConnectedUtc, department{name}, project{name},
-- allocatedDriver{name, driverCode}, geoFence{name})
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.ezytrack_assets (
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
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- raw.ezytrack_trips
-- Source: TRIPS_QUERY (tripId, startTimeUtc, endTimeUtc, durationSeconds,
-- distanceMeters, stopTimeSeconds, idleTimeSeconds,
-- startOdometerReadingMeters, startRunSeconds, asset{assetId},
-- driver{name, driverCode}, startGeoFence{name})
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.ezytrack_trips (
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
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- staging.ezytrack_dim_assets
-- Clean asset master. Mirrors raw.ezytrack_assets column-for-column, same
-- pattern as staging.sendem_dim_assets -- there is only one asset source
-- table in phase 1, so no additional joins are needed yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.ezytrack_dim_assets (
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
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- staging.ezytrack_fact_trips
-- Same source columns as raw.ezytrack_trips, plus the derived reporting
-- metrics from the prototype's format_trip_for_db(): distance_km,
-- start/end_odometer_km, runtime_start/end_hrs, time_in_motion_seconds.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.ezytrack_fact_trips (
    trip_id BIGINT PRIMARY KEY,
    asset_id BIGINT,
    start_time_utc TIMESTAMPTZ,
    end_time_utc TIMESTAMPTZ,
    duration_seconds INTEGER,
    distance_meters NUMERIC,
    distance_km NUMERIC,
    stop_time_seconds INTEGER,
    idle_time_seconds INTEGER,
    time_in_motion_seconds INTEGER,
    start_odometer_meters NUMERIC,
    start_odometer_km NUMERIC,
    end_odometer_meters NUMERIC,
    end_odometer_km NUMERIC,
    start_run_seconds INTEGER,
    end_run_seconds INTEGER,
    runtime_start_hrs NUMERIC,
    runtime_end_hrs NUMERIC,
    driver_name TEXT,
    driver_code TEXT,
    start_geofence_name TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);
