-- stg_ezytrack: cleaned, enriched EzyTrack/Telematics Guru staging tables.
--
-- Mirrors legacy telemetry_warehouse staging.ezytrack_* 1:1 in shape. No
-- clean/historical-only legacy schema exists for EzyTrack (unlike Sendem),
-- so there is no separate history-folding step here -- see
-- docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed.
--
-- Table names drop the "ezytrack_" prefix (schema already identifies the
-- source):
--   staging.ezytrack_dim_assets  -> stg_ezytrack.asset
--   staging.ezytrack_fact_trips  -> stg_ezytrack.trip
--
-- Grants: stg_ezytrack already has default privileges for
-- ge_platform_admin/ge_etl from 003_create_platform_roles.sql -- no
-- additional GRANT statements needed here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS stg_ezytrack;

-- =============================================================================
-- stg_ezytrack.asset
-- Clean asset master. Column-for-column mirror of raw_ezytrack.asset --
-- phase 1 has only one asset source, same relationship as stg_sendem.asset
-- to raw_sendem.asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_ezytrack.asset (
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

COMMENT ON TABLE stg_ezytrack.asset IS
    'Clean EzyTrack asset master, one row per asset_id.';

-- =============================================================================
-- stg_ezytrack.trip
-- Same source columns as raw_ezytrack.trip, plus the derived reporting
-- metrics ge_data_platform.sources.ezytrack.transform.build_fact_trips_df()
-- computes: distance_km, start/end_odometer_km, end_odometer_meters,
-- end_run_seconds, runtime_start/end_hrs, time_in_motion_seconds.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_ezytrack.trip (
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
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_ezytrack_trip_asset
    ON stg_ezytrack.trip (asset_id);
CREATE INDEX IF NOT EXISTS idx_stg_ezytrack_trip_start_time
    ON stg_ezytrack.trip (start_time_utc);

COMMENT ON TABLE stg_ezytrack.trip IS
    'One row per EzyTrack trip, enriched with derived distance/odometer/runtime metrics. end_time_utc/stop_time_seconds/time_in_motion_seconds legitimately NULL for still-open trips.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('010_create_stg_ezytrack.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
