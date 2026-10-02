-- raw_sendem.trip_driver_daily / stg_sendem.trip_driver_daily: the Sendem
-- driver-grain daily trip feed (GET /api/aggregates/trips/assetspeople,
-- Swagger model TripsAssetsPeople) for the ge_warehouse platform target.
--
-- The platform counterpart of legacy telemetry_warehouse's
--   raw.sendem_trips_assetspeople_daily      -> raw_sendem.trip_driver_daily
--   staging.sendem_fact_trips_assetspeople_daily -> stg_sendem.trip_driver_daily
-- (sql/legacy/telemetry_migrations/030_create_sendem_trips_assetspeople.sql),
-- shape for shape. Table names follow the platform convention (singular
-- noun, schema names the source): trip_daily is the asset-level feed,
-- trip_driver_daily the per-driver one.
--
-- Why it exists: the asset-level feed (trip_daily) has no engine or
-- runtime time. This feed carries TotalEngineSecond and the driving, idle
-- and trip-duration seconds per DriverId, the only Sendem source of a
-- comparable machine-hours figure. It is loaded BESIDE trip_daily, never
-- instead of it, by ge_data_platform.sources.sendem.sync --target platform.
--
-- Grain: one row per asset, driver, site and day, exactly as Sendem states
-- it. The primary key includes driver_id and site_id and the loader UPSERTs
-- on it, so re-running a window never duplicates a driver row. Rolling the
-- drivers up to a machine-day belongs to a mart/reporting object (planned;
-- the legacy warehouse does it in reporting.vw_sendem_asset_daily_activity),
-- never to the load.
--
-- Null vs zero: total_engine_second NULL means Sendem stated no engine time
-- for that driver row (unknown); 0 means it stated none was used. Nothing
-- here or in the loader coalesces one into the other.
--
-- Types as in 007/008: external ids BIGINT (DriverId is a large signed
-- int64, observed negative as well as positive), date_key INTEGER YYYYMMDD,
-- metrics NUMERIC. Default privileges from 003 cover both schemas.
--
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

-- =============================================================================
-- raw_sendem.trip_driver_daily
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_sendem.trip_driver_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    driver_id BIGINT NOT NULL,
    total_asset_driving_time_seconds NUMERIC,
    total_driver_driving_time_seconds NUMERIC,
    total_engine_second NUMERIC,
    total_idle_time_seconds NUMERIC,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_trip_duration NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    date DATE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, driver_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_sendem_trip_driver_daily_asset_date
    ON raw_sendem.trip_driver_daily (asset_id, date_key);

COMMENT ON TABLE raw_sendem.trip_driver_daily IS
    'One row per Sendem asset, driver, site and day, as returned by GET /api/aggregates/trips/assetspeople/{group_id} (TripsAssetsPeople). Source-faithful. total_engine_second is the only Sendem engine-time source; NULL = unknown, 0 = measured nought. See ge_data_platform.sources.sendem.transform.TRIP_ASSETS_PEOPLE_COLUMNS.';

-- =============================================================================
-- stg_sendem.trip_driver_daily
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.trip_driver_daily (
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
    driver_id BIGINT NOT NULL,
    total_asset_driving_time_seconds NUMERIC,
    total_driver_driving_time_seconds NUMERIC,
    total_engine_second NUMERIC,
    total_idle_time_seconds NUMERIC,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_trip_duration NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, driver_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_sendem_trip_driver_daily_asset_date
    ON stg_sendem.trip_driver_daily (asset_id, date_key);
CREATE INDEX IF NOT EXISTS idx_stg_sendem_trip_driver_daily_date
    ON stg_sendem.trip_driver_daily (date);

COMMENT ON TABLE stg_sendem.trip_driver_daily IS
    'Sendem driver-grain daily trips enriched with asset/site attributes (same enrichment as stg_sendem.trip_daily). One row per asset, driver, site and day; roll up to a machine-day only in a mart/reporting object, never here. No legacy clean.* history exists for this feed.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('015_create_sendem_trip_driver_daily.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
