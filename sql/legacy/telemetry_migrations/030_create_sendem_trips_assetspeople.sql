-- Sendem driver-grain trips (engine time) for telemetry_warehouse.
--
-- Why: the asset-level aggregate the pipeline has always loaded
-- (GET /api/aggregates/trips/assets -> raw.sendem_trips_assets_daily ->
-- staging.sendem_fact_trips_daily) carries trips, distance, fuel and energy
-- and NO engine or runtime time, so a Sendem machine could only ever prove
-- "activity", never a comparable daily-hours figure. The driver-grain
-- sibling endpoint (GET /api/aggregates/trips/assetspeople, Swagger model
-- TripsAssetsPeople) returns the same trip count/distance totals per
-- DriverId plus TotalEngineSecond, TotalAssetDrivingTimeSeconds,
-- TotalDriverDrivingTimeSeconds, TotalIdleTimeSeconds and TotalTripDuration.
-- Proven on real data (fleet 9308, 2026-09-21..30): the driver rows summed
-- per asset/day reconcile exactly to the asset-level feed on trip count and
-- distance (0 mismatches over all 2,209 September asset-days), and supply
-- the missing engine time.
--
-- What this adds (idempotent, additive, transactional; no existing table or
-- view column is renamed, retyped, reordered or dropped):
--   * raw.sendem_trips_assetspeople_daily      -- source-faithful driver grain
--   * staging.sendem_fact_trips_assetspeople_daily -- same grain, enriched
--     with asset/site attributes exactly like staging.sendem_fact_trips_daily
--   * reporting.vw_sendem_asset_daily_activity -- ONE row per Sendem asset
--     per day: the driver rows rolled up, engine seconds -> hours. This is
--     the single place the roll-up rule lives; Commercial (Tracking Watch /
--     Tracking Health) and the unified view below both read it.
--   * reporting.vw_daily_activity_all is re-created with the Sendem branch
--     filling its previously-NULL operating/moving/idle/duration minutes
--     from that view. Column names, types and order are unchanged.
--   * reporting.vw_sendem_trips_daily is NOT touched: its grain, columns and
--     consumers stay exactly as they are. The asset-level feed keeps
--     loading beside the new one.
--
-- Grain and double counting: the source is driver-grain. The reporting
-- view aggregates to asset + day FIRST (SUM over every DriverId/SiteId row
-- of that asset-day), so a machine with four drivers on one day is one row,
-- never four. Both tables' primary key includes driver_id and site_id, and
-- the loader UPSERTs on that key, so re-running a window can never
-- duplicate a driver row and therefore can never inflate the roll-up.
--
-- Null vs zero: SUM() ignores NULLs and is NULL only when every input is
-- NULL, which is exactly the rule wanted -- rows present with
-- total_engine_second = 0 roll up to engine_seconds = 0 (a measured
-- nought); rows present with total_engine_second NULL throughout roll up
-- to NULL (unknown), flagged by data_quality_status = 'engine_time_unknown'.
-- Unknown is never turned into zero.
--
-- Type notes follow 001_create_sendem_schema.sql: external ids are BIGINT
-- (Sendem returns large signed int64s; DriverId is observed negative as
-- well as positive), date_key stays an INTEGER YYYYMMDD, and every metric
-- is NUMERIC because the loader stages pandas frames through a temp table.
--
-- Roles: SELECT on the new reporting view is granted to powerbi_reader and
-- ge_telemetry_evidence_ro only if those roles exist on the database this
-- runs against (the local dev restore has neither). Default privileges set
-- by 024_create_powerbi_reader_role.sql already cover new reporting views
-- created by the same role; the explicit grant is belt and braces.
--
-- Apply (both the production warehouse and any dev restore of it):
--   psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse \
--        -f .\sql\legacy\telemetry_migrations\030_create_sendem_trips_assetspeople.sql
-- Backfill afterwards with scripts/backfill_sendem_trips_assetspeople.py.

BEGIN;

-- =============================================================================
-- 1. raw.sendem_trips_assetspeople_daily
--    Source: GET /api/aggregates/trips/assetspeople/{group_id}. One row per
--    DateKey/GroupId/SiteId/AssetId/DriverId, columns snake_cased by
--    ge_data_platform.common.database.prepare_dataframe_for_load from the
--    API's own names (TotalEngineSecond -> total_engine_second, etc.).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.sendem_trips_assetspeople_daily (
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
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, driver_id)
);

COMMENT ON TABLE raw.sendem_trips_assetspeople_daily IS
    'Sendem GET /api/aggregates/trips/assetspeople: daily trip totals per asset AND driver (TripsAssetsPeople). Driver grain, source-faithful. total_engine_second is the only Sendem engine-time source; NULL means Sendem stated no engine time for that driver row, 0 means it stated none was used.';

CREATE INDEX IF NOT EXISTS idx_raw_sendem_trips_assetspeople_daily_asset_date
    ON raw.sendem_trips_assetspeople_daily (asset_id, date_key);

-- =============================================================================
-- 2. staging.sendem_fact_trips_assetspeople_daily
--    The raw rows enriched with the asset and site dimensions at load time,
--    column for column like staging.sendem_fact_trips_daily, plus driver_id
--    and the engine/driving/idle/duration seconds. Same primary key as raw.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.sendem_fact_trips_assetspeople_daily (
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
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, driver_id)
);

COMMENT ON TABLE staging.sendem_fact_trips_assetspeople_daily IS
    'Sendem driver-grain daily trips (one row per asset, driver, site and day) enriched with asset/site attributes. Carries engine time (total_engine_second) that staging.sendem_fact_trips_daily never had. Roll up to a machine-day ONLY through reporting.vw_sendem_asset_daily_activity.';

CREATE INDEX IF NOT EXISTS idx_staging_sendem_fact_trips_assetspeople_daily_asset_date
    ON staging.sendem_fact_trips_assetspeople_daily (asset_id, date_key);
CREATE INDEX IF NOT EXISTS idx_staging_sendem_fact_trips_assetspeople_daily_date
    ON staging.sendem_fact_trips_assetspeople_daily (date);

-- =============================================================================
-- 3. reporting.vw_sendem_asset_daily_activity
--    Grain: ONE row per Sendem asset per day. The driver rows of that
--    asset-day are summed here and nowhere else. Every seconds column has
--    an hours twin (ROUND(seconds / 3600, 2)); engine_hours is the
--    comparable tracking-hours figure for Sendem.
-- =============================================================================

CREATE OR REPLACE VIEW reporting.vw_sendem_asset_daily_activity AS
WITH asset_day AS (
    SELECT
        f.date_key,
        f.asset_id,
        MAX(f.date) AS activity_date,
        CASE WHEN COUNT(DISTINCT f.group_id) = 1 THEN MAX(f.group_id) END AS group_id,
        CASE WHEN COUNT(DISTINCT f.site_id) = 1 THEN MAX(f.site_id) END AS site_id,
        COUNT(DISTINCT f.site_id) AS site_count,
        COUNT(DISTINCT f.driver_id) AS driver_count,
        COUNT(*) AS driver_row_count,
        -- SUM is NULL only when every contributing row is NULL: a measured
        -- nought stays 0, an unknown stays NULL.
        SUM(f.total_engine_second) AS engine_seconds,
        COUNT(f.total_engine_second) AS engine_time_rows,
        SUM(f.total_asset_driving_time_seconds) AS asset_driving_seconds,
        SUM(f.total_driver_driving_time_seconds) AS driver_driving_seconds,
        SUM(f.total_idle_time_seconds) AS idle_seconds,
        SUM(f.total_trip_duration) AS trip_duration_seconds,
        SUM(f.total_trip_count) AS trip_count,
        SUM(f.total_trip_distance_kilometres) AS distance_km,
        SUM(f.total_fuel_used_litres) AS fuel_used_litres,
        SUM(f.total_energy_used_kwh) AS energy_used_kwh,
        MAX(f.loaded_at) AS etl_last_updated
    FROM staging.sendem_fact_trips_assetspeople_daily f
    GROUP BY f.date_key, f.asset_id
)
SELECT
    'Sendem'::text AS provider,
    d.activity_date,
    d.date_key,
    d.asset_id AS provider_asset_id,
    dim.fleet_number AS asset_code,
    dim.description AS asset_name,
    d.group_id,
    d.site_id,
    CASE WHEN d.site_count > 1 THEN 'multiple_sites'::text ELSE sites.site_name END AS site_name,
    d.site_count > 1 AS spans_multiple_sites,
    d.driver_count,
    d.driver_row_count,
    d.engine_seconds,
    ROUND(d.engine_seconds / 3600.0, 2) AS engine_hours,
    d.asset_driving_seconds,
    ROUND(d.asset_driving_seconds / 3600.0, 2) AS asset_driving_hours,
    d.driver_driving_seconds,
    ROUND(d.driver_driving_seconds / 3600.0, 2) AS driver_driving_hours,
    d.idle_seconds,
    ROUND(d.idle_seconds / 3600.0, 2) AS idle_hours,
    d.trip_duration_seconds,
    ROUND(d.trip_duration_seconds / 3600.0, 2) AS trip_duration_hours,
    d.trip_count,
    d.distance_km,
    d.fuel_used_litres,
    d.energy_used_kwh,
    CASE WHEN d.engine_seconds IS NULL THEN 'engine_time_unknown'::text ELSE 'live'::text END AS data_quality_status,
    'staging_live'::text AS reporting_source,
    d.etl_last_updated
FROM asset_day d
LEFT JOIN reporting.vw_sendem_dim_assets_combined dim ON dim.asset_id = d.asset_id
LEFT JOIN reporting.vw_sendem_dim_sites_combined sites ON sites.site_id = d.site_id;

COMMENT ON VIEW reporting.vw_sendem_asset_daily_activity IS
    'One row per Sendem asset per day, rolled up from the driver-grain staging.sendem_fact_trips_assetspeople_daily (SUM over every driver/site row of the asset-day). engine_hours = SUM(total_engine_second)/3600 is the comparable Sendem tracking-hours figure; NULL means engine time unknown (every contributing row NULL), 0 means Sendem measured none. trip_count/distance_km reconcile to reporting.vw_sendem_trips_daily summed to the same asset-day.';

-- =============================================================================
-- 4. reporting.vw_daily_activity_all
--    Re-created with the Sendem branch consuming vw_sendem_asset_daily_activity
--    for the minute columns it used to leave NULL. Same 22 columns, same
--    names, same types, same order as 022_create_reporting_powerbi_views.sql
--    (CREATE OR REPLACE VIEW enforces that). The asset-day set is the UNION
--    of the three Sendem sources, so an asset-day present in only one of
--    them is still listed.
-- =============================================================================

CREATE OR REPLACE VIEW reporting.vw_daily_activity_all AS
WITH sendem_trips_daily AS (
    SELECT
        date_key,
        provider_asset_id::text AS provider_asset_id,
        MAX(activity_date) AS activity_date,
        MAX(asset_code) AS asset_code,
        MAX(asset_name) AS asset_name,
        CASE WHEN COUNT(DISTINCT site_id) > 1 THEN 'multiple_sites' ELSE MAX(site_name) END AS site_name,
        COUNT(DISTINCT site_id) > 1 AS spans_multiple_sites,
        SUM(trip_count) AS trip_count,
        SUM(distance_km) AS distance_km,
        MIN(CASE data_quality_status
                WHEN 'pending_review' THEN 0
                WHEN 'historical_backfill' THEN 1
                ELSE 2
            END) AS status_priority,
        MAX(etl_last_updated) AS etl_last_updated
    FROM reporting.vw_sendem_trips_daily
    GROUP BY date_key, provider_asset_id
),
sendem_events_daily AS (
    SELECT
        date_key,
        provider_asset_id::text AS provider_asset_id,
        MAX(activity_date) AS activity_date,
        MAX(asset_code) AS asset_code,
        MAX(asset_name) AS asset_name,
        MAX(site_name) AS site_name,
        SUM(event_occurrences) AS event_count,
        MAX(etl_last_updated) AS etl_last_updated,
        MIN(CASE data_quality_status
                WHEN 'pending_review' THEN 0
                WHEN 'historical_backfill' THEN 1
                ELSE 2
            END) AS status_priority,
        COUNT(DISTINCT reporting_source) AS distinct_sources
    FROM reporting.vw_sendem_events_daily
    GROUP BY date_key, provider_asset_id
),
sendem_activity_daily AS (
    SELECT
        date_key,
        provider_asset_id::text AS provider_asset_id,
        activity_date,
        asset_code,
        asset_name,
        site_name,
        engine_seconds,
        asset_driving_seconds,
        idle_seconds,
        trip_duration_seconds,
        etl_last_updated
    FROM reporting.vw_sendem_asset_daily_activity
),
sendem_days AS (
    SELECT date_key, provider_asset_id FROM sendem_trips_daily
    UNION
    SELECT date_key, provider_asset_id FROM sendem_events_daily
    UNION
    SELECT date_key, provider_asset_id FROM sendem_activity_daily
),
ezytrack_daily AS (
    SELECT
        (t.start_time_utc AT TIME ZONE 'Africa/Harare')::date AS activity_date,
        t.asset_id AS provider_asset_id,
        COUNT(*) AS trip_count,
        SUM(t.duration_seconds)::numeric / 60.0 AS duration_minutes,
        SUM(t.idle_time_seconds)::numeric / 60.0 AS idle_minutes,
        SUM(t.stop_time_seconds)::numeric / 60.0 AS stop_minutes,
        SUM(t.time_in_motion_seconds)::numeric / 60.0 AS moving_minutes,
        SUM(t.distance_km) AS distance_km,
        MIN(t.runtime_start_hrs) AS runtime_start_hours,
        MAX(t.runtime_end_hrs) AS runtime_end_hours,
        MAX(t.loaded_at) AS etl_last_updated
    FROM staging.ezytrack_fact_trips t
    GROUP BY (t.start_time_utc AT TIME ZONE 'Africa/Harare')::date, t.asset_id
)
SELECT
    provider,
    activity_date,
    provider_asset_id,
    asset_code,
    asset_name,
    department,
    project,
    site_name,
    trip_count,
    event_count,
    work_day_minutes,
    operating_minutes,
    moving_minutes,
    idle_minutes,
    stop_minutes,
    duration_minutes,
    distance_km,
    runtime_start_hours,
    runtime_end_hours,
    data_grain,
    data_quality_status,
    etl_last_updated
FROM (
    SELECT
        vw_trackunit_daily_activity.provider,
        vw_trackunit_daily_activity.activity_date,
        vw_trackunit_daily_activity.provider_asset_id,
        vw_trackunit_daily_activity.asset_code,
        vw_trackunit_daily_activity.asset_name,
        NULL::text AS department,
        NULL::text AS project,
        COALESCE(vw_trackunit_daily_activity.zone_name_stop, vw_trackunit_daily_activity.zone_name_start) AS site_name,
        NULL::numeric AS trip_count,
        NULL::numeric AS event_count,
        vw_trackunit_daily_activity.work_day_minutes::numeric AS work_day_minutes,
        vw_trackunit_daily_activity.operating_minutes::numeric AS operating_minutes,
        vw_trackunit_daily_activity.active_driving_minutes::numeric AS moving_minutes,
        NULL::numeric AS idle_minutes,
        NULL::numeric AS stop_minutes,
        NULL::numeric AS duration_minutes,
        vw_trackunit_daily_activity.distance_km,
        NULL::numeric AS runtime_start_hours,
        NULL::numeric AS runtime_end_hours,
        'daily_native'::text AS data_grain,
        vw_trackunit_daily_activity.data_quality_status,
        vw_trackunit_daily_activity.etl_last_updated
    FROM reporting.vw_trackunit_daily_activity

    UNION ALL

    SELECT
        'Sendem'::text AS provider,
        COALESCE(trips.activity_date, events.activity_date, activity.activity_date) AS activity_date,
        days.provider_asset_id,
        COALESCE(trips.asset_code, events.asset_code, activity.asset_code) AS asset_code,
        COALESCE(trips.asset_name, events.asset_name, activity.asset_name) AS asset_name,
        NULL::text AS department,
        NULL::text AS project,
        COALESCE(trips.site_name, events.site_name, activity.site_name) AS site_name,
        trips.trip_count,
        events.event_count,
        NULL::numeric AS work_day_minutes,
        -- Engine time from the driver-grain feed, rolled up per asset-day
        -- in vw_sendem_asset_daily_activity. NULL when that feed has no
        -- engine time for the asset-day: unknown, never nought.
        ROUND(activity.engine_seconds / 60.0, 2) AS operating_minutes,
        ROUND(activity.asset_driving_seconds / 60.0, 2) AS moving_minutes,
        ROUND(activity.idle_seconds / 60.0, 2) AS idle_minutes,
        NULL::numeric AS stop_minutes,
        ROUND(activity.trip_duration_seconds / 60.0, 2) AS duration_minutes,
        trips.distance_km,
        NULL::numeric AS runtime_start_hours,
        NULL::numeric AS runtime_end_hours,
        CASE
            WHEN trips.provider_asset_id IS NOT NULL AND events.provider_asset_id IS NOT NULL THEN 'daily_aggregated_from_trips_and_events'::text
            WHEN events.provider_asset_id IS NOT NULL THEN 'daily_aggregated_from_events'::text
            ELSE 'daily_aggregated_from_trips'::text
        END AS data_grain,
        CASE
            WHEN trips.status_priority = 0 OR events.status_priority = 0 THEN 'pending_review'::text
            WHEN trips.status_priority = 1 OR events.status_priority = 1 THEN 'historical_backfill'::text
            ELSE 'live_post_fix'::text
        END AS data_quality_status,
        GREATEST(trips.etl_last_updated, events.etl_last_updated, activity.etl_last_updated) AS etl_last_updated
    FROM sendem_days days
    LEFT JOIN sendem_trips_daily trips
        ON trips.date_key = days.date_key AND trips.provider_asset_id = days.provider_asset_id
    LEFT JOIN sendem_events_daily events
        ON events.date_key = days.date_key AND events.provider_asset_id = days.provider_asset_id
    LEFT JOIN sendem_activity_daily activity
        ON activity.date_key = days.date_key AND activity.provider_asset_id = days.provider_asset_id

    UNION ALL

    SELECT
        'EzyTrack'::text AS provider,
        d.activity_date,
        d.provider_asset_id::text AS provider_asset_id,
        a.asset_code,
        a.asset_name,
        a.department_name AS department,
        a.project_name AS project,
        a.current_geofence_name AS site_name,
        d.trip_count::numeric AS trip_count,
        NULL::numeric AS event_count,
        NULL::numeric AS work_day_minutes,
        NULL::numeric AS operating_minutes,
        d.moving_minutes,
        d.idle_minutes,
        d.stop_minutes,
        d.duration_minutes,
        d.distance_km,
        d.runtime_start_hours,
        d.runtime_end_hours,
        'daily_aggregated_from_trips'::text AS data_grain,
        'live'::text AS data_quality_status,
        d.etl_last_updated
    FROM ezytrack_daily d
    LEFT JOIN staging.ezytrack_dim_assets a ON a.asset_id = d.provider_asset_id
) combined;

COMMENT ON VIEW reporting.vw_daily_activity_all IS
    'Power BI: one row per provider asset per day across Trackunit, Sendem and EzyTrack. Sendem operating_minutes/moving_minutes/idle_minutes/duration_minutes are engine, asset-driving, idle and trip-duration time from reporting.vw_sendem_asset_daily_activity (driver-grain feed rolled up per asset-day); NULL where that feed holds no engine time for the asset-day. Column set unchanged since 022.';

-- =============================================================================
-- 5. Grants, only where the roles exist.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
        GRANT SELECT ON reporting.vw_sendem_asset_daily_activity TO powerbi_reader;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ge_telemetry_evidence_ro') THEN
        GRANT SELECT ON reporting.vw_sendem_asset_daily_activity TO ge_telemetry_evidence_ro;
    END IF;
END
$$;

COMMIT;
