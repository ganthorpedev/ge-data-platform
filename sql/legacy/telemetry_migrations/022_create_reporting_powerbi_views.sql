-- Power BI-facing reporting layer.
--
-- Read-only with respect to raw/staging/etl/warehouse: this file only ever
-- creates the `reporting` schema and CREATE OR REPLACE VIEW statements. It
-- never inserts, updates, deletes, truncates, or alters a table anywhere.
-- Safe to re-run at any time.
--
-- Sendem-specific note: staging.sendem_* only holds the live rolling window
-- (2026-06-24 onward) that the current ETL code writes to. clean.sendem_*
-- is a pre-existing historical backfill (2026-01-01 to 2026-06-30) that no
-- running job writes to and that predates the event-type inference fix
-- described in staging.sendem_dim_event_types. Per explicit instruction,
-- Sendem reporting views UNION both, preferring staging in the ~7-day
-- overlap window (2026-06-24 to 2026-06-30), and expose `reporting_source`
-- / `data_quality_status` on every row so this blend is never silently
-- hidden from Power BI.

CREATE SCHEMA IF NOT EXISTS reporting;

-- =============================================================================
-- Section 1: Sendem combined dimensions (staging preferred, clean fallback)
--
-- These exist purely as internal building blocks for the Sendem
-- provider-specific views and vw_assets_all below -- they keep the
-- "prefer staging row for a given key, else use clean" rule in exactly one
-- place instead of duplicating it three times.
-- =============================================================================

CREATE OR REPLACE VIEW reporting.vw_sendem_dim_assets_combined AS
WITH ranked AS (
    SELECT
        asset_id, site_id, asset_type, description, vin_number, country, group_id,
        registration_number, is_available, fleet_number, make, model, fuel_type, year,
        loaded_at, 'staging_live'::text AS reporting_source, 0 AS source_rank
    FROM staging.sendem_dim_assets

    UNION ALL

    SELECT
        asset_id, NULL::bigint AS site_id, asset_type, description, vin_number, country, group_id,
        registration_number, is_available, fleet_number, make, model, fuel_type, year,
        loaded_at, 'clean_historical'::text AS reporting_source, 1 AS source_rank
    FROM clean.sendem_dim_assets
)
SELECT DISTINCT ON (asset_id)
    asset_id, site_id, asset_type, description, vin_number, country, group_id,
    registration_number, is_available, fleet_number, make, model, fuel_type, year,
    loaded_at, reporting_source
FROM ranked
ORDER BY asset_id, source_rank;

COMMENT ON VIEW reporting.vw_sendem_dim_assets_combined IS
    'Internal helper: one row per Sendem asset_id, staging.sendem_dim_assets preferred, clean.sendem_dim_assets used only when the asset is missing from staging.';

CREATE OR REPLACE VIEW reporting.vw_sendem_dim_sites_combined AS
WITH ranked AS (
    SELECT site_id, site_name, NULL::bigint AS group_id, loaded_at,
           'staging_live'::text AS reporting_source, 0 AS source_rank
    FROM staging.sendem_dim_sites

    UNION ALL

    SELECT site_id, site_name, group_id, loaded_at,
           'clean_historical'::text AS reporting_source, 1 AS source_rank
    FROM clean.sendem_dim_sites
)
SELECT DISTINCT ON (site_id)
    site_id, site_name, group_id, loaded_at, reporting_source
FROM ranked
ORDER BY site_id, source_rank;

COMMENT ON VIEW reporting.vw_sendem_dim_sites_combined IS
    'Internal helper: one row per Sendem site_id, staging.sendem_dim_sites preferred, clean.sendem_dim_sites used only when the site is missing from staging.';

CREATE OR REPLACE VIEW reporting.vw_sendem_dim_event_types_combined AS
WITH ranked AS (
    SELECT event_type_id, event_name, group_id, metric_type, unit_type, event_category,
           loaded_at, 'staging_live'::text AS reporting_source, 0 AS source_rank
    FROM staging.sendem_dim_event_types

    UNION ALL

    SELECT event_type_id, event_name, group_id, metric_type, unit_type, event_category,
           loaded_at, 'clean_historical'::text AS reporting_source, 1 AS source_rank
    FROM clean.sendem_dim_event_types
)
SELECT DISTINCT ON (event_type_id)
    event_type_id, event_name, group_id, metric_type, unit_type, event_category,
    loaded_at, reporting_source
FROM ranked
ORDER BY event_type_id, source_rank;

COMMENT ON VIEW reporting.vw_sendem_dim_event_types_combined IS
    'Internal helper: one row per Sendem event_type_id, staging.sendem_dim_event_types preferred (includes the 6 previously-orphaned event types fixed there), clean.sendem_dim_event_types used only as fallback.';

-- =============================================================================
-- Section 2: Provider-specific reporting views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 reporting.vw_trackunit_daily_activity
-- Grain: one row per Trackunit asset per local report_date.
-- -----------------------------------------------------------------------------

-- Postgres will not let CREATE OR REPLACE VIEW rename/remove an existing
-- column in place (the old site_name/zone/address/geofence_name placeholder
-- columns are being replaced by the real V1 enrichment field names below),
-- so this view -- and anything that depends on it -- is dropped and
-- recreated. Everything CASCADE would drop (vw_daily_activity_all,
-- vw_assets_all) is itself CREATE OR REPLACEd later in this same file, so
-- this is safe to rerun.
DROP VIEW IF EXISTS reporting.vw_trackunit_daily_activity CASCADE;

CREATE VIEW reporting.vw_trackunit_daily_activity AS
SELECT
    'Trackunit'::text AS provider,
    da.report_date AS activity_date,
    da.asset_id AS provider_asset_id,
    da.pin AS asset_code,
    da.machine AS asset_name,
    da.machine_serial_number,
    da.brand,
    da.machine_type,
    da.model,
    dim.production_year AS asset_year,
    dim.telematics_device_id,
    dim.created_at AS asset_onboarded_at,
    da.start_time_utc,
    da.stop_time_utc,
    da.start_time_local,
    da.stop_time_local,
    da.work_day_minutes,
    da.work_day_hhmm,
    da.operating_minutes,
    da.operating_hhmm,
    da.active_driving_minutes,
    da.active_driving_hhmm,
    da.distance_km,
    da.operating_points,
    da.moving_points,
    da.distance_points,
    -- Location enrichment V1 (staging.trackunit_location_enrichment), proven
    -- in Manitou/manitou_trackunit_exploration.ipynb. Address/zip/city/
    -- country stay NULL -- no source field exists for them yet (AEMP's
    -- historical Locations series returns coordinates only); never
    -- fabricated. location_enrichment_status distinguishes "not yet run"
    -- from the job's own explicit outcomes.
    loc.zone_name_start,
    loc.zone_name_stop,
    loc.last_known_start_location_timestamp_utc,
    loc.last_known_start_location_timestamp_local,
    loc.start_latitude,
    loc.start_longitude,
    loc.start_altitude,
    loc.last_known_stop_location_timestamp_utc,
    loc.last_known_stop_location_timestamp_local,
    loc.stop_latitude,
    loc.stop_longitude,
    loc.stop_altitude,
    loc.address_start,
    loc.zip_start,
    loc.city_start,
    loc.country_start,
    loc.address_stop,
    loc.zip_stop,
    loc.city_stop,
    loc.country_stop,
    COALESCE(loc.location_enrichment_status, 'NOT_YET_ENRICHED') AS location_enrichment_status,
    loc.location_enriched_at,
    'staging_live'::text AS reporting_source,
    da.data_quality_status,
    da.loaded_at AS etl_last_updated,
    -- Appended after every pre-existing view column so downstream ordinal
    -- consumers keep the same column order.
    da.counter_reset_detected
FROM staging.trackunit_daily_activity da
LEFT JOIN staging.trackunit_dim_assets dim ON dim.asset_id = da.asset_id
LEFT JOIN staging.trackunit_location_enrichment loc
    ON loc.report_date = da.report_date AND loc.asset_id = da.asset_id;

COMMENT ON VIEW reporting.vw_trackunit_daily_activity IS
    'Power BI: one row per Trackunit machine per local (Africa/Harare) report date, LEFT JOINed to staging.trackunit_location_enrichment (V1). zone_name_start/zone_name_stop and the location timestamp/lat/lon/altitude fields are real once the enrichment job has run for that report_date/asset_id; address/zip/city/country stay NULL -- no source field exists for them yet. location_enrichment_status = NOT_YET_ENRICHED means the enrichment job has not run for that row yet. data_quality_status/counter_reset_detected explicitly expose cumulative-counter resets; existing columns retain their prior names and order, with the reset flag appended.';

-- -----------------------------------------------------------------------------
-- 2.2 reporting.vw_sendem_trips_daily
-- Grain: one row per Sendem asset per report date (native fact grain).
-- Source: clean.sendem_fact_trips_daily (historical backfill) UNIONed with
-- staging.sendem_fact_trips_daily (live), staging wins on grain collision.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_sendem_trips_daily AS
WITH unioned AS (
    SELECT
        date, date_key, group_id, site_id, site_name, asset_id, fleet_number,
        registration_number, description, make, model, asset_type,
        total_trip_count::numeric AS total_trip_count,
        total_trip_distance_kilometres, total_fuel_used_litres, total_energy_used_kwh,
        loaded_at,
        'staging_live'::text AS reporting_source,
        'live_post_fix'::text AS data_quality_status,
        0 AS source_rank
    FROM staging.sendem_fact_trips_daily

    UNION ALL

    SELECT
        date, date_key, group_id, site_id, site_name, asset_id, fleet_number,
        registration_number, description, make, model, asset_type,
        total_trip_count::numeric AS total_trip_count,
        total_trip_distance_kilometres, total_fuel_used_litres, total_energy_used_kwh,
        loaded_at,
        'clean_historical'::text AS reporting_source,
        'historical_backfill'::text AS data_quality_status,
        1 AS source_rank
    FROM clean.sendem_fact_trips_daily
),
deduped AS (
    SELECT DISTINCT ON (date_key, group_id, site_id, asset_id) *
    FROM unioned
    ORDER BY date_key, group_id, site_id, asset_id, source_rank
)
SELECT
    'Sendem'::text AS provider,
    t.date AS activity_date,
    t.date_key,
    t.asset_id AS provider_asset_id,
    t.fleet_number AS asset_code,
    t.description AS asset_name,
    t.registration_number,
    t.make,
    t.model,
    t.asset_type,
    t.group_id,
    t.site_id,
    COALESCE(t.site_name, sites.site_name) AS site_name,
    dim.vin_number,
    dim.country,
    dim.is_available,
    dim.fuel_type,
    dim.year AS asset_year,
    t.total_trip_count AS trip_count,
    t.total_trip_distance_kilometres AS distance_km,
    t.total_fuel_used_litres AS fuel_used_litres,
    t.total_energy_used_kwh AS energy_used_kwh,
    t.reporting_source,
    t.data_quality_status,
    t.loaded_at AS etl_last_updated
FROM deduped t
LEFT JOIN reporting.vw_sendem_dim_assets_combined dim ON dim.asset_id = t.asset_id
LEFT JOIN reporting.vw_sendem_dim_sites_combined sites ON sites.site_id = t.site_id;

COMMENT ON VIEW reporting.vw_sendem_trips_daily IS
    'Power BI: one row per Sendem asset per report date. UNION of clean.sendem_fact_trips_daily (2026-01-01..2026-06-30 historical backfill) and staging.sendem_fact_trips_daily (2026-06-24 onward, live); staging wins where both exist for the same date_key/group_id/site_id/asset_id. reporting_source and data_quality_status expose which source each row came from.';

-- -----------------------------------------------------------------------------
-- 2.3 reporting.vw_sendem_events_daily
-- Grain: one row per Sendem asset per report date per event type.
-- Source: clean.sendem_fact_events_daily (historical backfill) UNIONed with
-- staging.sendem_fact_events_daily (live), staging wins on grain collision.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_sendem_events_daily AS
WITH unioned AS (
    SELECT
        date, date_key, group_id, site_id, site_name, asset_id, fleet_number,
        registration_number, description, make, model, asset_type,
        event_type_id, NULLIF(event_name, '') AS event_name,
        NULLIF(event_category, '') AS event_category,
        NULLIF(metric_type, '') AS metric_type, NULLIF(unit_type, '') AS unit_type,
        total_event_occurrences::numeric AS total_event_occurrences,
        min_event_value, max_event_value, total_event_value,
        min_event_duration, max_event_duration, total_event_duration,
        loaded_at,
        'staging_live'::text AS reporting_source,
        'live_post_fix'::text AS data_quality_status,
        0 AS source_rank
    FROM staging.sendem_fact_events_daily

    UNION ALL

    SELECT
        date, date_key, group_id, site_id, site_name, asset_id, fleet_number,
        registration_number, description, make, model, asset_type,
        event_type_id, NULLIF(event_name, '') AS event_name,
        NULLIF(event_category, '') AS event_category,
        NULLIF(metric_type, '') AS metric_type, NULLIF(unit_type, '') AS unit_type,
        total_event_occurrences::numeric AS total_event_occurrences,
        min_event_value, max_event_value, total_event_value,
        min_event_duration, max_event_duration, total_event_duration,
        loaded_at,
        'clean_historical'::text AS reporting_source,
        'historical_backfill'::text AS data_quality_status,
        1 AS source_rank
    FROM clean.sendem_fact_events_daily
),
deduped AS (
    SELECT DISTINCT ON (date_key, group_id, site_id, asset_id, event_type_id) *
    FROM unioned
    ORDER BY date_key, group_id, site_id, asset_id, event_type_id, source_rank
)
SELECT
    'Sendem'::text AS provider,
    e.date AS activity_date,
    e.date_key,
    e.asset_id AS provider_asset_id,
    e.fleet_number AS asset_code,
    e.description AS asset_name,
    e.asset_type,
    e.group_id,
    e.site_id,
    COALESCE(e.site_name, sites.site_name) AS site_name,
    e.event_type_id,
    COALESCE(et.event_name, e.event_name) AS event_name,
    COALESCE(et.event_category, e.event_category) AS event_category,
    COALESCE(et.metric_type, e.metric_type) AS metric_type,
    COALESCE(et.unit_type, e.unit_type) AS unit_type,
    e.total_event_occurrences AS event_occurrences,
    e.min_event_value,
    e.max_event_value,
    e.total_event_value,
    e.min_event_duration,
    e.max_event_duration,
    e.total_event_duration,
    e.reporting_source,
    -- A row keeps its source-based status unless the event type itself can't
    -- be named by either dimension (a real orphan predating the fix) -- that
    -- case is flagged for review rather than silently labelled "historical".
    CASE
        WHEN COALESCE(et.event_name, e.event_name) IS NULL THEN 'pending_review'
        ELSE e.data_quality_status
    END AS data_quality_status,
    e.loaded_at AS etl_last_updated
FROM deduped e
LEFT JOIN reporting.vw_sendem_dim_event_types_combined et ON et.event_type_id = e.event_type_id
LEFT JOIN reporting.vw_sendem_dim_sites_combined sites ON sites.site_id = e.site_id;

COMMENT ON VIEW reporting.vw_sendem_events_daily IS
    'Power BI: one row per Sendem asset per report date per event type. UNION of clean.sendem_fact_events_daily (historical backfill) and staging.sendem_fact_events_daily (live); staging wins on grain collision. event_type_id values with no name in either dimension are marked data_quality_status = pending_review instead of being hidden.';

-- -----------------------------------------------------------------------------
-- 2.4 reporting.vw_ezytrack_trips
-- Grain: one row per EzyTrack / Telematics Guru trip.
-- Source: staging.ezytrack_fact_trips joined to staging.ezytrack_dim_assets.
-- (No separate v_trip_report view exists in this database -- confirmed via
-- information_schema.tables; these are the actual objects.)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_ezytrack_trips AS
SELECT
    'EzyTrack'::text AS provider,
    t.trip_id,
    t.asset_id AS provider_asset_id,
    a.asset_code,
    a.asset_name,
    a.asset_description,
    a.department_name AS department,
    a.project_name AS project,
    COALESCE(t.driver_name, a.allocated_driver_name) AS driver_name,
    COALESCE(t.driver_code, a.allocated_driver_code) AS driver_code,
    t.start_geofence_name,
    a.current_geofence_name AS asset_current_geofence_name,
    t.start_time_utc,
    t.end_time_utc,
    t.duration_seconds,
    ROUND(t.duration_seconds / 60.0, 1) AS duration_minutes,
    t.distance_meters,
    t.distance_km,
    t.idle_time_seconds,
    ROUND(t.idle_time_seconds / 60.0, 1) AS idle_minutes,
    t.stop_time_seconds,
    ROUND(t.stop_time_seconds / 60.0, 1) AS stop_minutes,
    t.time_in_motion_seconds,
    ROUND(t.time_in_motion_seconds / 60.0, 1) AS time_in_motion_minutes,
    t.start_odometer_km,
    t.end_odometer_km,
    t.runtime_start_hrs AS runtime_start_hours,
    t.runtime_end_hrs AS runtime_end_hours,
    a.is_enabled,
    a.last_connected_utc,
    'staging_live'::text AS reporting_source,
    'live'::text AS data_quality_status,
    t.loaded_at AS etl_last_updated
FROM staging.ezytrack_fact_trips t
LEFT JOIN staging.ezytrack_dim_assets a ON a.asset_id = t.asset_id;

COMMENT ON VIEW reporting.vw_ezytrack_trips IS
    'Power BI: one row per EzyTrack / Telematics Guru trip. start_geofence_name is the trip''s own start geofence; asset_current_geofence_name is the asset''s current geofence at dimension-load time -- kept separate since they answer different questions.';

-- -----------------------------------------------------------------------------
-- 2.5 reporting.vw_ezytrack_trip_report
-- Grain: one row per EzyTrack / Telematics Guru trip (same grain as
-- reporting.vw_ezytrack_trips -- this view adds no joins and drops no rows).
-- Source: reporting.vw_ezytrack_trips only. This is a report-facing layer
-- on top of that view, built to match a specific exported trip-report
-- layout column-for-column (quoted column names with spaces/units, exactly
-- as requested) -- it does not replace, alter, or duplicate the logic in
-- vw_ezytrack_trips itself.
--
-- "Start Date" timezone note: staging.ezytrack_fact_trips only ever carries
-- UTC timestamps (start_time_utc/end_time_utc) -- there is no EzyTrack-native
-- local timestamp anywhere in raw/staging. reporting.vw_daily_activity_all
-- already converts EzyTrack's start_time_utc using AT TIME ZONE
-- 'Africa/Harare' for day-bucketing (documented there as an assumption, since
-- EzyTrack has no configured timezone setting of its own, unlike Trackunit).
-- This view applies that same existing conversion, consistently, to produce
-- a local wall-clock "Start Date" for the report. The raw UTC timestamp is
-- also kept as start_time_utc below so the conversion is never silently
-- unrecoverable.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_ezytrack_trip_report AS
SELECT
    trip_id,
    provider_asset_id,
    start_time_utc,
    (start_time_utc AT TIME ZONE 'Africa/Harare') AS "Start Date",
    asset_code AS "Asset Code",
    department AS "Department",
    project AS "Project",
    asset_name AS "Asset",
    driver_name AS "Driver",
    start_geofence_name AS "Start Location",
    CASE
        WHEN time_in_motion_seconds IS NULL THEN NULL
        ELSE
            ((floor(time_in_motion_seconds)::bigint / 3600)::text || ':' ||
             lpad(((floor(time_in_motion_seconds)::bigint % 3600) / 60)::text, 2, '0') || ':' ||
             lpad((floor(time_in_motion_seconds)::bigint % 60)::text, 2, '0'))
    END AS "Time in Motion (hh:mm:ss)",
    CASE
        WHEN stop_time_seconds IS NULL THEN NULL
        ELSE
            ((floor(stop_time_seconds)::bigint / 3600)::text || ':' ||
             lpad(((floor(stop_time_seconds)::bigint % 3600) / 60)::text, 2, '0') || ':' ||
             lpad((floor(stop_time_seconds)::bigint % 60)::text, 2, '0'))
    END AS "Stop Time (hh:mm:ss)",
    CASE
        WHEN idle_time_seconds IS NULL THEN NULL
        ELSE
            ((floor(idle_time_seconds)::bigint / 3600)::text || ':' ||
             lpad(((floor(idle_time_seconds)::bigint % 3600) / 60)::text, 2, '0') || ':' ||
             lpad((floor(idle_time_seconds)::bigint % 60)::text, 2, '0'))
    END AS "Idle Time (hh:mm:ss)",
    start_odometer_km AS "Odometer Reading at Trip Start (km)",
    end_odometer_km AS "Odometer Reading at Trip End (km)",
    distance_km AS "Distance (km)",
    runtime_start_hours AS "Run Time Reading at Trip Start (hrs)",
    runtime_end_hours AS "Run Time Reading at Trip End (hrs)",
    CASE
        WHEN duration_seconds IS NULL THEN NULL
        ELSE
            ((floor(duration_seconds)::bigint / 3600)::text || ':' ||
             lpad(((floor(duration_seconds)::bigint % 3600) / 60)::text, 2, '0') || ':' ||
             lpad((floor(duration_seconds)::bigint % 60)::text, 2, '0'))
    END AS "Duration (hh:mm:ss)",
    -- No fuel-consumption field exists anywhere in raw/staging EzyTrack data
    -- (confirmed: staging.ezytrack_fact_trips has no fuel column). Always
    -- NULL rather than estimated/fabricated, per explicit instruction.
    NULL::numeric AS "Estimated Fuel Consumption (l)",
    reporting_source,
    data_quality_status,
    etl_last_updated
FROM reporting.vw_ezytrack_trips;

COMMENT ON VIEW reporting.vw_ezytrack_trip_report IS
    'Power BI: EzyTrack / Telematics Guru trip report, one row per trip, column names matching the required exported report layout exactly. Built strictly on top of reporting.vw_ezytrack_trips (no new joins, no dropped rows). "Start Date" is start_time_utc converted to Africa/Harare (same assumption already used in vw_daily_activity_all); start_time_utc is kept alongside it for traceability. "Estimated Fuel Consumption (l)" is always NULL -- no source field exists.';

-- =============================================================================
-- Section 3: Conformed cross-provider views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 reporting.vw_assets_all
-- Grain: one row per provider per provider_asset_id (asset list, not activity).
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_assets_all AS
SELECT
    'Trackunit'::text AS provider,
    a.asset_id::text AS provider_asset_id,
    COALESCE(a.external_reference, a.serial_number) AS asset_code,
    a.name AS asset_name,
    -- Trackunit has no separate fleet-number field; `name` is the machine
    -- number used operationally, so it is reused here rather than left NULL.
    a.name AS fleet_no,
    a.serial_number,
    a.telematics_device_serial AS device_serial,
    NULL::text AS department,
    NULL::text AS project,
    NULL::text AS site_or_geofence,
    NULL::boolean AS is_enabled,
    latest_activity.last_seen_at,
    a.loaded_at AS synced_at
FROM staging.trackunit_dim_assets a
LEFT JOIN (
    SELECT asset_id, MAX(stop_time_utc) AS last_seen_at
    FROM staging.trackunit_daily_activity
    GROUP BY asset_id
) latest_activity ON latest_activity.asset_id = a.asset_id

UNION ALL

SELECT
    'Sendem'::text AS provider,
    a.asset_id::text AS provider_asset_id,
    a.fleet_number AS asset_code,
    a.description AS asset_name,
    a.fleet_number AS fleet_no,
    a.vin_number AS serial_number,
    NULL::text AS device_serial,
    NULL::text AS department,
    NULL::text AS project,
    sites.site_name AS site_or_geofence,
    a.is_available AS is_enabled,
    latest_activity.last_seen_at,
    a.loaded_at AS synced_at
FROM reporting.vw_sendem_dim_assets_combined a
LEFT JOIN reporting.vw_sendem_dim_sites_combined sites ON sites.site_id = a.site_id
LEFT JOIN (
    SELECT provider_asset_id::text AS provider_asset_id, MAX(activity_date)::timestamp AS last_seen_at
    FROM reporting.vw_sendem_trips_daily
    GROUP BY provider_asset_id
) latest_activity ON latest_activity.provider_asset_id = a.asset_id::text

UNION ALL

SELECT
    'EzyTrack'::text AS provider,
    a.asset_id::text AS provider_asset_id,
    a.asset_code,
    a.asset_name,
    NULL::text AS fleet_no,
    NULL::text AS serial_number,
    NULL::text AS device_serial,
    a.department_name AS department,
    a.project_name AS project,
    a.current_geofence_name AS site_or_geofence,
    a.is_enabled,
    a.last_connected_utc AS last_seen_at,
    a.loaded_at AS synced_at
FROM staging.ezytrack_dim_assets a;

COMMENT ON VIEW reporting.vw_assets_all IS
    'Power BI: one row per asset per provider (asset list, not daily activity). Trackunit has no fleet-number field (name is reused); Sendem has no device_serial; EzyTrack has no serial_number/device_serial/fleet_no -- these are genuinely NULL, not missing joins. last_seen_at is derived from real activity/connection data, never fabricated.';

-- -----------------------------------------------------------------------------
-- 3.2 reporting.vw_daily_activity_all
-- Grain: one row per provider per provider_asset_id per activity_date.
-- Sendem events (native grain: per event type) are summed to the day before
-- being combined with Sendem trips, so this view never has more than one row
-- per Sendem asset per day. EzyTrack trips (native grain: per trip) are
-- summed to the local calendar day (Africa/Harare) for the same reason.
-- Use the provider-specific views above for full trip/event-type detail.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_daily_activity_all AS
WITH sendem_trips_daily AS (
    -- A single Sendem asset can have trips at more than one site/group on
    -- the same day (confirmed against live data), which is finer than this
    -- conformed view's one-row-per-asset-per-day grain. Roll those up here;
    -- reporting.vw_sendem_trips_daily itself still preserves the real
    -- per-site grain untouched.
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
        -- Worst-case status/source across event types for the day: never
        -- hide a pending_review or a historical row behind a live one.
        MIN(CASE data_quality_status
                WHEN 'pending_review' THEN 0
                WHEN 'historical_backfill' THEN 1
                ELSE 2
            END) AS status_priority,
        COUNT(DISTINCT reporting_source) AS distinct_sources
    FROM reporting.vw_sendem_events_daily
    GROUP BY date_key, provider_asset_id
),
ezytrack_daily AS (
    SELECT
        (t.start_time_utc AT TIME ZONE 'Africa/Harare')::date AS activity_date,
        t.asset_id AS provider_asset_id,
        COUNT(*) AS trip_count,
        SUM(t.duration_seconds) / 60.0 AS duration_minutes,
        SUM(t.idle_time_seconds) / 60.0 AS idle_minutes,
        SUM(t.stop_time_seconds) / 60.0 AS stop_minutes,
        SUM(t.time_in_motion_seconds) / 60.0 AS moving_minutes,
        SUM(t.distance_km) AS distance_km,
        MIN(t.runtime_start_hrs) AS runtime_start_hours,
        MAX(t.runtime_end_hrs) AS runtime_end_hours,
        MAX(t.loaded_at) AS etl_last_updated
    FROM staging.ezytrack_fact_trips t
    GROUP BY 1, 2
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
    -- Trackunit: native one-row-per-asset-per-day grain.
    SELECT
        provider,
        activity_date,
        provider_asset_id::text AS provider_asset_id,
        asset_code,
        asset_name,
        NULL::text AS department,
        NULL::text AS project,
        -- One site_name column can't represent both a start and a stop zone;
        -- stop is the day's most recent known location, so it wins when the
        -- two differ. Detailed start/stop zones are on
        -- reporting.vw_trackunit_daily_activity itself.
        COALESCE(zone_name_stop, zone_name_start) AS site_name,
        NULL::numeric AS trip_count,
        NULL::numeric AS event_count,
        work_day_minutes::numeric AS work_day_minutes,
        operating_minutes::numeric AS operating_minutes,
        active_driving_minutes::numeric AS moving_minutes,
        NULL::numeric AS idle_minutes,
        NULL::numeric AS stop_minutes,
        NULL::numeric AS duration_minutes,
        distance_km,
        NULL::numeric AS runtime_start_hours,
        NULL::numeric AS runtime_end_hours,
        'daily_native'::text AS data_grain,
        data_quality_status,
        etl_last_updated
    FROM reporting.vw_trackunit_daily_activity

    UNION ALL

    -- Sendem: trips and events are each rolled up to one row per asset per
    -- day above (sendem_trips_daily / sendem_events_daily), then combined
    -- with a FULL OUTER JOIN so a day with only events (or only trips)
    -- isn't lost.
    SELECT
        'Sendem'::text AS provider,
        COALESCE(trips.activity_date, events.activity_date) AS activity_date,
        COALESCE(trips.provider_asset_id, events.provider_asset_id) AS provider_asset_id,
        COALESCE(trips.asset_code, events.asset_code) AS asset_code,
        COALESCE(trips.asset_name, events.asset_name) AS asset_name,
        NULL::text AS department,
        NULL::text AS project,
        COALESCE(trips.site_name, events.site_name) AS site_name,
        trips.trip_count::numeric AS trip_count,
        events.event_count,
        NULL::numeric AS work_day_minutes,
        NULL::numeric AS operating_minutes,
        NULL::numeric AS moving_minutes,
        NULL::numeric AS idle_minutes,
        NULL::numeric AS stop_minutes,
        NULL::numeric AS duration_minutes,
        trips.distance_km,
        NULL::numeric AS runtime_start_hours,
        NULL::numeric AS runtime_end_hours,
        CASE
            WHEN trips.provider_asset_id IS NOT NULL AND events.provider_asset_id IS NOT NULL THEN 'daily_aggregated_from_trips_and_events'
            WHEN events.provider_asset_id IS NOT NULL THEN 'daily_aggregated_from_events'
            ELSE 'daily_aggregated_from_trips'
        END AS data_grain,
        CASE
            WHEN trips.status_priority = 0 OR events.status_priority = 0 THEN 'pending_review'
            WHEN trips.status_priority = 1 OR events.status_priority = 1 THEN 'historical_backfill'
            ELSE 'live_post_fix'
        END AS data_quality_status,
        GREATEST(trips.etl_last_updated, events.etl_last_updated) AS etl_last_updated
    FROM sendem_trips_daily trips
    FULL OUTER JOIN sendem_events_daily events
        ON events.date_key = trips.date_key AND events.provider_asset_id = trips.provider_asset_id

    UNION ALL

    -- EzyTrack: trips are per-trip grain, summed up to the local calendar day.
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
    'Power BI: one row per provider per asset per local report date, for cross-provider dashboards. data_grain marks how each row was derived (daily_native / daily_aggregated_from_events / daily_aggregated_from_trips / daily_native_plus_events_aggregated) since providers do not share a native grain. Detailed trip- and event-type-level data lives in the provider-specific views, not here.';

-- -----------------------------------------------------------------------------
-- 3.3 reporting.vw_provider_sync_health
-- Grain: one row per (provider, job_name) -- the latest sync_runs row.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting.vw_provider_sync_health AS
WITH latest_run AS (
    SELECT DISTINCT ON (source_system, job_name)
        source_system, job_name, started_at, finished_at, status,
        rows_fetched, rows_loaded, error_message
    FROM etl.sync_runs
    ORDER BY source_system, job_name, started_at DESC
),
latest_success AS (
    SELECT DISTINCT ON (source_system, job_name)
        source_system, job_name, finished_at AS last_success_finished_at
    FROM etl.sync_runs
    WHERE status = 'SUCCESS'
    ORDER BY source_system, job_name, started_at DESC
)
SELECT
    CASE lr.source_system
        WHEN 'trackunit' THEN 'Trackunit'
        WHEN 'sendem' THEN 'Sendem'
        WHEN 'ezytrack' THEN 'EzyTrack'
        ELSE lr.source_system
    END AS provider,
    lr.job_name,
    lr.started_at AS last_started_at,
    lr.finished_at AS last_finished_at,
    lr.status AS last_status,
    lr.rows_fetched,
    lr.rows_loaded,
    lr.error_message AS last_error_message,
    ROUND(EXTRACT(EPOCH FROM (now() - ls.last_success_finished_at)) / 3600.0, 1) AS hours_since_success,
    CASE
        WHEN ls.last_success_finished_at IS NULL THEN 'never_succeeded'
        WHEN lr.status IN ('FAILED', 'ABANDONED') THEN 'failing'
        WHEN now() - ls.last_success_finished_at > INTERVAL '48 hours' THEN 'stale'
        WHEN now() - ls.last_success_finished_at > INTERVAL '6 hours' THEN 'aging'
        ELSE 'healthy'
    END AS health_status
FROM latest_run lr
LEFT JOIN latest_success ls
    ON ls.source_system = lr.source_system AND ls.job_name = lr.job_name;

COMMENT ON VIEW reporting.vw_provider_sync_health IS
    'Power BI: latest sync_runs status per provider/job. health_status thresholds (aging > 6h since last success, stale > 48h) are a starting point -- adjust once real scheduling cadence is confirmed.';
