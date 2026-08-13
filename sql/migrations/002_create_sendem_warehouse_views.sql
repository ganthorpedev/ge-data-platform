-- Sendem/MiX warehouse reporting views.
-- Built only from staging.* tables, whose columns are taken directly from
-- sql/migrations/001_create_sendem_schema.sql (the DDL actually applied and proven by
-- the working sync). No table structure is changed here -- views only.
--
-- Rerunnable: CREATE OR REPLACE VIEW is used throughout.

CREATE SCHEMA IF NOT EXISTS warehouse;

-- =============================================================================
-- 1. warehouse.v_sendem_assets
--    Clean asset master view for reporting.
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_assets AS
SELECT
    asset_id,
    site_id,
    fleet_number,
    registration_number,
    description AS asset_description,
    asset_type,
    make,
    model,
    fuel_type,
    year,
    country,
    group_id,
    is_available,
    loaded_at
FROM staging.sendem_dim_assets;

-- =============================================================================
-- 2. warehouse.v_sendem_sites
--    Clean site master view.
--    group_id is intentionally omitted: staging.sendem_dim_sites has no
--    group_id column, because the Sendem sites dimension endpoint does not
--    return a GroupId field (confirmed in transforms/sendem_transform.py and
--    the applied schema in 001_create_sendem_schema.sql).
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_sites AS
SELECT
    site_id,
    site_name
FROM staging.sendem_dim_sites;

-- =============================================================================
-- 3. warehouse.v_sendem_event_types
--    Clean event type/dictionary view.
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_event_types AS
SELECT
    event_type_id,
    group_id,
    event_name,
    event_category,
    metric_type,
    unit_type,
    loaded_at
FROM staging.sendem_dim_event_types;

-- =============================================================================
-- 4. warehouse.v_sendem_daily_trips
--    Human-readable trip fact view. Descriptive attributes (fleet_number,
--    registration_number, description, asset_type, make, model, site_name)
--    are pulled from the current dimension tables rather than the fact
--    table's enrichment-time copies, so this view always reflects the
--    latest master data.
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_daily_trips AS
SELECT
    f.date_key,
    f.date,
    f.group_id,
    f.site_id,
    s.site_name,
    f.asset_id,
    a.fleet_number,
    a.registration_number,
    a.description AS asset_description,
    a.asset_type,
    a.make,
    a.model,
    f.total_trip_count,
    f.total_trip_distance_kilometres,
    f.total_fuel_used_litres,
    f.total_energy_used_kwh,
    f.loaded_at
FROM staging.sendem_fact_trips_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id;

-- =============================================================================
-- 5. warehouse.v_sendem_daily_events
--    Human-readable event fact view. Same dimension-preferred pattern as
--    v_sendem_daily_trips, plus event type details.
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_daily_events AS
SELECT
    f.date_key,
    f.date,
    f.group_id,
    f.site_id,
    s.site_name,
    f.asset_id,
    a.fleet_number,
    a.registration_number,
    a.description AS asset_description,
    f.event_type_id,
    et.event_name,
    et.event_category,
    et.unit_type,
    f.total_event_occurrences,
    f.min_event_value,
    f.max_event_value,
    f.total_event_value,
    f.min_event_duration,
    f.max_event_duration,
    f.total_event_duration,
    f.loaded_at
FROM staging.sendem_fact_events_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id
LEFT JOIN staging.sendem_dim_event_types et ON f.event_type_id = et.event_type_id;

-- =============================================================================
-- 6. warehouse.v_sendem_asset_daily_summary
--    One row per asset per day. staging.sendem_fact_trips_daily is the base
--    grain (its primary key is already date_key/group_id/site_id/asset_id);
--    event totals are aggregated to that same grain and left-joined on.
-- =============================================================================

CREATE OR REPLACE VIEW warehouse.v_sendem_asset_daily_summary AS
WITH events_agg AS (
    SELECT
        date_key,
        group_id,
        site_id,
        asset_id,
        SUM(total_event_occurrences) AS total_event_occurrences,
        SUM(total_event_duration) AS total_event_duration,
        COUNT(DISTINCT event_type_id) AS distinct_event_types
    FROM staging.sendem_fact_events_daily
    GROUP BY date_key, group_id, site_id, asset_id
)
SELECT
    t.date_key,
    t.date,
    t.group_id,
    t.site_id,
    s.site_name,
    t.asset_id,
    a.fleet_number,
    a.registration_number,
    a.description AS asset_description,
    t.total_trip_count,
    t.total_trip_distance_kilometres,
    t.total_fuel_used_litres,
    t.total_energy_used_kwh,
    COALESCE(e.total_event_occurrences, 0) AS total_event_occurrences,
    COALESCE(e.total_event_duration, 0) AS total_event_duration,
    COALESCE(e.distinct_event_types, 0) AS distinct_event_types,
    t.loaded_at
FROM staging.sendem_fact_trips_daily t
LEFT JOIN staging.sendem_dim_assets a ON t.asset_id = a.asset_id
LEFT JOIN staging.sendem_dim_sites s ON t.site_id = s.site_id
LEFT JOIN events_agg e
    ON t.date_key = e.date_key
   AND t.group_id = e.group_id
   AND t.site_id = e.site_id
   AND t.asset_id = e.asset_id;
