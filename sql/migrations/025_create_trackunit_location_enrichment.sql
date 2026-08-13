-- Trackunit location enrichment V1.
--
-- A separate table, separate raw tables, and a separate job/connector
-- methods from the already-working metric ETL (staging.trackunit_daily_activity
-- and ge_data_platform/sources/trackunit/daily_activity.py) -- neither of those is touched
-- by this migration or by anything that loads into these new tables.
--
-- Proven in Manitou/manitou_trackunit_exploration.ipynb (machine 5986,
-- report_date 2026-07-05): both start/stop boundary location timestamps and
-- both zone names matched the manual report exactly; a point-in-polygon
-- check independently confirmed the zone. AEMP's historical Locations
-- time-series returns latitude/longitude/altitude/datetime only -- no
-- address/zip/city/country -- so those columns stay NULL in V1 rather than
-- being faked from the *current*-location endpoint (which is "now", not the
-- historical report date).
--
-- Rerunnable: CREATE SCHEMA IF NOT EXISTS / CREATE TABLE IF NOT EXISTS
-- throughout. Never truncates or deletes existing rows.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

-- =============================================================================
-- raw.trackunit_aemp_locations
-- Source: AEMP time-series, {AEMP_BASE_URL}/{account}/Fleet/Equipment/ID/
-- {pin}/Locations/{start}/{end}/{page}. One row per (asset, timestamp) point
-- actually fetched during enrichment (a 48h lookback window per boundary,
-- so the same point can be re-fetched across runs -- upserted, not
-- duplicated). boundary_type records which boundary's lookback window this
-- point was fetched for for traceability; a point seen for both boundaries
-- is upserted (last write wins), not duplicated.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.trackunit_aemp_locations (
    asset_id TEXT NOT NULL,
    location_timestamp_utc TIMESTAMPTZ NOT NULL,
    latitude NUMERIC,
    longitude NUMERIC,
    altitude NUMERIC,
    altitude_units TEXT,
    boundary_type TEXT,
    report_date DATE,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (asset_id, location_timestamp_utc)
);

-- =============================================================================
-- raw.trackunit_site_history
-- Source: GET /api/site/v1/sites/history (assetIds/fromTime/toTime). One row
-- per (asset, siteId, enteredAt) site-assignment interval. leftAt NULL means
-- the assignment is still open/current as of when it was fetched.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.trackunit_site_history (
    asset_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    entered_at TIMESTAMPTZ NOT NULL,
    left_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (asset_id, site_id, entered_at)
);

-- =============================================================================
-- raw.trackunit_sites
-- Source: GET /api/site/v1/sites/{siteId}. One row per site actually looked
-- up during enrichment (not the full sites master -- resolved on demand and
-- cached per run to minimize API calls). polygon_geojson is the raw
-- polygon.geometry as text for traceability/debugging (e.g. re-running the
-- point-in-polygon check proven in the notebook); not parsed further here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.trackunit_sites (
    site_id TEXT PRIMARY KEY,
    site_name TEXT,
    city TEXT,
    country TEXT,
    polygon_geojson TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- staging.trackunit_location_enrichment
-- Grain: one row per (report_date, asset_id) -- same grain and same primary
-- key as staging.trackunit_daily_activity, joined 1:1 by the reporting view.
-- Rerunning enrichment for the same report_date/asset_id UPSERTs the same
-- row (idempotent), never inserts a duplicate.
--
-- Address/zip/city/country columns are present (per explicit request, so
-- the pending fields are visible in the schema, not silently absent) but
-- are ALWAYS NULL in V1 -- the transform never writes to them. AEMP's
-- historical Locations time-series has no address fields at all, and the
-- current-location endpoint's locationAddress is "now", not the historical
-- report date, so it must never be used to fill these in. Do not populate
-- them until a real reverse-geocoding source is introduced.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.trackunit_location_enrichment (
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

    -- Always NULL in V1 -- see comment above.
    address_start TEXT,
    zip_start TEXT,
    city_start TEXT,
    country_start TEXT,
    address_stop TEXT,
    zip_stop TEXT,
    city_stop TEXT,
    country_stop TEXT,

    -- One of: NOT_APPLICABLE_ZERO_ACTIVITY, ENRICHED, PARTIAL, NOT_FOUND.
    -- See docs/powerbi_reporting_data_dictionary.md for the exact meaning of
    -- each value -- never silently blank.
    location_enrichment_status TEXT NOT NULL,
    location_enriched_at TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (report_date, asset_id)
);
