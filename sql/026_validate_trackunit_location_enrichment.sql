-- Trackunit location enrichment V1 validation pack.
-- Read-only. Creates nothing, alters nothing.

-- =============================================================================
-- 1. No duplicate enrichment rows (report_date, asset_id) is the primary key
--    Healthy result: 0 rows.
-- =============================================================================

SELECT report_date, asset_id, COUNT(*)
FROM staging.trackunit_location_enrichment
GROUP BY report_date, asset_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 2. Row counts by location_enrichment_status
-- =============================================================================

SELECT location_enrichment_status, COUNT(*) AS row_count
FROM staging.trackunit_location_enrichment
GROUP BY location_enrichment_status
ORDER BY location_enrichment_status;

-- =============================================================================
-- 3. Positive-work-day rows with missing enrichment
--    Rows in staging.trackunit_daily_activity that had real activity
--    (work_day_minutes > 0, i.e. real start/stop boundaries) but have no
--    matching staging.trackunit_location_enrichment row at all yet.
--    Not a failure -- V1 has only run for specific dates/machines so far --
--    but this is the queue of what still needs enrichment.
-- =============================================================================

SELECT da.report_date, da.asset_id, da.machine, da.work_day_minutes
FROM staging.trackunit_daily_activity da
LEFT JOIN staging.trackunit_location_enrichment loc
    ON loc.report_date = da.report_date AND loc.asset_id = da.asset_id
WHERE da.work_day_minutes > 0
  AND loc.asset_id IS NULL
ORDER BY da.report_date DESC, da.machine
LIMIT 50;

-- =============================================================================
-- 4. Sample comparison for machine 5986 / 2026-07-05
--    Compare directly against the notebook-proven expected values:
--    start 2026-07-04 20:38 local, stop 2026-07-05 17:01 local,
--    zone TPZ-Bay 13 - FL at both boundaries.
-- =============================================================================

SELECT
    report_date,
    asset_id,
    zone_name_start,
    zone_name_stop,
    last_known_start_location_timestamp_local,
    last_known_stop_location_timestamp_local,
    start_latitude, start_longitude, start_altitude,
    stop_latitude, stop_longitude, stop_altitude,
    location_enrichment_status
FROM staging.trackunit_location_enrichment
WHERE asset_id = (SELECT asset_id FROM staging.trackunit_daily_activity WHERE machine = '5986' AND report_date = '2026-07-05')
  AND report_date = '2026-07-05';

-- =============================================================================
-- 5. Zone match sample -- all enriched rows' start/stop zone names
-- =============================================================================

SELECT report_date, asset_id, zone_name_start, zone_name_stop, location_enrichment_status
FROM staging.trackunit_location_enrichment
WHERE location_enrichment_status IN ('ENRICHED', 'PARTIAL')
ORDER BY report_date DESC, asset_id
LIMIT 20;

-- =============================================================================
-- 6. Location timestamp sample -- UTC vs local side by side, both boundaries
-- =============================================================================

SELECT
    report_date,
    asset_id,
    last_known_start_location_timestamp_utc,
    last_known_start_location_timestamp_local,
    last_known_stop_location_timestamp_utc,
    last_known_stop_location_timestamp_local
FROM staging.trackunit_location_enrichment
WHERE location_enrichment_status IN ('ENRICHED', 'PARTIAL')
ORDER BY report_date DESC, asset_id
LIMIT 20;

-- =============================================================================
-- 7. Null address/zip/city/country count
--    Expected result: equals the total row count below (100% NULL) -- this
--    is the correct, honest V1 state, NOT a failure. No reverse-geocoding
--    source exists yet; these must never be silently filled in.
-- =============================================================================

SELECT
    COUNT(*) AS total_rows,
    COUNT(address_start) AS non_null_address_start,
    COUNT(zip_start) AS non_null_zip_start,
    COUNT(city_start) AS non_null_city_start,
    COUNT(country_start) AS non_null_country_start,
    COUNT(address_stop) AS non_null_address_stop,
    COUNT(zip_stop) AS non_null_zip_stop,
    COUNT(city_stop) AS non_null_city_stop,
    COUNT(country_stop) AS non_null_country_stop
FROM staging.trackunit_location_enrichment;

-- =============================================================================
-- 8. Raw table sanity: no duplicate location points, no orphan site history
-- =============================================================================

SELECT asset_id, location_timestamp_utc, COUNT(*)
FROM raw.trackunit_aemp_locations
GROUP BY asset_id, location_timestamp_utc
HAVING COUNT(*) > 1;

SELECT sh.asset_id, sh.site_id, sh.entered_at
FROM raw.trackunit_site_history sh
LEFT JOIN raw.trackunit_sites s ON s.site_id = sh.site_id
WHERE s.site_id IS NULL;

-- =============================================================================
-- 9. reporting.vw_trackunit_daily_activity sample for machine 5986
-- =============================================================================

SELECT
    provider_asset_id, activity_date, asset_code, asset_name,
    zone_name_start, zone_name_stop,
    last_known_start_location_timestamp_local, last_known_stop_location_timestamp_local,
    start_latitude, start_longitude, stop_latitude, stop_longitude,
    address_start, city_start, country_start,
    location_enrichment_status
FROM reporting.vw_trackunit_daily_activity
WHERE asset_code = 'MAN00000L00885986' OR asset_name = '5986'
ORDER BY activity_date DESC;
