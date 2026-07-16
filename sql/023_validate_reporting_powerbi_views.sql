-- Power BI reporting layer validation pack.
-- Read-only. Creates nothing, alters nothing.

-- =============================================================================
-- 1. All expected reporting views exist
--    Healthy result: 11 rows (3 combined-dimension helpers + 4 provider
--    views + 1 report-facing view (vw_ezytrack_trip_report) + 3 conformed
--    views).
-- =============================================================================

SELECT table_name
FROM information_schema.views
WHERE table_schema = 'reporting'
ORDER BY table_name;

-- =============================================================================
-- 2. Row counts per provider (each provider-specific view + the conformed views)
-- =============================================================================

SELECT 'vw_trackunit_daily_activity' AS view_name, provider, COUNT(*) AS row_count
FROM reporting.vw_trackunit_daily_activity GROUP BY provider
UNION ALL
SELECT 'vw_sendem_trips_daily', provider, COUNT(*) FROM reporting.vw_sendem_trips_daily GROUP BY provider
UNION ALL
SELECT 'vw_sendem_events_daily', provider, COUNT(*) FROM reporting.vw_sendem_events_daily GROUP BY provider
UNION ALL
SELECT 'vw_ezytrack_trips', provider, COUNT(*) FROM reporting.vw_ezytrack_trips GROUP BY provider
UNION ALL
SELECT 'vw_assets_all', provider, COUNT(*) FROM reporting.vw_assets_all GROUP BY provider
UNION ALL
SELECT 'vw_daily_activity_all', provider, COUNT(*) FROM reporting.vw_daily_activity_all GROUP BY provider
ORDER BY view_name, provider;

-- =============================================================================
-- 3. Row counts by reporting_source (proves the clean+staging blend is real
--    and visible, not silently collapsed)
-- =============================================================================

SELECT 'vw_sendem_trips_daily' AS view_name, reporting_source, data_quality_status, COUNT(*) AS row_count
FROM reporting.vw_sendem_trips_daily GROUP BY reporting_source, data_quality_status
UNION ALL
SELECT 'vw_sendem_events_daily', reporting_source, data_quality_status, COUNT(*)
FROM reporting.vw_sendem_events_daily GROUP BY reporting_source, data_quality_status
ORDER BY view_name, reporting_source, data_quality_status;

-- =============================================================================
-- 4. Duplicate checks, using each view's documented grain
--    Healthy result: 0 rows in every one of these.
-- =============================================================================

-- 4a. Trackunit: one row per asset per report date
SELECT provider_asset_id, activity_date, COUNT(*)
FROM reporting.vw_trackunit_daily_activity
GROUP BY provider_asset_id, activity_date
HAVING COUNT(*) > 1;

-- 4b. Sendem trips: one row per date_key/group/site/asset (post-dedup)
SELECT date_key, group_id, site_id, provider_asset_id, COUNT(*)
FROM reporting.vw_sendem_trips_daily
GROUP BY date_key, group_id, site_id, provider_asset_id
HAVING COUNT(*) > 1;

-- 4c. Sendem events: one row per date_key/group/site/asset/event_type (post-dedup)
SELECT date_key, group_id, site_id, provider_asset_id, event_type_id, COUNT(*)
FROM reporting.vw_sendem_events_daily
GROUP BY date_key, group_id, site_id, provider_asset_id, event_type_id
HAVING COUNT(*) > 1;

-- 4d. EzyTrack: one row per trip_id
SELECT trip_id, COUNT(*)
FROM reporting.vw_ezytrack_trips
GROUP BY trip_id
HAVING COUNT(*) > 1;

-- 4e. vw_assets_all: one row per provider/provider_asset_id
SELECT provider, provider_asset_id, COUNT(*)
FROM reporting.vw_assets_all
GROUP BY provider, provider_asset_id
HAVING COUNT(*) > 1;

-- 4f. vw_daily_activity_all: one row per provider/provider_asset_id/activity_date
SELECT provider, provider_asset_id, activity_date, COUNT(*)
FROM reporting.vw_daily_activity_all
GROUP BY provider, provider_asset_id, activity_date
HAVING COUNT(*) > 1;

-- =============================================================================
-- 5. Overlap check: prove staging wins over clean in the collision window
--    For each Sendem trip grain key present in BOTH clean and staging, the
--    deduped reporting view must show reporting_source = staging_live.
--    Healthy result: 0 rows (i.e. no collision key resolved to clean).
-- =============================================================================

WITH collisions AS (
    SELECT s.date_key, s.group_id, s.site_id, s.asset_id
    FROM staging.sendem_fact_trips_daily s
    INNER JOIN clean.sendem_fact_trips_daily c
        ON c.date_key = s.date_key AND c.group_id = s.group_id
       AND c.site_id = s.site_id AND c.asset_id = s.asset_id
)
SELECT v.date_key, v.group_id, v.site_id, v.provider_asset_id, v.reporting_source
FROM reporting.vw_sendem_trips_daily v
INNER JOIN collisions col
    ON col.date_key = v.date_key AND col.group_id = v.group_id
   AND col.site_id = v.site_id AND col.asset_id = v.provider_asset_id
WHERE v.reporting_source <> 'staging_live';

-- Same check for events (grain includes event_type_id)
WITH collisions AS (
    SELECT s.date_key, s.group_id, s.site_id, s.asset_id, s.event_type_id
    FROM staging.sendem_fact_events_daily s
    INNER JOIN clean.sendem_fact_events_daily c
        ON c.date_key = s.date_key AND c.group_id = s.group_id
       AND c.site_id = s.site_id AND c.asset_id = s.asset_id
       AND c.event_type_id = s.event_type_id
)
SELECT v.date_key, v.group_id, v.site_id, v.provider_asset_id, v.event_type_id, v.reporting_source
FROM reporting.vw_sendem_events_daily v
INNER JOIN collisions col
    ON col.date_key = v.date_key AND col.group_id = v.group_id
   AND col.site_id = v.site_id AND col.asset_id = v.provider_asset_id
   AND col.event_type_id = v.event_type_id
WHERE v.reporting_source <> 'staging_live';

-- =============================================================================
-- 6. Orphan checks for Sendem events after joining event types
--    Rows where neither staging nor clean's event-type dimension can name
--    the event_type_id. These are flagged data_quality_status =
--    'pending_review' in the view rather than hidden -- confirm the count
--    here matches what the view reports.
-- =============================================================================

SELECT event_type_id, COUNT(*) AS row_count
FROM reporting.vw_sendem_events_daily
WHERE data_quality_status = 'pending_review'
GROUP BY event_type_id
ORDER BY event_type_id;

-- =============================================================================
-- 7. Null critical asset/date fields
--    Healthy result: 0 rows in each.
-- =============================================================================

SELECT 'vw_trackunit_daily_activity' AS view_name, provider_asset_id, activity_date
FROM reporting.vw_trackunit_daily_activity
WHERE provider_asset_id IS NULL OR activity_date IS NULL;

SELECT 'vw_sendem_trips_daily', provider_asset_id, activity_date
FROM reporting.vw_sendem_trips_daily
WHERE provider_asset_id IS NULL OR activity_date IS NULL;

SELECT 'vw_sendem_events_daily', provider_asset_id, activity_date
FROM reporting.vw_sendem_events_daily
WHERE provider_asset_id IS NULL OR activity_date IS NULL;

SELECT 'vw_ezytrack_trips', provider_asset_id, start_time_utc
FROM reporting.vw_ezytrack_trips
WHERE provider_asset_id IS NULL OR start_time_utc IS NULL;

SELECT 'vw_daily_activity_all', provider, provider_asset_id, activity_date
FROM reporting.vw_daily_activity_all
WHERE provider_asset_id IS NULL OR activity_date IS NULL;

-- =============================================================================
-- 8. Negative time/distance metrics
--    Healthy result: 0 rows.
-- =============================================================================

SELECT 'vw_trackunit_daily_activity' AS view_name, provider_asset_id, activity_date
FROM reporting.vw_trackunit_daily_activity
WHERE work_day_minutes < 0 OR operating_minutes < 0 OR active_driving_minutes < 0 OR distance_km < 0;

SELECT 'vw_sendem_trips_daily', provider_asset_id, activity_date
FROM reporting.vw_sendem_trips_daily
WHERE trip_count < 0 OR distance_km < 0 OR fuel_used_litres < 0 OR energy_used_kwh < 0;

SELECT 'vw_ezytrack_trips', trip_id
FROM reporting.vw_ezytrack_trips
WHERE duration_minutes < 0 OR distance_km < 0 OR idle_minutes < 0
   OR stop_minutes < 0 OR time_in_motion_minutes < 0;

SELECT 'vw_daily_activity_all', provider, provider_asset_id, activity_date
FROM reporting.vw_daily_activity_all
WHERE work_day_minutes < 0 OR operating_minutes < 0 OR moving_minutes < 0
   OR idle_minutes < 0 OR stop_minutes < 0 OR duration_minutes < 0 OR distance_km < 0;

-- =============================================================================
-- 9. Latest dates per provider
-- =============================================================================

SELECT provider, MAX(activity_date) AS latest_date, COUNT(*) AS row_count
FROM reporting.vw_daily_activity_all
GROUP BY provider
ORDER BY provider;

-- =============================================================================
-- 10. Min/max date checks: proves Sendem reporting views carry the full
--     historical range (clean backfill), not just the live staging window.
-- =============================================================================

SELECT 'vw_sendem_trips_daily' AS view_name, MIN(activity_date) AS min_date, MAX(activity_date) AS max_date
FROM reporting.vw_sendem_trips_daily
UNION ALL
SELECT 'vw_sendem_events_daily', MIN(activity_date), MAX(activity_date)
FROM reporting.vw_sendem_events_daily
UNION ALL
SELECT 'vw_trackunit_daily_activity', MIN(activity_date), MAX(activity_date)
FROM reporting.vw_trackunit_daily_activity
UNION ALL
SELECT 'vw_ezytrack_trips', MIN(start_time_utc::date), MAX(start_time_utc::date)
FROM reporting.vw_ezytrack_trips;

-- =============================================================================
-- 11. Provider sync health output
-- =============================================================================

SELECT * FROM reporting.vw_provider_sync_health ORDER BY provider;

-- =============================================================================
-- 12. Sample 20 rows from each reporting view
-- =============================================================================

SELECT * FROM reporting.vw_trackunit_daily_activity ORDER BY activity_date DESC, provider_asset_id LIMIT 20;
SELECT * FROM reporting.vw_sendem_trips_daily ORDER BY activity_date DESC, provider_asset_id LIMIT 20;
SELECT * FROM reporting.vw_sendem_events_daily ORDER BY activity_date DESC, provider_asset_id LIMIT 20;
SELECT * FROM reporting.vw_ezytrack_trips ORDER BY start_time_utc DESC LIMIT 20;
SELECT * FROM reporting.vw_assets_all ORDER BY provider, provider_asset_id LIMIT 20;
SELECT * FROM reporting.vw_daily_activity_all ORDER BY activity_date DESC, provider, provider_asset_id LIMIT 20;

-- =============================================================================
-- 13. vw_ezytrack_trip_report checks
-- =============================================================================

-- 13a. View exists (also covered by check 1, repeated here for a targeted result)
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'reporting' AND table_name = 'vw_ezytrack_trip_report';

-- 13b. Row count matches reporting.vw_ezytrack_trips exactly
--      Healthy result: one row, report_count = source_count.
SELECT
    (SELECT COUNT(*) FROM reporting.vw_ezytrack_trips) AS source_count,
    (SELECT COUNT(*) FROM reporting.vw_ezytrack_trip_report) AS report_count;

-- 13c. No duplicate trip rows (trip_id is available on this view)
--      Healthy result: 0 rows.
SELECT trip_id, COUNT(*)
FROM reporting.vw_ezytrack_trip_report
GROUP BY trip_id
HAVING COUNT(*) > 1;

-- 13d. Sample 20 latest rows, ordered by Start Date descending, showing
--      exactly the requested report column names.
SELECT
    "Start Date",
    "Asset Code",
    "Department",
    "Project",
    "Asset",
    "Driver",
    "Start Location",
    "Time in Motion (hh:mm:ss)",
    "Stop Time (hh:mm:ss)",
    "Idle Time (hh:mm:ss)",
    "Odometer Reading at Trip Start (km)",
    "Odometer Reading at Trip End (km)",
    "Distance (km)",
    "Run Time Reading at Trip Start (hrs)",
    "Run Time Reading at Trip End (hrs)",
    "Duration (hh:mm:ss)",
    "Estimated Fuel Consumption (l)"
FROM reporting.vw_ezytrack_trip_report
ORDER BY "Start Date" DESC
LIMIT 20;

-- 13e. Confirm formatted fields are text (hh:mm:ss strings), not numeric
--      seconds, and numeric fields are still numeric. Healthy result: the
--      four "(hh:mm:ss)" columns show data_type = 'text'; the odometer/
--      distance/runtime/fuel columns show data_type = 'numeric'.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'reporting' AND table_name = 'vw_ezytrack_trip_report'
ORDER BY ordinal_position;

-- 13f. Spot-check: a formatted duration column should never look like a bare
--      integer (i.e. every non-null value must contain a ':'). Healthy
--      result: 0 rows.
SELECT trip_id, "Duration (hh:mm:ss)"
FROM reporting.vw_ezytrack_trip_report
WHERE "Duration (hh:mm:ss)" IS NOT NULL
  AND "Duration (hh:mm:ss)" NOT LIKE '%:%:%';
