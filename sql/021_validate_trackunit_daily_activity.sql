-- Trackunit daily activity validation pack.
-- Read-only. Creates nothing, alters nothing.

-- =============================================================================
-- 1. Duplicate daily rows by report_date + asset_id
--    Healthy result: 0 rows.
-- =============================================================================

SELECT report_date, asset_id, COUNT(*) AS duplicate_count
FROM staging.trackunit_daily_activity
GROUP BY report_date, asset_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 2. Duplicate raw metric rows by asset_id + metric_name + metric_timestamp_utc
--    Healthy result: 0 rows in each of the three result sets.
-- =============================================================================

SELECT 'trackunit_aemp_operating_hours' AS source_table, asset_id, metric_name, metric_timestamp_utc, COUNT(*) AS duplicate_count
FROM raw.trackunit_aemp_operating_hours
GROUP BY asset_id, metric_name, metric_timestamp_utc
HAVING COUNT(*) > 1;

SELECT 'trackunit_aemp_moving_hours' AS source_table, asset_id, metric_name, metric_timestamp_utc, COUNT(*) AS duplicate_count
FROM raw.trackunit_aemp_moving_hours
GROUP BY asset_id, metric_name, metric_timestamp_utc
HAVING COUNT(*) > 1;

SELECT 'trackunit_aemp_distance' AS source_table, asset_id, metric_name, metric_timestamp_utc, COUNT(*) AS duplicate_count
FROM raw.trackunit_aemp_distance
GROUP BY asset_id, metric_name, metric_timestamp_utc
HAVING COUNT(*) > 1;

-- =============================================================================
-- 3. Rows with negative minutes
--    Healthy result: 0 rows.
-- =============================================================================

SELECT report_date, asset_id, machine, work_day_minutes, operating_minutes, active_driving_minutes
FROM staging.trackunit_daily_activity
WHERE work_day_minutes < 0
   OR operating_minutes < 0
   OR active_driving_minutes < 0;

-- =============================================================================
-- 4. Rows with activity but null start/stop
--    A row with operating/moving minutes > 0 should have both boundary
--    timestamps populated. Healthy result: 0 rows.
-- =============================================================================

SELECT report_date, asset_id, machine, operating_minutes, active_driving_minutes,
       start_time_utc, stop_time_utc
FROM staging.trackunit_daily_activity
WHERE (operating_minutes > 0 OR active_driving_minutes > 0)
  AND (start_time_utc IS NULL OR stop_time_utc IS NULL);

-- =============================================================================
-- 5. Row counts by report_date
-- =============================================================================

SELECT report_date, COUNT(*) AS row_count
FROM staging.trackunit_daily_activity
GROUP BY report_date
ORDER BY report_date DESC;

-- =============================================================================
-- 6. Latest loaded report dates
--    Most recently touched report_date rows first, by loaded_at. Useful for
--    confirming a rolling/backfill run actually refreshed the dates it
--    targeted (loaded_at moves forward on every UPSERT, even when the
--    underlying business values didn't change).
-- =============================================================================

SELECT report_date, MAX(loaded_at) AS last_loaded_at, COUNT(*) AS row_count
FROM staging.trackunit_daily_activity
GROUP BY report_date
ORDER BY last_loaded_at DESC
LIMIT 20;

-- =============================================================================
-- 7. sync_runs status history for provider trackunit
-- =============================================================================

SELECT sync_run_id, source_system, job_name, start_date, end_date, status,
       rows_fetched, rows_loaded, error_message, started_at, finished_at
FROM etl.sync_runs
WHERE source_system = 'trackunit'
ORDER BY started_at DESC
LIMIT 20;

-- =============================================================================
-- 8. Missing serial check
--    Healthy result: 0 rows, or a short list you can explain (e.g. assets
--    with no telematics device attached).
-- =============================================================================

SELECT report_date, asset_id, machine, pin
FROM staging.trackunit_daily_activity
WHERE machine_serial_number IS NULL;

-- =============================================================================
-- 9. Null machine check
--    Healthy result: 0 rows.
-- =============================================================================

SELECT report_date, asset_id, pin
FROM staging.trackunit_daily_activity
WHERE machine IS NULL;

-- =============================================================================
-- 10. Sample output, ordered by machine
--     Mirrors the manual Activity Report's columns for easy side-by-side
--     comparison. Replace the report_date below with whichever date you are
--     validating -- this hardcoded date is a validation-example only, not
--     production logic.
-- =============================================================================

SELECT
    machine,
    pin,
    machine_serial_number,
    start_time_local,
    stop_time_local,
    work_day_hhmm,
    operating_hhmm,
    active_driving_hhmm,
    distance_km,
    operating_points,
    moving_points,
    distance_points
FROM staging.trackunit_daily_activity
WHERE report_date = '2026-07-01'
ORDER BY machine;
