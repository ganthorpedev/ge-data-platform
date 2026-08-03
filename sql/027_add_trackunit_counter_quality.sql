-- Trackunit cumulative-counter reset hardening.
--
-- Idempotent and non-destructive:
--   * adds explicit row-level quality fields to the existing daily grain;
--   * detects historical mid-series decreases (plus already-staged negative
--     deltas), converts only the affected metric to NULL, and marks those rows
--     COUNTER_RESET while leaving raw readings untouched;
--   * enforces that future derived operating/moving/distance values cannot be
--     negative; and
--   * propagates quality through the existing Power BI view without changing
--     the name, type, or order of any pre-existing view column. The new reset
--     flag is appended at the end.

BEGIN;

ALTER TABLE staging.trackunit_daily_activity
    ADD COLUMN IF NOT EXISTS counter_reset_detected BOOLEAN;

ALTER TABLE staging.trackunit_daily_activity
    ADD COLUMN IF NOT EXISTS data_quality_status TEXT;

UPDATE staging.trackunit_daily_activity
SET counter_reset_detected = COALESCE(counter_reset_detected, FALSE),
    data_quality_status = COALESCE(data_quality_status, 'live')
WHERE counter_reset_detected IS NULL
   OR data_quality_status IS NULL;

-- Backfill resets detectable anywhere inside the historical raw series. The
-- three updates stay metric-specific so one reset never erases a different,
-- valid business metric. Raw readings are deliberately preserved.
WITH ordered AS (
    SELECT
        asset_id,
        (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
        metric_value,
        LAG(metric_value) OVER (
            PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
            ORDER BY metric_timestamp_utc
        ) AS previous_value
    FROM raw.trackunit_aemp_operating_hours
    WHERE metric_value IS NOT NULL
), resets AS (
    SELECT DISTINCT asset_id, report_date
    FROM ordered
    WHERE metric_value < previous_value
)
UPDATE staging.trackunit_daily_activity AS daily
SET operating_minutes = NULL,
    operating_hhmm = NULL,
    counter_reset_detected = TRUE,
    data_quality_status = 'COUNTER_RESET'
FROM resets
WHERE daily.asset_id = resets.asset_id
  AND daily.report_date = resets.report_date;

WITH ordered AS (
    SELECT
        asset_id,
        (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
        metric_value,
        LAG(metric_value) OVER (
            PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
            ORDER BY metric_timestamp_utc
        ) AS previous_value
    FROM raw.trackunit_aemp_moving_hours
    WHERE metric_value IS NOT NULL
), resets AS (
    SELECT DISTINCT asset_id, report_date
    FROM ordered
    WHERE metric_value < previous_value
)
UPDATE staging.trackunit_daily_activity AS daily
SET active_driving_minutes = NULL,
    active_driving_hhmm = NULL,
    counter_reset_detected = TRUE,
    data_quality_status = 'COUNTER_RESET'
FROM resets
WHERE daily.asset_id = resets.asset_id
  AND daily.report_date = resets.report_date;

WITH ordered AS (
    SELECT
        asset_id,
        (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
        metric_value,
        LAG(metric_value) OVER (
            PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
            ORDER BY metric_timestamp_utc
        ) AS previous_value
    FROM raw.trackunit_aemp_distance
    WHERE metric_value IS NOT NULL
), resets AS (
    SELECT DISTINCT asset_id, report_date
    FROM ordered
    WHERE metric_value < previous_value
)
UPDATE staging.trackunit_daily_activity AS daily
SET distance_km = NULL,
    counter_reset_detected = TRUE,
    data_quality_status = 'COUNTER_RESET'
FROM resets
WHERE daily.asset_id = resets.asset_id
  AND daily.report_date = resets.report_date;

-- Also repair historical negative deltas when their raw boundary readings are
-- unavailable. A negative final-minus-first value is definitive evidence of
-- a reset.
UPDATE staging.trackunit_daily_activity
SET operating_minutes = CASE WHEN operating_minutes < 0 THEN NULL ELSE operating_minutes END,
    operating_hhmm = CASE WHEN operating_minutes < 0 THEN NULL ELSE operating_hhmm END,
    active_driving_minutes = CASE
        WHEN active_driving_minutes < 0 THEN NULL ELSE active_driving_minutes
    END,
    active_driving_hhmm = CASE
        WHEN active_driving_minutes < 0 THEN NULL ELSE active_driving_hhmm
    END,
    distance_km = CASE WHEN distance_km < 0 THEN NULL ELSE distance_km END,
    counter_reset_detected = TRUE,
    data_quality_status = 'COUNTER_RESET'
WHERE operating_minutes < 0
   OR active_driving_minutes < 0
   OR distance_km < 0;

ALTER TABLE staging.trackunit_daily_activity
    ALTER COLUMN counter_reset_detected SET DEFAULT FALSE,
    ALTER COLUMN counter_reset_detected SET NOT NULL,
    ALTER COLUMN data_quality_status SET DEFAULT 'live',
    ALTER COLUMN data_quality_status SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'staging.trackunit_daily_activity'::regclass
          AND conname = 'trackunit_daily_activity_nonnegative_counters'
    ) THEN
        ALTER TABLE staging.trackunit_daily_activity
            ADD CONSTRAINT trackunit_daily_activity_nonnegative_counters
            CHECK (
                (operating_minutes IS NULL OR operating_minutes >= 0)
                AND (active_driving_minutes IS NULL OR active_driving_minutes >= 0)
                AND (distance_km IS NULL OR distance_km >= 0)
            );
    END IF;
END
$$;

COMMENT ON COLUMN staging.trackunit_daily_activity.counter_reset_detected IS
    'True when any cumulative operating, moving, or distance counter decreased within the report-day series.';

COMMENT ON COLUMN staging.trackunit_daily_activity.data_quality_status IS
    'live for normal rows; COUNTER_RESET when a derived cumulative metric was nulled after a counter decrease.';

CREATE OR REPLACE VIEW reporting.vw_trackunit_daily_activity AS
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
    da.counter_reset_detected
FROM staging.trackunit_daily_activity da
LEFT JOIN staging.trackunit_dim_assets dim ON dim.asset_id = da.asset_id
LEFT JOIN staging.trackunit_location_enrichment loc
    ON loc.report_date = da.report_date AND loc.asset_id = da.asset_id;

COMMENT ON VIEW reporting.vw_trackunit_daily_activity IS
    'Power BI: one row per Trackunit machine per local report date, including location enrichment and explicit cumulative-counter reset quality. Existing columns retain their names/order; counter_reset_detected is appended.';

COMMIT;
