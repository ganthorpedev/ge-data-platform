-- Reliability housekeeping support for etl.sync_runs.
--
-- The canonical schema defines status as unrestricted TEXT, so ABANDONED is
-- already a supported value and no destructive constraint rewrite is needed.
-- This partial index makes the scheduled stale-STARTED lookup/update bounded.
-- Rerunnable and data-preserving.

CREATE INDEX IF NOT EXISTS ix_sync_runs_started_at_when_started
    ON etl.sync_runs (started_at)
    WHERE status = 'STARTED';

COMMENT ON COLUMN etl.sync_runs.status IS
    'Run lifecycle status: STARTED, SUCCESS, FAILED, or ABANDONED.';

-- Keep the existing reporting contract while treating an abandoned latest
-- run as unhealthy in exactly the same way as a failed latest run. Column
-- names, types, and order are unchanged.
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
    'Power BI: latest sync_runs status per provider/job; FAILED and ABANDONED latest runs are failing.';
