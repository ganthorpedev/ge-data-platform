# Data quality

**Status: mixed** -- see each section. All IMPLEMENTED behavior below runs
against `telemetry_warehouse`.

## Counter reset handling

**Status: IMPLEMENTED**, both as an ongoing per-load check and as a
one-time historical repair. Full detail in
`docs/sources/trackunit.md#counter-reset-handling`; summary: a mid-series
decrease in a Trackunit cumulative counter nulls only that metric's derived
value for that asset/day and sets `data_quality_status='COUNTER_RESET'` /
`counter_reset_detected=true` on the row, without touching raw readings or
other metrics on the same row. This is the platform's only working example
of provider-specific data-quality logic, and the model for how any future
source's quality rules should be scoped (per-metric, non-destructive to
raw, explicit on the row rather than silently dropped).

## Validation SQL philosophy

Two tiers, deliberately different in cost and trigger:

1. **Bounded, automatic, post-load** (`PostgresLoader.run_post_load_validation`,
   `POST_LOAD_VALIDATION_QUERIES` in `ge_data_platform/common/database.py`):
   runs after every Sendem, EzyTrack, and Trackunit daily-activity load,
   scoped to rows touched within `ETL_VALIDATION_LOOKBACK_HOURS` (default
   24) via `loaded_at`. Checks are cheap by design -- e.g. "any negative
   trip metric in the last N hours," not a full-history scan. Controlled by
   `ETL_VALIDATION_MODE`:
   - `off` -- skipped, logged.
   - `warn` (default) -- findings and query errors are logged; the job does
     not fail.
   - `strict` -- a critical finding, or a query error in a critical check,
     fails the job. Non-critical (informational) findings never block, even
     in `strict` mode.
2. **Manual, full-history, read-only packs** (`sql/validation/*.sql`): run
   by hand after a backfill or reconciliation, or whenever a full-history
   check is warranted. Not run automatically by any job.

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_sendem_pipeline.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_sendem_idempotency.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_ezytrack_idempotency.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_trackunit_daily_activity.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_reporting_powerbi_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_trackunit_location_enrichment.sql
```

Trackunit location enrichment relies entirely on its manual pack -- there is
no automatic post-load check for it (`POST_LOAD_VALIDATION_QUERIES["trackunit_location"]`
is deliberately empty).

The `ge_warehouse` platform baseline has its own validation pack,
`sql/validation/validate_ge_warehouse_baseline.sql` (schema existence, role
grants, `core.dim_date` correctness -- see
`docs/development/migrations.md`), unrelated to the two tiers above since it
checks platform structure, not provider data.

## Duplicate/idempotency checks

`sql/validation/validate_sendem_idempotency.sql` and
`validate_ezytrack_idempotency.sql` exist specifically to confirm that
re-running a sync window does not duplicate rows -- i.e. that the UPSERT
conflict keys documented per source in `docs/sources/` are actually being
honored in practice, not just in code review.

## Schema validation

`ge_data_platform.common.migrations.discover_migrations` mechanically
rejects a duplicate migration-file numeric prefix and enforces the 3-digit
zero-padded convention -- see `docs/development/migrations.md`. There is no
runtime (post-deploy) schema-drift check today; migrations are the only
schema-validation mechanism.

## Data freshness

**Status: IMPLEMENTED** via `telemetry_provider_freshness_sensor` -- see
`docs/operations/monitoring-and-alerting.md`. Freshness is defined per
provider as "time since the last `SUCCESS` row in `etl.sync_runs`," not as
a check on the data's own content.

## Future: `ops.data_quality_result`

**Status: PLANNED wiring, IMPLEMENTED structure only.** The table exists in
`ge_warehouse` (`sql/migrations/002_create_ops_metadata.sql`) intended to
persist exactly the bounded post-load validation results described above
(`check_name`, `critical`, `status`, `issue_count`, tied to a
`pipeline_run_id`) instead of only logging them. No code writes to it yet --
today's `PASS`/`FAIL`/`ERROR` results are visible only in job logs.

## Inspecting run history

```sql
-- Recent provider outcomes
SELECT sync_run_id, source_system, job_name, status, started_at, finished_at,
       rows_fetched, rows_loaded, error_message
FROM etl.sync_runs
ORDER BY started_at DESC
LIMIT 100;

-- Failures, abandoned, and stale-awaiting-cleanup rows
SELECT sync_run_id, source_system, job_name, status, started_at, finished_at, error_message
FROM etl.sync_runs
WHERE status IN ('FAILED', 'ABANDONED')
   OR (status = 'STARTED' AND started_at < CURRENT_TIMESTAMP - INTERVAL '12 hours')
ORDER BY started_at DESC;

-- Last success and current age, by provider
SELECT source_system,
       MAX(finished_at) FILTER (WHERE status = 'SUCCESS') AS last_success_at,
       CURRENT_TIMESTAMP - MAX(finished_at) FILTER (WHERE status = 'SUCCESS') AS success_age
FROM etl.sync_runs
GROUP BY source_system
ORDER BY source_system;
```

Replace the `12 hours` literal above if `ETL_ABANDONED_RUN_HOURS` is
configured differently.
