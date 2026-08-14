# Data quality

**Status: mixed** -- see each section. All IMPLEMENTED behavior below runs
against `telemetry_warehouse` unless noted otherwise; Trackunit counter-reset
handling also runs against `ge_warehouse` (`raw_trackunit`/`stg_trackunit`,
opt-in via `--target platform` -- see `docs/sources/trackunit.md`). Sendem
ingestion also runs against `ge_warehouse` (`raw_sendem`/`stg_sendem`,
opt-in via `--target platform` -- see `docs/sources/sendem.md`), and so does
EzyTrack (`raw_ezytrack`/`stg_ezytrack`, opt-in via `--target platform` --
see `docs/sources/ezytrack.md`) and Evolution (`raw_evolution`/`stg_evolution`,
opt-in via `--target platform` -- see `docs/sources/evolution.md`). All four
sources skip their bounded post-load validation checks for the platform
target (hardcoded to legacy schema names).

## Counter reset handling

**Status: IMPLEMENTED as an ongoing per-load check (both `telemetry_warehouse`
and `ge_warehouse`). The one-time historical repair migration exists but was
never actually applied to this local `telemetry_warehouse`.**

Full detail in `docs/sources/trackunit.md#counter-reset-handling`; summary: a
mid-series decrease in a Trackunit cumulative counter nulls only that
metric's derived value for that asset/day and sets
`data_quality_status='COUNTER_RESET'` / `counter_reset_detected=true` on the
row, without touching raw readings or other metrics on the same row. This is
the platform's only working example of provider-specific data-quality logic,
and the model for how any future source's quality rules should be scoped
(per-metric, non-destructive to raw, explicit on the row rather than
silently dropped).

**Discovered during the Trackunit `raw_trackunit`/`stg_trackunit` migration**
(see `docs/migration/legacy-to-platform-migration.md#trackunit-migration-completed`):
`sql/legacy/telemetry_migrations/027_add_trackunit_counter_quality.sql` --
which adds `counter_reset_detected`/`data_quality_status` and backfills them
for historical rows -- was written but never actually run against this local
`telemetry_warehouse`. Live catalog inspection confirmed
`staging.trackunit_daily_activity` has neither column, and 238 (asset,
report_date) pairs in the raw AEMP series show a genuine mid-day counter
decrease that legacy staging never nulled (its derived metric still holds
the pre-fix value). The ongoing per-load check in `transform.py` is and was
correct; only the one-time historical repair was skipped. `telemetry_warehouse`
is read-only for platform-migration work, so this was not fixed there.
Instead, `scripts/backfill_trackunit_historical.py` applies 027's exact
detection logic while writing into `ge_warehouse`, so `stg_trackunit.daily_activity`
starts from the corrected state 027 always intended -- 230 rows there are
`COUNTER_RESET`, a documented, verified divergence from the (still-uncorrected)
legacy value for those same rows. `scripts/validate_trackunit_migration.py`
checks this independently rather than trusting the backfill's own output.

**Discovered during the Sendem `raw_sendem`/`stg_sendem` migration** (see
`docs/migration/legacy-to-platform-migration.md#sendem-migration`): legacy
`telemetry_warehouse.clean.sendem_fact_trips_daily`/`_events_daily` hold six
months of history (2026-01-01 to 2026-06-30) that the live incremental API
sync never re-derives (it only ever carries a rolling window). Left alone,
migrating only `raw`/`staging` would have silently dropped that history.
`scripts/backfill_sendem_historical.py` folds `clean.*`'s exclusive keys
into `stg_sendem.trip_daily`/`event_daily` (legacy `staging` loads first and
always wins on the ~577/~4,934 overlapping keys -- `clean` and `staging`
independently re-derived the same June 2026 days and differ by float
precision only, not business content); `scripts/validate_sendem_migration.py`
independently recomputes the expected `staging ∪ clean` union and the
overlap-resolution outcome, rather than trusting the backfill's own
bookkeeping. Separately, 2 `event_type_id`s referenced only by
`clean.sendem_fact_events_daily` (41 rows) exist in no dimension table
anywhere; the backfill synthesizes the same "Unknown Sendem Event Type"
inferred placeholder row `ge_data_platform.sources.sendem.transform.build_dim_event_types()`
would produce live, so no fact row is left orphaned.

**Discovered during the EzyTrack `raw_ezytrack`/`stg_ezytrack` migration**
(see `docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`):
legacy `telemetry_warehouse`'s own EzyTrack catch-up cursor
(`etl.sync_runs`, `source_system='ezytrack'`) was found stale and unhealthy
during the pre-live-test inventory -- last `SUCCESS` 2026-07-21, every
catch-up/reconciliation attempt since `FAILED` with `GraphQL cost rate limit
exceeded`. `ge_warehouse` has no `etl` schema at all, so
`PostgresLoader.get_last_successful_run` (which `ge_data_platform.sources.ezytrack.sync`
uses to compute its catch-up window) now returns `None` immediately for a
platform-settings loader -- **before** issuing any query (originally gated
on `enable_sync_tracking is False`; that flag was later generalized to
`tracking_backend == "platform"` when `ops.pipeline_run`/`ops.table_load`
were wired -- see `docs/operations/pipeline-operations.md#ops-metadata-wiring-status`
-- the guard itself is unchanged). Without this guard, a platform-target
run would either crash (querying a table that doesn't exist) or, worse,
misread legacy's 3-week-stale cursor and attempt an unintended
`max_catchup_hours`-capped (168h) catch-up on its very first invocation.
Verified directly: `PostgresLoader.from_platform_settings(...).get_last_successful_run("ezytrack")`
returns `None` against an engine stub that raises if `connect()` is ever
called, even after `ops.pipeline_run` was wired and genuinely holds
`SUCCESS` rows for platform-target EzyTrack runs. This is a permanent
regression test (`tests/ezytrack/test_ezytrack_platform_target.py`,
`tests/platform/test_ops_audit.py`), not just a one-time manual check.

**Discovered during the Evolution `raw_evolution`/`stg_evolution` migration**
(see `docs/migration/legacy-to-platform-migration.md#evolution-migration-completed`):
the frozen legacy `telemetry_migrations/029` and the still-current
`ge_data_platform.common.database.validate_combined_for_full_replace` both
assume `dbo.vwProjectsReports`'s `Id` column is a `BIGINT` row identifier and
that `(company, id)` is a usable natural primary key. Live read-only
inspection of both GE and TLS Evolution databases disproves this: `Id` is
`VARCHAR` and takes only 11 distinct values total (a transaction-type/module
code, mapping 1:1 to `Module`), and even a 5-column composite key leaves
thousands of duplicate rows per company -- **`dbo.vwProjectsReports` has no
reliable natural key at the row grain.** This is exactly why every real
legacy sync attempt has failed: `etl.sync_runs` shows 3 `FAILED` rows for
`evolution_project_reports` (2026-08-12), two of them refused by
`validate_combined_for_full_replace`'s own duplicate-key check doing exactly
its job -- no data was ever corrupted, the safety net worked as designed.
The frozen legacy migration and legacy validation function were left
unmodified (`telemetry_warehouse` stays untouched, out of scope); instead,
`raw_evolution.project_report`/`stg_evolution.project_report` use a
load-time surrogate `BIGSERIAL` primary key and a new, separate validation
function, `validate_project_report_batch_for_platform_load`, which enforces
only the assumptions the source data actually supports and explicitly
allows duplicate rows. `scripts/validate_evolution_migration.py` reconciles
row counts, null profiles, and exact `Decimal` monetary aggregates against
the exact batch captured at load time by `scripts/run_evolution_first_load.py`
(not a fresh Evolution re-query, which could manufacture a false mismatch
against a live-changing source).

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
