# Reliability operations runbook

This runbook covers deployment, scheduling, recovery, and diagnosis for the
Sendem, EzyTrack, and Trackunit reliability hardening. It assumes PowerShell
unless noted.

> Carried over from the flat `telemetry_etl` project during the
> `ge_data_platform` package restructure. Module invocations below already
> reflect the new `ge_data_platform.*` paths; the production installation
> path itself (still `C:\Local Warehouse\Telemetry\telemetry-etl` today) is
> a separate, later cutover and is not changed by this restructure.

## Production deployment order

1. Stop the telemetry schedules and sensors in Dagster; let any active provider
   subprocess finish or terminate it through Dagster before replacing files.
2. Back up the database according to the existing production procedure, deploy
   the project files, and run `python -m pip install -e . --no-deps` with
   the Python 3.13 installation used by the Windows SYSTEM services.
3. Merge new keys from `.env.example` into the canonical project-root `.env`.
   Keep credentials out of source control and ensure the SYSTEM account can read
   the file. Ensure SYSTEM and the operators who run manual recovery commands
   can create and modify project-root `.ge_data_platform_locks` so both
   execution paths contend on the same OS lock.
4. Apply migrations 027 and 028 in order, then run the offline tests, CLI help
   checks, and Dagster definition listings documented below.
5. Keep run monitoring enabled in the external `DAGSTER_HOME\dagster.yaml`, then
   restart the Dagster daemon and webserver services so code and configuration
   are reloaded.
6. Run the smallest live provider smoke tests one provider at a time, inspect
   the run/table logs and reporting views, and only then enable the intended
   schedules and sensors.

## Configuration and `.env` precedence

Keep one canonical `.env` beside `README.md` at the project root. Copy
`.env.example`, supply real values locally, and never commit it.

Configuration is loaded in this order, with the first value found winning:

1. Environment variables already present in the Python/Dagster process.
2. `<project-root>/.env`, loaded by absolute path regardless of the working
   directory.
3. A legacy `.env` in the current working directory. This compatibility path is
   temporary and emits a warning so its values can be moved to the canonical
   file.

The canonical file therefore overrides a nested legacy file, while service-level
environment variables override both. Restart the Dagster webserver and daemon
after changing `.env`; permanent processes do not automatically reload it.
`.gitignore` excludes `.env` and `.env.*`, with only `.env.example` explicitly
allowed.

### Reliability environment variables

The full credential and connection template is `.env.example`. The variables
introduced or made configurable by this reliability pass are below.

| Variable | Default | Meaning |
| --- | ---: | --- |
| `POSTGRES_POOL_TIMEOUT_SECONDS` | `30` | Seconds to wait for a pooled PostgreSQL connection. Connections are pre-pinged and recycled after 1,800 seconds. |
| `HTTP_MAX_RETRIES` | `3` | Maximum total attempts, including the first request, for a transient provider request. |
| `HTTP_BACKOFF_SECONDS` | `2` | Backoff factor in seconds for exponential transient-failure retries. |
| `HTTP_CONNECT_TIMEOUT_SECONDS` | `30` | Per-request connection timeout. |
| `HTTP_READ_TIMEOUT_SECONDS` | `120` | Per-request response-read timeout. |
| `TRACKUNIT_REQUEST_DELAY_SECONDS` | `1` | Pacing delay before each Trackunit AEMP request. |
| `TRACKUNIT_MAX_RETRIES` | `7` | Maximum total 429 attempts before the Trackunit request fails. |
| `TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS` | `30` | Initial fallback wait when 429 has no valid `Retry-After` seconds value. |
| `TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS` | `300` | Cap applied to the server/fallback 429 delay before 0-3 seconds of jitter. |
| `EZYTRACK_MAX_CATCHUP_HOURS` | `168` | Oldest window a normal cursor-based catch-up may fetch automatically. |
| `EZYTRACK_CATCHUP_OVERLAP_MINUTES` | `30` | Overlap subtracted from the last successful cursor. |
| `EZYTRACK_MAX_PAGES` | `500` | Hard page limit for each EzyTrack paginated trip request. |
| `EZYTRACK_RECONCILIATION_LOOKBACK_HOURS` | `48` | Fixed lookback used by `--reconcile` and the daily reconciliation job. |
| `ETL_ABANDONED_RUN_HOURS` | `12` | Age after which inactive `STARTED` rows are eligible for `ABANDONED`. |
| `ETL_VALIDATION_MODE` | `warn` | Post-load mode: `off`, `warn`, or `strict`. |
| `ETL_VALIDATION_LOOKBACK_HOURS` | `24` | Recent-data window used by bounded post-load SQL checks. |
| `SENDEM_JOB_TIMEOUT_MINUTES` | `60` | Dagster subprocess limit for Sendem jobs. |
| `EZYTRACK_JOB_TIMEOUT_MINUTES` | `60` | Dagster subprocess limit for EzyTrack jobs, including reconciliation. |
| `TRACKUNIT_JOB_TIMEOUT_MINUTES` | `360` | Limit for a standalone Trackunit job and shared total budget for the daily activity-plus-enrichment job. |
| `ETL_SUBPROCESS_TERMINATE_GRACE_SECONDS` | `10` | Grace period after terminate before a timed-out subprocess is killed. |
| `TELEMETRY_ALERTS_ENABLED` | `false` | Enables generic webhook delivery when set to a true value such as `true`, `1`, `yes`, or `on`. |
| `TELEMETRY_ALERT_WEBHOOK_URL` | empty | Destination for generic JSON operational alerts; no provider is hardcoded. |
| `TELEMETRY_ALERT_COOLDOWN_MINUTES` | `360` | Deduplication window for the same alert key. |
| `SENDEM_MAX_SUCCESS_AGE_HOURS` | `12` | Sendem freshness threshold. |
| `EZYTRACK_MAX_SUCCESS_AGE_HOURS` | `12` | EzyTrack freshness threshold. |
| `TRACKUNIT_MAX_SUCCESS_AGE_HOURS` | `30` | Trackunit freshness threshold. |

The EzyTrack reliability controls work with the existing
`TELEMATICS_LOOKBACK_HOURS` (first run, default `6`),
`TELEMATICS_CHUNK_HOURS` (default `1`), and `TELEMATICS_PAGE_SIZE` (default
`50`). `TRACKUNIT_TIMEZONE` remains the operational date timezone and defaults
to `Africa/Harare`.

## Network retry and rate-limit policy

All providers apply bounded retries only to connection failures, timeouts, and
HTTP 500, 502, 503, and 504. The shared default is three total attempts, with
exponential timing governed by `HTTP_BACKOFF_SECONDS=2`. Connect and read
timeouts are always explicit. Ordinary 4xx responses are not retried.

Trackunit adds two provider-specific controls to every authenticated GET:

- On 401, it refreshes OAuth and repeats the request exactly once. A second
  401 raises an error that identifies the request context.
- On 429, it uses `Retry-After` when it is a valid non-negative number of
  seconds. A missing, date-form, negative, or invalid value uses exponential
  fallback. The chosen delay is capped at the configured rate-limit maximum,
  then receives 0-3 seconds of random jitter. The log includes metric, PIN,
  request context, attempt, maximum attempts, and wait time. Exhausting the
  bounded attempts raises instead of returning partial data. With defaults, a
  persistent 429 produces six sleeps before the seventh and final response:
  `30, 60, 120, 240, 300, 300` seconds, each plus jitter.
- AEMP calls are additionally paced by `TRACKUNIT_REQUEST_DELAY_SECONDS`.
  Trackunit does not use adapter-level retries, so its manual transient policy
  is not duplicated by urllib3; transient and 429 counters remain separate.

Sendem and EzyTrack use the shared retry session. EzyTrack read-only GraphQL
POSTs are safe to repeat for the listed transient failures. On an EzyTrack HTTP
401 or GraphQL authentication error, the client re-authenticates and retries
exactly once; a second authentication failure raises. Its GraphQL cost-limit
error is not retried automatically. Other provider rate-limit or authentication
errors outside the explicitly controlled cases fail clearly and can be
recovered using the idempotent commands below.

## Sendem empty payloads

Sendem transforms retain their expected loader columns when trips, assets, or
sites are empty. Trips can still load when either dimension is missing; the
unavailable enrichment fields remain `NULL` instead of causing merge `KeyError`
failures. The scheduled job no longer calls the unused people or organisations
endpoints. Their legacy transform inputs remain accepted for compatibility, but
they are not part of any current load plan.

## EzyTrack gap recovery

A normal `python -m ge_data_platform.sources.ezytrack.sync` run finds the newest successful
EzyTrack row in `etl.sync_runs`. The current schema uses that run's
`started_at` timestamp as the window-end cursor because the fetch window is
anchored immediately before the row is inserted. The next UTC half-open window
starts at that cursor minus `EZYTRACK_CATCHUP_OVERLAP_MINUTES` and ends at the
current time. It is capped at `EZYTRACK_MAX_CATCHUP_HOURS` so an unexpectedly old
cursor cannot create an unbounded provider request.

If no successful run exists, the job preserves the existing
`TELEMATICS_LOOKBACK_HOURS` first-run window. A reconciliation run ignores the
cursor and uses `EZYTRACK_RECONCILIATION_LOOKBACK_HOURS`.

Both modes retain hourly chunking, pagination, and UPSERTs. Each chunk tracks
the cursors it has seen and raises if `endCursor` repeats, if a next page has no
cursor, or if another page would exceed `EZYTRACK_MAX_PAGES`. A failed chunk
fails the entire sync run; no partial result is labelled `SUCCESS`. Overlap
duplicates are deduplicated by trip ID before loading and remain safe at the
database UPSERT keys.

The catch-up cap is intentionally not a promise to heal arbitrarily long
outages. Use reconciliation with an explicitly reviewed lookback for any gap
older than the cap.

## Dagster definitions and schedules

All cron times use `Africa/Harare`. Provider and housekeeping schedules default
to `STOPPED`, so review and start them explicitly after deployment. The two
monitoring sensors default to `RUNNING`: they do not launch provider loads, and
alert delivery still follows `TELEMETRY_ALERTS_ENABLED` and webhook settings.

The intended automation is:

| Definition | Cadence | Work |
| --- | --- | --- |
| `sendem_sync_schedule` | `35 */3 * * *` | Existing Sendem run every three hours. |
| `ezytrack_sync_schedule` | `45 */3 * * *` | Existing EzyTrack normal cursor/catch-up run every three hours. |
| `ezytrack_daily_reconciliation_schedule` | `15 1 * * *` | Daily EzyTrack fixed-lookback `--reconcile` run at 01:15. |
| `trackunit_daily_refresh_schedule` | `5 2 * * *` | Daily rolling two-day activity load followed by location enrichment. |
| `trackunit_intraday_refresh_schedule` | `20 */3 * * *` | Safe intraday rolling one-day activity-only refresh; no enrichment. |
| `trackunit_rolling_7_days_schedule` | `45 1 * * 0` | Sunday 01:45 rolling seven-day Trackunit reconciliation. |
| `stale_started_run_cleanup_schedule` | `20 * * * *` | Hourly cleanup of eligible inactive `STARTED` rows. |
| `telemetry_run_failure_sensor` | event-driven | Alerts once for each failed Dagster run. |
| `telemetry_provider_freshness_sensor` | every 15 minutes | Checks last successful provider syncs and emits cooldown-deduplicated stale alerts. |

`trackunit_intraday_refresh_schedule` restores frequent Trackunit activity
polling without reintroducing the 429 pressure the old every-three-hours
schedule caused: it fetches only the current rolling day (`--rolling-days 1`,
a materially smaller window than the old schedule's rolling two days) and
never runs location enrichment, keeping that heavier work exclusive to
`trackunit_daily_refresh_schedule`. All four Trackunit activity/enrichment
definitions (`trackunit_daily_refresh`, `trackunit_intraday_refresh`,
`trackunit_rolling_7_days`, `trackunit_location_enrichment`) also guard
against each other because they write the same staging table. Schedule-time
checks skip conflicting active runs, provider-level OS locks cover manual
Dagster launches, and the production configuration example adds a run-tag
concurrency limit for Trackunit. Both direct Trackunit CLI entry points
acquire that same cross-process lock, so a recovery command also refuses to
start while Trackunit activity or enrichment is running. The retained lock
files live under project-root `.ge_data_platform_locks`; only the OS lock on an
open handle is authoritative, so a crash cannot leave a permanent logical
lock.

The repository exports these ten Dagster jobs through
`ge_data_platform.orchestration.definitions`: `dagster_smoke_test`, `sendem_sync`,
`ezytrack_sync`, `ezytrack_daily_reconciliation`, `trackunit_daily_refresh`,
`trackunit_intraday_refresh`, `trackunit_rolling_7_days`,
`trackunit_location_enrichment`, `accounts_evolution_project_reports_sync`,
and `stale_started_run_cleanup`. Standalone jobs are available for recovery
even when they do not have their own schedule.

Load and inspect the code location before enabling anything:

```powershell
python -c "from ge_data_platform.orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = 'C:\Local Warehouse\Telemetry\dagster_home'
dagster job list -m ge_data_platform.orchestration.definitions
dagster schedule list -m ge_data_platform.orchestration.definitions
dagster sensor list -m ge_data_platform.orchestration.definitions
```

The lists must contain only the expected schedules and sensors above. Start the
seven schedules from the Dagster Automation UI after migrations, tests, and
smoke checks pass. Confirm both sensors are running.

### Dagster process monitoring

Subprocess limits are enforced by `orchestration/runner.py`, independently of
Dagster's own run monitoring. Provider stdout/stderr continues to stream into
the Dagster event log. On timeout the runner terminates the child, waits for the
configured grace period, force-kills it if necessary, and raises a Dagster
failure containing the timeout and captured output. The Dagster run reaches a
terminal state, allowing the overlap guard to admit a future schedule.

`trackunit_daily_refresh` holds one Trackunit lock and one
`TRACKUNIT_JOB_TIMEOUT_MINUTES` budget across its two sequential children.
Activity receives the initial budget; enrichment receives only the remaining
wall-clock time and is not started if activity consumes it all. Standalone
Trackunit activity or enrichment jobs each receive the full configured limit.

The production `dagster.yaml` is outside this repository under
`C:\Local Warehouse\Telemetry\dagster_home`. Merge the supported settings from
`dagster.yaml.example` into that existing file without replacing its storage,
scheduler, run launcher, or other production settings:

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 25200
  max_resume_run_attempts: 0
  poll_interval_seconds: 60
  cancellation_thread_poll_interval_seconds: 10
  free_slots_after_run_end_seconds: 0

concurrency:
  runs:
    tag_concurrency_limits:
      - key: telemetry/provider
        value: trackunit
        limit: 1
```

The seven-hour global ceiling sits beyond the combined default six-hour
Trackunit daily-refresh budget, leaving time for graceful termination and
bookkeeping. The shorter provider-specific limits remain authoritative inside
the subprocess runner.
Restart the Windows SYSTEM Dagster daemon and webserver after changing
`dagster.yaml`, then confirm the instance loads with:

```powershell
$env:DAGSTER_HOME = 'C:\Local Warehouse\Telemetry\dagster_home'
dagster instance info
```

## Failure bookkeeping and `ABANDONED` runs

Every provider starts a row in `etl.sync_runs` and finishes it as `SUCCESS` or
`FAILED`. If provider work fails and the database update to `FAILED` also fails,
the bookkeeping error is logged but the original provider/transform/load
exception is re-raised. The surviving `STARTED` row is then visible for stale
cleanup instead of masking the root cause.

Housekeeping considers a `STARTED` row stale after
`ETL_ABANDONED_RUN_HOURS`. It does not mark rows while Dagster reports relevant
ETL work as active. An unrecognised source/job mapping also fails closed and
leaves every candidate unchanged for operator review. Eligible rows become `ABANDONED`, retain their original
identity and timestamps, receive a cleanup note, and generate an operational
alert. This is status repair only: it neither deletes data nor retries a
provider. Diagnose the original run and use the provider recovery commands.

## Alerts and freshness

Dagster monitors run failures, provider freshness, and housekeeping transitions
to `ABANDONED`. Alerts identify the provider/job, run ID where applicable,
failure text, event timestamp, and last successful sync time. The failure sensor
uses Dagster's persisted run-status cursor to alert once per failed run. The
freshness sensor stores each provider's last alert time in its durable cursor
and suppresses repeats for `TELEMETRY_ALERT_COOLDOWN_MINUTES`; recovery clears
that provider's cooldown. The cooldown also advances after log-only or failed
delivery attempts so a disabled or broken endpoint cannot produce a tight
15-minute spam loop. An `ABANDONED` alert is naturally one-time because it is
emitted only as the row transitions out of `STARTED`.

Delivery is generic webhook JSON. If alerts are disabled or the webhook URL is
empty, the complete alert is logged and ETL continues. Webhook delivery trouble
is also treated as an operational log event, not a reason to replace the job's
original result. Set the provider freshness thresholds longer than their normal
schedule plus expected runtime; the defaults are listed above.

## PostgreSQL loading and UTC timestamps

The SQLAlchemy engine checks pooled connections before checkout, recycles them
after 1,800 seconds, and uses `POSTGRES_POOL_TIMEOUT_SECONDS` when a pool is
busy. This matters for Trackunit runs that may spend hours fetching before the
next database operation.

Loads retain the established temporary-table merge and conflict keys. The
temporary `to_sql` stage writes with `method="multi"` in chunks of 500 rows;
the chunk size keeps wide tables comfortably below PostgreSQL's bind-parameter
limit while avoiding row-by-row inserts. It does not change UPSERT identity or
overwrite policy. New `loaded_at` values and application-generated run/window
timestamps are timezone-aware UTC, independent of the Windows machine's local
clock.

## Post-load validation

The Sendem and EzyTrack jobs, plus the Trackunit daily-activity job, run a
bounded recent-data subset of the existing validation SQL:

- `ETL_VALIDATION_MODE=off` skips it and logs the choice.
- `ETL_VALIDATION_MODE=warn` is the default. Findings and query errors are
  clearly logged without unnecessarily failing production.
- `ETL_VALIDATION_MODE=strict` fails the job on a critical finding or a query
  error in a critical check. Informational findings and informational-check
  query errors stay non-blocking.

`ETL_VALIDATION_LOOKBACK_HOURS` bounds these automatic checks; expensive
full-history packs are not run after every small sync. After a backfill or
reconciliation, run the appropriate read-only pack manually:

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_sendem_pipeline.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_sendem_idempotency.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_ezytrack_idempotency.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_trackunit_daily_activity.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_reporting_powerbi_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\validation\validate_trackunit_location_enrichment.sql
```

Add `-h`, `-p`, and `-U` if the normal PostgreSQL client defaults do not target
the warehouse. Do not put the password on the command line.
Trackunit location enrichment relies on its manual
validate_trackunit_location_enrichment.sql pack rather than an automatic
post-load check.

Trackunit cumulative counter decreases are not clamped. The affected derived
operating, moving, or distance metric is stored as `NULL`, while
`counter_reset_detected=true` and `data_quality_status='COUNTER_RESET'` make the
reason explicit in staging and reporting. Valid positive deltas are unchanged.

## Manual provider recovery

All recovery commands use the normal UPSERT paths. First confirm no scheduled
run for that provider is active, identify the failed/missing window from
`etl.sync_runs`, and run from the project root.

### Sendem

The normal window still comes from `SYNC_LOOKBACK_DAYS`. For a reviewed recovery
window, the CLI override takes precedence for that invocation:

```powershell
python -m ge_data_platform.sources.sendem.sync --lookback-days 14
```

### EzyTrack

Use the normal command to continue from the newest successful cursor, up to the
configured catch-up cap:

```powershell
python -m ge_data_platform.sources.ezytrack.sync
```

Use reconciliation when the success cursor is unsuitable or the gap predates
the cap. Review provider cost/rate limits before increasing a long window:

```powershell
$env:EZYTRACK_RECONCILIATION_LOOKBACK_HOURS = '72'
python -m ge_data_platform.sources.ezytrack.sync --reconcile
```

An environment assignment in the current PowerShell session overrides `.env`.
Start a new session or remove that override before returning to the configured
default.

### Trackunit

Recover the smallest known date or inclusive range. Report dates are local
`TRACKUNIT_TIMEZONE` calendar dates; API windows are converted to UTC.

```powershell
python -m ge_data_platform.sources.trackunit.daily_activity --date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --from-date 2026-07-27 --to-date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --rolling-days 7
```

If location enrichment is required, run it after activity succeeds for each
date:

```powershell
python -m ge_data_platform.sources.trackunit.location --date 2026-08-02
```

`--machines` and `--limit` are diagnostic/smoke-test filters, not a replacement
for the full production date load. After a subset test, run the unfiltered date
command. A failed multi-date activity run identifies the exact failing date;
rerunning that date or range is safe. Trackunit retry and pacing logs should be
kept with the recovery record.

## Migrations

For an existing production database that already has migrations through 026,
apply the reliability migrations in numeric order before starting the hardened
jobs:

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\027_add_trackunit_counter_quality.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\028_add_sync_run_abandoned_support.sql
```

Migration 027 adds Trackunit counter-quality columns, repairs historical
negative derived values to `NULL` with `COUNTER_RESET`, adds a non-negative
constraint, and recreates the existing reporting view with non-breaking quality
fields. Migration 028 adds the partial index used by stale-`STARTED` cleanup and
documents `ABANDONED` as a supported status. Both migrations are idempotent and
preserve historical rows. They are not automatically executed by a provider
job.

Migration 027 groups historical raw readings into local report dates using
`Africa/Harare`, matching the production default. If `TRACKUNIT_TIMEZONE` is
configured differently, review and adjust those three migration expressions
before applying it.

Do not use a blind numeric migration runner for a brand-new database: reporting
SQL 022 depends on the Trackunit location tables created by 025 and on the
legacy `clean.sendem_*` tables. An empty database needs this object-creation
order before the validation packs:

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\001_create_sendem_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -c 'CREATE SCHEMA IF NOT EXISTS clean;'
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\sendem_tables.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\002_create_sendem_warehouse_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\004_create_etl_sync_table_loads.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\010_create_ezytrack_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\020_create_trackunit_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\025_create_trackunit_location_enrichment.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\022_create_reporting_powerbi_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\027_add_trackunit_counter_quality.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\028_add_sync_run_abandoned_support.sql
```

Apply the separately controlled Power BI reader-role script only after replacing
its credential placeholder according to the existing deployment procedure.
The updated base Trackunit schema already includes the quality columns, but
rerunning 027 remains safe.

Verify the migration results:

```sql
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'staging'
  AND table_name = 'trackunit_daily_activity'
  AND column_name IN ('counter_reset_detected', 'data_quality_status')
ORDER BY column_name;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'etl'
  AND tablename = 'sync_runs'
  AND indexname = 'ix_sync_runs_started_at_when_started';

SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'staging.trackunit_daily_activity'::regclass
  AND conname = 'trackunit_daily_activity_nonnegative_counters';

SELECT COUNT(*) AS negative_derived_rows
FROM staging.trackunit_daily_activity
WHERE operating_minutes < 0
   OR active_driving_minutes < 0
   OR distance_km < 0;

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'reporting'
  AND table_name = 'vw_trackunit_daily_activity'
  AND column_name IN ('data_quality_status', 'counter_reset_detected')
ORDER BY column_name;

SELECT
    report_date,
    asset_id,
    operating_minutes,
    active_driving_minutes,
    distance_km,
    counter_reset_detected,
    data_quality_status,
    loaded_at
FROM staging.trackunit_daily_activity
WHERE counter_reset_detected
ORDER BY report_date DESC, asset_id
LIMIT 100;
```

Reset rows must have `data_quality_status='COUNTER_RESET'`; each affected
derived metric is `NULL`, while unaffected metrics and raw counter readings are
preserved. `sql/validation/validate_trackunit_daily_activity.sql` also checks the
staging/reporting propagation and non-negative invariant.

## Inspect sync and table-load history

Recent provider outcomes, durations, counts, and errors:

```sql
SELECT
    sync_run_id,
    source_system,
    job_name,
    start_date,
    end_date,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (COALESCE(finished_at, CURRENT_TIMESTAMP) - started_at))::bigint
        AS elapsed_seconds,
    rows_fetched,
    rows_loaded,
    error_message
FROM etl.sync_runs
ORDER BY started_at DESC
LIMIT 100;
```

Failures, abandoned work, and stale rows still awaiting housekeeping, using the
default 12-hour threshold:

```sql
SELECT sync_run_id, source_system, job_name, status, started_at, finished_at, error_message
FROM etl.sync_runs
WHERE status IN ('FAILED', 'ABANDONED')
   OR (status = 'STARTED' AND started_at < CURRENT_TIMESTAMP - INTERVAL '12 hours')
ORDER BY started_at DESC;
```

If `ETL_ABANDONED_RUN_HOURS` is not 12, replace the literal interval with the
configured threshold when using this inspection query.

Last success and current age by provider:

```sql
SELECT
    source_system,
    MAX(finished_at) FILTER (WHERE status = 'SUCCESS') AS last_success_at,
    CURRENT_TIMESTAMP - MAX(finished_at) FILTER (WHERE status = 'SUCCESS') AS success_age
FROM etl.sync_runs
GROUP BY source_system
ORDER BY source_system;
```

Table-level attempts with their parent run:

```sql
SELECT
    l.id,
    l.sync_run_id,
    r.source_system,
    r.job_name,
    l.provider,
    l.schema_name,
    l.table_name,
    l.status,
    l.rows_input,
    l.rows_loaded,
    l.started_at,
    l.finished_at,
    l.error_message
FROM etl.sync_table_loads AS l
JOIN etl.sync_runs AS r ON r.sync_run_id = l.sync_run_id
ORDER BY l.started_at DESC, l.id DESC
LIMIT 200;
```

For one run, add `WHERE l.sync_run_id = '<sync-run-uuid>'::uuid` before the
`ORDER BY`. A `STARTED` table-load row within a terminal parent run indicates
where execution was interrupted.

## Automated tests and definition checks

Run the full offline suite and all required CLI parser checks before deployment:

```powershell
python -m pytest
python -m ge_data_platform.sources.sendem.sync --help
python -m ge_data_platform.sources.ezytrack.sync --help
python -m ge_data_platform.sources.trackunit.daily_activity --help
python -c "from ge_data_platform.orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = 'C:\Local Warehouse\Telemetry\dagster_home'
dagster job list -m ge_data_platform.orchestration.definitions
dagster schedule list -m ge_data_platform.orchestration.definitions
dagster sensor list -m ge_data_platform.orchestration.definitions
```

The focused tests mock API calls and sleep functions. They cover Trackunit 401,
429, transient 5xx, valid deltas and counter resets; EzyTrack cursor protection
and catch-up windows; Sendem empty payloads; original-exception preservation;
stale-run cleanup; subprocess timeouts; and UTC `loaded_at`. A passing unit suite
does not prove credentials, provider access, or production database permissions.

## Live provider smoke tests

Live smoke tests require all real provider credentials and a migrated PostgreSQL
database. They call external APIs and write to that database through normal
UPSERTs; do not point them at an unreviewed database. Run one provider at a time
while its Dagster schedule is stopped.

Use the smallest meaningful windows:

```powershell
python -m ge_data_platform.sources.sendem.sync --lookback-days 1

$env:EZYTRACK_RECONCILIATION_LOOKBACK_HOURS = '1'
python -m ge_data_platform.sources.ezytrack.sync --reconcile

python -m ge_data_platform.sources.trackunit.daily_activity --date 2026-08-02 --limit 1
python -m ge_data_platform.sources.trackunit.location --date 2026-08-02 --limit 1
```

Choose a known safe Trackunit date instead of copying the example blindly.
Subset flags deliberately load only a subset, so follow them with the unfiltered
date run. Confirm each `etl.sync_runs` row closes, inspect
`etl.sync_table_loads`, query the affected staging/reporting rows, and run the
provider validation pack. A real 429 cannot be forced safely; the mocked test
proves the timing branch, while a naturally occurring production 429 should log
metric, PIN, attempt, and wait without creating duplicate staging rows.

Before re-enabling automation, also confirm there are no unexplained stale
`STARTED` rows, counter resets show `NULL` derived metrics plus
`COUNTER_RESET`, and the existing reporting views query successfully.
