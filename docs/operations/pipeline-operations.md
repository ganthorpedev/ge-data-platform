# Pipeline operations

**Status: IMPLEMENTED.** This document describes real, running behavior
against `telemetry_warehouse` (the only database any job actually writes to
today). `ge_warehouse`'s `ops` schema exists structurally but nothing here
is wired to write to it yet -- see "ops metadata wiring status" below.

For retry/backoff mechanics and manual recovery commands, see
`docs/operations/retries-and-recovery.md`. For alerting and freshness, see
`docs/operations/monitoring-and-alerting.md`. For validation, see
`docs/operations/data-quality.md`.

## Production deployment order

1. Stop the telemetry schedules and sensors in Dagster; let any active
   provider subprocess finish, or terminate it through Dagster, before
   replacing files.
2. Back up the database per the existing production procedure, deploy the
   project files, and run `python -m pip install -e . --no-deps` with the
   Python 3.13 installation used by the Windows SYSTEM services.
3. Merge new keys from `.env.example` into the canonical project-root
   `.env`. Ensure the SYSTEM account can read it, and that both SYSTEM and
   any operator running manual recovery commands can create/modify the
   project-root `.ge_data_platform_locks` directory so both execution paths
   contend on the same OS lock (see
   `docs/operations/retries-and-recovery.md#overlap-protection`).
4. Apply any pending migrations (see `docs/development/migrations.md`), then
   run the offline tests, CLI help checks, and Dagster definition listings
   (`docs/development/testing.md`).
5. Keep run monitoring enabled in the external `DAGSTER_HOME\dagster.yaml`
   (merge from `dagster.yaml.example`, never overwrite SYSTEM's existing
   storage/scheduler/run-launcher config), then restart the Dagster daemon
   and webserver services so code and configuration reload.
6. Run the smallest live provider smoke tests one provider at a time (see
   `docs/development/testing.md#live-provider-smoke-tests`), inspect the
   run/table logs and reporting views, and only then enable the intended
   schedules and sensors.

## Configuration and `.env` precedence

One canonical `.env` at the project root, copied from `.env.example`, never
committed (`.gitignore` excludes all `.env*` except `.env.example`).
Resolution order, first value found wins:

1. Environment variables already present in the process.
2. `<project-root>/.env`, loaded by absolute path regardless of the working
   directory.
3. A legacy `.env` in the current working directory -- temporary
   compatibility, logs a migration warning.

Restart the Dagster webserver and daemon after changing `.env`; long-running
processes do not reload it automatically.

## Dagster jobs and schedules

All cron times are `Africa/Harare`. Provider and housekeeping schedules
default to `STOPPED` -- review and start each explicitly after deployment.
The two monitoring sensors default to `RUNNING` (they only observe and
alert; they never launch a provider job themselves).

| Definition | Cadence | Work |
|---|---|---|
| `trackunit_daily_refresh_schedule` | `5 2 * * *` | Rolling two-day activity load, then location enrichment (one shared timeout budget, one lock -- see below) |
| `trackunit_intraday_refresh_schedule` | `20 */3 * * *` | Rolling one-day activity-only refresh; no enrichment |
| `sendem_sync_schedule` | `35 */3 * * *` | Sendem incremental sync |
| `ezytrack_sync_schedule` | `45 */3 * * *` | EzyTrack cursor-based catch-up sync |
| `ezytrack_daily_reconciliation_schedule` | `15 1 * * *` | EzyTrack fixed-lookback `--reconcile` run |
| `trackunit_rolling_7_days_schedule` | `45 1 * * 0` | Sunday weekly 7-day Trackunit reconciliation |
| `stale_started_run_cleanup_schedule` | `20 * * * *` | Hourly STARTED->ABANDONED housekeeping |
| `telemetry_run_failure_sensor` | event-driven | Alerts once per failed Dagster run |
| `telemetry_provider_freshness_sensor` | every 15 min | Checks last successful syncs, emits cooldown-deduplicated staleness alerts |

10 Dagster jobs are exported from `ge_data_platform.orchestration.definitions`:
`dagster_smoke_test`, `sendem_sync`, `ezytrack_sync`,
`ezytrack_daily_reconciliation`, `trackunit_daily_refresh`,
`trackunit_intraday_refresh`, `trackunit_rolling_7_days`,
`trackunit_location_enrichment`, `accounts_evolution_project_reports_sync`,
`stale_started_run_cleanup`. Standalone jobs remain available for manual
recovery even without their own schedule
(`trackunit_location_enrichment`, `accounts_evolution_project_reports_sync`).

`trackunit_intraday_refresh_schedule` exists specifically to restore
frequent Trackunit polling without reintroducing 429 pressure: it fetches
only the current rolling day (`--rolling-days 1`, smaller than the daily
job's rolling two days) and never runs location enrichment. All four
Trackunit activity/enrichment definitions share one overlap group because
they write the same staging table -- see
`docs/operations/retries-and-recovery.md#overlap-protection`.

`trackunit_daily_refresh` holds one Trackunit lock and one
`TRACKUNIT_JOB_TIMEOUT_MINUTES` budget across its two sequential children:
activity gets the initial budget, enrichment gets only what's left, and is
not started at all if activity consumed the entire budget.

Load and inspect the code location before enabling anything:

```powershell
python -c "from ge_data_platform.orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = '<path to DAGSTER_HOME>'
dagster job list -m ge_data_platform.orchestration.definitions
dagster schedule list -m ge_data_platform.orchestration.definitions
dagster sensor list -m ge_data_platform.orchestration.definitions
```

Expect exactly 10 jobs, 7 schedules, 2 sensors.

## Subprocess monitoring

`ge_data_platform.orchestration.runner.run_module` launches every provider
module as `python -m <module>` with the repository root as its working
directory (required for the editable install to resolve regardless of
Dagster's own cwd), streams stdout/stderr into the Dagster run log while
retaining a bounded tail for failure messages, and enforces a configurable
wall-clock timeout per module (`SENDEM_JOB_TIMEOUT_MINUTES`,
`EZYTRACK_JOB_TIMEOUT_MINUTES`, `TRACKUNIT_JOB_TIMEOUT_MINUTES`,
`ACCOUNTS_EVOLUTION_PROJECT_REPORTS_JOB_TIMEOUT_MINUTES` -- defaults 60, 60,
360, 60). On timeout the runner terminates the child, waits
`ETL_SUBPROCESS_TERMINATE_GRACE_SECONDS` (default 10), force-kills if still
alive, and raises a Dagster failure containing the timeout and captured
output -- the Dagster run always reaches a terminal state, so the overlap
guard can admit a future schedule.

The production `dagster.yaml` lives outside this repository. Merge (never
replace) these keys from `dagster.yaml.example`:

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 25200   # 7h -- beyond the 6h Trackunit daily-refresh budget
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

## `ops` metadata wiring status

**Structure exists; nothing writes to it yet.** Every provider job today
still records its run in the legacy `etl.sync_runs`/`etl.sync_table_loads`
(via `PostgresLoader.start_sync_run`/`finish_sync_run`/`_run_load_plan`
against `telemetry_warehouse`). The platform successor,
`ops.pipeline_run`/`ops.table_load`, exists in `ge_warehouse`
(`sql/migrations/002_create_ops_metadata.sql`) but no job has been
repointed at it -- that's ingestion-migration work, explicitly out of scope
until sources are ported (see
`docs/migration/legacy-to-platform-migration.md`).

`ops.source_watermark`, `ops.data_quality_result`, and `ops.alert_event` are
new, additive tables with no legacy equivalent and no application code
writing to them at all yet -- see `docs/architecture/architecture-decisions.md#adr-005`.
