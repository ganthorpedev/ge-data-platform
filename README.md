# Telemetry ETL

Production ETL for synchronising Sendem/MiX, EzyTrack/Telematics Guru, and
Trackunit telemetry into the PostgreSQL `telemetry_warehouse` database. Each
provider has its own connector, transform, and load plan; loads use the existing
UPSERT keys so overlapping windows and recovery reruns are idempotent.

```text
Provider API -> connector -> transform -> raw / staging -> warehouse / reporting
                                      \-> etl.sync_runs / etl.sync_table_loads
```

- `raw` keeps provider-specific records close to their source shape.
- `staging` contains cleaned and enriched provider tables.
- `warehouse` and `reporting` expose stable reporting outputs, including the
  Power BI views.
- `etl` records job and table-load outcomes for operations and recovery.

## Quick start

The production runtime is Python 3.13 with dependencies pinned in
`requirements.txt`, including `dagster==1.13.14` and
`dagster-webserver==1.13.14`. Run commands from the project root. Do not create
a new virtual environment as part of a production deployment.

```powershell
Set-Location 'C:\Local Warehouse\Telemetry\telemetry-etl'
python -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Fill the project-root `.env` with real credentials and connection details.
Never commit `.env`; `.gitignore` excludes all `.env*` files except the safe
`.env.example` template.

Configuration precedence is:

1. Environment variables already set on the process.
2. The canonical `<project-root>/.env`.
3. A legacy `.env` in the current working directory, temporarily supported
   with a migration warning.

Apply the SQL migrations before enabling the hardened schedules. Existing
production databases need `sql/027_add_trackunit_counter_quality.sql` followed
by `sql/028_add_sync_run_abandoned_support.sql`; both are idempotent. See the
[reliability operations runbook](docs/reliability_operations.md#migrations)
for the exact commands and verification queries.

## Run providers manually

```powershell
# Sendem: configured lookback, or an explicit recovery lookback
python -m jobs.sync_sendem
python -m jobs.sync_sendem --lookback-days 7

# EzyTrack: cursor-based catch-up, or fixed-window reconciliation
python -m jobs.sync_ezytrack
python -m jobs.sync_ezytrack --reconcile

# Trackunit: exact day, inclusive range, or rolling recovery
python -m jobs.sync_trackunit_daily_activity --date 2026-08-02
python -m jobs.sync_trackunit_daily_activity --from-date 2026-07-27 --to-date 2026-08-02
python -m jobs.sync_trackunit_daily_activity --rolling-days 7

# Run after Trackunit activity for a date when location enrichment is needed
python -m jobs.sync_trackunit_location_enrichment --date 2026-08-02
```

See the [operations runbook](docs/reliability_operations.md) before a backfill.
It covers rate limits, timeouts, safe provider recovery, validation modes,
Dagster schedules and sensors, alerting, stale `ABANDONED` runs, and SQL for
inspecting `etl.sync_runs` and `etl.sync_table_loads`.

## Validate

The automated suite mocks provider requests and sleeping; it does not call live
APIs.

```powershell
python -m pytest
python -m jobs.sync_sendem --help
python -m jobs.sync_ezytrack --help
python -m jobs.sync_trackunit_daily_activity --help
python -c "from orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = 'C:\Local Warehouse\Telemetry\dagster_home'
dagster job list -m orchestration.definitions
dagster schedule list -m orchestration.definitions
dagster sensor list -m orchestration.definitions
```

Live smoke tests are separate: they require valid provider credentials and a
real PostgreSQL database with the production migrations applied, and they write
through the normal UPSERT paths. Use the smallest safe windows described in the
[runbook](docs/reliability_operations.md#live-provider-smoke-tests).

## More documentation

- [Reliability operations runbook](docs/reliability_operations.md)
- [Dagster production configuration example](dagster.yaml.example)
- [EzyTrack authentication](docs/ezytrack_telematics_auth.md)
- [Power BI reporting data dictionary](docs/powerbi_reporting_data_dictionary.md)
