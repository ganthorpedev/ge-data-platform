# GE Data Platform

ETL for synchronising Sendem/MiX, EzyTrack/Telematics Guru, Trackunit
telemetry, and Accounts/Evolution data into the PostgreSQL
`telemetry_warehouse` database. Each source has its own client, transform,
and load plan under `src/ge_data_platform/sources/`; loads use the existing
UPSERT keys so overlapping windows and recovery reruns are idempotent.

```text
Provider API -> client -> transform -> raw / staging -> warehouse / reporting
                                   \-> etl.sync_runs / etl.sync_table_loads
```

- `raw` keeps provider-specific records close to their source shape. Accounts
  data (`raw.evolution_project_reports`) currently loads straight to `raw`
  only -- no staging split yet, matching the source notebook's single
  combined table.
- `staging` contains cleaned and enriched provider tables.
- `warehouse` and `reporting` expose stable reporting outputs, including the
  Power BI views.
- `etl` records job and table-load outcomes for operations and recovery,
  shared by every source (telemetry and accounts alike).

> This repository is `ge-data-platform` (Python package `ge_data_platform`),
> the restructured successor to the flat `telemetry_etl` project. The
> database is still named `telemetry_warehouse` and production still runs
> from its existing location -- neither has moved yet; that is a later,
> deliberate phase.

## Quick start

The development runtime is Python 3.13. Dependencies are declared in
`pyproject.toml` (pinned versions carried over from the project's
`requirements.txt`, kept for operational compatibility). Run commands from
the repository root. Do not create a virtual environment.

```powershell
Set-Location <path to ge-data-platform>
python -m pip install -e . --no-deps
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
production databases need `sql/migrations/027_add_trackunit_counter_quality.sql`
followed by `sql/migrations/028_add_sync_run_abandoned_support.sql`; both are
idempotent. See the
[reliability operations runbook](docs/reliability_operations.md#migrations)
for the exact commands and verification queries.

## Run providers manually

```powershell
# Sendem: configured lookback, or an explicit recovery lookback
python -m ge_data_platform.sources.sendem.sync
python -m ge_data_platform.sources.sendem.sync --lookback-days 7

# EzyTrack: cursor-based catch-up, or fixed-window reconciliation
python -m ge_data_platform.sources.ezytrack.sync
python -m ge_data_platform.sources.ezytrack.sync --reconcile

# Trackunit: exact day, inclusive range, or rolling recovery
python -m ge_data_platform.sources.trackunit.daily_activity --date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --from-date 2026-07-27 --to-date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --rolling-days 1
python -m ge_data_platform.sources.trackunit.daily_activity --rolling-days 7

# Run after Trackunit activity for a date when location enrichment is needed
python -m ge_data_platform.sources.trackunit.location --date 2026-08-02

# Accounts: Evolution Project Reports (full extract of GE + TLS every run)
python -m ge_data_platform.sources.evolution.project_reports
```

The Accounts/Evolution pipeline needs migration
`sql/migrations/029_create_accounts_evolution_project_reports_schema.sql`
applied (idempotent, safe to rerun) and the `EVOLUTION_*` variables in `.env`
filled in -- see `.env.example`.

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\029_create_accounts_evolution_project_reports_schema.sql
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
python -m ge_data_platform.sources.sendem.sync --help
python -m ge_data_platform.sources.ezytrack.sync --help
python -m ge_data_platform.sources.trackunit.daily_activity --help
python -c "from ge_data_platform.orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = '<path to a local Dagster home>'
dagster job list -m ge_data_platform.orchestration.definitions
dagster schedule list -m ge_data_platform.orchestration.definitions
dagster sensor list -m ge_data_platform.orchestration.definitions
# Equivalent -w form, using the repo's workspace.yaml:
dagster schedule list -w workspace.yaml
```

Expect exactly 10 jobs, 7 schedules, and 2 sensors, including the
`trackunit_intraday_refresh` job and `trackunit_intraday_refresh_schedule`
(`20 */3 * * *`, `Africa/Harare`).

Live smoke tests are separate: they require valid provider credentials and a
real PostgreSQL database with the production migrations applied, and they write
through the normal UPSERT paths. Use the smallest safe windows described in the
[runbook](docs/reliability_operations.md#live-provider-smoke-tests).

## More documentation

- [Reliability operations runbook](docs/reliability_operations.md)
- [Dagster production configuration example](dagster.yaml.example)
- [EzyTrack authentication](docs/ezytrack_telematics_auth.md)
- [Power BI reporting data dictionary](docs/powerbi_reporting_data_dictionary.md)
