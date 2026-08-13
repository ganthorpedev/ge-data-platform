# Local setup

## Requirements

Python **3.13** (`pyproject.toml`: `requires-python = ">=3.13"`). **No
virtual environment is required or expected** -- this project is installed
editable, directly, without one.

## Install

```powershell
Set-Location <path to ge-data-platform>
python -m pip install -e . --no-deps
```

`--no-deps` plus the pinned versions in `pyproject.toml`'s `dependencies`
(carried over from `requirements.txt`, kept for operational compatibility)
means installed package versions are exactly what's declared, not whatever
pip's resolver would otherwise pick. The editable install is what makes
`ge_data_platform.*` importable regardless of the current working
directory -- required because Dagster launches provider subprocesses with
the repository root as their working directory, not `src/`.

SQL Server access (Accounts/Evolution source) additionally requires the
"ODBC Driver 17 for SQL Server" system driver installed separately --
`pyodbc` alone does not bundle it.

## `.env` setup

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Fill in real values. **Never commit `.env`** -- `.gitignore` excludes every
`.env*` file except `.env.example`. See
`docs/security/secrets-and-access.md` for the full policy, and
`docs/operations/pipeline-operations.md#configuration-and-env-precedence`
for how `.env` is loaded and overridden.

Two database names are configured independently and must not be confused:
`POSTGRES_DB` (legacy `telemetry_warehouse`, used by everything that runs
today) and `GE_WAREHOUSE_DB` (the new `ge_warehouse` platform baseline, not
yet used by any provider job). Both share the same `POSTGRES_HOST`/`_PORT`/
`_USER`/`_PASSWORD`.

## Running tests

```powershell
python -m pytest
```

No live credentials or database are required -- see
`docs/development/testing.md` for what's mocked vs. real.

## Compile checks

```powershell
python -m compileall src tests scripts
```

## Dagster local loading

```powershell
python -c "from ge_data_platform.orchestration.definitions import defs; defs.get_repository_def(); print('Dagster definitions loaded')"
$env:DAGSTER_HOME = '<path to a local Dagster home>'
dagster job list -m ge_data_platform.orchestration.definitions
dagster schedule list -m ge_data_platform.orchestration.definitions
dagster sensor list -m ge_data_platform.orchestration.definitions
```

`workspace.yaml` (repository root) also supports the `-w` form for ad hoc
local validation:

```powershell
dagster schedule list -w workspace.yaml
```

Expect exactly 10 jobs, 7 schedules, 2 sensors -- see
`docs/operations/pipeline-operations.md#dagster-jobs-and-schedules`.

## Setting up `ge_warehouse` locally

```powershell
python -m scripts.setup_ge_warehouse --all
```

Creates the `ge_warehouse` database if missing, applies every
`sql/migrations/*.sql` file in order, and runs the baseline validation
pack. Never touches `telemetry_warehouse`. See
`docs/development/migrations.md`.
