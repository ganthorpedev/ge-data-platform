# Testing

## Unit / reliability tests

```powershell
python -m pytest
```

210 tests today, organized by source under `tests/`:
`tests/trackunit/`, `tests/sendem/`, `tests/ezytrack/`, `tests/evolution/`,
`tests/orchestration/`, `tests/platform/` (`ge_warehouse` settings,
migration discovery, schema-naming statics -- see
`docs/development/migrations.md` -- plus `test_ops_audit.py`: the shared
`ops.pipeline_run`/`ops.table_load` audit mechanism in
`ge_data_platform.common.audit`, and its dispatch/isolation from legacy
`etl.sync_runs`/`etl.sync_table_loads` in `PostgresLoader`).

These mock provider HTTP/SQL calls and `time.sleep` -- **they never call a
live API or a live database**, with one deliberate exception:
`tests/platform/test_migrations_discovery.py::test_discover_migrations_finds_real_baseline_files_in_order`
exercises the actual `sql/migrations/` directory on disk (not a mock),
since real local catalog/filesystem validation is safe and more convincing
than a fabricated directory structure when it's genuinely available.

Coverage focus, by area: Trackunit 401/429/transient-5xx/counter-reset
behavior; EzyTrack cursor protection and catch-up window math; Sendem empty
payloads; original-exception preservation through failed bookkeeping; stale
STARTED-run cleanup; subprocess timeouts; UTC `loaded_at` stamping;
`ge_warehouse` settings/migration-discovery/schema-naming.

**A passing unit suite does not prove credentials, provider access, or
production database permissions** -- it proves the code's logic, not the
environment.

## SQL validation

Two tiers -- see `docs/operations/data-quality.md#validation-sql-philosophy`
for the full explanation:

- Bounded, automatic, post-load checks (run by the jobs themselves).
- Manual, full-history packs (`sql/validation/*.sql`), run by hand.

`sql/validation/validate_ge_warehouse_baseline.sql` is a third, distinct
pack: it checks `ge_warehouse` *platform structure* (schemas, roles,
`core.dim_date` correctness), not provider data, and is exercised via
`python -m scripts.setup_ge_warehouse --validate`.

## Compile checks

```powershell
python -m compileall src tests scripts
```

## `pip check`

```powershell
python -m pip check
```

Confirms the pinned dependency set in `pyproject.toml` has no internal
conflicts.

## CLI parser checks

Non-mutating -- confirms every entry point's argument parser is valid
without calling an API or touching a database:

```powershell
python -m ge_data_platform.sources.sendem.sync --help
python -m ge_data_platform.sources.ezytrack.sync --help
python -m ge_data_platform.sources.trackunit.daily_activity --help
python -m ge_data_platform.sources.trackunit.location --help
python -m ge_data_platform.sources.evolution.project_reports --help
```

## Live provider smoke tests

**Require real provider credentials and a migrated PostgreSQL database.**
They call external APIs and write through the normal UPSERT/full-replace
paths -- never point them at an unreviewed database, and always run one
provider at a time with its Dagster schedule stopped.

```powershell
python -m ge_data_platform.sources.sendem.sync --lookback-days 1

$env:EZYTRACK_RECONCILIATION_LOOKBACK_HOURS = '1'
python -m ge_data_platform.sources.ezytrack.sync --reconcile

python -m ge_data_platform.sources.trackunit.daily_activity --date <known-safe-date> --limit 1
python -m ge_data_platform.sources.trackunit.location --date <known-safe-date> --limit 1
```

Subset flags (`--limit`, `--machines`) deliberately load only a slice --
always follow with the unfiltered date once validated. After each smoke
test: confirm the `etl.sync_runs` row closed, inspect
`etl.sync_table_loads`, spot-check the affected staging/reporting rows, and
run that provider's manual validation pack. A real 429 cannot be forced
safely for a live test; the mocked unit test proves the timing branch, and
a naturally occurring production 429 should log metric/PIN/attempt/wait
without creating duplicate staging rows.

The same commands with `--target platform` added (e.g. `python -m
ge_data_platform.sources.sendem.sync --target platform --lookback-days 1`)
exercise the `ge_warehouse` platform target instead -- same external-API
calls, but the run/table-load bookkeeping lands in
`ops.pipeline_run`/`ops.table_load` rather than
`etl.sync_runs`/`etl.sync_table_loads`. After a platform-target smoke test,
confirm the `ops.pipeline_run` row closed `SUCCESS` and inspect
`ops.table_load` the same way.

## Side-by-side migration validation philosophy

Once source ingestion is ported to `ge_warehouse` (see
`docs/migration/legacy-to-platform-migration.md`), the validation approach
is comparative, not just "does the new pipeline run": for each source, run
the new `raw_<source>`/`stg_<source>` load and the legacy
`raw`/`staging` load against the same window, then diff row counts and, for
overlapping windows, spot-check values -- never assume parity, prove it. No
comparison tooling for this exists yet; it is scoped for that migration
phase, not built ahead of it.
