# GE Warehouse architecture (Phase 3 baseline)

This document defines the `ge_warehouse` database architecture: schema
layout, naming rules, the ops-metadata model, the role model, and the
migration framework conventions. It is the design companion to
`docs/legacy_to_platform_migration_inventory.md` (the object-by-object
current → target mapping) and `README.md` (day-to-day operations).

**Scope of this phase**: establish the architecture and an empty-but-real
`ge_warehouse` database with its schemas, ops metadata tables, roles, and
`core.dim_date`. Source ingestion pipelines are **not** ported into
`raw_<source>` / `stg_<source>` yet — see "Deferred to the next phase" below.
`telemetry_warehouse` is untouched and remains the live, production-facing
database throughout.

## Schemas

```
raw_trackunit   raw_sendem   raw_ezytrack   raw_evolution   raw_fieldops
stg_trackunit   stg_sendem   stg_ezytrack   stg_evolution   stg_fieldops
core
mart_fleet   mart_finance   mart_operations   mart_maintenance   mart_procurement   mart_commercial
ops
```

No generic `raw`, `staging`, `warehouse`, `reporting`, or `etl` schema is
created in `ge_warehouse` — those names are reserved for `telemetry_warehouse`
and must never reappear as permanent objects here. `sql/validation/validate_ge_warehouse_baseline.sql`
asserts their absence on every run.

### Raw — `raw_<source>`

Source-faithful, persisted as close to the provider's own shape as possible.
Table names are **singular** (`raw_trackunit.asset`, not `assets`). Raw is
never source-blended: a `raw_sendem` table only ever holds Sendem data.

### Staging — `stg_<source>`

Cleaned, typed, normalized, deduplicated — but still single-source. Staging
answers "what does this one source say", not "what is GE's truth". No
cross-source joins happen here.

### Core — `core`

Canonical, conformed GE business entities and facts — the only schema where
data from more than one source is blended into one row. `core.dim_date` is
the only object built this phase (see below); everything else in the
"Expected future" lists from the request is intentionally **not** created
yet:

```
core.dim_asset  core.dim_client  core.dim_site  core.dim_client_site
core.dim_project  core.dim_supplier  core.dim_employee
core.fact_asset_daily_activity  core.fact_trip  core.fact_breakdown
core.fact_job_card  core.fact_project_financial  core.fact_invoice
core.fact_purchase  core.fact_gl_transaction
```

`core.dim_asset` in particular requires deliberately analyzing the
identifiers available across Trackunit (UUID-shaped TEXT), Sendem (BIGINT),
EzyTrack (BIGINT), Evolution (fleet_number as free text) and FieldOps (not
yet integrated) before a conformance key can be chosen — done in the next
phase, not guessed at here.

### Marts — `mart_<domain>`

Business-domain-facing, denormalized, built only on `core` (never directly
on `raw_*`/`stg_*`). All six domain schemas are created empty this phase;
`docs/legacy_to_platform_migration_inventory.md` records which existing
`reporting.*` views will eventually be rebuilt into which mart.

### Ops — `ops`

Operational metadata: pipeline run history, per-table load history, source
watermarks, data-quality results, alert events, and applied-migration
tracking. See the mapping table below.

### Source identifier maps — deliberately deferred

The request's example pattern —

```
core.asset_source_map (asset_key, source_system, source_id)
```

— is the right shape (one shared, reusable mapping table with a
`source_system` discriminator column, **not** a separate table per source,
and **not** an EAV design). It is not created this phase: its `asset_key`
column would reference `core.dim_asset(asset_key)`, which does not exist
yet. Building the map before the dimension it maps into exists would leave
it with nothing to key against and no way to be validated. This is recorded
here as the chosen pattern so the next phase does not need to re-litigate
it — only to build `core.dim_asset` first and then this table against it.

## Ops metadata: current → target mapping

`etl.sync_runs` / `etl.sync_table_loads` are reviewed and preserved
deliberately — no column is dropped, and only the renames that are directly
forced by the *table* rename (or that fix a pre-existing, confirmed naming
inconsistency) are applied. See `docs/legacy_to_platform_migration_inventory.md`
for the full audit; summary:

| Legacy | Platform | Change |
|---|---|---|
| `etl.sync_runs.sync_run_id` (UUID PK) | `ops.pipeline_run.pipeline_run_id` | Renamed only because the table itself is renamed (`sync_run_id` on a table called `pipeline_run` would be confusing) |
| `etl.sync_runs.source_system` | `ops.pipeline_run.source_system` | Unchanged |
| `etl.sync_runs.job_name` | `ops.pipeline_run.job_name` | Unchanged |
| `etl.sync_runs.start_date` / `end_date` | `ops.pipeline_run.start_date` / `end_date` | Unchanged (still `INTEGER` YYYYMMDD-or-NULL, matching real usage) |
| `etl.sync_runs.started_at` / `finished_at` | `ops.pipeline_run.started_at` / `finished_at` | Unchanged |
| `etl.sync_runs.status` | `ops.pipeline_run.status` | Unchanged: `STARTED` / `SUCCESS` / `FAILED` / `ABANDONED` remain the only values |
| `etl.sync_runs.rows_fetched` / `rows_loaded` | `ops.pipeline_run.rows_fetched` / `rows_loaded` | Unchanged |
| `etl.sync_runs.error_message` | `ops.pipeline_run.error_message` | Unchanged |
| `etl.sync_table_loads.id` (BIGSERIAL PK) | `ops.table_load.table_load_id` | Renamed only because the table itself is renamed |
| `etl.sync_table_loads.sync_run_id` (FK) | `ops.table_load.pipeline_run_id` | Renamed to match the renamed parent PK it references |
| `etl.sync_table_loads.provider` | `ops.table_load.source_system` | **Deliberate** rename: `etl.sync_table_loads` called this column `provider` while `etl.sync_runs` called the same concept `source_system` — a pre-existing inconsistency between the two tables. Fixed once, explicitly, here — not a blind rename. |
| `etl.sync_table_loads.schema_name` / `table_name` | `ops.table_load.schema_name` / `table_name` | Unchanged |
| `etl.sync_table_loads.rows_input` / `rows_loaded` | `ops.table_load.rows_input` / `rows_loaded` | Unchanged |
| `etl.sync_table_loads.started_at` / `finished_at` | `ops.table_load.started_at` / `finished_at` | Unchanged |
| `etl.sync_table_loads.status` / `error_message` | `ops.table_load.status` / `error_message` | Unchanged |
| `etl.sync_table_loads.created_at` | `ops.table_load.created_at` | Unchanged |

New, additive tables (no legacy equivalent — nothing is replaced by these,
they formalize things the old code only ever did ad hoc or logged and threw
away):

| Table | Formalizes |
|---|---|
| `ops.source_watermark` | The last-successful-run lookup `PostgresLoader.get_last_successful_run()` currently derives ad hoc from `etl.sync_runs` every call. Not written to by any job yet — see "Deferred". |
| `ops.data_quality_result` | The `POST_LOAD_VALIDATION_QUERIES` results that `PostgresLoader.run_post_load_validation()` currently only logs and discards. Not written to yet — see "Deferred". |
| `ops.alert_event` | The webhook alerts fired by `orchestration/alerts.py`, currently fire-and-forget with no persisted record. Not written to yet — see "Deferred". |
| `ops.schema_version` | This repository's own migration framework did not exist before now — legacy migrations were tracked only by convention (a numbered filename, applied manually via `psql`). `ops.schema_version` is real from the first `ge_warehouse` migration onward. |

None of `ops.source_watermark`, `ops.data_quality_result`, or
`ops.alert_event` are written to by application code in this phase — the
existing `PostgresLoader` and `orchestration/alerts.py` still operate
exactly as before, against `telemetry_warehouse`, unchanged. Wiring them up
is source-ingestion-pipeline work, explicitly out of scope here.

## Roles

Three group roles, all `NOLOGIN` (no password, ever, in source control).
Actual login accounts (the ETL service account, a Power BI account, an
individual admin) are created out-of-band by an operator and granted
membership in one of these — never defined in a committed `.sql` file:

```sql
GRANT ge_etl TO <some_login_role>;   -- done manually, outside git
```

| Role | Intent | Grants |
|---|---|---|
| `ge_platform_admin` | Owns/administers platform objects | `ALL` on every `ge_warehouse` schema and its current + future objects (via `ALTER DEFAULT PRIVILEGES`) |
| `ge_etl` | Writes ingestion, staging, core, marts, ops | `USAGE, CREATE` on every schema; `SELECT, INSERT, UPDATE, DELETE` on current + future tables in every schema |
| `ge_bi_readonly` | Reads approved reporting/mart objects only | `USAGE` + `SELECT` (current + future) on `mart_*` schemas **only** — no grant on `raw_*`, `stg_*`, `core`, or `ops`, so a fully-qualified query against e.g. `raw_trackunit.asset` fails with a permission error for this role, matching the explicit "do not expose raw source data broadly to BI users" instruction |

This mirrors (and generalizes) the existing, live `excel_reader` role on
`telemetry_warehouse`, which already follows the same "grant only on the
reporting-facing schema" pattern — `excel_reader` itself is untouched.

## Migration framework

- **Location**: `sql/migrations/` — a fresh sequence starting at `001`,
  scoped entirely to `ge_warehouse`. The pre-existing, already-applied
  `telemetry_warehouse` migrations (numbered `001`–`029`) are moved verbatim,
  with git history preserved, to `sql/legacy/telemetry_migrations/`. They are
  not renumbered, not edited, and do not belong to the `ge_warehouse`
  migration sequence in any way — the two numbering sequences are
  intentionally independent and target different databases.
- **Convention**: `NNN_verb_object.sql`, zero-padded to 3 digits.
- Every migration is:
  - **Idempotent** — `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT
    EXISTS`, `DO $$ ... IF NOT EXISTS ... $$` guards for roles/grants; safe
    to run against a partially-created or already-fully-created database.
  - **Transactional** — each file runs inside a single `BEGIN...COMMIT`; a
    failure partway through leaves the database exactly as it was before the
    file ran.
  - **Self-registering** — the last statement in every migration inserts its
    own filename into `ops.schema_version` (`ON CONFLICT DO NOTHING`), so
    "what has been applied" is always a live query, not tribal knowledge.
- Applied via `scripts/setup_ge_warehouse.py` (creates the database itself
  if missing — `CREATE DATABASE` cannot run inside a migration transaction —
  then applies every `sql/migrations/*.sql` file in numeric order).

## `core.dim_date`

The one `core` object built this phase, because it is genuinely
source-independent: no source's asset/customer/project identifiers are
needed to define what a calendar date is. Grain: one row per calendar date.
Range: 2015-01-01 through 2035-12-31 (comfortably covers all currently
loaded historical data — the earliest is the 2026-01-01 Sendem backfill —
plus 10 years of runway; cheap to extend later, never a source of truth
concern). Validated by `sql/validation/validate_ge_warehouse_baseline.sql`:
unique `date_key`, unique `date`, correct year/month/day/quarter/day-of-week
derivation, correct leap-day handling, and that the row count matches the
expected range exactly.

## Deferred to the next phase

Explicitly not done now, per the phase-2/phase-3 boundary in the request:

- Porting any ingestion pipeline into `raw_<source>` / `stg_<source>`.
- `core.dim_asset` and the cross-source asset conformance design it requires.
- Any other `core` dimension or fact table.
- Any object in any `mart_*` schema.
- `core.asset_source_map` / `client_source_map` / `site_source_map` /
  `project_source_map` (pattern chosen above; not built).
- Wiring `ops.source_watermark`, `ops.data_quality_result`, or
  `ops.alert_event` into any running job.
- Repointing Dagster, Power BI, or any Windows Scheduled Task at
  `ge_warehouse`.
- Migrating the `clean.*` historical Sendem backfill into `core`.
- FieldOps integration of any kind (no source pipeline exists yet).
