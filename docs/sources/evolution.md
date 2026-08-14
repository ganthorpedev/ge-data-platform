# Accounts / Evolution

**Status: IMPLEMENTED. Production Dagster job still writes only to
`telemetry_warehouse` (LEGACY target, default) -- and has never actually
succeeded there (see below). `raw_evolution`/`stg_evolution` now exist in
`ge_warehouse`, populated by a first platform load and validated, and the
same code can write there via `--target platform` -- opt-in, not the
default, not wired to any schedule. See
`docs/migration/legacy-to-platform-migration.md#evolution-migration-completed`
for the full migration record, including a wrong source-key assumption this
migration discovered and fixed for the platform target only.**

**Evolution is a source system, not a finance architecture.** It happens to
be the current source for most finance-relevant data, but the future
`mart_finance` is not "the Evolution mart" -- see
`docs/architecture/platform-overview.md#source-system-vs-business-domain`.

Code: `ge_data_platform.sources.evolution` (`connection.py`,
`project_reports.py`). Unlike the API-based sources, extraction, transform,
and sync for this one dataset live together in a single cohesive module
(`project_reports.py`) rather than split into three files -- there is one
source system extracting one dataset via one shared code path across two
companies, so the three-file split used for Trackunit/Sendem/EzyTrack would
add structure without benefit here. Only SQL Server connection handling
(shared plumbing, not dataset-specific) stays separate, in `connection.py`.

## SQL Server source

GE and TLS are two databases on the *same* SQL Server instance, reached with
the same ODBC driver and credentials (`EVOLUTION_SQL_DRIVER`,
`EVOLUTION_SQL_SERVER`, `EVOLUTION_SQL_USERNAME`, `EVOLUTION_SQL_PASSWORD`)
-- only the target database name differs
(`EVOLUTION_GE_DATABASE`/`EVOLUTION_TLS_DATABASE`). `connection.py` opens a
`pyodbc` connection per company with an explicit, configurable timeout
(`EVOLUTION_SQL_CONNECT_TIMEOUT_SECONDS`, default 30s) so an unreachable
server fails fast rather than hanging the job, and always closes the
connection in a `finally` block.

## Current dataset: project reports (`dbo.vwProjectsReports`)

The one dataset extracted today: `sql/sources/evolution/project_reports/extract_project_reports.sql`,
selected from a configurable view (`EVOLUTION_PROJECT_REPORTS_VIEW`, default
`dbo.vwProjectsReports`) via `pd.read_sql_query(..., coerce_float=False)` --
`coerce_float=False` is required so money columns arrive as Python
`decimal.Decimal` rather than being rounded through a binary float, matching
the source query's own `CAST(... AS DECIMAL(20, 4))`.

The view name is never interpolated into SQL unchecked: it is validated by
`ge_data_platform.config.settings.validate_evolution_view_name` (a strict
`schema.object` identifier pattern -- no whitespace, semicolons, brackets, or
extra parts accepted) both at settings-load time and again immediately
before use, and the *validated, bracket-quoted* form is what actually
reaches the query text.

**Transform** (in the same module): combines GE + TLS extracts, classifies
each row's `business_unit` via date- and cost-code-based rules (a
2026-03-01 cutover date, distinct TLS legacy-vs-current cost-code tables,
and GE's own hire/commercial/training cost-code groups -- all ported
verbatim from the working discovery notebook, none reinterpreted), then
converts every column to `snake_case`.

**Load**: a **full replace**, not an UPSERT --
`PostgresLoader.replace_accounts_evolution_project_reports` stages the
combined extract, validates it (non-empty, no missing/duplicate `(company,
id)` keys), and only then atomically deletes and re-inserts
`raw.evolution_project_reports` inside one transaction. This is because
`dbo.vwProjectsReports` is re-extracted in full every run with no evidenced
incremental key -- a row removed from the source must also disappear from
PostgreSQL, which an UPSERT alone would never achieve. Any failure before
the final transaction commits leaves the previous successful load completely
untouched.

```powershell
python -m ge_data_platform.sources.evolution.project_reports
```

## Known constraints (from the code, not guessed)

- **`id` is NOT a row identifier.** The documented assumption in the legacy
  schema/validation (`(company, id)` as the natural primary key, `id`
  `BIGINT`) was disproven during the Evolution `raw_evolution`/`stg_evolution`
  migration: live read-only inspection of `dbo.vwProjectsReports` on both GE
  and TLS shows `Id` is `VARCHAR` and takes only 11 distinct values total
  (`Inv`, `JL`, `CB`, `APTx`, `ARTx`, `Crn`, `Grv`, `IJr`, `OGrv`, `Rts`,
  `SADJ`), mapping 1:1 to `Module` -- it is a transaction-type/module code.
  **`dbo.vwProjectsReports` has no reliable natural/business key at the row
  grain** -- even the widest practical composite (`id, cost_type, module,
  reference, d_date`) leaves thousands of duplicate rows per company. This
  is why every real legacy sync attempt has failed (`etl.sync_runs` shows 3
  `FAILED` rows, two refused by `validate_combined_for_full_replace`'s own
  duplicate-key check doing exactly its job). The legacy schema/validation
  code is left unmodified (out of scope -- `telemetry_warehouse` stays
  untouched); `raw_evolution.project_report`/`stg_evolution.project_report`
  use a load-time surrogate `BIGSERIAL` primary key instead and allow
  duplicate rows, validated by the separate
  `validate_project_report_batch_for_platform_load`. See
  `sql/migrations/011_create_raw_evolution.sql` and
  `sql/sources/evolution/project_reports/validate_source_assumptions.sql`
  (still useful for the raw type/range/scale checks; its `Id`-uniqueness
  assumption is now known to be false).
- Money columns (`credit`, `debit`, `inclusive_amount`, `tax_amount`) are
  `NUMERIC(20, 4)`; `quantity_invoiced` is left unscaled since the source
  query never casts it. A minority of rows (1,614 GE / 860 TLS) carry more
  than 4 decimal places in the source and are rounded to 4dp by the
  extraction query's own `CAST` -- an intentional, pre-existing behavior
  (not introduced by this migration), immaterial for currency values.
- `DDate` legitimately extends past the current date on both databases
  (forward-dated/scheduled transactions) -- not a data error.

## Snapshot semantics

`dbo.vwProjectsReports` is **accumulated history re-extracted in full every
run** (task classification C): every row ever posted stays in the view (no
evidence of expiry/archival), but there is no incremental key or change
cursor, so each run reads the entire view again. The existing **full
replace** load strategy (atomic `DELETE` + `INSERT` in one transaction, not
an UPSERT or append) is therefore correct and was kept as-is: a row genuinely
removed from the source (e.g. a voided/corrected posting) must also
disappear from PostgreSQL, which an append-only or UPSERT-only strategy
could never achieve on its own. Verified directly during the migration:
re-running the identical extract reproduces byte-identical row counts and
monetary aggregates with no growth (see
`docs/migration/legacy-to-platform-migration.md#evolution-migration-completed`).
Per-extraction snapshot history (e.g. an `extracted_at`-partitioned append
table) would be a useful future enhancement but is out of scope here --
documented as a possibility, not a commitment.

## `ge_warehouse` platform target

**Status: IMPLEMENTED, opt-in, not scheduled.**

`ge_data_platform.sources.evolution.project_reports` accepts
`--target {legacy,platform}` (default `legacy` -- current behavior,
unchanged):

```powershell
python -m ge_data_platform.sources.evolution.project_reports --target platform
```

`--target platform`:

- writes into `ge_warehouse` instead of `telemetry_warehouse`, split into
  two layers rather than legacy's one: `raw_evolution.project_report`
  (source-faithful, no `business_unit`) and `stg_evolution.project_report`
  (adds `business_unit`) -- via `PostgresLoader.from_platform_settings` and
  `replace_evolution_project_reports_platform` in `common/database.py`;
- reuses the exact same extraction/transform functions as legacy
  (`extract_all`, and `build_raw`/`add_business_unit_classification`, which
  together produce byte-identical output to legacy's single-step
  `build_combined`);
- records the run and each table load (`raw_evolution.project_report`,
  `stg_evolution.project_report`) in `ops.pipeline_run`/`ops.table_load`
  instead of `etl.sync_runs`/`etl.sync_table_loads` (same `start_sync_run`/
  `finish_sync_run` call sites in `project_reports.py`;
  `PostgresLoader.tracking_backend` selects the destination -- see
  `ge_data_platform.common.audit`) and still skips post-load validation
  (hardcoded to legacy schema names) -- same precedent as
  Trackunit/Sendem/EzyTrack;
- uses `validate_project_report_batch_for_platform_load` instead of legacy's
  `validate_combined_for_full_replace` (see "Known constraints" above for
  why they differ). The Evolution SQL Server itself is only ever read, never
  written, regardless of `--target` -- audit tracking is entirely a
  `ge_warehouse`/PostgreSQL-side concern.

No Dagster job or schedule passes `--target platform`; it is exercised only
by manual invocation today. A real run (both GE and TLS sources) was used to
prove `ops.pipeline_run`/`ops.table_load` population during this
audit-wiring change -- 1 `ops.pipeline_run` row
(`source_system=evolution_project_reports`, `status=SUCCESS`) and 2
`ops.table_load` rows (one per destination table), matching the extracted
row count exactly (full-replace, so `rows_input == rows_loaded` for both).
See
`docs/migration/legacy-to-platform-migration.md#evolution-migration-completed`
for the first-load and reconciliation results, and
`scripts/run_evolution_first_load.py`/`scripts/validate_evolution_migration.py`
for the tooling used.

## Likely future datasets (PLANNED examples only)

No second Evolution dataset is implemented or scheduled. If Evolution grows
beyond project reports, likely candidates (illustrative, not committed)
would be invoicing or general-ledger extracts feeding `core.fact_invoice` /
`core.fact_gl_transaction` (see `docs/warehouse/core-model.md`) -- nothing
here should be read as a roadmap commitment.

## Reconciliation schedule

`accounts_evolution_project_reports_sync` has no cron schedule today --
it is a standalone Dagster job (`ACCOUNTS_EVOLUTION_PROJECT_REPORTS_RUN_TAGS`),
run manually or on demand. See
`docs/operations/pipeline-operations.md#dagster-jobs-and-schedules`.
