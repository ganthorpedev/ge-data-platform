# Accounts / Evolution

**Status: IMPLEMENTED and running against `telemetry_warehouse` (LEGACY
target database). Not yet ported to `raw_evolution`/`stg_evolution`.**

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

- `id` is assumed `BIGINT`, unique only within one company's database (the
  natural/primary key is the `(company, id)` pair, not `id` alone) -- this
  is a documented assumption pending confirmation, not a proven fact; see
  `sql/sources/evolution/project_reports/validate_source_assumptions.sql`
  for the read-only checks that confirm or refute it against each live
  Evolution database.
- Money columns (`credit`, `debit`, `inclusive_amount`, `tax_amount`) are
  `NUMERIC(20, 4)`; `quantity_invoiced` is left unscaled since the source
  query never casts it.

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
