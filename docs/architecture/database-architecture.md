# Database architecture

This document is the current, factual inventory of every schema in
`ge_warehouse` and what's actually inside it, verified against
`sql/migrations/` and the live local database (see
`sql/validation/validate_ge_warehouse_baseline.sql`, which passes all 15
checks against it). For what each layer *means*, see
`docs/architecture/data-layers.md`. For the still-live legacy database, see
"The legacy database" below.

## `ge_warehouse` schemas

| Schema | Layer | Status | Contents today |
|---|---|---|---|
| `raw_trackunit` | raw | IMPLEMENTED | 7 tables, populated -- full historical backfill from `telemetry_warehouse` plus live API data (see `docs/migration/legacy-to-platform-migration.md#trackunit-migration-completed`) |
| `raw_sendem` | raw | IMPLEMENTED | empty |
| `raw_ezytrack` | raw | IMPLEMENTED | empty |
| `raw_evolution` | raw | IMPLEMENTED | empty |
| `raw_fieldops` | raw | IMPLEMENTED | empty |
| `stg_trackunit` | staging | IMPLEMENTED | 3 tables, populated -- see `docs/migration/legacy-to-platform-migration.md#trackunit-migration-completed` |
| `stg_sendem` | staging | IMPLEMENTED | empty |
| `stg_ezytrack` | staging | IMPLEMENTED | empty |
| `stg_evolution` | staging | IMPLEMENTED | empty |
| `stg_fieldops` | staging | IMPLEMENTED | empty |
| `core` | core | IMPLEMENTED | `core.dim_date` (7,670 rows, 2015-01-01..2035-12-31) only |
| `mart_fleet` | mart | IMPLEMENTED | empty |
| `mart_finance` | mart | IMPLEMENTED | empty |
| `mart_operations` | mart | IMPLEMENTED | empty |
| `mart_maintenance` | mart | IMPLEMENTED | empty |
| `mart_procurement` | mart | IMPLEMENTED | empty |
| `mart_commercial` | mart | IMPLEMENTED | empty |
| `ops` | control plane | IMPLEMENTED | 6 tables, see below |

Created by `sql/migrations/001_create_platform_schemas.sql` (all 18 schemas
plus `ops.schema_version`), `002_create_ops_metadata.sql` (the remaining
`ops` tables), `003_create_platform_roles.sql` (roles/grants), and
`004_create_core_dim_date.sql` (`core.dim_date`). Applied to the local
development database via `python -m scripts.setup_ge_warehouse --all`; see
`docs/development/migrations.md`.

### `raw_trackunit` / `stg_trackunit` tables

The only source fully migrated so far -- see
`docs/migration/legacy-to-platform-migration.md#trackunit-migration-completed`
for the full backfill/validation procedure and results.

| Table | Row count (dev, after historical backfill) |
|---|---|
| `raw_trackunit.asset` | 107 |
| `raw_trackunit.aemp_operating_hour` | ~125,000 |
| `raw_trackunit.aemp_moving_hour` | ~123,000 |
| `raw_trackunit.aemp_distance` | ~123,000 |
| `raw_trackunit.aemp_location` | 28,501 |
| `raw_trackunit.site_history` | 39 |
| `raw_trackunit.site` | 11 |
| `stg_trackunit.asset` | 107 |
| `stg_trackunit.daily_activity` | ~860 (grows with every fresh sync) |
| `stg_trackunit.location_enrichment` | 105 |

`stg_trackunit.daily_activity` includes `counter_reset_detected`/
`data_quality_status` as real, populated columns from the start -- unlike
legacy `staging.trackunit_daily_activity`, which never actually got these
columns (see `docs/operations/data-quality.md`).

### `ops` tables

| Table | Wired into a running job? | Purpose |
|---|---|---|
| `ops.schema_version` | Yes -- every `sql/migrations/*.sql` file registers itself here on apply | Tracks which `ge_warehouse` migrations have run |
| `ops.pipeline_run` | No | Platform successor to legacy `etl.sync_runs` |
| `ops.table_load` | No | Platform successor to legacy `etl.sync_table_loads` |
| `ops.source_watermark` | No | New: intended to formalize the last-successful-cursor lookup that legacy `PostgresLoader.get_last_successful_run()` currently derives ad hoc |
| `ops.data_quality_result` | No | New: intended to persist the bounded post-load validation results that legacy code currently only logs |
| `ops.alert_event` | No | New: intended to persist the webhook alerts that legacy `orchestration/alerts.py` currently only fires and forgets |

"Wired into a running job" means application code (a source sync, an
orchestration op) writes to it. None of the last five are written to by
anything yet -- see `docs/architecture/platform-overview.md`'s status table
and `docs/operations/pipeline-operations.md`.

### PostgreSQL `public` schema

`public` is PostgreSQL's own default schema, created automatically by
`CREATE DATABASE`. It is **not** a GE platform layer, is not used by any
`ge_warehouse` migration, and should stay empty. `sql/validation/validate_ge_warehouse_baseline.sql`
does not currently assert this explicitly (only that no *legacy-named*
schema like `raw`/`staging`/`etl` was accidentally created) -- treat any
object appearing in `public` as a mistake to investigate, not a valid
platform object.

## Reserved/forbidden names

`ge_warehouse` must never contain a schema named `raw`, `staging`,
`warehouse`, `reporting`, `etl`, or `clean` -- those exact names are reserved
for the legacy database, and their presence in `ge_warehouse` would strongly
suggest someone ran a legacy migration against the wrong database.
`sql/validation/validate_ge_warehouse_baseline.sql` checks for this on every
run.

## Roles

`ge_platform_admin`, `ge_etl`, `ge_bi_readonly` -- all `NOLOGIN` group roles,
created by `003_create_platform_roles.sql`. See
`docs/security/secrets-and-access.md` for the full grant design; no login
account has been created or granted membership in any of them yet (that is
an operator action outside version control, not a migration).

## The legacy database (`telemetry_warehouse`)

**Status: LEGACY -- this is the real, currently-running production
database.** Unaffected by anything in this document; described here only
for contrast.

| Schema | Purpose |
|---|---|
| `raw` | Provider-shaped ETL tables, shared indiscriminately across Sendem/EzyTrack/Trackunit/Evolution |
| `staging` | Cleaned/enriched provider tables |
| `clean` | Pre-existing historical Sendem backfill (2026-01-01..2026-06-30); no running job writes to it |
| `etl` | `sync_runs` / `sync_table_loads` -- the legacy equivalent of `ops.pipeline_run` / `ops.table_load` |
| `reporting` | Power BI-facing views (documented in full in `docs/powerbi_reporting_data_dictionary.md`) |
| `warehouse` | Dead: created by an early migration, superseded by `reporting` before ever being used; confirmed empty in the live catalog |

Full column-by-column object inventory and the current -> target mapping:
`docs/migration/legacy-to-platform-migration.md`.
