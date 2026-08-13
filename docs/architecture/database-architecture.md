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
| `raw_sendem` | raw | IMPLEMENTED | 5 tables, populated -- full historical backfill from `telemetry_warehouse` plus live API data (see `docs/migration/legacy-to-platform-migration.md#sendem-migration`) |
| `raw_ezytrack` | raw | IMPLEMENTED | 2 tables, populated -- full historical backfill from `telemetry_warehouse` plus live API data (see `docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`) |
| `raw_evolution` | raw | IMPLEMENTED | 1 table, populated -- first platform load direct from Evolution SQL Server (no legacy data existed to backfill), see `docs/migration/legacy-to-platform-migration.md#evolution-migration-completed` |
| `raw_fieldops` | raw | IMPLEMENTED | empty |
| `stg_trackunit` | staging | IMPLEMENTED | 3 tables, populated -- see `docs/migration/legacy-to-platform-migration.md#trackunit-migration-completed` |
| `stg_sendem` | staging | IMPLEMENTED | 5 tables, populated -- includes legacy `clean.sendem_fact_*_daily` history back to 2026-01-01, see `docs/migration/legacy-to-platform-migration.md#sendem-migration` |
| `stg_ezytrack` | staging | IMPLEMENTED | 2 tables, populated -- see `docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed` |
| `stg_evolution` | staging | IMPLEMENTED | 1 table, populated -- see `docs/migration/legacy-to-platform-migration.md#evolution-migration-completed` |
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

### `raw_sendem` / `stg_sendem` tables

Second source fully migrated -- see
`docs/migration/legacy-to-platform-migration.md#sendem-migration` for the
full backfill/validation procedure and results.

| Table | Row count (dev, after historical backfill + one live sync) |
|---|---|
| `raw_sendem.asset` | 258 |
| `raw_sendem.site` | 168 |
| `raw_sendem.event_description` | 114 |
| `raw_sendem.trip_daily` | 1,998 (rolling window only, 2026-06-24 onward) |
| `raw_sendem.event_daily` | 17,451 (rolling window only, 2026-06-24 onward) |
| `stg_sendem.asset` | 258 |
| `stg_sendem.site` | 168 |
| `stg_sendem.event_type` | 122 (includes 2 inferred "Unknown Sendem Event Type" placeholder rows) |
| `stg_sendem.trip_daily` | 13,437 (2026-01-01 onward -- legacy `clean.sendem_fact_trips_daily` history folded in) |
| `stg_sendem.event_daily` | 123,639 (2026-01-01 onward -- legacy `clean.sendem_fact_events_daily` history folded in) |

`raw_sendem.trip_daily`/`event_daily` intentionally do NOT carry the
`clean.*` history -- `clean.*` predates `site_id` on the asset dimension and
is already staging-shaped (enriched with site/asset attributes), not
raw-shaped, so it has no raw-layer counterpart. Only `stg_sendem.trip_daily`/
`event_daily` carry the full 2026-01-01 history. See
`docs/migration/legacy-to-platform-migration.md#sendem-migration`.

### `raw_ezytrack` / `stg_ezytrack` tables

Third source fully migrated -- see
`docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`
for the full backfill/validation procedure and results. Unlike Sendem,
EzyTrack has no legacy `clean.*` schema -- this is a straight 1:1 structural
copy.

| Table | Row count (dev, after historical backfill + one live sync) |
|---|---|
| `raw_ezytrack.asset` | 55 (51 historical + 4 from live-ingestion testing) |
| `raw_ezytrack.trip` | 848 (823 historical + 25 from live-ingestion testing) |
| `stg_ezytrack.asset` | 55 |
| `stg_ezytrack.trip` | 848 |

### `raw_evolution` / `stg_evolution` tables

Fourth source migrated -- see
`docs/migration/legacy-to-platform-migration.md#evolution-migration-completed`
for the full first-load/validation procedure and results. **First platform
load, not a historical migration**: `telemetry_warehouse` has never actually
held any Evolution data (the legacy migration that would create it was
never applied). Unlike the other three sources, `id` is not a usable row
key -- `dbo.vwProjectsReports` has no reliable natural key at all, so both
tables use a load-time surrogate `BIGSERIAL` primary key
(`project_report_id`) and allow duplicate source rows.

| Table | Row count (dev, after first load) |
|---|---|
| `raw_evolution.project_report` | 29,948 (GE 21,582 + TLS 8,366) |
| `stg_evolution.project_report` | 29,948 |

`stg_evolution.project_report` adds one derived column vs.
`raw_evolution.project_report`: `business_unit`, classified from
`company`/`d_date`/`cost_type` (ported verbatim from the source notebook).
Load strategy is full replace (atomic `DELETE` + `INSERT`), not an UPSERT --
`dbo.vwProjectsReports` is re-extracted in full every run with no evidenced
incremental key. See `docs/sources/evolution.md#snapshot-semantics`.

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
| `clean` | Pre-existing historical Sendem backfill (2026-01-01..2026-06-30); no running job writes to it. Its exclusive history is now also folded into `ge_warehouse`'s `stg_sendem.trip_daily`/`event_daily` -- see `docs/migration/legacy-to-platform-migration.md#sendem-migration`. `telemetry_warehouse.clean` itself is untouched (read-only source). |
| `etl` | `sync_runs` / `sync_table_loads` -- the legacy equivalent of `ops.pipeline_run` / `ops.table_load` |
| `reporting` | Power BI-facing views (documented in full in `docs/powerbi_reporting_data_dictionary.md`) |
| `warehouse` | Dead: created by an early migration, superseded by `reporting` before ever being used; confirmed empty in the live catalog |

Full column-by-column object inventory and the current -> target mapping:
`docs/migration/legacy-to-platform-migration.md`.
