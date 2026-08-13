# Naming conventions

## Python

Root package: `ge_data_platform`, installed from `src/ge_data_platform` (see
`pyproject.toml`'s `[tool.setuptools.packages.find]`).

```text
ge_data_platform.config          settings/environment loading
ge_data_platform.common          shared: dates, http, logging, overlap, database, migrations
ge_data_platform.sources.<source>  one subpackage per source system
ge_data_platform.orchestration   Dagster definitions/schedules/runner/monitoring/alerts
```

`<source>` is one of `trackunit`, `sendem`, `ezytrack`, `evolution` today;
`fieldops` is reserved for when that source is implemented (PLANNED, no
package exists yet).

### Source ingestion jobs

**Status: PLANNED convention for the future `raw_<source>`/`stg_<source>`
build, described here so it's decided ahead of time rather than improvised
per source as the migration happens.**

```text
<source>_<dataset>_ingest
```

Examples (illustrative -- these functions/entry points do not exist yet):

```text
trackunit_daily_activity_ingest
evolution_project_reports_ingest
```

This is distinct from the **current, real** module names under
`ge_data_platform.sources.<source>`, which are named for what they do, not
by this future convention (e.g. `ge_data_platform.sources.trackunit.daily_activity`,
`ge_data_platform.sources.evolution.project_reports`) -- those names are
IMPLEMENTED and used today; adopt the `_ingest` suffix convention only for
new code written against `raw_<source>`/`stg_<source>`, not as a rename of
existing modules.

### Warehouse and mart builds

**Status: PLANNED convention**, for when `core`/`mart_*` building code is
written:

```text
build_dim_asset
build_fact_asset_daily_activity
build_fleet_activity_mart
```

## Database

- Lowercase `snake_case` everywhere -- schemas, tables, columns.
- Source-scoped schemas: `raw_<source>`, `stg_<source>`.
- Domain-scoped schemas: `mart_<domain>`.
- Conformed dimensions (PLANNED, except `core.dim_date`): `dim_<entity>`,
  e.g. `dim_asset`, `dim_client`, `dim_date`.
- Conformed facts (PLANNED): `fact_<process>`, e.g.
  `fact_asset_daily_activity`, `fact_trip`.
- Source identifier maps (PLANNED): `<entity>_source_map`, e.g.
  `asset_source_map`. See `docs/warehouse/source-mapping.md`.
- Table names are **singular** where practical: `raw_trackunit.asset`, not
  `raw_trackunit.assets`. This applies to table names, not schema names --
  `mart_operations` is intentionally plural because it names a business
  domain, not an entity. (`tests/platform/test_schema_naming.py` enforces
  lowercase `snake_case` on schema names; it does not enforce singular vs.
  plural, since that rule applies at the table level, which doesn't exist
  yet to check.)
- Migration files: `sql/migrations/NNN_verb_object.sql`, a zero-padded
  3-digit sequence number. See `docs/development/migrations.md`.

## Legacy naming being retired

These names are correct and unchanged in the currently-running
`telemetry_warehouse` -- retiring them means "stop using this pattern in new
`ge_warehouse` code," not "go rename the live database":

| Legacy pattern | Where it lives | Platform replacement |
|---|---|---|
| `raw` / `staging` / `warehouse` / `reporting` / `etl` (one generic schema per layer, shared by all sources) | `telemetry_warehouse` | `raw_<source>` / `stg_<source>` / `core` / `mart_<domain>` / `reporting` (planned) / `ops` |
| `etl.sync_runs` / `etl.sync_table_loads` | `telemetry_warehouse` | `ops.pipeline_run` / `ops.table_load` |
| `jobs.sync_<source>` / `connectors.<source>_client` / `transforms.<source>_transform` (flat, pre-package-restructure module paths) | old `telemetry_etl` (no longer exists; see `docs/migration/legacy-to-platform-migration.md`) | `ge_data_platform.sources.<source>.*` |
| Numeric-prefix collision risk (`sql/003_*.sql` used twice in the old repo) | old `telemetry_etl` | `sql/migrations/NNN_*.sql` with mechanical duplicate-prefix detection (`ge_data_platform.common.migrations.discover_migrations`) |

See `docs/architecture/architecture-decisions.md` for why each of these
changes was made, not just what changed.
