# Legacy-to-platform migration

This is the migration plan and the complete current -> target object
inventory for moving off `telemetry_warehouse` (LEGACY, live production)
onto `ge_warehouse` (the platform baseline described in
`docs/architecture/`). It supersedes and absorbs the standalone inventory
document written when `ge_warehouse`'s baseline was first created; that
content lives here now, kept current alongside the rest of the
documentation instead of drifting in a separate file.

**Status: baseline + Trackunit + Sendem raw/staging migrated and validated.**
Trackunit's and Sendem's historical `raw`/`staging` data have been migrated
into `raw_trackunit`/`stg_trackunit` and `raw_sendem`/`stg_sendem` in
`ge_warehouse`, with full reconciliation against `telemetry_warehouse` and a
real fresh-ingestion test for each -- see
[Trackunit migration (completed)](#trackunit-migration-completed) and
[Sendem migration (completed)](#sendem-migration-completed) below. EzyTrack,
Evolution, and FieldOps have not moved. No Dagster schedule writes to
`ge_warehouse` -- `telemetry_warehouse` remains the only database any
*scheduled* job or Power BI report actually uses; Trackunit's and Sendem's
platform-target code paths exist but are exercised only by manual
invocation.

## The principle

> **Nuke the old naming, not the data.**

Nothing about `telemetry_warehouse`'s actual historical data is being
discarded. What's being replaced is the *naming and schema structure*
(`raw`/`staging`/`etl` shared indiscriminately across sources) in favor of
the source-scoped, domain-scoped layout in `docs/architecture/data-layers.md`.
Every migration step below is additive to `ge_warehouse` and read-only
against `telemetry_warehouse` until an explicit, reviewed cutover step says
otherwise.

## Phase pipeline

```mermaid
flowchart TD
    A[telemetry_warehouse] --> B["parallel ge_warehouse\n(this phase: schemas + ops + core.dim_date)"]
    B --> C[source-by-source migration]
    C --> D[validation]
    D --> E[core]
    E --> F[marts]
    F --> G[consumer cutover]
    G --> H[legacy archive]
```

| Phase | What happens | Status |
|---|---|---|
| `telemetry_warehouse` | Live production, unchanged throughout | ONGOING (LEGACY) |
| parallel `ge_warehouse` | Schemas, `ops` metadata structure, roles, `core.dim_date` | IMPLEMENTED |
| source-by-source migration | Each source's ingestion ported to write `raw_<source>`/`stg_<source>` | IN PROGRESS -- Trackunit and Sendem done (raw/staging only); EzyTrack/Evolution/FieldOps NOT STARTED |
| validation | Row counts, spot checks, comparison queries between old and new for the same window | DONE for Trackunit (`scripts/validate_trackunit_migration.py`) and Sendem (`scripts/validate_sendem_migration.py`); NOT STARTED for other sources |
| `core` | Cross-source conformance (`dim_asset` first, then facts) built on validated staging data | NOT STARTED -- blocked on source-map design, `docs/warehouse/source-mapping.md` |
| marts | `mart_<domain>` objects built on `core` | NOT STARTED |
| consumer cutover | Power BI/Excel/applications repointed from `telemetry_warehouse.reporting` to the new `reporting` schema | NOT STARTED |
| legacy archive | `telemetry_warehouse` retired/archived once every consumer has cut over | NOT STARTED |

## Intended source migration order

```text
Trackunit
Sendem
EzyTrack
Evolution
FieldOps
```

This order is **not yet formally chosen** by any decision recorded in
`docs/architecture/architecture-decisions.md` -- it is presented here as the
current working assumption, kept in the order the sources appear across
this documentation set, because no repository evidence (code, tests,
tickets) indicates a different order has been committed to. Revisit this
list, and record the actual decision as a new ADR, before starting Phase 4
(source-by-source migration) for real.

## Current -> target object inventory

Verified against `sql/legacy/telemetry_migrations/`, `sql/validation/`,
`src/ge_data_platform/common/database.py`, and a read-only inspection of the
live `telemetry_warehouse` catalog (schemas, tables, columns, views, row
counts) -- not inferred from filenames.

Legend for **Migration method**: `structural-only` (target schema exists,
no data moved yet); `deferred (conformance needed)` (needs the cross-source
identifier design in `docs/warehouse/source-mapping.md` first);
`deprecate` (no target -- superseded or dead); `rebuild-on-core` (the
object's *logic*, not its DDL, gets rebuilt once `core` exists under it).

### Raw layer

| Current | Target | Method | Notes |
|---|---|---|---|
| `raw.trackunit_assets` | `raw_trackunit.asset` | **migrated + validated** | 107 rows |
| `raw.trackunit_aemp_operating_hour(s)` | `raw_trackunit.aemp_operating_hour` | **migrated + validated** | 125,031 historical rows (+ live rows from fresh-ingestion testing) |
| `raw.trackunit_aemp_moving_hours` | `raw_trackunit.aemp_moving_hour` | **migrated + validated** | 122,932 historical rows |
| `raw.trackunit_aemp_distance` | `raw_trackunit.aemp_distance` | **migrated + validated** | 122,933 historical rows |
| `raw.trackunit_aemp_locations` | `raw_trackunit.aemp_location` | **migrated + validated** | 28,501 rows |
| `raw.trackunit_site_history` | `raw_trackunit.site_history` | **migrated + validated** | 39 rows |
| `raw.trackunit_sites` | `raw_trackunit.site` | **migrated + validated** | 11 rows |
| `raw.sendem_assets` | `raw_sendem.asset` | **migrated + validated** | 257 rows (+1 from live-ingestion testing); re-fetchable |
| `raw.sendem_sites` | `raw_sendem.site` | **migrated + validated** | 168 rows |
| `raw.sendem_event_descriptions` | `raw_sendem.event_description` | **migrated + validated** | 114 rows |
| `raw.sendem_trips_assets_daily` | `raw_sendem.trip_daily` | **migrated + validated** | 1,906 historical rows (+ live rows from fresh-ingestion testing); not re-fetchable (rolling API window) -- no `clean.*` history (raw-shaped only, see below) |
| `raw.sendem_events_assets_daily` | `raw_sendem.event_daily` | **migrated + validated** | 16,285 historical rows (+ live rows from fresh-ingestion testing); not re-fetchable |
| `raw.ezytrack_assets` | `raw_ezytrack.asset` | structural-only | 51 rows |
| `raw.ezytrack_trips` | `raw_ezytrack.trip` | structural-only | 823 rows |
| `raw.evolution_project_reports` (defined; not yet applied on the local dev database) | `raw_evolution.project_report` | structural-only | Full-refresh source, always re-derivable from the live view |
| -- (no source exists) | `raw_fieldops.*` | structural-only (empty schema) | See `docs/sources/fieldops.md` |

### Staging layer

| Current | Target | Method | Notes |
|---|---|---|---|
| `staging.trackunit_dim_assets` | `stg_trackunit.asset` | **migrated + validated** | 107 rows |
| `staging.trackunit_daily_activity` | `stg_trackunit.daily_activity` | **migrated + validated, with a documented correction** | 856 historical rows; 230 carry a corrected `counter_reset_detected`/`data_quality_status` the legacy row never had -- see [Trackunit migration (completed)](#trackunit-migration-completed) |
| `staging.trackunit_location_enrichment` | `stg_trackunit.location_enrichment` | **migrated + validated** | 105 rows; address/zip/city/country stay NULL (V1) |
| `staging.sendem_dim_assets` / `_dim_sites` / `_dim_event_types` | `stg_sendem.asset` / `.site` / `.event_type` | **migrated + validated** | 257 / 168 / 120 rows (+2 inferred placeholder event types, see below) |
| `staging.sendem_fact_trips_daily` / `_fact_events_daily` | `stg_sendem.trip_daily` / `.event_daily` | **migrated + validated, with legacy `clean.*` history folded in** | 1,906 / 16,285 rolling-window rows + 11,439 / 106,188 exclusive historical rows from `clean.*` -- see [Sendem migration (completed)](#sendem-migration-completed) |
| `staging.ezytrack_dim_assets` | `stg_ezytrack.asset` | structural-only | 51 rows |
| `staging.ezytrack_fact_trips` | `stg_ezytrack.trip` | structural-only | 823 rows |
| -- (no staging step today; `raw.evolution_project_reports` already carries `business_unit`) | `stg_evolution.project_report` | deferred | Would need a definition of what "cleaning" adds beyond what raw already does |

### Historical backfill (`clean` schema -- legacy, out of band)

| Current | Target | Method | Notes |
|---|---|---|---|
| `clean.sendem_dim_assets` / `_dim_sites` / `_dim_event_types` | -- (not separately migrated) | **investigated -- confirmed redundant** | 257 / 165 / 114 rows; key-set diff against `staging.sendem_dim_*` found **zero** ids exclusive to `clean` (0/0/0) -- proper subsets of current staging dims, carrying no unique master-data value. See [Sendem migration (completed)](#sendem-migration-completed). |
| `clean.sendem_fact_trips_daily` | `stg_sendem.trip_daily` | **migrated + validated** | 12,016 rows (2026-01-01 to 2026-06-30); not re-fetchable from the API. 11,439 exclusive keys folded in; 577 overlapping keys resolved in favor of `staging`'s live-pipeline value (float-precision drift only, not a business difference). No `core` conformance needed -- this is staging-grade, already-enriched data, folded straight into `stg_sendem`. |
| `clean.sendem_fact_events_daily` | `stg_sendem.event_daily` | **migrated + validated** | 111,122 rows; not re-fetchable. 106,188 exclusive keys folded in; 4,934 overlapping keys resolved the same way. 2 `event_type_id`s (41 rows) referenced by no dimension anywhere were resolved with inferred "Unknown Sendem Event Type" placeholder rows. |

**Note on `core` conformance:** the original assumption in this table (when
only Trackunit had been investigated) was that `clean.*`'s history would
need `core.dim_asset`/`core.fact_trip` conformance before it could be
preserved. The actual Sendem investigation found this unnecessary: `clean.*`
is shape-identical to `staging.sendem_fact_*_daily` (same grain, same
enrichment, an added `source_system` column) with no cross-source
conformance to do -- it is single-source, staging-grade data, not derived
business data, so it belongs in `stg_sendem` directly. `core`/`mart_*` are
still not built in this phase (see "What this phase intentionally did not
do" below).

### Legacy `warehouse` schema (dead)

Confirmed empty in the live catalog (defined by
`002_create_sendem_warehouse_views.sql`, superseded by `reporting` before
ever being dropped or used). Method: **deprecate** -- no target object.

### Reporting layer

| Current | Target | Method |
|---|---|---|
| `reporting.vw_trackunit_daily_activity`, `vw_sendem_trips_daily`, `vw_sendem_events_daily`, `vw_ezytrack_trips`, `vw_ezytrack_trip_report` | `mart_fleet.*` (future) | rebuild-on-core |
| `reporting.vw_assets_all` | `core.dim_asset` | deferred (conformance needed) -- this view *is* the asset-conformance problem, see `docs/warehouse/source-mapping.md` |
| `reporting.vw_daily_activity_all` | `mart_fleet.*` (future) | deferred (conformance needed) |
| `reporting.vw_provider_sync_health` | a future view over `ops.pipeline_run` | rebuild-on-core |
| `reporting.vw_sendem_dim_*_combined` (internal helpers) | -- | deprecate once `core` dims exist |

Full column-level detail for every current `reporting.*` object:
`docs/powerbi_reporting_data_dictionary.md`.

### Ops metadata

| Current | Target | Method | Notes |
|---|---|---|---|
| `etl.sync_runs` | `ops.pipeline_run` | structural-only (this phase) | 55 rows; `sync_run_id` -> `pipeline_run_id` (table itself renamed) |
| `etl.sync_table_loads` | `ops.table_load` | structural-only (this phase) | 299 rows; `id` -> `table_load_id`, `sync_run_id` -> `pipeline_run_id`, `provider` -> `source_system` (deliberate consistency fix) |

Full column mapping: `docs/architecture/architecture-decisions.md#adr-005`.

### Roles

| Current | Target | Method |
|---|---|---|
| `excel_reader` (grants SELECT on `telemetry_warehouse.reporting`) | `ge_bi_readonly` | structural-only -- new role, not a rename; `excel_reader` stays as-is on the legacy database |
| `postgres` (superuser; every job connects as this user today) | `ge_platform_admin` (admin) + `ge_etl` (writer) | structural-only -- today's ETL has no dedicated non-superuser login, flagged as a follow-up hardening item, not fixed by this phase |

## Bootstrapping an empty legacy `telemetry_warehouse`

Not the normal case (production already has most legacy migrations
applied), but if ever needed: a blind numeric-order runner is insufficient
because `022_create_reporting_powerbi_views.sql` depends on objects created
by `025_create_trackunit_location_enrichment.sql` and on the legacy
`clean.sendem_*` tables, both out of numeric order. Required sequence:

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\001_create_sendem_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -c 'CREATE SCHEMA IF NOT EXISTS clean;'
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\sendem_tables.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\002_create_sendem_warehouse_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\004_create_etl_sync_table_loads.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\010_create_ezytrack_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\020_create_trackunit_schema.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\025_create_trackunit_location_enrichment.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\022_create_reporting_powerbi_views.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\027_add_trackunit_counter_quality.sql
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\028_add_sync_run_abandoned_support.sql
```

## Trackunit migration (completed)

The first real source migration: `raw.trackunit_*`/`staging.trackunit_*` ->
`raw_trackunit.*`/`stg_trackunit.*` in `ge_warehouse`. `core`/`mart_fleet`
were deliberately **not** touched -- this phase stops at staging.

### Legacy objects found (inventory, before any DDL)

Verified against the live `telemetry_warehouse` catalog: 7 `raw.trackunit_*`
tables, 3 `staging.trackunit_*` tables, 1 `reporting.vw_trackunit_daily_activity`
view (untouched by this migration). No object beyond this list exists. Exact
row counts, keys, constraints, and indexes were inventoried before writing
any migration -- see the object tables above for the mapping and counts.

### New objects created

`sql/migrations/005_create_raw_trackunit.sql` (7 tables) and
`006_create_stg_trackunit.sql` (3 tables), applied via
`python -m scripts.setup_ge_warehouse --migrate`. Table names drop the
redundant `trackunit_` prefix (the schema already identifies the source);
column shapes are otherwise unchanged, with one addition:
`stg_trackunit.daily_activity` includes `counter_reset_detected`/
`data_quality_status` as first-class, enforced (`NOT NULL`, defaulted,
`CHECK`-constrained) columns from creation -- legacy never actually got
these (see below).

### A discovered gap, and how it was handled

`027_add_trackunit_counter_quality.sql` was written to add and backfill
`counter_reset_detected`/`data_quality_status` on legacy
`staging.trackunit_daily_activity`, but was **never actually applied** to
this local `telemetry_warehouse` -- confirmed by catalog inspection (no such
columns exist) and by finding 238 (asset, report_date) pairs in the raw AEMP
series with a genuine mid-day counter decrease that legacy's stored derived
metric does not reflect. Since `telemetry_warehouse` is read-only for this
work, 027 could not be run there. Instead,
`scripts/backfill_trackunit_historical.py` copies every other column from
the legacy row unchanged, then applies 027's exact detection logic while
writing into `ge_warehouse` -- so `stg_trackunit.daily_activity` starts from
the corrected state 027 always intended, not a copy of the gap.

### Historical backfill and reconciliation results

`scripts/backfill_trackunit_historical.py` (local-only, read-only against
`telemetry_warehouse`, `INSERT ... ON CONFLICT DO NOTHING`, restart-safe)
copied all 7 raw tables and 3 staging tables. `scripts/validate_trackunit_migration.py`
independently reconciles both databases object-by-object (row counts, key
parity, date/time coverage, null profiles, numeric sums, location-enrichment
status distribution, and -- for `daily_activity` -- an independent
recomputation of the expected counter-reset set, not trust in the backfill's
own bookkeeping):

```text
TRACKUNIT HISTORICAL MIGRATION VALIDATION

raw_trackunit.asset                  PASS
raw_trackunit.aemp_operating_hour    PASS
raw_trackunit.aemp_moving_hour       PASS
raw_trackunit.aemp_distance          PASS
raw_trackunit.aemp_location          PASS
raw_trackunit.site_history           PASS
raw_trackunit.site                   PASS
stg_trackunit.asset                  PASS
stg_trackunit.daily_activity         PASS
stg_trackunit.location_enrichment    PASS

Overall: PASS
```

Exact-equality checks (row counts, keys, numeric sums, null profiles,
date/time coverage) passed with **zero tolerance** everywhere except the 230
`stg_trackunit.daily_activity` rows with a documented, independently-verified
counter reset -- for those, the check is that the new value is correctly
`NULL`/flagged, not that it matches the stale legacy value.

Re-running the backfill script a second time (idempotency test) inserted
`0` new rows into every table (`read N, inserted 0` for all 10 objects) and
left the quality distribution (230 `COUNTER_RESET` / 626 `live`) unchanged;
re-running the full reconciliation afterward still reports `Overall: PASS`.

### Fresh-ingestion test

After historical parity passed, `daily_activity.py`/`location.py` were
adapted (not forked -- see `docs/sources/trackunit.md#ge_warehouse-platform-target`)
to support `--target platform`, then run live once:

```powershell
python -m ge_data_platform.sources.trackunit.daily_activity --date 2026-08-12 --limit 3 --target platform
```

against `ge_warehouse` / `raw_trackunit` + `stg_trackunit`, with no Dagster
schedule involved. Result: 3 assets, 471 raw metric points, 3
`stg_trackunit.daily_activity` rows written -- including one genuine,
live-detected counter reset (asset `3849`: `active_driving_minutes` nulled,
`data_quality_status='COUNTER_RESET'`). `telemetry_warehouse`'s own data
does not extend past 2026-08-02, so no legacy comparison is possible for
this window -- stated here rather than invented. Re-running the identical
command a second time produced the same row counts and the same business
values (only `loaded_at` audit timestamps advanced) -- confirming the
existing UPSERT semantics hold unchanged under the platform target.

### What was intentionally not done in the Trackunit phase

- `core.dim_asset`/`core.asset_source_map` -- not built (pattern documented,
  not implemented -- see `docs/warehouse/source-mapping.md`).
- `mart_fleet.*` -- not built.
- No Dagster schedule points at `ge_warehouse`; `--target platform` is
  manual-only.
- Legacy Trackunit code/tables were not removed or altered.
- `ops.pipeline_run`/`ops.table_load` are not written to for platform-target
  runs (sync tracking is skipped, not redirected).

## Sendem migration (completed)

The second real source migration: `raw.sendem_*`/`staging.sendem_*` ->
`raw_sendem.*`/`stg_sendem.*` in `ge_warehouse`, PLUS folding in legacy
`clean.sendem_fact_trips_daily`/`_events_daily` history that exists nowhere
else. `core`/`mart_fleet` were deliberately **not** touched -- this phase
stops at staging, same as Trackunit.

### Legacy objects found (inventory, before any DDL)

Verified against the live `telemetry_warehouse` catalog (read-only session):
5 `raw.sendem_*` tables, 5 `staging.sendem_*` tables, 5 `clean.sendem_*`
tables, 5 `reporting.vw_sendem_*` views (untouched by this migration), and
`warehouse.*` confirmed empty (dead, no target). No object beyond this list
exists. Row counts, keys (all single-column or composite `PRIMARY KEY`, no
extra `UNIQUE`/`FOREIGN KEY` constraints, matching the applied DDL exactly),
and indexes (each table's PK index only) were inventoried before writing any
migration -- see the object tables above for the full mapping and counts.

`clean.*` predates `raw_sendem.asset`/`stg_sendem.asset`'s `site_id` column
(confirmed absent from `clean.sendem_dim_assets`) and is already enriched
with site/asset attributes (fleet_number, registration_number, site_name,
etc.) -- i.e. staging-shaped, not raw-shaped. A key-set diff against current
`staging.sendem_dim_*` found it to be a strict subset on every dimension
(assets: 257/257 common, 0 exclusive either side; sites: 165 common, 0
`clean`-exclusive, 3 `staging`-exclusive; event types: 114 common, 0
`clean`-exclusive, 6 `staging`-exclusive) -- classified as **normalized
staging data, confirmed redundant on the dimension side**, with **genuine,
irreplaceable history on the fact side** (`clean.sendem_fact_trips_daily`/
`_events_daily`, 2026-01-01 to 2026-06-30, all 181 days present with no
gaps).

### New objects created

`sql/migrations/007_create_raw_sendem.sql` (5 tables) and
`008_create_stg_sendem.sql` (5 tables), applied via
`python -m scripts.setup_ge_warehouse --migrate`. Table names drop the
redundant `sendem_` prefix (the schema already identifies the source),
matching the Trackunit precedent; column shapes otherwise mirror the
*current* live `raw.sendem_*`/`staging.sendem_*` shape (not the older shape
`clean.*`/`sql/legacy/telemetry_migrations/sendem_tables.sql` used).

### How legacy `clean.*` history was handled

- **Dims (`clean.sendem_dim_assets`/`_dim_sites`/`_dim_event_types`): NOT
  copied.** Confirmed redundant by the key-set diff above -- copying them
  would add zero unique asset/site/event-type ids, and dims are
  current-state master data, not a time series, so there is no history to
  lose by skipping them.
- **Facts (`clean.sendem_fact_trips_daily`/`_events_daily`): migrated into
  `stg_sendem.trip_daily`/`event_daily`, not `raw_sendem`.** `clean.*` has
  no raw-shaped counterpart (it was never the API's raw response, already
  enriched at the point it was captured), so it has nothing to contribute to
  `raw_sendem`, which stays a faithful mirror of the current live
  `raw.sendem_*` rolling window only.
- **Overlap resolution:** `scripts/backfill_sendem_historical.py` loads
  legacy `staging` fact rows first, then merges `clean` rows via
  `INSERT ... ON CONFLICT (<pk>) DO NOTHING`. On the 577 trip / 4,934 event
  `(date_key, group_id, site_id, asset_id[, event_type_id])` keys present in
  *both* sources (the 2026-06-24..06-30 overlap window), `staging`'s
  live-pipeline value is kept; sampled differences were float-precision
  drift only (e.g. `112.7` vs `112.69999999999999`), never a business-content
  difference. Only `clean`'s 11,439 / 106,188 **exclusive** keys (2026-01-01
  to 2026-06-23) actually extend history.
- **Orphaned event types:** `clean.sendem_fact_events_daily` references 2
  `event_type_id`s (41 rows total) present in **no** dimension table
  anywhere -- not `raw`, not `clean`'s own dim, not current `staging`.
  `apply_inferred_event_types()` in the backfill script synthesizes the same
  `"Unknown Sendem Event Type"` / `event_category='unknown'` placeholder row
  `ge_data_platform.sources.sendem.transform.build_dim_event_types()`
  produces for this exact situation live, so no fact row is orphaned.

### Historical backfill and reconciliation results

`scripts/backfill_sendem_historical.py` (local-only, read-only against
`telemetry_warehouse`, `INSERT ... ON CONFLICT DO NOTHING`, restart-safe)
copied all 5 raw tables, all 5 staging tables, and merged both `clean.*`
fact tables. `scripts/validate_sendem_migration.py` independently
reconciles both databases object-by-object (row counts, key parity, null
profiles, numeric sums, date/time coverage, and -- for the two merged fact
tables -- an independent recomputation of `union(staging, clean)` plus a
row-by-row check that every `staging` key's *value* survived unchanged and
every `clean`-exclusive key's value survived unchanged):

```text
SENDEM HISTORICAL MIGRATION VALIDATION

raw_sendem.asset                                      PASS
raw_sendem.site                                       PASS
raw_sendem.event_description                          PASS
raw_sendem.trip_daily                                 PASS
raw_sendem.event_daily                                PASS
stg_sendem.asset                                       PASS
stg_sendem.site                                        PASS
stg_sendem.event_type                                  PASS
stg_sendem.trip_daily                                  PASS
stg_sendem.event_daily                                 PASS
clean.* dims (not migrated -- subset confirmation)      PASS

Overall: PASS
```

Every check passed with **zero tolerance** -- exact row counts, exact key
parity, exact numeric sums (`total_trip_distance_kilometres`,
`total_event_occurrences`), exact date/time coverage, and exact
row-for-row value equality on both the `staging`-wins overlap resolution and
the `clean`-exclusive historical rows. No discrepancy was hidden behind a
tolerance band anywhere.

Re-running the backfill script a second time (idempotency test) inserted
`0` new rows into every one of the 5 raw + 5 staging tables (`read N,
inserted 0` throughout, including the `clean` merge step and the inferred
event-type step reporting "No orphaned event_type_ids found"); re-running
the full reconciliation afterward still reported `Overall: PASS`.

### Fresh-ingestion test

After historical parity passed, `sync.py` was adapted (not forked -- see
`docs/sources/sendem.md#ge_warehouse-platform-target`) to support
`--target platform`, then run live once:

```powershell
python -m ge_data_platform.sources.sendem.sync --target platform --lookback-days 1
```

against `ge_warehouse` / `raw_sendem` + `stg_sendem`, window 20260812 to
20260813 (the smallest window `--lookback-days` supports), with no Dagster
schedule involved. Result: SUCCESS, 258 assets (1 new asset appeared live,
not present in the earlier snapshot -- real-world drift, not a defect), 92
trip rows and 1,166 event rows loaded into both `raw_sendem` and
`stg_sendem`. `telemetry_warehouse`'s own Sendem sync had not advanced past
2026-08-03 at the time of this test (confirmed by direct inspection), so no
legacy comparison is possible for the 2026-08-12/13 window -- stated here
rather than invented. Historical rows (2026-01-01 to 2026-08-03) were
confirmed unchanged and un-duplicated after the live run.

Re-running the identical command a second time produced the same row counts
(`raw_sendem.trip_daily` stayed at 1,998 total rows, `event_daily` at
17,451; `stg_sendem.trip_daily`/`event_daily` stayed at 13,437/123,639) and
the same business values (verified row-for-row on a sample) -- only
`loaded_at` audit timestamps advanced to the second run's single load
timestamp -- confirming the existing UPSERT semantics hold unchanged under
the platform target. `ops.pipeline_run`/`ops.table_load` stayed at 0 rows
across both runs (sync tracking is skipped, not redirected, for the
platform target).

### What was intentionally not done in the Sendem phase

- `core.dim_asset`/`core.asset_source_map` -- not built (clean.* facts
  needed no cross-source conformance to preserve -- see the "Note on `core`
  conformance" above).
- `mart_fleet.*` -- not built.
- No Dagster schedule points at `ge_warehouse`; `--target platform` is
  manual-only.
- Legacy Sendem code/tables were not removed or altered; `telemetry_warehouse.clean`
  itself is untouched (read-only source, not modified or dropped).
- `ops.pipeline_run`/`ops.table_load` are not written to for platform-target
  runs (sync tracking is skipped, not redirected).
- `reporting.vw_sendem_*` views were not touched or ported.

## What this phase intentionally did not do

- No row of EzyTrack, Evolution, or FieldOps application data copied from
  `telemetry_warehouse` into `ge_warehouse` (Trackunit and Sendem are the
  two exceptions -- see above).
- No `core` object except `core.dim_date`.
- No `core.*_source_map` table (pattern chosen, not built -- see
  `docs/warehouse/source-mapping.md`).
- No object in any `mart_*` schema.
- Dagster not repointed at `ge_warehouse`; no schedule/sensor change.
- No PostgreSQL schema in `telemetry_warehouse` renamed, altered, or
  dropped.
