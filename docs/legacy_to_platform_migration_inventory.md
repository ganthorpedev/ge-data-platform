# Legacy → Platform migration inventory

This is the complete, SQL-verified inventory of every database object the
application creates or consumes today (`telemetry_warehouse`), mapped to its
future home in the new platform database (`ge_warehouse`). It was produced by
reading every file under `sql/` (now `sql/legacy/telemetry_migrations/` for
the numbered migrations) and `src/ge_data_platform/common/database.py`, and by
inspecting the live `telemetry_warehouse` catalog on the local development
Postgres instance (`localhost:5432`) read-only — nothing here was inferred
from filenames alone.

This inventory is a **design document only**. No data has been copied and no
application code has been repointed. See `docs/ge_warehouse_architecture.md`
for the schema semantics, ops-metadata mapping, and role design this table
feeds into.

Legend for **Migration method**:

- `structural-only` — this phase creates no equivalent object; only the
  target *schema* now exists. No data movement happens until Phase 3.
- `deferred (conformance needed)` — cannot be ported as a 1:1 structural
  copy; requires the cross-source asset/entity conformance design explicitly
  deferred to the next phase.
- `deprecate` — no target object; the legacy object is superseded or dead
  and will not be carried forward.
- `rebuild-on-core` — the object's *logic* (not its DDL) will be rebuilt as
  a mart view once `core` facts/dimensions exist; it is not ported verbatim.

## Raw layer

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| raw | sendem_assets | table | Sendem/MiX | Asset master, source-faithful | raw_sendem | asset | structural-only | 257 rows; re-fetchable from API, low backfill risk | None | Row count parity after Phase 3 port |
| raw | sendem_sites | table | Sendem/MiX | Site master | raw_sendem | site | structural-only | 168 rows; re-fetchable | None | Row count parity |
| raw | sendem_event_descriptions | table | Sendem/MiX | Event-type dictionary | raw_sendem | event_description | structural-only | 114 rows; re-fetchable | None | Row count parity |
| raw | sendem_trips_assets_daily | table | Sendem/MiX | Daily trip aggregate (source-native grain, not per-trip) | raw_sendem | trip_daily | structural-only | 1,906 rows; API only retains a rolling window, so historical rows are **not** re-fetchable | date_key is INTEGER YYYYMMDD, not DATE — a known source quirk, carry forward as-is | Row count + date_key range parity |
| raw | sendem_events_assets_daily | table | Sendem/MiX | Daily event aggregate | raw_sendem | event_daily | structural-only | 16,285 rows; not re-fetchable (rolling window) | Same date_key quirk | Row count parity |
| raw | trackunit_assets | table | Trackunit/Manitou | Asset master | raw_trackunit | asset | structural-only | 107 rows; re-fetchable | asset_id is TEXT (UUID-shaped), not BIGINT — do not coerce | Row count parity |
| raw | trackunit_aemp_operating_hours | table | Trackunit/Manitou | Cumulative operating-hour time series | raw_trackunit | aemp_operating_hour | structural-only | 125,031 rows; historical AEMP series has finite retention upstream — treat as not fully re-fetchable | Cumulative-counter resets (see `027_add_trackunit_counter_quality.sql`) must be re-derived, not copied blind | Row count + min/max timestamp parity |
| raw | trackunit_aemp_moving_hours | table | Trackunit/Manitou | Cumulative moving-hour time series | raw_trackunit | aemp_moving_hour | structural-only | 122,932 rows; same retention concern | Same counter-reset concern | Row count parity |
| raw | trackunit_aemp_distance | table | Trackunit/Manitou | Cumulative distance time series | raw_trackunit | aemp_distance | structural-only | 122,933 rows; same retention concern | Same counter-reset concern | Row count parity |
| raw | trackunit_aemp_locations | table | Trackunit/Manitou | Location enrichment time-series points (V1) | raw_trackunit | aemp_location | structural-only | 28,501 rows; 48h lookback re-fetch window only, older points not re-fetchable | None beyond normal type carry-over | Row count parity |
| raw | trackunit_site_history | table | Trackunit/Manitou | Asset↔site assignment intervals | raw_trackunit | site_history | structural-only | 39 rows; re-fetchable from API | None | Row count parity |
| raw | trackunit_sites | table | Trackunit/Manitou | Site master (enrichment-resolved subset, not full master) | raw_trackunit | site | structural-only | 11 rows; re-fetchable | Only sites actually looked up during enrichment are present — not a full sites list | Row count parity |
| raw | ezytrack_assets | table | EzyTrack/Telematics Guru | Asset master | raw_ezytrack | asset | structural-only | 51 rows; re-fetchable | None | Row count parity |
| raw | ezytrack_trips | table | EzyTrack/Telematics Guru | Per-trip fact (source-native grain) | raw_ezytrack | trip | structural-only | 823 rows; re-fetchable within provider's retention | None | Row count parity |
| raw | evolution_project_reports | table (defined, not yet applied locally) | Accounts/Evolution | Full-refresh project-accounting extract (GE + TLS) | raw_evolution | project_report | structural-only | Full-refresh source (no incremental key) — historically the destination is always fully re-derivable from the live view | id is BIGINT, unique only within (company, id), not globally | Row count parity + (company, id) uniqueness check |
| — | — | — | FieldOps | No existing pipeline of any kind today | raw_fieldops | (none yet) | structural-only (empty schema) | N/A — no source exists yet | Entirely new source; out of scope this phase | Schema-exists check only |

## Staging layer

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| staging | sendem_dim_assets | table | Sendem/MiX | Cleaned asset dimension | stg_sendem | asset | structural-only | 257 rows | None | Row count parity |
| staging | sendem_dim_sites | table | Sendem/MiX | Cleaned site dimension | stg_sendem | site | structural-only | 168 rows | None | Row count parity |
| staging | sendem_dim_event_types | table | Sendem/MiX | Cleaned event-type dimension (includes inferred "unknown" rows) | stg_sendem | event_type | structural-only | 120 rows | None | Row count parity |
| staging | sendem_fact_trips_daily | table | Sendem/MiX | Cleaned daily trip fact | stg_sendem | trip_daily | structural-only | 1,906 rows | date_key quirk carries forward | Row count parity |
| staging | sendem_fact_events_daily | table | Sendem/MiX | Cleaned daily event fact | stg_sendem | event_daily | structural-only | 16,285 rows | date_key quirk carries forward | Row count parity |
| staging | trackunit_dim_assets | table | Trackunit/Manitou | Cleaned asset dimension | stg_trackunit | asset | structural-only | 107 rows | asset_id TEXT | Row count parity |
| staging | trackunit_daily_activity | table | Trackunit/Manitou | Daily activity fact (work/operating/driving minutes, distance) | stg_trackunit | daily_activity | structural-only | 856 rows | Must carry `counter_reset_detected` / `data_quality_status` forward explicitly, not silently drop them | Row count parity + quality-flag distribution parity |
| staging | trackunit_location_enrichment | table | Trackunit/Manitou | Location enrichment fact (V1) | stg_trackunit | location_enrichment | structural-only | 105 rows | address/zip/city/country columns are intentionally always NULL in V1 — do not backfill/fabricate | Row count parity |
| staging | ezytrack_dim_assets | table | EzyTrack/Telematics Guru | Cleaned asset dimension | stg_ezytrack | asset | structural-only | 51 rows | None | Row count parity |
| staging | ezytrack_fact_trips | table | EzyTrack/Telematics Guru | Cleaned per-trip fact with derived metrics (distance_km, runtime hours, etc.) | stg_ezytrack | trip | structural-only | 823 rows | None | Row count parity |
| — | (none yet) | — | Accounts/Evolution | No staging step today — `raw.evolution_project_reports` already carries business_unit classification and is used directly by reporting | stg_evolution | project_report (future) | deferred | N/A | Would require deciding what "cleaning" means beyond what raw already does | N/A this phase |

## Historical backfill (`clean` schema — legacy, out-of-band)

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| clean | sendem_dim_assets / _dim_sites / _dim_event_types | table | Sendem/MiX | Pre-existing historical backfill dimensions (2026-01-01 to 2026-06-30), no running job writes to these | core (future, as dim rows) | dim_asset / dim_site (conformed, cross-provider) | deferred (conformance needed) | 257 / 165 / 114 rows — one-time historical, must not be silently dropped | Predates the event-type-inference fix documented in `002_create_sendem_warehouse_views.sql`; needs the same "prefer staging, else clean" logic re-implemented against `core`, not against `stg_sendem` | Row-count + spot-check against existing `reporting.vw_sendem_*_combined` output |
| clean | sendem_fact_trips_daily | table | Sendem/MiX | Historical daily trip backfill | core (future) | fact_trip / fact_asset_daily_activity | deferred (conformance needed) | 12,016 rows — one-time historical, not re-fetchable from the API (rolling window) | Grain and column set already match `sendem_fact_trips_daily`; the risk is entirely in the union/dedupe logic against live staging, not the DDL | Row count parity; total distance/fuel/energy sums parity |
| clean | sendem_fact_events_daily | table | Sendem/MiX | Historical daily event backfill | core (future) | fact_asset_daily_activity (event component) | deferred (conformance needed) | 111,122 rows — one-time historical, not re-fetchable | Same union/dedupe risk as above | Row count parity |

## Legacy `warehouse` schema (dead)

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| warehouse | (schema exists, zero objects in the live catalog) | schema | Sendem/MiX | Defined by `002_create_sendem_warehouse_views.sql` (`warehouse.v_sendem_*` views) but superseded by the `reporting` schema (`022_create_reporting_powerbi_views.sql`) before/without ever being dropped | — | — | deprecate | None — confirmed empty via live catalog inspection, not inferred | None | Confirmed no objects currently exist in `telemetry_warehouse.warehouse` |

## Reporting layer (Power BI-facing views)

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| reporting | vw_trackunit_daily_activity | view | Trackunit | Power BI daily activity, joined to location enrichment | mart_fleet | (future) fleet_daily_activity | rebuild-on-core | N/A (view, no data) | Depends on `core.dim_asset` conformance first | Column-for-column diff against legacy view once rebuilt |
| reporting | vw_sendem_trips_daily | view | Sendem | Power BI daily trip fact (clean ∪ staging) | mart_fleet | (future) fleet_trip_daily | rebuild-on-core | N/A | Same conformance dependency | Same |
| reporting | vw_sendem_events_daily | view | Sendem | Power BI daily event fact | mart_fleet | (future) fleet_event_daily | rebuild-on-core | N/A | Same | Same |
| reporting | vw_ezytrack_trips | view | EzyTrack | Power BI per-trip fact | mart_fleet | (future) fleet_trip | rebuild-on-core | N/A | Same | Same |
| reporting | vw_ezytrack_trip_report | view | EzyTrack | Fixed-layout exported trip report | mart_fleet | (future) fleet_trip_report | rebuild-on-core | N/A | Built strictly on `vw_ezytrack_trips`; keep that dependency order | Same |
| reporting | vw_assets_all | view | All | Conformed cross-provider asset list | core | dim_asset | deferred (conformance needed) | N/A | This *is* the asset-conformance problem explicitly deferred to next phase | N/A this phase |
| reporting | vw_daily_activity_all | view | All | Conformed cross-provider daily activity | mart_fleet | (future) fleet_daily_activity_all | deferred (conformance needed) | N/A | Depends on dim_asset | N/A this phase |
| reporting | vw_provider_sync_health | view | Ops | Latest sync status per provider/job | mart_operations or ops | (future) view over ops.pipeline_run | rebuild-on-core | N/A | Straightforward rebuild once `ops.pipeline_run` is populated | Diff against legacy view logic |
| reporting | vw_sendem_dim_assets_combined / _sites_combined / _event_types_combined | view | Sendem | Internal helpers (staging ∪ clean, staging preferred) | — | — | deprecate | N/A | Superseded once `core` dims exist; not a Power BI-facing object itself | N/A |

## ETL metadata

| Current schema | Current object | Type | Source system | Purpose | Target schema | Target object | Migration method | Historical data requirement | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|---|---|---|---|
| etl | sync_runs | table | All | One row per pipeline run (STARTED/SUCCESS/FAILED/ABANDONED) | ops | pipeline_run | structural-only this phase | 55 rows; operationally useful history, low urgency | PK renamed `sync_run_id` → `pipeline_run_id` (table itself renamed; every other column kept identical) | Row count parity once ported |
| etl | sync_table_loads | table | All | One row per per-table load attempt within a run | ops | table_load | structural-only this phase | 299 rows | PK renamed `id` → `table_load_id`; FK renamed `sync_run_id` → `pipeline_run_id`; `provider` renamed → `source_system` for consistency with `pipeline_run.source_system` (a pre-existing naming inconsistency, fixed deliberately, not blindly) | Row count parity once ported |

## Roles / grants

| Current object | Type | Purpose | Target object | Migration method | Compatibility concern | Validation method |
|---|---|---|---|---|---|---|
| `excel_reader` (live role; the never-applied `024_create_powerbi_reader_role.sql` documents the same intent under the name `powerbi_reader`) | role | Read-only grant on `reporting.*` (currently SELECT on 11 objects) | `ge_bi_readonly` | structural-only (new role, not a rename of the old one — the old role and database are untouched) | Old role name (`excel_reader`) stays as-is on `telemetry_warehouse`; do not rename or drop it | `has_schema_privilege` / `information_schema.role_table_grants` check against `ge_warehouse` |
| `postgres` (superuser; ETL jobs currently connect as this user per `POSTGRES_USER`) | role | De facto admin + de facto ETL writer, conflated | `ge_platform_admin` (admin) + `ge_etl` (writer) | structural-only (new roles created; `postgres` superuser usage on `telemetry_warehouse` is untouched) | Today's ETL code has no dedicated non-superuser login — flagged as a follow-up hardening item, not fixed in this phase | Role existence + grant check only |

## Summary of what this phase intentionally does NOT do

- No row of application data is copied from `telemetry_warehouse` into `ge_warehouse`.
- No `core` fact or dimension table is created except `core.dim_date` (source-independent, deliberately safe to seed now).
- No `core.*_source_map` table is created — the conceptual pattern is documented in `docs/ge_warehouse_architecture.md`, but building it before `core.dim_asset` exists would leave it with nothing to key against.
- No mart (`mart_fleet`, etc.) contains a single object yet — only the empty schema.
- Dagster is not repointed at `ge_warehouse`. No schedule, sensor, or job changes.
