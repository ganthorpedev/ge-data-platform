# Sendem / MiX

**Status: IMPLEMENTED. Production Dagster schedule still writes only to
`telemetry_warehouse` (LEGACY target, default). `raw_sendem`/`stg_sendem`
also exist in `ge_warehouse` now, historically backfilled (including six
months of legacy `clean.*` history the rolling-window API sync never
re-derives) and validated, and the same code can write there via
`--target platform` -- opt-in, not the default, not wired to any schedule.
See `docs/migration/legacy-to-platform-migration.md#sendem-migration` for
the full migration record.**

Source system for fleet telemetry (trips and driving events), via the
Sendem/MiX Customer Insights API. Feeds the **Fleet** business domain
alongside Trackunit and EzyTrack.

Code: `ge_data_platform.sources.sendem` (`client.py`, `sync.py`,
`transform.py`).

## Purpose and ingestion

`ge_data_platform.sources.sendem.sync` orchestrates one run: fetch assets,
sites, event descriptions, and daily trip/event aggregates for a rolling
window (`SYNC_LOOKBACK_DAYS`, default 7 days) -> transform -> load. Unlike
Trackunit and EzyTrack, Sendem's API returns *pre-aggregated daily* trip and
event data directly (`get_trips_assets_daily`/`get_events_assets_daily`,
keyed by an integer `YYYYMMDD` date range) -- there is no per-trip fetch to
paginate or chunk.

```powershell
python -m ge_data_platform.sources.sendem.sync
python -m ge_data_platform.sources.sendem.sync --lookback-days 14   # manual recovery override
```

The `--lookback-days` CLI flag overrides `SYNC_LOOKBACK_DAYS` for one
invocation only -- it does not change the configured default.

## Engine time: the driver-grain trip feed

**Status: IMPLEMENTED 2026-10-02 (code, migrations, backfill script);
APPLIED and backfilled on the local `telemetry_warehouse_dev` restore
(2026-07-09..2026-10-02); NOT yet applied to the production
`telemetry_warehouse` or `ge_warehouse` -- see "Rolling out" below.**

The asset-level aggregate (`GET /api/aggregates/trips/assets`) carries
trips, distance, fuel and energy and **no engine or runtime time**, so a
Sendem machine could only ever prove "activity" to Commercial, never a
comparable hours figure. Its driver-grain sibling,
`GET /api/aggregates/trips/assetspeople/{groupId}` (Swagger model
`TripsAssetsPeople`, `SendemClient.get_trips_assets_people_daily`), returns
the same trip count/distance/fuel/energy totals **per `DriverId`** plus
`TotalEngineSecond`, `TotalAssetDrivingTimeSeconds`,
`TotalDriverDrivingTimeSeconds`, `TotalIdleTimeSeconds` and
`TotalTripDuration`. Every sync run now fetches it for the same window,
beside -- never instead of -- the asset-level feed.

| Object | Grain | Key |
|---|---|---|
| `raw.sendem_trips_assetspeople_daily` (`raw_sendem.trip_driver_daily` on the platform target) | one row per asset, driver, site, day -- source-faithful | `(date_key, group_id, site_id, asset_id, driver_id)` |
| `staging.sendem_fact_trips_assetspeople_daily` (`stg_sendem.trip_driver_daily`) | same grain, enriched with asset/site attributes like `sendem_fact_trips_daily` | same |
| `reporting.vw_sendem_asset_daily_activity` | **one row per asset per day**: the driver rows summed | `(date_key, provider_asset_id)` |

DDL: `sql/legacy/telemetry_migrations/030_create_sendem_trips_assetspeople.sql`
(legacy) and `sql/migrations/015_create_sendem_trip_driver_daily.sql`
(platform). Both are idempotent and additive; `030` also re-creates
`reporting.vw_daily_activity_all` so its Sendem branch fills
`operating_minutes` (engine), `moving_minutes` (asset driving),
`idle_minutes` and `duration_minutes` (trip duration) from the new view
without changing any column's name, type or position.
`reporting.vw_sendem_trips_daily` is untouched.

**Aggregation rule (lives only in the view).** `engine_seconds =
SUM(total_engine_second)` over every driver/site row of the asset-day;
`engine_hours = ROUND(engine_seconds / 3600, 2)`; the same for driving,
idle and trip-duration seconds; `driver_count`, `driver_row_count`,
`trip_count`, `distance_km` beside them. Because the primary key includes
`driver_id` and the loader UPSERTs on it, a re-run of any window rewrites
the same driver rows and can never inflate the roll-up.

**Null vs zero.** `SUM` ignores NULLs and is NULL only when every input is
NULL: driver rows with `total_engine_second = 0` roll up to `engine_seconds
= 0` (a measured nought); rows with it NULL throughout roll up to NULL
(unknown, `data_quality_status = 'engine_time_unknown'`). Nothing in the
transform, loader or view coalesces one into the other. (Observed on the
2026-07-09..10-02 window: 0 unknown asset-days, 499 measured-nought,
5,821 positive.) A nought *beside trips on the same day* is a
contradiction some units produce (they report trips without sensing the
engine); the view reports it as Sendem stated it and Commercial treats
that day's engine figure as unusable, not nought.

**Proof (2026-10-02, `telemetry_warehouse_dev`, `scripts/validate_sendem_engine_hours.py`).**
Fleet 9308 (AssetId `1535407637279948800`), 21-30 Sep 2026: 3.88, 6.58,
0.08, 0.13, 0.91, 3.40, 3.32, 2.59, 2.76, 6.51 engine hours from 3-6
driver rows per day -- e.g. 26 Sep is 29 + 12058 + 56 + 114 = 12257 s =
3.40 h, 15 trips, 2.9 km, which the asset-level feed states as 15 trips,
2.9 km. Fleet 4172 on 20 Jul (10 drivers, 54109 s) and 8818 on 10 Jul
(1 driver, 774996 s) each aggregate once. Over all 6,326 asset-days the
roll-up's trip count and distance match a fresh asset-level API fetch on
every day; the 118 asset-days where they differ from the *stored*
asset-level rows are late-arriving trips the stored rows (loaded up to
2026-10-01 09:35) predate. Re-running the 21-30 Sep window left the row
count (12,844) and the engine-second total unchanged.

**Backfill.** The scheduled sync only covers its rolling window, so
history is loaded once with the same client, transform and UPSERT:

```powershell
python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02
python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02 --database telemetry_warehouse_dev
python -m scripts.backfill_sendem_trips_assetspeople --date-from 2026-07-09 --date-to 2026-10-02 --target platform
```

It takes the Sendem overlap lock, walks the range in 14-day windows,
UPSERTs on the natural key (re-runnable, overlap-safe), records
`job_name = sendem_assetspeople_backfill` in `etl.sync_runs` /
`ops.pipeline_run`, and refuses a non-local `POSTGRES_HOST` unless
`--allow-remote-host` is given. No `clean.*` history exists for this feed
(the API only serves it from the current retention window), so
`scripts/backfill_sendem_historical.py` has nothing to copy for it.

**Rolling out** (not done by the change that introduced this):

1. Apply `030_create_sendem_trips_assetspeople.sql` to the production
   `telemetry_warehouse` (and `015_...` to `ge_warehouse` via
   `scripts.setup_ge_warehouse --migrate`).
2. Deploy this code; the next `sendem_sync_schedule` run loads the feed.
3. Run the backfill above for the window Commercial's Tracking Watch
   reads (the asset-level feed's own history starts 2026-06-24 live).
4. `reporting.vw_hours_control_telemetry_evidence` (the governed
   Commercial view, production only) still carries no engine fields for
   Sendem; Commercial reads engine time through the provider views
   (`TELEMETRY_DATABASE_URL`) today.

## Retry behavior

Uses the shared retry session (`ge_data_platform.common.http.build_retrying_session`):
transient failures (connection errors, timeouts, HTTP 500/502/503/504) are
retried up to `HTTP_MAX_RETRIES` (default 3) total attempts with exponential
backoff from `HTTP_BACKOFF_SECONDS` (default 2s). Ordinary 4xx responses are
never retried -- a failure there fails the run immediately, and is
recoverable via the manual `--lookback-days` command above once the
underlying cause is fixed.

## Empty payload handling

**Status: IMPLEMENTED.** Sendem's transform retains the loader's expected
columns even when trips, assets, or sites come back empty -- an empty
dimension does not raise a `KeyError` during the trip/event merge; the
unavailable enrichment fields (site name, asset description, etc.) are
simply `NULL` on the resulting rows. Trips can therefore still load even if,
for example, the sites dimension is temporarily empty.

The scheduled job no longer calls the people or organisations dimension
endpoints (`get_people`/`get_organisations` remain on `SendemClient` for
compatibility but are not part of the current load plan).

## Validation and loading semantics

Every load is an UPSERT (see `ge_data_platform.common.database.PostgresLoader.load_sendem_tables`),
keyed on `(date_key, group_id, site_id, asset_id)` for trips and
`(date_key, group_id, site_id, asset_id, event_type_id)` for events -- safe
to re-run a window without duplicating rows. A bounded post-load validation
pass runs after every load (see `docs/operations/data-quality.md`), checking
for negative trip/event metrics and facts missing an asset or site
dimension, within the recent `ETL_VALIDATION_LOOKBACK_HOURS` window.

External identifiers (`asset_id`, `site_id`, `event_type_id`, `group_id`)
are `BIGINT`, not `TEXT`, even though "TEXT for external IDs" might seem
like the more source-neutral default -- Sendem's API returns these as large
signed integers that arrive as `int64` in pandas, and loading an `int64`
column into a `TEXT` destination is not reliably supported by the
SQLAlchemy/psycopg2 path this loader uses.

## Known provider limitations

- `date_key` stays an `INTEGER` (`YYYYMMDD`), never converted to a `DATE`,
  because the API always returns it that way and a `DATE` column would fail
  on every insert. This is a known, deliberate mismatch, not an oversight.
- The sites dimension has no `group_id` column returned by the API (unlike
  assets and event types, which do).

## Reconciliation schedule

`sendem_sync_schedule`, every 3 hours (`35 */3 * * *`, `Africa/Harare`). See
`docs/operations/pipeline-operations.md#dagster-jobs-and-schedules`.

## `ge_warehouse` platform target

**Status: IMPLEMENTED, opt-in, not scheduled.**

`ge_data_platform.sources.sendem.sync` accepts `--target {legacy,platform}`
(default `legacy` -- current behavior, unchanged):

```powershell
python -m ge_data_platform.sources.sendem.sync --target platform
python -m ge_data_platform.sources.sendem.sync --target platform --lookback-days 1
```

`--target platform`:

- writes to `raw_sendem.*`/`stg_sendem.*` in `ge_warehouse` instead of
  `raw.*`/`staging.*` in `telemetry_warehouse` (same client/transform code,
  same retry/empty-payload/UPSERT behavior -- only the destination
  schema/table names and database differ, via
  `PostgresLoader.from_platform_settings` and the `target=` parameter on
  `load_sendem_tables` in `common/database.py`);
- records the run and each table load in `ops.pipeline_run`/`ops.table_load`
  instead of `etl.sync_runs`/`etl.sync_table_loads` (same call sites in
  `sync.py`; `PostgresLoader.tracking_backend` selects the destination --
  see `docs/operations/pipeline-operations.md#ops-metadata-wiring-status`
  and `ge_data_platform.common.audit`). An empty-payload day still gets a
  `SUCCESS` `ops.pipeline_run` row with `rows_loaded=0` where genuinely
  applicable -- an empty result is never miscoded as a failure;
- skips post-load validation (its checks are hardcoded to legacy schema
  names).

No Dagster job or schedule passes `--target platform`; it is exercised only
by manual invocation today. A real `--lookback-days 1` run was used to prove
`ops.pipeline_run`/`ops.table_load` population during this audit-wiring
change -- 1 `ops.pipeline_run` row (`source_system=sendem`,
`status=SUCCESS`) and 10 `ops.table_load` rows, all referencing that run.
`stg_sendem.trip_daily`/`stg_sendem.event_daily`
additionally carry historical rows folded in from legacy
`clean.sendem_fact_trips_daily`/`_events_daily` (2026-01-01 onward) by
`scripts/backfill_sendem_historical.py` -- a live platform-target sync
UPSERTs on the same natural key as any other row, so it can never duplicate
or conflict with that history. See
`docs/migration/legacy-to-platform-migration.md#sendem-migration` for the
historical backfill, reconciliation, and a real fresh-ingestion test run
against it.
