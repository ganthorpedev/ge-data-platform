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
