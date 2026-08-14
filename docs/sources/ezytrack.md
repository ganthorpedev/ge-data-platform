# EzyTrack / Telematics Guru

**Status: IMPLEMENTED. Production Dagster schedules still write only to
`telemetry_warehouse` (LEGACY target, default). `raw_ezytrack`/`stg_ezytrack`
also exist in `ge_warehouse` now, historically backfilled and validated, and
the same code can write there via `--target platform` -- opt-in, not the
default, not wired to any schedule. See
`docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`
for the full migration record.**

Source system for fleet telemetry (assets and trips) via the Telematics Guru
GraphQL API. Feeds the **Fleet** business domain alongside Trackunit and
Sendem. Phase-1 scope only: assets + trips (no fuel, events, geofence
history, or driver-master data yet -- intentionally omitted to keep GraphQL
queries cheap against the API's cost-based rate limit).

Code: `ge_data_platform.sources.ezytrack` (`client.py`, `sync.py`,
`transform.py`).

## Authentication

**Dynamic, not static.** `EzytrackClient.authenticate()` issues
`GET {TELEMATICS_AUTH_URL}` with an `x-www-form-urlencoded` body of
username/password/grant_type, and stores the returned `access_token` in
memory only -- never on disk, in the database, or in a log line (only the
non-secret `token_type`/`expires_in` are ever printed). Telematics Guru
tokens expire after roughly 24 hours, so a token can never be safely pasted
into `.env`; `sync.py` authenticates once, explicitly, right after starting
the sync-run row, and reuses that one client for the whole run.

If a request comes back unauthorized -- either a raw HTTP 401, or (what this
API actually returns) an HTTP 200 with a GraphQL `errors` array containing
`AUTH_NOT_AUTHENTICATED`/`UNAUTHENTICATED`/`UNAUTHORIZED` -- the client
re-authenticates once and retries that one request once. A second failure
propagates. This is a distinct failure mode from a rate limit (below):
re-authenticating never helps a cost-limit rejection, and a rate limit is
never treated as an auth failure.

`TELEMATICS_TOKEN` (a legacy static token variable) is deprecated and read
by no code path; it exists in `.env.example` only for backward-compatible
visibility.

## Assets and trips

`fetch_assets()` -- one GraphQL query, all assets for the configured
organisation, no pagination needed.

`fetch_trips(start, end, page_size)` -- cursor-paginated
(`pageInfo.endCursor`/`hasNextPage`), ordered by `startTimeUtc`. Two loop
protections, both raising a loud `RuntimeError` rather than looping forever:
a repeated `endCursor`, or exceeding `EZYTRACK_MAX_PAGES` (default 500)
pages for one window.

A GraphQL cost-limit rejection (`GRAPHQL_COST_RATE_LIMIT_EXCEEDED`) raises
`RateLimitError` immediately -- it is **never** silently swallowed into a
partial result, unlike the original prototype notebook this client was
built from. `RateLimitError` carries the failed window, page size, and how
many records were already fetched, so a mid-pagination failure is always
traceable.

## Catch-up behavior

**Status: IMPLEMENTED.** A normal `python -m ge_data_platform.sources.ezytrack.sync`
run finds the latest successful EzyTrack row in `etl.sync_runs` and uses its
`started_at` as a window-end cursor (the schema doesn't yet store an exact
window-end timestamp, so `started_at` -- captured immediately before the
fetch window is calculated -- is the backward-compatible proxy). The next
window starts at that cursor minus `EZYTRACK_CATCHUP_OVERLAP_MINUTES`
(default 30) and ends at "now," capped so it never reaches further back than
`EZYTRACK_MAX_CATCHUP_HOURS` (default 168 = 7 days) before "now." If no
successful run exists yet, the job falls back to `TELEMATICS_LOOKBACK_HOURS`
(default 6) as a first-run window.

The catch-up cap is **not** a promise to heal an arbitrarily long outage --
for any gap older than the cap, use reconciliation (below) with an
explicitly reviewed lookback.

Every mode -- first-run, catch-up, and reconciliation -- fetches trips in
small chunks (`TELEMATICS_CHUNK_HOURS`, default 1 hour) rather than one
large request, each chunk paginated at `TELEMATICS_PAGE_SIZE` (default 50).
This is strict, all-or-nothing: if any chunk fails for any reason, the whole
run is marked `FAILED` with that chunk's window named in `error_message`,
and the run is re-raised. There is no partial `SUCCESS`.

## Reconciliation

```powershell
python -m ge_data_platform.sources.ezytrack.sync --reconcile
```

Ignores the success cursor entirely and uses the longer, fixed
`EZYTRACK_RECONCILIATION_LOOKBACK_HOURS` (default 48). Scheduled daily at
`15 1 * * *` (`Africa/Harare`) as `ezytrack_daily_reconciliation_schedule`,
separate from the 3-hourly incremental `ezytrack_sync_schedule`.

## Quota / rate-limit issues

`RateLimitError` (the GraphQL cost limit) is a distinct concern from
authentication and is never retried automatically -- repeatedly rerunning
the job does not help and risks compounding the problem. Check
`etl.sync_runs` / `reporting.vw_provider_sync_health` (LEGACY) before
retrying, and consider a smaller `TELEMATICS_CHUNK_HOURS`/`_PAGE_SIZE` if
cost-limit failures recur.

## Pagination/cursor protections

Covered above under "Assets and trips": repeated-cursor detection and
`EZYTRACK_MAX_PAGES` both fail loudly rather than looping. Overlapping
chunk windows can return the same trip twice; `sync.py` deduplicates the
combined chunk results by `tripId` before transform/load
(`_dedupe_trips_by_id`).

## Reconciliation schedules

`ezytrack_sync_schedule` (`45 */3 * * *`) and
`ezytrack_daily_reconciliation_schedule` (`15 1 * * *`), both
`Africa/Harare`. See
`docs/operations/pipeline-operations.md#dagster-jobs-and-schedules`.

## Verifying authentication without a full sync

```powershell
python -m scripts.check_ezytrack_auth
```

Confirms the auth request succeeds and that `token_type`/`expires_in` came
back; never prints the access token itself. Non-zero exit code on failure.

## `ge_warehouse` platform target

**Status: IMPLEMENTED, opt-in, not scheduled.**

`ge_data_platform.sources.ezytrack.sync` accepts `--target {legacy,platform}`
(default `legacy` -- current behavior, unchanged) and an optional
`--lookback-hours` override:

```powershell
python -m ge_data_platform.sources.ezytrack.sync --target platform --lookback-hours 1
```

`--target platform`:

- writes to `raw_ezytrack.*`/`stg_ezytrack.*` in `ge_warehouse` instead of
  `raw.*`/`staging.*` in `telemetry_warehouse` (same client/transform code,
  same auth/retry/pagination/cursor-repeat/max-page/dedup behavior -- only
  the destination schema/table names and database differ, via
  `PostgresLoader.from_platform_settings` and the `target=` parameter on
  `load_ezytrack_tables` in `common/database.py`);
- records the run and each table load in `ops.pipeline_run`/`ops.table_load`
  instead of `etl.sync_runs`/`etl.sync_table_loads` (same
  `start_sync_run`/`finish_sync_run`/`start_table_load`/`finish_table_load`
  call sites in `sync.py`; `PostgresLoader.tracking_backend` selects the
  destination -- see `ge_data_platform.common.audit`);
- skips post-load validation (its checks are hardcoded to legacy schema
  names).

**Catch-up safety (the one shared-code behavior this migration required, and
had to preserve when `ops.pipeline_run` was wired):** a platform-target run
must never read legacy `telemetry_warehouse.etl.sync_runs` for its catch-up
cursor -- especially since that cursor can go stale (confirmed during the
original migration: legacy's last successful EzyTrack sync was 2026-07-21,
with repeated `GraphQL cost rate limit exceeded` failures on every catch-up/
reconciliation attempt since). Now that `ops.pipeline_run` is wired and
genuinely holds `SUCCESS` rows for platform-target EzyTrack runs, the same
guarantee had to be re-verified against a *second* possible source of a
stale/unintended catch-up: `ops.pipeline_run`'s own history.
`PostgresLoader.get_last_successful_run` still returns `None` immediately
whenever `tracking_backend == "platform"` -- **before** issuing any
query, so it is unaffected by what `ops.pipeline_run` actually contains --
so a platform-target run always computes its window as
first-run/explicit-window, identical in shape to how `--reconcile` already
ignores the cursor by design. It never attempts a `max_catchup_hours`-capped
historical catch-up inferred from either legacy state or `ops.pipeline_run`.
`--lookback-hours` lets an operator supply an explicit small window (e.g. 1
hour) for a manual test, overriding `TELEMATICS_LOOKBACK_HOURS` for that one
run only. Live-verified during the `ops` audit-wiring change: two
consecutive `--target platform --lookback-hours 1` runs both logged `Sync
window (first-run, UTC): ...`, even though the first run had already left a
real `SUCCESS` row in `ops.pipeline_run` before the second one started --
see `tests/platform/test_ops_audit.py::test_get_last_successful_run_returns_none_for_platform_even_with_real_success_history`
for the equivalent automated proof.

No Dagster job or schedule passes `--target platform`; it is exercised only
by manual invocation today. See
`docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`
for the historical backfill, reconciliation, and a real fresh-ingestion test
run against it (including the fresh-window idempotency proof).
