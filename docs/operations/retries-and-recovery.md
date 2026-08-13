# Retries and recovery

**Status: IMPLEMENTED.** All behavior below is read directly from the
current source code, against `telemetry_warehouse` (the only database any
job writes to today).

## Network retry policy

**Shared baseline** (`ge_data_platform.common.http.build_retrying_session`,
used by Sendem and EzyTrack): transient failures only -- connection errors,
timeouts, HTTP 500/502/503/504. `HTTP_MAX_RETRIES` (default 3) total
attempts including the first, exponential backoff from
`HTTP_BACKOFF_SECONDS` (default 2s). Ordinary 4xx responses are never
retried. `HTTP_CONNECT_TIMEOUT_SECONDS`/`HTTP_READ_TIMEOUT_SECONDS`
(defaults 30s/120s) are always explicit.

**Trackunit** does not use the shared session -- it implements its own
policy in `ge_data_platform.sources.trackunit.client` to avoid doubling up
retry logic with the AEMP-specific 429 handling:

- **401** on any authenticated request: refresh the OAuth token, retry
  exactly once. A second 401 raises, naming the request context.
- **429**: prefer the `Retry-After` header (valid non-negative seconds
  only); otherwise exponential backoff from
  `TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS` (default 30s). Capped at
  `TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS` (default 300s) plus 0-3s random
  jitter. `TRACKUNIT_MAX_RETRIES` (default 7) is the maximum *total*
  attempts. Defaults produce waits of `30, 60, 120, 240, 300, 300` seconds
  (each plus jitter) before the 7th and final attempt.
- **Transient** (connection errors, timeouts, 5xx): up to `HTTP_MAX_RETRIES`
  attempts, same backoff as the shared baseline.
- AEMP calls are additionally paced by `TRACKUNIT_REQUEST_DELAY_SECONDS`
  (default 1s) before every request -- one call at a time, never parallel.

**EzyTrack**: on a 401 or a GraphQL `AUTH_NOT_AUTHENTICATED`/
`UNAUTHENTICATED`/`UNAUTHORIZED` error (this API returns the latter as an
HTTP 200 with an `errors` array, not a raw 401), the client re-authenticates
once and retries once. A GraphQL cost-limit rejection
(`GRAPHQL_COST_RATE_LIMIT_EXCEEDED`) is **never** retried automatically --
see `docs/sources/ezytrack.md#quota--rate-limit-issues`.

## EzyTrack gap recovery

See `docs/sources/ezytrack.md#catch-up-behavior` for the full mechanism
(cursor-based catch-up capped at `EZYTRACK_MAX_CATCHUP_HOURS`, reconciliation
mode for older gaps). Every chunk that fails, for any reason, fails the
entire run -- there is no partial `SUCCESS`. Overlap between adjacent chunk
windows is deduplicated by `tripId` before load and is additionally safe at
the database UPSERT keys.

## Sendem retries

Covered by the shared baseline above. See
`docs/sources/sendem.md#empty-payload-handling` for how Sendem additionally
tolerates an empty dimension response without failing the trip/event
merge.

## Overlap protection

Every provider entry point and Dagster job is grouped into one of four
overlap groups (`ge_data_platform.common.overlap`):
`SENDEM_OVERLAP_GROUP`, `EZYTRACK_OVERLAP_GROUP`, `TRACKUNIT_OVERLAP_GROUP`,
`ACCOUNTS_EVOLUTION_OVERLAP_GROUP`. Protection is layered:

1. **Schedule-time check** (`ge_data_platform.orchestration.schedules._skip_if_overlapping`):
   before launching a scheduled run, checks whether any Dagster job in the
   same overlap group already has a non-terminal run, and skips with a
   clear `SkipReason` if so.
2. **OS file lock** (`ge_data_platform.common.overlap.provider_overlap_guard`):
   a non-blocking exclusive lock on
   `<project-root>/.ge_data_platform_locks/<group>.lock`, held for the
   duration of the provider's work. This closes the race between the
   schedule-time check and actual launch, and is the *only* protection for
   manually-launched Dagster runs and direct CLI invocations (both go
   through the same lock). The lock file is retained after use -- the OS
   lock on the open handle, not the file's existence, is authoritative, so a
   crashed process can never leave a permanently held logical lock.
3. **Dagster run-tag concurrency** (`dagster.yaml.example`, production-only,
   merged into the external `dagster.yaml`): a `telemetry/provider=trackunit`
   tag concurrency limit of 1, defense-in-depth on top of the OS lock.

The lock lives under the project directory (not `DAGSTER_HOME` or a
per-user temp directory) specifically because the production Windows host
runs Dagster as `SYSTEM` while manual recovery commands run as an operator
-- both processes must contend for the same lock file, which a per-user
location wouldn't guarantee.

A Dagster wrapper that already holds a lock (e.g. `trackunit_daily_refresh`
holding the Trackunit lock across its two sequential subprocess children)
passes that fact to its child via a private, child-only environment
variable (`_TELEMETRY_ETL_INHERITED_OVERLAP_GROUP`) so the child doesn't try
to reacquire the non-reentrant parent lock. This variable is never written
to the parent process's environment, so it can never leak into a later,
unrelated direct CLI invocation.

## Stale run cleanup

`ge_data_platform.orchestration.monitoring.cleanup_stale_started_runs`
(scheduled hourly as `stale_started_run_cleanup_schedule`, `20 * * * *`):
finds `etl.sync_runs` rows still `STARTED` after `ETL_ABANDONED_RUN_HOURS`
(default 12), then -- **before touching any of them** -- checks whether any
corresponding Dagster job is still actively running. If even one stale
candidate could plausibly belong to active Dagster work, the **entire
batch** is deferred, not just that one row: `PostgresLoader.mark_abandoned_runs`
updates every eligible row in one statement, so partial certainty isn't
good enough. An unrecognized source/job mapping fails closed the same way
(defers, does not guess). Eligible rows are marked `ABANDONED`, keep their
original identity/timestamps, get a cleanup note appended to
`error_message`, and generate an `abandoned` alert (see
`docs/operations/monitoring-and-alerting.md`). This is status repair only --
it never deletes data or retries a provider; diagnose the original failure
and use the recovery commands below.

## Subprocess timeout

Covered in `docs/operations/pipeline-operations.md#subprocess-monitoring`.

## Original-exception preservation

If provider work fails and the database update to mark it `FAILED` *also*
fails (e.g. Postgres itself is unreachable), the bookkeeping error is logged
but the **original** exception is what propagates and fails the job --
`finish_sync_run_failed_safe` swallows the bookkeeping failure specifically
so it never masks the real root cause. The row is left `STARTED`, which the
stale-run cleanup above will eventually catch.

## Manual provider recovery

All recovery commands use the normal UPSERT (or, for Evolution, full
replace) paths -- there is no separate "recovery mode." First confirm no
scheduled run for that provider is currently active, identify the
failed/missing window from `etl.sync_runs`
(`docs/operations/data-quality.md#inspecting-run-history`), and run from the
project root.

**Sendem** -- explicit lookback overrides `SYNC_LOOKBACK_DAYS` for one run:

```powershell
python -m ge_data_platform.sources.sendem.sync --lookback-days 14
python -m ge_data_platform.sources.sendem.sync --target platform --lookback-days 1   # ge_warehouse, opt-in, not scheduled
```

`--target platform` writes to `raw_sendem.*`/`stg_sendem.*` in `ge_warehouse`
instead of `raw.*`/`staging.*` in `telemetry_warehouse` -- same fetch/
transform/retry/empty-payload behavior, opt-in and manual-only (no Dagster
schedule passes it), same pattern as Trackunit's `--target platform`. See
`docs/migration/legacy-to-platform-migration.md#sendem-migration`.

**EzyTrack** -- normal catch-up, or reconciliation for a gap the catch-up
cap can't reach:

```powershell
python -m ge_data_platform.sources.ezytrack.sync
$env:EZYTRACK_RECONCILIATION_LOOKBACK_HOURS = '72'
python -m ge_data_platform.sources.ezytrack.sync --reconcile
```

A PowerShell session environment assignment overrides `.env` for that
session only -- start a new session or remove the override before returning
to the configured default.

**EzyTrack platform target** -- `ge_warehouse`, opt-in, not scheduled:

```powershell
python -m ge_data_platform.sources.ezytrack.sync --target platform --lookback-hours 1
```

A platform-target run always computes its window as first-run/explicit-window
(never a legacy-cursor-derived catch-up) -- `ge_warehouse` has no `etl`
schema, so `PostgresLoader.get_last_successful_run` returns `None`
immediately for a platform-settings loader, before any query. `--lookback-hours`
lets an operator supply an explicit small window for a manual test. See
`docs/sources/ezytrack.md#ge_warehouse-platform-target` and
`docs/migration/legacy-to-platform-migration.md#ezytrack-migration-completed`.

**Trackunit** -- recover the smallest known date or range (report dates are
local `TRACKUNIT_TIMEZONE` calendar dates; API windows are converted to
UTC):

```powershell
python -m ge_data_platform.sources.trackunit.daily_activity --date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --from-date 2026-07-27 --to-date 2026-08-02
python -m ge_data_platform.sources.trackunit.daily_activity --rolling-days 7
python -m ge_data_platform.sources.trackunit.location --date 2026-08-02   # after activity, if enrichment is needed
```

`--machines`/`--limit` are diagnostic filters only -- always follow a
filtered run with the unfiltered date once validated. A failed multi-date
run names the exact failing date in its error; re-running that date or
range is safe (UPSERT).
