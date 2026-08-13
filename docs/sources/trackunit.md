# Trackunit / Manitou

**Status: IMPLEMENTED and running against `telemetry_warehouse` (LEGACY
target database). Not yet ported to `raw_trackunit`/`stg_trackunit`.**

Source system for fleet telemetry: asset master data, cumulative
operating/moving-hour and distance counters (via AEMP), and location
enrichment. Feeds the **Fleet** business domain (see
`docs/architecture/platform-overview.md#source-system-vs-business-domain`),
alongside Sendem and EzyTrack.

Code: `ge_data_platform.sources.trackunit` (`client.py`, `daily_activity.py`,
`transform.py`, `location.py`, `location_transform.py`).

## Authentication

OAuth2 password grant against `TRACKUNIT_TOKEN_URL` (default
`https://auth.trackunit.com/token`), using `TRACKUNIT_CLIENT_ID`/
`_CLIENT_SECRET` (HTTP Basic) plus `TRACKUNIT_USERNAME`/`_PASSWORD` in the
form body. The access token is cached in memory on the client instance for
the life of one run (`TrackunitClient._access_token`).

On a `401` from any authenticated request, the client refreshes the token
once and retries that exact request once. A second `401` after the refresh
raises a `RuntimeError` naming the failing request context -- there is no
unlimited retry loop.

## Assets

`GET {asset_base_url}/v1/assets`, paginated (`page`/`size`, size 100),
followed to the end via `totalPages` in the envelope
(`TrackunitClient.get_all_assets`). Returns id, name, `externalReference`
(used as the PIN when present), `serialNumber` (PIN fallback), asset type,
brand, model, production year, owner account id, and the linked telematics
device's id/serial.

## AEMP cumulative counters

Three time-series metrics are fetched per asset per report day, via
`GET {aemp_base_url}/{account_id}/Fleet/Equipment/ID/{pin}/{metric}/{start}/{end}/{page}`:

- `CumulativeOperatingHours`
- `CumulativeMovingHours`
- `Distance`

Each report day's UTC window is derived from the local calendar day
(`TRACKUNIT_TIMEZONE`, default `Africa/Harare`) via
`ge_data_platform.common.dates.local_day_to_utc_window` -- nothing hardcodes
a UTC window.

**Rate limiting and pacing:**

- Every AEMP call is preceded by `TRACKUNIT_REQUEST_DELAY_SECONDS` (default
  1s) of pacing -- one call at a time, no parallel requests.
- On `429`, the client prefers the response's `Retry-After` header (if a
  valid non-negative number of seconds) and otherwise falls back to
  exponential backoff from `TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS`
  (default 30s, doubling per attempt). Either way the wait is capped at
  `TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS` (default 300s) plus 0-3s of
  random jitter. `TRACKUNIT_MAX_RETRIES` (default 7) is the maximum *total*
  attempts, not retries after the first. With defaults, a persistent 429
  produces waits of `30, 60, 120, 240, 300, 300` seconds (each plus jitter)
  before the seventh and final attempt.
- Transient failures (connection errors, timeouts, HTTP 500/502/503/504) are
  retried separately, up to `HTTP_MAX_RETRIES` (default 3) total attempts
  with exponential backoff from `HTTP_BACKOFF_SECONDS` (default 2s). This
  client deliberately does not use `ge_data_platform.common.http`'s
  adapter-level retries, so the two retry counters are never doubled up.
- Ordinary 4xx responses (other than 401/429, and 403 on `get_site`, below)
  are never retried.

## Daily activity build

`ge_data_platform.sources.trackunit.daily_activity` orchestrates: fetch
assets -> fetch the three AEMP metrics per asset per report day -> transform
(`ge_data_platform.sources.trackunit.transform.build_daily_activity_rows`)
-> load. Modes: `--date`, `--from-date`/`--to-date` (inclusive backfill
range), `--rolling-days N` (last N local days ending today; default when no
date argument is given is `--rolling-days 2`). Every load is an UPSERT keyed
on `(report_date, asset_id)`, so re-running any date, range, or rolling
window updates existing rows rather than duplicating them.

An asset with no PIN/serial gets a valid zero row for that day (not an
error). A run covering multiple dates is all-or-nothing: any failure on any
date, asset, or metric -- including a 429 that exhausts its retries -- stops
the whole run and marks the whole `etl.sync_runs` row `FAILED`; there is no
partial `SUCCESS` across a multi-date run.

## Counter-reset handling

**Status: IMPLEMENTED, both historically (one-time SQL repair) and on an
ongoing basis (every load).**

Cumulative counters occasionally decrease mid-series (a device or counter
reset). `ge_data_platform.sources.trackunit.transform.cumulative_counter_reset_detected`
checks each metric's points for a decrease and, per metric, per asset, per
day:

- nulls only that metric's derived value (`operating_minutes` for an
  operating-hours reset, `active_driving_minutes` for a moving-hours reset,
  `distance_km` for a distance reset) -- other metrics on the same row are
  untouched;
- sets `counter_reset_detected = true` and `data_quality_status =
  'COUNTER_RESET'` on the row.

This runs on every new load, not only historically. The one-time historical
repair (`sql/legacy/telemetry_migrations/027_add_trackunit_counter_quality.sql`)
backfilled the same logic against already-loaded rows when these columns
were introduced; it does not need to run again for new data, which the
Python transform already handles. Raw AEMP readings are never modified --
only the derived staging value is nulled. See
`docs/operations/data-quality.md`.

## Location enrichment (V1)

**Status: IMPLEMENTED, separate pipeline from daily activity.**

`ge_data_platform.sources.trackunit.location` reads already-loaded
`staging.trackunit_daily_activity` rows for one report date (run
`daily_activity` for that date first) and, for each asset with start/stop
activity boundaries:

1. Fetches AEMP historical Locations for a 48-hour lookback window ending at
   each boundary, and takes the latest point at or before the boundary.
2. Fetches Site History for the asset across the same window, resolves
   which site (zone) was active at each boundary, and resolves that site's
   name (cached per run, so a given site is only looked up once regardless
   of how many assets reference it).
3. Writes one UPSERTed row per `(report_date, asset_id)` to
   `staging.trackunit_location_enrichment`.

This is a fully separate job from `daily_activity`: separate raw tables,
separate `source_system` in `etl.sync_runs`
(`"trackunit_location"`, not `"trackunit"`), and a failure here never
affects `daily_activity`'s own run history.

**A `403` on a specific site lookup (`GET /sites/{site_id}`) is non-fatal**:
it's logged, that site id is cached as denied so it's never re-requested
this run, and the affected row is marked
`location_enrichment_status = 'SITE_ACCESS_DENIED'` (a `PARTIAL` variant)
instead of aborting the whole sync. This is distinct from a `401`
(auth-wide failure), which still stops the run.

Address/zip/city/country columns exist on `staging.trackunit_location_enrichment`
but are **always NULL** in V1 -- AEMP's historical Locations series returns
coordinates only (no address fields), and the current-location endpoint's
address is "now," not the historical report date, so it must never be used
to fill these in. Do not add reverse geocoding without revisiting this.

## Overlap protection

All four Trackunit Dagster jobs (`trackunit_daily_refresh`,
`trackunit_intraday_refresh`, `trackunit_rolling_7_days`,
`trackunit_location_enrichment`) and both direct CLI entry points
(`daily_activity`, `location`) share one overlap group (`TRACKUNIT_OVERLAP_GROUP`)
because they write the same staging table. See
`docs/operations/retries-and-recovery.md#overlap-protection`.

## Known provider limitations (from the code, not guessed)

- Trackunit has no separate fleet-number field; the legacy reporting view
  reuses `name` for that purpose.
- AEMP's historical Locations series has no address/zip/city/country --
  coordinates and a timestamp only.
- `raw.trackunit_sites` only ever contains sites actually looked up during
  enrichment, not a full sites master.
- A `403` on `get_site` means *this account* cannot see that specific site
  -- it is not evidence of a broken integration and must not be treated as
  one.

## Reconciliation schedules

See `docs/operations/pipeline-operations.md#dagster-jobs-and-schedules` for
the full cron table (`trackunit_daily_refresh`, `trackunit_intraday_refresh`,
`trackunit_rolling_7_days`).
