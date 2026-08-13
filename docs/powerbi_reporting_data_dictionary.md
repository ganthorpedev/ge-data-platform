# Power BI Reporting Data Dictionary

Database: `telemetry_warehouse` (PostgreSQL)
Power BI schema: **`reporting`**

> This dictionary describes the current, live reporting layer, which still
> lives in `telemetry_warehouse.reporting`. A new platform database,
> `ge_warehouse`, is under construction (see
> `docs/architecture/platform-overview.md` and
> `docs/warehouse/reporting-layer.md`) but is not reporting-facing yet --
> Power BI must keep pointing at `telemetry_warehouse.reporting` until a
> future phase explicitly cuts it over to `ge_warehouse`'s `mart_*` schemas.

## The one rule

**Power BI must only ever connect to the `reporting` schema.** Never point a
Power BI data source at `raw`, `staging`, `etl`, or `warehouse` directly, even
for a "quick" report. Those schemas hold provider-shaped ETL internals, sync
logs, and (for Sendem) both a currently-unused historical schema and the live
ETL target — none of it is meant to be interpreted without the cleanup and
blending logic that lives in the `reporting` views. `warehouse` is currently
unused and out of scope for this step.

The `powerbi_reader` role (see `sql/legacy/telemetry_migrations/024_create_powerbi_reader_role.sql`, not
yet run) is deliberately granted `USAGE`/`SELECT` on `reporting` only, with no
grants anywhere else, so this rule is enforced at the database level once
that file is applied.

Every view below is created/replaced by `sql/legacy/telemetry_migrations/022_create_reporting_powerbi_views.sql`,
which only ever creates the `reporting` schema and `CREATE OR REPLACE VIEW`
statements — it never inserts, updates, deletes, truncates, or alters a
table. It is safe to re-run at any time.

---

## Sendem: how `clean` and `staging` are combined

Before reading the view descriptions below, this is the most important
structural fact in the whole reporting layer:

Sendem has **two** underlying schemas holding fact/dimension tables, and they
are not duplicates:

| Schema | Role | Date range (trips/events) | Written to by current ETL? |
|---|---|---|---|
| `clean` | Historical backfill | 2026-01-01 → 2026-06-30 | No — pre-existing, static |
| `staging` | Live, current | 2026-06-24 → 2026-07-02 (and growing) | Yes — `sendem_hourly_sync` |

There is a **~7-day overlap window (2026-06-24 to 2026-06-30)** where both
schemas have rows for the same `(date_key, group_id, site_id, asset_id)` (and
`event_type_id` for events). `clean` also predates a data-quality fix:
`staging.sendem_dim_event_types` has 6 event types (out of 120) that do not
exist in `clean.sendem_dim_event_types` at all — these were previously
orphaned/unnamed and have since been fixed in the live pipeline.

Per explicit decision, the Sendem reporting views (`vw_sendem_trips_daily`,
`vw_sendem_events_daily`) **UNION both schemas** so Power BI gets the full
~6-month history, not just the live rolling window, with these rules:

1. **`staging` wins on any grain collision.** If the same
   `(date_key, group_id, site_id, asset_id[, event_type_id])` exists in both
   `clean` and `staging`, the `staging` row is kept and the `clean` row is
   discarded. Verified empirically: `sql/validation/validate_reporting_powerbi_views.sql`
   section 5 checks every known collision key and confirms zero rows resolve
   to `clean`.
2. **Dimensions prefer `staging`, fall back to `clean`.** Internal helper
   views (`vw_sendem_dim_assets_combined`, `vw_sendem_dim_sites_combined`,
   `vw_sendem_dim_event_types_combined`) pick the `staging` row for a given
   key when it exists, and only use the `clean` row when the key is missing
   from `staging` entirely. This preserves the 6 event-type fixes.
3. **Every Sendem reporting row carries `reporting_source`**: `staging_live`
   or `clean_historical`, so the blend is never hidden.
4. **Every Sendem reporting row carries `data_quality_status`**:
   `live_post_fix` (from `staging`), `historical_backfill` (from `clean`),
   or `pending_review` (see below).
5. **`pending_review` is real, not decorative.** 41 rows in
   `clean.sendem_fact_events_daily` reference 2 distinct `event_type_id`
   values (`3762264000645059186`, `8616181455636609494`) that have no name in
   *either* dimension table — these are true orphans predating the fix, with
   even the fact table's own denormalized `event_name`/`event_category`
   columns empty. `vw_sendem_events_daily` marks these `pending_review`
   instead of silently labelling them `historical_backfill`.

This blend logic is intentionally centralized in the three
`vw_sendem_dim_*_combined` helper views so it exists in exactly one place.

---

## Provider-specific views

### `reporting.vw_trackunit_daily_activity`

- **Grain**: one row per Trackunit machine per local (Africa/Harare) report date.
- **Source**: `staging.trackunit_daily_activity` LEFT JOIN `staging.trackunit_dim_assets` LEFT JOIN `staging.trackunit_location_enrichment` (V1, keyed on `report_date, asset_id`).
- **Provider value**: `Trackunit`

| Column | Meaning | Nullable? |
|---|---|---|
| `provider_asset_id` | Trackunit `asset_id` (UUID-shaped text) | No |
| `asset_code` | PIN (`externalReference` else `serialNumber`) | Yes |
| `asset_name` | Machine name/number, e.g. `"3846"` | Yes |
| `machine_serial_number` | Telematics device serial | Yes |
| `brand`, `machine_type`, `model` | Asset attributes | Yes |
| `asset_year`, `telematics_device_id`, `asset_onboarded_at` | From the asset dimension | Yes |
| `start_time_utc`/`stop_time_utc`/`start_time_local`/`stop_time_local` | Activity boundaries; **NULL is a valid zero-activity day**, not an error | Yes |
| `work_day_minutes`, `operating_minutes`, `active_driving_minutes` (+ `_hhmm` variants) | Complete, validated to the minute against manual Activity Reports | No (0 is valid) |
| `distance_km` | Odometer delta for the day | Yes (NULL when no distance points) |
| `operating_points`/`moving_points`/`distance_points` | Raw point counts backing each metric — use to sanity-check thin data | No |
| `zone_name_start`/`zone_name_stop` | Site History zone active at each boundary — **proven exact match** (see Location Enrichment V1 below) | Yes, until enriched |
| `last_known_start_location_timestamp_utc/_local`, `start_latitude`/`start_longitude`/`start_altitude` | Latest AEMP location point at or before the start boundary | Yes, until enriched |
| `last_known_stop_location_timestamp_utc/_local`, `stop_latitude`/`stop_longitude`/`stop_altitude` | Same, for the stop boundary | Yes, until enriched |
| `address_start`/`zip_start`/`city_start`/`country_start`, `address_stop`/`zip_stop`/`city_stop`/`country_stop` | **Always NULL in V1** — no source field exists yet. Never fabricated. | Yes, always |
| `location_enrichment_status` | `NOT_YET_ENRICHED` (job hasn't run for this row), `NOT_APPLICABLE_ZERO_ACTIVITY` (valid zero-activity day, nothing to enrich), `ENRICHED`, `PARTIAL`, or `NOT_FOUND` — see Location Enrichment V1 below | No |
| `location_enriched_at` | When the enrichment job last wrote this row | Yes, until enriched |
| `reporting_source` | Always `staging_live` (Trackunit has no historical/live split) | No |
| `data_quality_status` | Always `live` | No |
| `etl_last_updated` | `loaded_at` from staging | No |

**Complete**: work day / operating / active driving minutes (exact-to-the-minute validated), distance (±0.1km validated), zone name (exact match, see below).
**Pending**: street address/zip/city/country — column shells exist, values do not (V1 has no reverse-geocoding source).

#### Location Enrichment V1

Added by `jobs/sync_trackunit_location_enrichment.py`, a job completely
separate from the metric ETL (`jobs/sync_trackunit_daily_activity.py`) —
different table (`staging.trackunit_location_enrichment`), different
`etl.sync_runs` source_system (`trackunit_location`, not `trackunit`),
different raw tables (`raw.trackunit_aemp_locations`,
`raw.trackunit_site_history`, `raw.trackunit_sites`). Neither job calls the
other; a failure in one cannot fail the other's `sync_runs` row.

**Proven** in `Manitou/manitou_trackunit_exploration.ipynb` (machine 5986,
report date 2026-07-05) and reproduced by the production job on first run:

| | Expected (manual report) | Enrichment job |
|---|---|---|
| Start boundary location time | 2026-07-04 20:38 local | 2026-07-04 20:38:15 local |
| Stop boundary location time | 2026-07-05 17:01 local | 2026-07-05 17:01:17 local |
| Zone (start and stop) | `TPZ-Bay 13 - FL` | `TPZ-Bay 13 - FL` (exact) |

**How it works**: for each `staging.trackunit_daily_activity` row with real
start/stop boundaries, fetch AEMP's historical Locations time-series for a
48-hour lookback window ending at each boundary, take the latest point
`<=` the boundary (coordinates + altitude only — see below), then fetch
Site History for the same asset/window and resolve which site was active
at each boundary via a `[enteredAt, leftAt)` interval match (`leftAt IS NULL`
= still open). The site's *name* is a separate lookup (Site History returns
only an id), cached per run so a site is never looked up twice.

**Why address/zip/city/country are NULL and must stay that way in V1**:
AEMP's historical Locations time-series returns `{Latitude, Longitude,
Altitude, AltitudeUnits, datetime}` only — no address fields at all. The
*current*-location endpoint (`GET /v1/locations/:assetId`) does return a
`locationAddress`, but it reflects wherever the asset is *right now*, not
the historical report date — using it for a historical row would be wrong,
not just incomplete. Filling these in requires either a real reverse-
geocoding service (not yet introduced) or accepting the Site History zone
*name* as the location label instead of a street address — that decision is
deliberately left to a future step, not made here.

**Idempotency**: one row per `(report_date, asset_id)`, UPSERTed — rerunning
enrichment for the same date/machines updates the same rows, never
duplicates them. `raw.trackunit_aemp_locations` is keyed on
`(asset_id, location_timestamp_utc)`; the same point commonly appears in
both the start and stop boundary's 48h lookback windows (they can overlap)
and is de-duplicated before loading.

**Coverage as of this write-up**: V1 has only been run for machine 5986 /
2026-07-05 as a controlled test. Every other `staging.trackunit_daily_activity`
row shows `location_enrichment_status = NOT_YET_ENRICHED` until the job is
run for those report_dates/machines too — this is expected, not a defect
(see `sql/validation/validate_trackunit_location_enrichment.sql` check 3 for the
current backlog).

### `reporting.vw_sendem_trips_daily`

- **Grain**: one row per Sendem asset per report date per site/group (the real Sendem trip fact grain — an asset visiting two sites in one day produces two rows here, by design; see `vw_daily_activity_all` for the day-level rollup).
- **Source**: UNION of `clean.sendem_fact_trips_daily` + `staging.sendem_fact_trips_daily`, deduplicated as described above, LEFT JOIN combined asset/site dimensions.
- **Provider value**: `Sendem`

| Column | Meaning | Nullable? |
|---|---|---|
| `provider_asset_id` | Sendem `asset_id` (bigint) | No |
| `asset_code` | `fleet_number` | Yes |
| `asset_name` | `description` (Sendem's descriptive asset name) | Yes |
| `registration_number`, `make`, `model`, `asset_type` | Vehicle attributes | Yes |
| `group_id`, `site_id`, `site_name` | Org hierarchy | Yes |
| `vin_number`, `country`, `is_available`, `fuel_type`, `asset_year` | From the combined asset dimension | Yes |
| `trip_count`, `distance_km`, `fuel_used_litres`, `energy_used_kwh` | Daily trip metrics | No (0 valid) |
| `reporting_source`, `data_quality_status` | See blend rules above | No |
| `etl_last_updated` | `loaded_at` of the winning row | No |

**Complete**: trip count, distance. **Partial**: `fuel_used_litres`/`energy_used_kwh` are frequently NULL — genuinely not reported for most Sendem assets (not a defect).

### `reporting.vw_sendem_events_daily`

- **Grain**: one row per Sendem asset per report date per site/group per **event type** — finer than the trips grain.
- **Source**: UNION of `clean.sendem_fact_events_daily` + `staging.sendem_fact_events_daily`, deduplicated, LEFT JOIN combined event-type/site dimensions.
- **Provider value**: `Sendem`

| Column | Meaning | Nullable? |
|---|---|---|
| `event_type_id`, `event_name`, `event_category`, `metric_type`, `unit_type` | Event classification. Name/category prefer the combined dimension, fall back to the fact row's own denormalized copy | `event_name` etc. NULL only for the 2 known orphan event types |
| `event_occurrences` | Count of events that day | No |
| `min_event_value`/`max_event_value`/`total_event_value` | Value aggregates (meaning depends on `metric_type`, e.g. RPM) | Yes |
| `min_event_duration`/`max_event_duration`/`total_event_duration` | Duration aggregates, seconds | Yes |
| `reporting_source`, `data_quality_status` | See blend rules; `pending_review` = orphan event type, see above | No |

**Known limitation**: 41 rows (2 event types) are unnamed in both source dimensions. They still carry occurrence/value/duration metrics — only the descriptive label is missing.

### `reporting.vw_ezytrack_trips`

- **Grain**: one row per EzyTrack / Telematics Guru trip.
- **Source**: `staging.ezytrack_fact_trips` LEFT JOIN `staging.ezytrack_dim_assets`. Confirmed via `information_schema.tables` that no separate `v_trip_report` view exists in this database — these are the actual objects.
- **Provider value**: `EzyTrack`

| Column | Meaning | Nullable? |
|---|---|---|
| `department`, `project` | From the asset dimension | Yes |
| `driver_name`/`driver_code` | Trip's own driver if set, else the asset's currently-allocated driver | Yes |
| `start_geofence_name` | The trip's own start geofence | Yes |
| `asset_current_geofence_name` | The asset's *current* geofence at dimension-load time — **not the same concept as the trip's start geofence**, kept as a separate column rather than coalesced together | Yes |
| `duration_minutes`, `idle_minutes`, `stop_minutes`, `time_in_motion_minutes` | Derived from the `_seconds` source columns (also exposed) | No |
| `distance_km`, `start_odometer_km`, `end_odometer_km` | Distance/odometer | Yes |
| `runtime_start_hours`/`runtime_end_hours` | Engine hour-meter readings at trip start/end | Yes |
| `is_enabled`, `last_connected_utc` | Asset status | Yes |

**Complete**: all fields the user asked for are present. **Known gap**: EzyTrack has no `serial_number`/`device_serial` field anywhere in this database's staging tables.

### `reporting.vw_ezytrack_trip_report`

- **Grain**: one row per EzyTrack / Telematics Guru trip — identical to `vw_ezytrack_trips` (row counts verified equal: 393 = 393).
- **Source**: `reporting.vw_ezytrack_trips` **only**. No new joins, no dropped rows — this is a pure report-facing relabeling/formatting layer on top of the existing view, not a replacement for it. `vw_ezytrack_trips` is untouched and remains the source of truth for detailed/raw-shaped EzyTrack reporting.
- **Why it exists**: matches a specific exported trip-report layout column-for-column, including exact column names with spaces and units (e.g. `"Distance (km)"`) so a Power BI report can consume it directly without renaming.

| Report column | Source | Type | Notes |
|---|---|---|---|
| `"Start Date"` | `start_time_utc AT TIME ZONE 'Africa/Harare'` | `timestamp` | See timezone note below. Raw `start_time_utc` is also kept on this view for traceability. |
| `"Asset Code"` | `asset_code` | text | |
| `"Department"` | `department` | text | |
| `"Project"` | `project` | text | |
| `"Asset"` | `asset_name` | text | |
| `"Driver"` | `driver_name` | text | Same logic already used in `vw_ezytrack_trips` (trip's own driver, else the asset's allocated driver) — not recomputed here. |
| `"Start Location"` | `start_geofence_name` | text | The trip's own start geofence, not the asset's current geofence. |
| `"Time in Motion (hh:mm:ss)"` | `time_in_motion_seconds`, formatted | text | |
| `"Stop Time (hh:mm:ss)"` | `stop_time_seconds`, formatted | text | |
| `"Idle Time (hh:mm:ss)"` | `idle_time_seconds`, formatted | text | |
| `"Odometer Reading at Trip Start (km)"` | `start_odometer_km` | numeric | |
| `"Odometer Reading at Trip End (km)"` | `end_odometer_km` | numeric | |
| `"Distance (km)"` | `distance_km` | numeric | |
| `"Run Time Reading at Trip Start (hrs)"` | `runtime_start_hours` | numeric | |
| `"Run Time Reading at Trip End (hrs)"` | `runtime_end_hours` | numeric | |
| `"Duration (hh:mm:ss)"` | `duration_seconds`, formatted | text | |
| `"Estimated Fuel Consumption (l)"` | — | numeric | **Always `NULL`.** No fuel field exists anywhere in raw/staging EzyTrack data — never estimated or fabricated. |

**`hh:mm:ss` formatting rule**: total hours, not wrapped at 24 (e.g. 100000 seconds → `27:46:40`, not a modulo-24 time-of-day). `NULL` seconds → `NULL` string, never `"0:00:00"`. Verified against both synthetic edge cases (3661s → `1:01:01`, 0s → `0:00:00`, NULL → NULL) and the real data (max real trip duration is 13,766s, well under 24h, but the formula handles longer values correctly regardless).

**"Start Date" timezone note**: `staging.ezytrack_fact_trips` only ever carries UTC timestamps — there is no EzyTrack-native local timestamp anywhere in raw/staging, and (as noted above) EzyTrack has no configured timezone setting of its own in this codebase. `vw_daily_activity_all` already converts EzyTrack's `start_time_utc` using `AT TIME ZONE 'Africa/Harare'` for day-bucketing; this view applies that same existing conversion, consistently, rather than introducing a new assumption. If that assumption is ever revisited, both call sites need to change together.

---

## Conformed cross-provider views

### `reporting.vw_assets_all`

- **Grain**: one row per provider per `provider_asset_id` (an asset list, not activity).
- **Sources**: `staging.trackunit_dim_assets`, `reporting.vw_sendem_dim_assets_combined` + `vw_sendem_dim_sites_combined`, `staging.ezytrack_dim_assets`.

| Column | Trackunit | Sendem | EzyTrack |
|---|---|---|---|
| `asset_code` | PIN | `fleet_number` | `asset_code` |
| `fleet_no` | `name` (Trackunit has no separate fleet-number field — see below) | `fleet_number` | NULL (no equivalent field) |
| `serial_number` | Trackunit `serial_number` | `vin_number` (closest analog to a serial for a vehicle) | NULL (no field) |
| `device_serial` | Telematics device serial | NULL (no equivalent captured) | NULL (no field) |
| `department`/`project` | NULL (no concept) | NULL (no concept) | `department_name`/`project_name` |
| `site_or_geofence` | NULL (location enrichment pending) | Site name (via combined site dimension) | Current geofence name |
| `is_enabled` | NULL (dimension has no enabled flag) | `is_available` | `is_enabled` |
| `last_seen_at` | `MAX(stop_time_utc)` from the asset's own activity history — a real derived value, not fabricated | `MAX(activity_date)` from the asset's own trips history, cast to a timestamp at local midnight (no intraday time is available at this daily grain) | `last_connected_utc` (direct from source) |
| `synced_at` | Dimension row's `loaded_at` | Combined dimension row's `loaded_at` | Dimension row's `loaded_at` |

**Known provider differences** (do not "fix" these — they are real):
Trackunit has no fleet-number field distinct from its asset name; Sendem has
no telematics device serial; EzyTrack has no serial_number/device_serial/fleet_no
at all in this codebase's staging tables.

### `reporting.vw_daily_activity_all`

- **Grain**: one row per provider per `provider_asset_id` per local `activity_date`.
- **Why aggregation is needed**: none of the three providers share a native "one row per asset per day" shape once you look closely:
  - Trackunit's `staging.trackunit_daily_activity` **is already** that grain — used as-is (`data_grain = 'daily_native'`).
  - Sendem trips can have **multiple rows per asset per day** when an asset visits more than one site/group (confirmed against live data, e.g. asset `1260370989010284544` on `2026-03-04` has two site rows). These are summed to the day before being combined with the day-level event rollup.
  - Sendem events are natively per-event-type — summed to the day (`SUM(event_occurrences)`), taking the worst-case `data_quality_status` across event types so a `pending_review` or `historical_backfill` row is never hidden behind a `live_post_fix` one.
  - EzyTrack trips are per-trip — summed to the local (Africa/Harare) calendar day.
- **`data_grain` values**: `daily_native` (Trackunit), `daily_aggregated_from_trips_and_events` / `daily_aggregated_from_events` / `daily_aggregated_from_trips` (Sendem, depending on which of trips/events exist that day), `daily_aggregated_from_trips` (EzyTrack).
- **Column-by-provider support**:

| Column | Trackunit | Sendem | EzyTrack |
|---|---|---|---|
| `work_day_minutes` | ✓ | NULL (no shift-boundary concept) | NULL |
| `operating_minutes` | ✓ | NULL | NULL |
| `moving_minutes` | ✓ (`active_driving_minutes`) | NULL | ✓ (`time_in_motion_seconds` summed) |
| `idle_minutes`/`stop_minutes` | NULL | NULL | ✓ |
| `duration_minutes` | NULL (use `work_day_minutes` — the equivalent concept) | NULL | ✓ (sum of trip durations) |
| `trip_count` | NULL (Trackunit has no trip concept) | ✓ | ✓ |
| `event_count` | NULL | ✓ | NULL (no event source in this database) |
| `distance_km` | ✓ | ✓ | ✓ |
| `runtime_start_hours`/`runtime_end_hours` | NULL | NULL | ✓ |
| `department`/`project`/`site_name` | NULL/NULL/NULL (pending enrichment) | NULL/NULL/site name | ✓/✓/current geofence |

**Do not use this view for trip-level or event-type-level detail** — that
detail only exists in the provider-specific views. This view exists for
cross-provider daily comparisons (e.g. "which assets across all three fleets
had zero activity yesterday").

### `reporting.vw_provider_sync_health`

- **Grain**: one row per `(provider, job_name)` — the latest `etl.sync_runs` row for that job.
- **Source**: `etl.sync_runs` only (no join to `etl.sync_table_loads` was needed — the run-level row already carries `rows_fetched`/`rows_loaded`/`error_message`).
- **`health_status` logic**: `never_succeeded` (no SUCCESS row exists yet) → `failing` (most recent run is FAILED) → `stale` (>48h since last success) → `aging` (>6h since last success) → `healthy`. These 6h/48h thresholds are a starting point — tune them once real scheduling cadence (hourly Sendem/EzyTrack, daily Trackunit) is confirmed in production.

---

## Timezone handling

- Trackunit's report day is explicitly configured (`TRACKUNIT_TIMEZONE`,
  default `Africa/Harare`) and `staging.trackunit_daily_activity.report_date`
  is already the correct local calendar date — used as-is.
- Sendem's `date`/`date_key` columns are produced by the Sendem API itself;
  this codebase does not re-bucket them by timezone.
- EzyTrack has no per-provider timezone setting in this codebase. For the
  daily rollup in `vw_daily_activity_all`, EzyTrack trips are bucketed to a
  local calendar day using `Africa/Harare` (`start_time_utc AT TIME ZONE
  'Africa/Harare'`) — this is an **assumption** that the fleet operates in
  the same timezone as Trackunit's configured business timezone, not a
  configured EzyTrack setting. `vw_ezytrack_trips` itself exposes raw UTC
  timestamps only, so this assumption only affects the conformed daily view.

---

## Recommended refresh approach

- **Trackunit**: `python -m ge_data_platform.sources.trackunit.daily_activity` (no args)
  defaults to a rolling 2-day sync — schedule hourly. `reporting.vw_trackunit_daily_activity`
  reflects new rows immediately (plain view, no materialization).
- **Sendem**: `sendem_hourly_sync` job (existing, unchanged) keeps `staging.sendem_*`
  current; `clean.sendem_*` is static and never needs re-running.
- **EzyTrack**: `ezytrack_hourly_sync` job (existing, unchanged); note the
  live `vw_provider_sync_health` sample below shows this job currently
  `FAILED` on a GraphQL rate limit — worth checking before relying on
  EzyTrack freshness in Power BI.
- **Power BI dataset refresh**: since these are plain views (not materialized
  views), a Power BI scheduled refresh just needs to run after each
  provider's ETL job completes. Polling `reporting.vw_provider_sync_health`
  for `health_status = 'healthy'` before refreshing is a reasonable gate if
  Power BI's refresh can be scripted/conditional; otherwise a fixed schedule
  ~15-30 minutes after each hourly job is simplest.

---

## Sample Power BI SQL queries

Cross-provider daily activity for the last 30 days:
```sql
SELECT *
FROM reporting.vw_daily_activity_all
WHERE activity_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY activity_date DESC, provider, asset_code;
```

Asset list for a fleet picker/slicer:
```sql
SELECT provider, provider_asset_id, asset_code, asset_name, is_enabled
FROM reporting.vw_assets_all
ORDER BY provider, asset_code;
```

Sendem trip history including the historical backfill, flagged by source:
```sql
SELECT activity_date, asset_code, asset_name, trip_count, distance_km,
       reporting_source, data_quality_status
FROM reporting.vw_sendem_trips_daily
ORDER BY activity_date;
```

ETL freshness check (for a Power BI health tile):
```sql
SELECT provider, job_name, last_status, health_status, hours_since_success
FROM reporting.vw_provider_sync_health
ORDER BY provider;
```

Trackunit machines with genuine zero-activity days (not a data failure):
```sql
SELECT activity_date, asset_code, asset_name
FROM reporting.vw_trackunit_daily_activity
WHERE operating_points = 0 AND moving_points = 0
ORDER BY activity_date DESC;
```

EzyTrack trip report, ready to plug into Power BI with no column renaming:
```sql
SELECT *
FROM reporting.vw_ezytrack_trip_report
ORDER BY "Start Date" DESC;
```

Trackunit rows still waiting on location enrichment (the backlog):
```sql
SELECT activity_date, asset_code, asset_name, location_enrichment_status
FROM reporting.vw_trackunit_daily_activity
WHERE location_enrichment_status = 'NOT_YET_ENRICHED'
  AND work_day_minutes > 0
ORDER BY activity_date DESC;
```

---

## Known limitations

1. **Trackunit street address/zip/city/country are placeholder NULL columns**
   (`address_start`/`zip_start`/`city_start`/`country_start` and the `_stop`
   equivalents) — V1 location enrichment (below) resolved the *zone name*
   and coordinates exactly, but AEMP's historical Locations time-series has
   no address fields at all, and the current-location endpoint can't be
   used for historical rows (it reflects "now"). A real reverse-geocoding
   source, or a decision to use the zone name as the location label instead,
   is still pending.
1a. **Trackunit location enrichment (zone/coordinates) has only been run for
   machine 5986 / 2026-07-05 so far** (the controlled V1 test). Every other
   row shows `location_enrichment_status = 'NOT_YET_ENRICHED'` until the job
   (`jobs/sync_trackunit_location_enrichment.py`) is run for those dates/machines.
2. **41 Sendem event rows (2 event types)** in the historical backfill have
   no name in any dimension — `pending_review`, not silently hidden.
3. **EzyTrack has no `serial_number`/`device_serial`** field captured
   anywhere in this database.
4. **Trackunit has no separate fleet-number field** — `vw_assets_all.fleet_no`
   reuses the asset `name` for Trackunit rows.
5. **`clean.sendem_*` is a static historical snapshot.** If Sendem's actual
   history needs correcting or extending further back, that requires a
   deliberate backfill decision — this reporting layer only reads `clean`,
   it never writes to it.
6. **EzyTrack daily bucketing timezone is an assumption** (`Africa/Harare`),
   not a configured setting — see Timezone Handling above.
7. **`vw_provider_sync_health` thresholds (6h/48h)** are defaults, not
   tuned against confirmed production scheduling cadence.
8. **`powerbi_reader` role is not yet created** — `sql/legacy/telemetry_migrations/024_create_powerbi_reader_role.sql`
   exists for review but has deliberately not been run.
9. **`vw_ezytrack_trip_report` has no fuel data.** `"Estimated Fuel Consumption (l)"`
   is always `NULL` — no fuel field exists anywhere in raw/staging EzyTrack
   data. Its `"Start Date"` column shares the same `Africa/Harare` timezone
   assumption as item 6 above (same conversion, not a new one).
