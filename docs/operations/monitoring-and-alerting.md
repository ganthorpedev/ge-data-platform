# Monitoring and alerting

**Status: IMPLEMENTED**, against `telemetry_warehouse`/`etl.sync_runs`.
`ops.alert_event` exists structurally in `ge_warehouse` but nothing
persists alerts to it yet -- alert delivery today is webhook-only, not
database-backed.

Code: `ge_data_platform.orchestration.alerts` (payload/cooldown/freshness
logic) and `ge_data_platform.orchestration.monitoring` (the two Dagster
sensors and the housekeeping op).

## `telemetry_run_failure_sensor`

Event-driven, `default_status=RUNNING`. Fires once per Dagster run that
transitions to failure, using Dagster's own persisted run-status cursor for
deduplication (so a restart doesn't re-alert on old failures). Looks up the
failing job's provider and last successful sync
(`_provider_and_source_for_dagster_job`, `get_last_successful_run`) so the
alert includes both the failure and recent history -- a lookup failure
(e.g. Postgres itself is down) never suppresses the alert, since that would
hide exactly the kind of failure most worth alerting on.

## `telemetry_provider_freshness_sensor`

Runs at least every 15 minutes (`FRESHNESS_CHECK_INTERVAL_SECONDS`),
`default_status=RUNNING`. For each configured provider
(`SENDEM_MAX_SUCCESS_AGE_HOURS`=12, `EZYTRACK_MAX_SUCCESS_AGE_HOURS`=12,
`TRACKUNIT_MAX_SUCCESS_AGE_HOURS`=30 by default), checks how long it's been
since the last `SUCCESS` row in `etl.sync_runs` and alerts if that exceeds
the threshold. Alert state (per-provider last-alerted timestamp) is stored
in the sensor's durable Dagster cursor (JSON-encoded,
`encode_freshness_cursor`/`decode_freshness_cursor`), not in a database
table -- a malformed or missing cursor resets safely rather than erroring.

**Cooldown**: once alerted, a provider will not alert again for
`TELEMETRY_ALERT_COOLDOWN_MINUTES` (default 360). The cooldown clock
advances on log-only and failed-delivery attempts too, specifically so a
disabled or broken webhook endpoint cannot produce a tight repeat-alert
loop every 15 minutes. Recovery (the provider becomes fresh again) clears
its cooldown immediately, so the next real staleness gets a fresh alert
rather than waiting out an old cooldown window.

## Alert delivery

Generic JSON webhook (`send_operational_alert` -> `TELEMETRY_ALERT_WEBHOOK_URL`,
10s timeout), used identically for all three alert types below -- no
provider-specific delivery mechanism. If `TELEMETRY_ALERTS_ENABLED=false` or
the webhook URL is empty, the complete alert payload is logged instead and
the calling job/sensor continues normally: **alert delivery trouble is
always an observability concern, never a reason to fail or mask the
underlying provider result.**

Alert types (`event_type` in the payload):

| Type | Source | Trigger |
|---|---|---|
| `run_failure` | `telemetry_run_failure_sensor` | Any Dagster run failure |
| `freshness` | `telemetry_provider_freshness_sensor` | Last successful sync older than the provider's threshold |
| `abandoned` | `stale_started_run_cleanup` op | A `STARTED` run just got marked `ABANDONED` (see `docs/operations/retries-and-recovery.md#stale-run-cleanup`) |

Every payload includes: event type, provider, job name, run id (where
applicable), failure message, event timestamp, last known success time, and
a `dedupe_key`. An `ABANDONED` alert is naturally one-shot -- it only fires
as the row transitions out of `STARTED`, not on every subsequent
housekeeping pass.

## What's not implemented

No alert is persisted to a database table today (`ops.alert_event` exists
but nothing writes to it -- see
`docs/operations/pipeline-operations.md#ops-metadata-wiring-status`). The
only durable record of an alert having fired is the Dagster sensor cursor
(for cooldown purposes) and whatever the receiving webhook does with it.
