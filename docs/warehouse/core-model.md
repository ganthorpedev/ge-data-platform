# Core model

See `docs/architecture/data-layers.md#core----core` for what `core` means
and the question it answers. This document is narrower: what's actually in
`core` today, and what's expected to be added, without turning into a
generic data-warehousing textbook.

## Implemented

| Object | Kind | Status |
|---|---|---|
| `core.dim_date` | dimension | IMPLEMENTED -- see `docs/warehouse/dim-date.md` |

## Planned dimensions

Conformed, cross-source entities. None of these exist yet. Each is blocked
on the same prerequisite: a deliberate identifier-conformance design (see
`docs/warehouse/source-mapping.md`), not a schema/DDL decision.

```text
core.dim_asset
core.dim_client
core.dim_site
core.dim_client_site
core.dim_project
core.dim_supplier
core.dim_employee
```

`core.dim_asset` is explicitly the next one, and explicitly gated: it must
not be built by guessing a conformance key across Trackunit (UUID-shaped
TEXT ids), Sendem (BIGINT ids), EzyTrack (BIGINT ids), Evolution (free-text
fleet numbers), and FieldOps (unknown -- no source exists yet). That
analysis is deferred to the next phase, not skipped.

## Planned facts

Conformed business events/measurements, keyed against `dim_*` rows. None
exist yet:

```text
core.fact_asset_daily_activity
core.fact_trip
core.fact_breakdown
core.fact_job_card
core.fact_project_financial
core.fact_invoice
core.fact_purchase
core.fact_gl_transaction
```

`core.fact_asset_daily_activity` is the most obviously-scoped first
candidate once `core.dim_asset` exists -- it's a direct conformance of the
three sources' existing daily-activity concepts (Trackunit's
`staging.trackunit_daily_activity`, Sendem's
`staging.sendem_fact_trips_daily`/`_events_daily`, EzyTrack's
`staging.ezytrack_fact_trips`), which is exactly the join the legacy
`reporting.vw_daily_activity_all` view already does ad hoc, per-query,
today (see `docs/powerbi_reporting_data_dictionary.md`). Building the fact
table means doing that conformance once, governed, instead of inside a
view.

## Dimensions vs. facts, in this platform specifically

A **dimension** answers "who/what/where/when" -- an asset, a client, a
site, a date. It changes slowly (an asset's make/model doesn't change every
day) and is looked up by a stable key.

A **fact** answers "what happened, how much, when" -- a trip, a day of
activity, an invoice line. It changes constantly (new rows arrive on every
load) and references dimension keys rather than repeating dimension
attributes inline.

Concretely in this platform: `core.dim_asset` will hold one row per
conformed asset, however many source systems know about it (see the source
map pattern in `docs/warehouse/source-mapping.md`); `core.fact_asset_daily_activity`
will hold one row per asset per day per activity measurement, referencing
`core.dim_asset.asset_key` and `core.dim_date.date_key` rather than
repeating "Trackunit asset abc-123, brand=Manitou" on every activity row.

## What this document deliberately does not do

It does not propose column-level schemas for the planned objects above --
that's premature until the source-map/conformance design is settled, and
would risk becoming exactly the kind of "planned architecture presented as
fact" this documentation effort is required to avoid. See
`docs/migration/legacy-to-platform-migration.md` for the phased plan that
leads here.
