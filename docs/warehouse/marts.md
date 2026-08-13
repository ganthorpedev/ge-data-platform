# Marts

**Status: PLANNED. All six schemas exist (`sql/migrations/001_create_platform_schemas.sql`); none contains a single table or view yet.**

See `docs/architecture/data-layers.md#marts----mart_domain` for what a mart
is and the rule that marts are built only on `core`, never directly on
`raw_*`/`stg_*`. This document is about the six business-domain boundaries
themselves.

## `mart_fleet`

Fleet activity and asset utilization -- the domain fed by Trackunit, Sendem,
and EzyTrack today (see `docs/architecture/platform-overview.md`'s
source-to-domain table). The most obviously-populated mart once
`core.dim_asset` and `core.fact_asset_daily_activity` exist, since three
sources' worth of legacy reporting (`reporting.vw_daily_activity_all`,
`vw_assets_all`, and the provider-specific trip/event views -- see
`docs/powerbi_reporting_data_dictionary.md`) already answers fleet
questions today, just without a conformed `core` underneath it.

*Potential future dataset* (illustrative only, not committed): a
`mart_fleet.asset_utilization` view combining `core.fact_asset_daily_activity`
with `core.dim_asset` for a single utilization-by-asset-by-day dataset.

## `mart_finance`

Financial data -- currently sourced from Evolution project reports. Not
"the Evolution mart" (see `docs/architecture/platform-overview.md`): if a
second financial source is ever added, it feeds this same mart, not a
separate one.

## `mart_operations`

Cross-cutting operational datasets not specific to fleet or finance --
likely candidate domain for FieldOps once that source exists (PLANNED, see
`docs/sources/fieldops.md`).

## `mart_maintenance`

Maintenance/breakdown/job-card data. `core.fact_breakdown` and
`core.fact_job_card` (both PLANNED, see `docs/warehouse/core-model.md`)
are the most likely facts this mart would eventually build on -- no source
for either currently exists in this repository.

## `mart_procurement`

Purchasing data. `core.fact_purchase` (PLANNED) is the likely underlying
fact -- no source currently exists.

## `mart_commercial`

Commercial/sales-facing data. Evolution's project-report `business_unit`
classification already distinguishes commercial categories (Commercial
Whole Goods, Commercial Parts and Services, Commercial Training -- see
`docs/sources/evolution.md`), which is the most likely eventual input, once
`core` exists to conform it through.

## What this document deliberately does not do

It does not invent table names, column lists, or grain definitions for any
mart object -- none of that can be decided honestly before the `core`
facts/dimensions underneath them exist (see `docs/warehouse/core-model.md`).
Anything above marked "potential future dataset" is exactly that: a
plausible direction, not a design.
