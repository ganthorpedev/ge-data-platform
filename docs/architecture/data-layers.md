# Data layers

This document defines what each layer of `ge_warehouse` means, precisely,
and what question it answers. Every other document in this repository uses
these definitions without re-explaining them -- if a term here and a term
elsewhere disagree, this document is authoritative.

For *which schemas currently exist and what's inside them*, see
`docs/architecture/database-architecture.md`. This document is about
semantics, not inventory.

## RAW -- `raw_<source>`

**Status: IMPLEMENTED (schemas exist; no tables yet -- see below).**

> What did the source system tell us?

Source-faithful, persisted as close to the provider's own shape as the
platform can reasonably store it:

- Provider-native identifiers are preserved exactly (a Trackunit asset id
  stays the UUID-shaped string Trackunit assigned it; a Sendem asset id
  stays the integer Sendem assigned it). No renaming, no reinterpretation.
- Minimal business interpretation -- raw does not decide what counts as a
  "trip" versus a "breakdown," it stores what the source called a trip.
- Historical/replay capability where the source allows it: raw is the
  layer you'd reload staging and core from if a downstream bug needed a
  clean re-derivation, for exactly as far back as raw's own retention goes
  (see each source doc in `docs/sources/` for what is and isn't
  re-fetchable from the provider itself).

One schema per source (`raw_trackunit`, `raw_sendem`, `raw_ezytrack`,
`raw_evolution`, `raw_fieldops`). Raw is never blended across sources -- a
`raw_sendem` table only ever holds Sendem data, and no `raw_*` schema ever
joins to another `raw_*` schema.

**Example objects (illustrative -- not yet built in `ge_warehouse`):**

```text
raw_trackunit.asset
raw_trackunit.aemp_operating_hour
raw_evolution.project_report
```

The equivalent, currently-real objects live in `telemetry_warehouse.raw`
(LEGACY) -- see `docs/architecture/database-architecture.md` for that
inventory and `docs/migration/legacy-to-platform-migration.md` for the exact
current-object -> future-object mapping.

## STAGING -- `stg_<source>`

**Status: IMPLEMENTED (schemas exist; no tables yet.)**

> What does this source's data mean after technical normalization?

Cleaned, typed, normalized, and deduplicated -- but still entirely
single-source:

- Column types are corrected (e.g. a source's YYYYMMDD integer date becomes
  an actual date where that's safe to do; see each source's own quirks in
  `docs/sources/`).
- Duplicate records from pagination/retry overlap are removed.
- Provider-specific quality rules are applied (Trackunit's cumulative-counter
  reset detection is a staging-layer concern -- see
  `docs/operations/data-quality.md`).

**Staging is still not GE corporate truth.** It answers "what does Trackunit
say happened," not "what happened, from GE's perspective, once Trackunit,
Sendem, and EzyTrack all had input." That question belongs to `core`.

One schema per source (`stg_trackunit`, `stg_sendem`, `stg_ezytrack`,
`stg_evolution`, `stg_fieldops`), mirroring `raw_<source>`.

## CORE -- `core`

**Status: IMPLEMENTED for `core.dim_date` only. Everything else PLANNED.**

> What does GE consider the canonical business truth?

The one schema where data from more than one source is conformed into a
single row. Provider identity generally disappears here unless the concept
is genuinely provider-specific (there is no reason to conform "Trackunit
machine type" and "Sendem asset type" into one column if they don't actually
mean the same thing).

Three kinds of object live in `core`:

- **`dim_*` -- conformed dimensions.** One row per real-world business
  entity (an asset, a client, a site), regardless of which source system(s)
  know about it. `core.dim_date` (IMPLEMENTED) is the only one that exists
  today -- it's genuinely source-independent, so it didn't have to wait for
  the rest of `core`'s design. `core.dim_asset` and the rest are PLANNED and
  explicitly blocked on a deliberate cross-source identifier design (see
  `docs/warehouse/source-mapping.md`) -- they are not being guessed at
  ahead of that analysis.
- **`fact_*` -- conformed facts.** One row per business event or
  measurement, keyed against `dim_*` rows rather than source identifiers.
  All PLANNED.
- **`*_source_map` -- source identifier maps.** How a `core` dimension's key
  relates back to each source system's own identifier for the same
  real-world entity. PLANNED -- see `docs/warehouse/source-mapping.md`.

See `docs/warehouse/core-model.md` for the current and planned `core` object
list.

## MARTS -- `mart_<domain>`

**Status: IMPLEMENTED (all six schemas exist; no tables yet.)**

> What dataset does this business function need?

Business-domain-facing datasets built **only on `core`** -- never directly
on `raw_*`/`stg_*`. A mart can, and often will, combine multiple `core`
facts/dimensions the way a specific business function needs them combined;
that combination work belongs in the mart, not duplicated into `core` itself
or left to Power BI.

Six domain schemas exist today, empty: `mart_fleet`, `mart_finance`,
`mart_operations`, `mart_maintenance`, `mart_procurement`,
`mart_commercial`. See `docs/warehouse/marts.md` for what each domain is
expected to eventually hold.

## REPORTING -- `reporting` (schema does not exist yet)

**Status: PLANNED. Not created by this documentation task, and not created
by any work so far.**

> What has GE published as a stable, breaking-change-controlled contract for
> external consumers?

The distinction that matters:

```text
mart_*     = internally governed business data products
reporting  = stable, consumer-facing contracts built on top of marts
```

A mart can be reshaped as the business's understanding of a domain evolves --
that's expected, internal, in-repo-reviewed change. A `reporting` object, once
published, is something Power BI reports, Excel workbooks, or another
application depend on; changing its shape is a breaking change to something
outside this repository's control. Keeping the two separate means marts stay
free to evolve while `reporting` stays a deliberately narrower, slower-moving
surface.

`reporting` should sit on top of `mart_*` (view-only, ideally) and should
**not** become a second major transformation layer -- if a `reporting` view
needs real transformation logic that isn't already in a mart, that logic
belongs in the mart, not smuggled into `reporting`.

The legacy, currently-live equivalent is `telemetry_warehouse.reporting`,
documented in full in `docs/powerbi_reporting_data_dictionary.md` (LEGACY,
still the only Power BI-facing layer that actually exists today). See
`docs/warehouse/reporting-layer.md`.

## OPS -- `ops`

**Status: IMPLEMENTED (structure only).**

`ops` is not part of the data path in the diagram above -- it's a control
plane that sits alongside every layer, tracking platform execution rather
than business facts: pipeline runs, per-table load outcomes, source
watermarks, data-quality results, alert events, and applied-migration
history. See `docs/warehouse/reporting-layer.md`'s sibling operational
counterpart, `docs/operations/pipeline-operations.md`, for how it's used,
and `docs/architecture/database-architecture.md#ops` for its current tables.

The legacy equivalent, `telemetry_warehouse.etl` (`sync_runs` /
`sync_table_loads`), is what every job actually writes to today -- `ops` is
not wired into any running job yet. See
`docs/migration/legacy-to-platform-migration.md` for the exact column
mapping between the two.
