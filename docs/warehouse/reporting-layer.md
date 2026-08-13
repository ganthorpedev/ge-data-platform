# Reporting layer

**Status: PLANNED. The `reporting` schema does not exist in `ge_warehouse`
and is not created by this document or any related work.**

## Why we expect a future `reporting` schema

`mart_*` datasets are internally governed and can be reshaped as GE's
understanding of a business domain evolves -- that's expected, in-repo,
reviewed change. Once something is actually consumed outside this
repository's control (a Power BI report, an Excel workbook, another
application's extract), changing its shape becomes a breaking change for
someone who isn't in this codebase to negotiate with. `reporting` exists to
draw that line explicitly, rather than let every mart become a de facto
frozen contract the moment the first report is built on it.

## Key principle

```text
mart_*     = internally governed business data products
reporting  = stable contracts for consumers
```

`reporting` should sit on top of `mart_*` only -- ideally as plain views --
and must not become a second major transformation layer. If a `reporting`
view seems to need real transformation logic that isn't already in the mart
beneath it, that logic belongs in the mart, not in `reporting`. See
`docs/architecture/architecture-decisions.md#adr-008` for the full
reasoning.

## Potential consumers

- Power BI (the only actual consumer of the legacy equivalent today)
- Excel workbooks
- Internal applications
- Controlled extracts for external parties

## Why BI should not normally connect directly to raw/staging

`raw_*`/`stg_*` are source-shaped and source-specific -- a Power BI report
built directly against `raw_trackunit.aemp_operating_hour` would break the
moment Trackunit's API shape changes, expose provider-specific identifiers
and quirks (see `docs/sources/trackunit.md`) that have no business meaning,
and bypass every conformance/quality rule `core` and the marts exist to
apply. This mirrors the legacy database's own existing rule -- see
`docs/powerbi_reporting_data_dictionary.md`'s "The one rule": Power BI must
only ever connect to `reporting`, never to `raw`/`staging`/`etl`/`warehouse`
directly.

## The current, real equivalent

`telemetry_warehouse.reporting` is the only reporting-facing layer that
actually exists and is actually used today -- fully documented in
`docs/powerbi_reporting_data_dictionary.md` (LEGACY status: it describes the
live production Power BI layer, not this platform's design). Read that
document for what a working reporting layer against this platform's sources
looks like in practice, including its own internal lesson: some of its
views (the `staging` ∪ `clean` UNION/dedupe logic in particular) mix
transformation into what should be a stable contract -- exactly the pattern
`ge_warehouse`'s future `reporting` schema is designed to avoid by pushing
that logic into marts instead.

## Not created by this task

Per explicit instruction, no `reporting` schema, view, or migration is
created as part of this documentation effort. This document describes
intent only.
