# Glossary

Project-specific definitions. Where a term has a broader data-warehousing
meaning, this defines it as used *in this platform* specifically -- see
`docs/architecture/data-layers.md` for the full explanation behind the
short definitions below.

**Source system** -- Where data originates (Trackunit, Sendem, EzyTrack,
Evolution, FieldOps). Distinct from a *business domain*; see
`docs/architecture/platform-overview.md#source-system-vs-business-domain`.

**Raw** -- `raw_<source>`. Source-faithful, minimally-interpreted data,
provider identifiers preserved exactly. Answers "what did the source system
tell us?"

**Staging** -- `stg_<source>`. Cleaned, typed, normalized, deduplicated --
still single-source, still not GE's canonical truth.

**Core** -- The one schema (`core`) where data from more than one source is
conformed into a single business truth. Holds `dim_*`, `fact_*`, and
`*_source_map` objects.

**Dimension** -- A `core.dim_*` table: one row per real-world business
entity (asset, client, site, date), looked up by a stable key, changing
slowly. See `docs/warehouse/core-model.md`.

**Fact** -- A `core.fact_*` table: one row per business event or
measurement, referencing dimension keys rather than repeating dimension
attributes. See `docs/warehouse/core-model.md`.

**Source map** -- A `core.*_source_map` table (e.g. `asset_source_map`)
recording how each source system's own identifier for an entity relates to
that entity's conformed `core` key. See `docs/warehouse/source-mapping.md`.

**Mart** -- `mart_<domain>`. A business-domain-facing dataset built only on
`core`. Named for a business domain (Fleet, Finance, ...), never for a
source system.

**Reporting contract** -- A (planned) `reporting` schema object: a stable,
externally-depended-upon view built on top of a mart, distinct from the
mart itself because changing it is a breaking change for a consumer outside
this repository's control. See `docs/warehouse/reporting-layer.md`.

**Pipeline run** -- One execution of a source's ingestion job, recorded as
one row in `ops.pipeline_run` (platform) or `etl.sync_runs` (legacy,
currently the only one anything writes to), with lifecycle status
`STARTED` -> `SUCCESS` | `FAILED` | `ABANDONED`.

**Watermark** -- The cursor marking how far a source's ingestion has
successfully progressed (e.g. EzyTrack's last-successful-window-end), used
to resume incrementally rather than re-fetch everything. Today derived ad
hoc from `etl.sync_runs`; `ops.source_watermark` (PLANNED wiring) is meant
to formalize this. See `docs/sources/ezytrack.md#catch-up-behavior`.

**Reconciliation** -- A deliberately wider, cursor-ignoring re-fetch of a
source's data (e.g. EzyTrack's `--reconcile`, Trackunit's weekly 7-day
rolling job), used to catch anything a normal incremental run might have
missed, distinct from routine incremental syncing. See
`docs/operations/retries-and-recovery.md`.

**Idempotency** -- The property that re-running the same sync window
produces the same end state rather than duplicate or corrupted rows. Every
provider load in this platform is either an UPSERT keyed on a documented
conflict key, or (Evolution) a validated full replace -- see each source's
document under `docs/sources/`.

**Backfill** -- Loading a historical date range explicitly (as opposed to
the normal rolling/incremental window), e.g.
`trackunit.daily_activity --from-date ... --to-date ...`. See
`docs/operations/retries-and-recovery.md#manual-provider-recovery`.

**Data quality** -- In this platform, concretely: bounded automated
post-load checks (negative values, missing dimension joins), provider-level
quality flags (Trackunit's `COUNTER_RESET`), and manual full-history
validation packs. See `docs/operations/data-quality.md`.

**Dagster** -- The orchestration framework (`ge_data_platform.orchestration`)
that schedules, runs, and monitors every provider sync as a supervised
subprocess. See `docs/operations/pipeline-operations.md`.

**Legacy warehouse** -- `telemetry_warehouse`, the currently-live production
PostgreSQL database. Every job and Power BI report in production reads from
or writes to it today; nothing in this documentation set changes that.
