# Architecture decisions

Lightweight ADR-style record of the decisions behind this platform's shape.
Each entry is deliberately short -- the detailed reasoning lives in the
architecture docs these entries link to; this page is the "why," indexed.

---

### ADR-001: One GE's warehouse rather than separate telemetry/accounts/ops warehouses

**Status:** Accepted

**Context:** The legacy `telemetry_warehouse` grew around three telemetry
vendors; Accounts/Evolution data was added into the same generic
`raw`/`staging`/`etl` schemas later, with no real schema separation from the
telemetry sources. A second, fully separate warehouse for Evolution (and
later FieldOps) was considered.

**Decision:** Build one warehouse, `ge_warehouse`, that all current and
future source systems feed into, conformed together in a single `core`
layer.

**Consequences:** Cross-source business questions (fleet activity vs.
project cost, for example) can be answered inside the warehouse instead of
by hand in Power BI. The cost is that `core`'s design must anticipate
multiple, structurally different source systems (API telemetry, SQL Server
accounting extracts, and eventually FieldOps) from the start rather than
being scoped to one domain. See `docs/architecture/platform-overview.md`.

---

### ADR-002: Source-system schemas for raw/staging

**Status:** Accepted

**Context:** The legacy database shares one `raw` schema and one `staging`
schema across every source. That means the only way to tell which tables
belong to which provider is by table-name prefix convention
(`sendem_assets`, `trackunit_assets`), enforced by nothing.

**Decision:** One schema per source at both the raw and staging layers:
`raw_<source>`, `stg_<source>`.

**Consequences:** Adding a new source (FieldOps) or removing one never
touches another source's schema. Table names inside each schema can drop
the redundant source prefix (`raw_trackunit.asset`, not
`raw_trackunit.trackunit_asset`). See `docs/architecture/data-layers.md`.

---

### ADR-003: Provider-neutral `core` layer

**Status:** Accepted (design); not yet built beyond `core.dim_date`

**Context:** Without a conformed layer, every mart or report that needs
"the asset" regardless of source has to re-derive that conformance itself.
The legacy `reporting.vw_assets_all` view is exactly this problem solved
once, ad hoc, at the reporting layer -- it works, but it means conformance
logic lives in a view instead of a governed dimension.

**Decision:** `core` holds conformed `dim_*`/`fact_*` objects, keyed
independently of any one source's identifiers, with `*_source_map` tables
recording how each source's identifier maps to the conformed key.

**Consequences:** `core.dim_asset` cannot be built casually -- it requires
deliberately analyzing the identifier shapes across Trackunit (UUID-like
TEXT), Sendem (BIGINT), EzyTrack (BIGINT), and Evolution (free-text fleet
number) first. That analysis is explicitly deferred, not skipped -- see
`docs/warehouse/source-mapping.md` and `docs/warehouse/core-model.md`.

---

### ADR-004: Business-domain marts

**Status:** Accepted (design); no mart contains an object yet

**Context:** Business functions (fleet management, finance, procurement)
need datasets shaped around their own questions, not around any one
source's schema.

**Decision:** `mart_<domain>` schemas, one per business domain, built only
on `core` -- never directly on `raw_*`/`stg_*`.

**Consequences:** A mart can combine multiple `core` facts freely since
`core` is already conformed; it must not reach past `core` back down to a
source-specific table, or it silently reintroduces the very coupling `core`
exists to remove. See `docs/warehouse/marts.md`.

---

### ADR-005: Operational metadata under `ops` rather than `etl`

**Status:** Accepted

**Context:** The legacy `etl` schema name describes a specific technology
choice (an "ETL" job), not the concept it actually holds (pipeline
execution history). It also predates the ops-metadata expansion this
platform wants (watermarks, persisted data-quality results, persisted
alerts) -- none of which fit naturally under a name literally meaning
"extract-transform-load."

**Decision:** `ops`, holding `pipeline_run`, `table_load`,
`source_watermark`, `data_quality_result`, `alert_event`, and
`schema_version`. `etl.sync_runs` -> `ops.pipeline_run` and
`etl.sync_table_loads` -> `ops.table_load` are the only renamed objects;
every column is preserved except two renames forced by the table rename
itself, plus one deliberate fix (`provider` -> `source_system`, correcting a
genuine pre-existing inconsistency between the two legacy tables). See
`docs/migration/legacy-to-platform-migration.md` for the full column
mapping.

**Consequences:** `ops` can grow (watermarks, persisted data quality,
persisted alerts) without the name becoming misleading. None of the new
tables are wired into application code yet -- this is a structural decision
made ahead of the ingestion migration, not a claim that ops observability
has already improved.

---

### ADR-006: Standalone `ge-data-platform` repository and `src/` Python package

**Status:** Accepted

**Context:** The legacy `telemetry_etl` code lived as a flat set of modules
(`connectors/`, `jobs/`, `transforms/`, `loaders/`) inside a larger,
dirty-working-tree monorepo, with no `pyproject.toml`, no installable
package, and import paths that depended on the current working directory.

**Decision:** A standalone repository (`ge-data-platform`), a real
installable package (`ge_data_platform`) under `src/`, with an editable
install (`pip install -e . --no-deps`) so imports resolve regardless of
working directory -- required because Dagster launches provider subprocesses
with the repository root as their working directory.

**Consequences:** No `sys.path` manipulation anywhere in the codebase. Tests
and Dagster both import the same installed package. The full mechanical
mapping from every old flat module to its new location is recorded in this
repository's git history (the restructure commits) rather than repeated
here.

---

### ADR-007: Parallel migration/cutover instead of destructive in-place migration

**Status:** Accepted

**Context:** `telemetry_warehouse` is live production. Renaming it,
altering its schemas in place, or migrating source-by-source directly
inside it would mean the platform's biggest schema change happens with no
safe rollback and no side-by-side validation window.

**Decision:** Build `ge_warehouse` alongside `telemetry_warehouse` --
same Postgres server, different database -- and migrate source by source
into it, validating each against the legacy equivalent before cutting any
consumer over. `telemetry_warehouse` is explicitly preserved as a read-only
reference throughout, not archived or dropped until every consumer has
cut over.

**Consequences:** Both databases exist simultaneously for the duration of
the migration, which costs disk and requires discipline to keep them
straight (see `docs/security/secrets-and-access.md`'s guidance on
`POSTGRES_DB` vs. `GE_WAREHOUSE_DB`). The payoff is that a migration mistake
in `ge_warehouse` never risks the production data or reports still served
from `telemetry_warehouse`. See
`docs/migration/legacy-to-platform-migration.md`.

---

### ADR-008: Future `reporting` schema as a consumer contract layer, not a transformation layer

**Status:** Accepted (design); schema does not exist yet

**Context:** The legacy `telemetry_warehouse.reporting` schema already mixes
two concerns in places: some views are genuinely just a stable Power
BI-facing contract over clean data, while others (the `staging` ∪ `clean`
UNION/dedupe views, for instance) contain real transformation logic that
arguably belongs further upstream.

**Decision:** When `reporting` is built for `ge_warehouse`, it sits on top
of `mart_*` only, ideally as plain views, and is treated as a slower-moving,
externally-depended-upon contract -- distinct from marts, which can be
reshaped internally without that constraint.

**Consequences:** Any transformation logic a future `reporting` view seems
to need is a signal that the logic belongs in the mart underneath it
instead. This ADR does not create the schema -- see
`docs/warehouse/reporting-layer.md`.
