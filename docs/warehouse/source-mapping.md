# Source mapping

**Status: PLANNED. No source-map table exists in `ge_warehouse` yet.**

## The problem

Every source system identifies the same real-world asset differently:

```text
asset_key | source_system | source_id
412       | trackunit     | MAN00000V00883846   (PIN, TEXT)
412       | fieldops      | <uuid>               (planned source)
412       | ezytrack      | <provider id>        (BIGINT)
```

`asset_key` (`412` above) is a GE-conformed identifier that doesn't exist in
any source system -- it's assigned by the platform, once, to represent "this
one physical asset" regardless of how many source systems know about it and
under what identifier.

**Power BI, and every mart, should deal with the GE asset, not provider
identifiers.** A fleet utilization report joining Trackunit activity to
Sendem activity for the same physical machine should never need to know
that one system calls it `MAN00000V00883846` and the other calls it
`4419281`. That translation belongs in one governed place -- the source map
-- not repeated in every report or mart query.

## Chosen pattern (decided, not yet built)

One shared, reusable mapping table per conformed entity, with a
`source_system` discriminator column:

```sql
core.asset_source_map   (asset_key, source_system, source_id)
core.client_source_map  (client_key, source_system, source_id)
core.site_source_map    (site_key, source_system, source_id)
core.project_source_map (project_key, source_system, source_id)
```

**Not** a separate table per source (`trackunit_asset_map`,
`sendem_asset_map`, ...) -- that would duplicate the same three-column shape
five times per entity and make "give me every source's id for this asset"
a five-way UNION instead of one filtered query.

**Not** a single generic EAV-style table spanning every entity type
(`entity_source_map(entity_type, entity_key, source_system, source_id)`) --
that was considered and rejected as over-generalized: it loses the foreign
key relationship to each specific `dim_*` table's actual primary key, and
makes "is this asset_key valid" a query-time check instead of a
database-enforced constraint.

## Why this isn't built yet

`core.asset_source_map.asset_key` would need to reference
`core.dim_asset.asset_key`, which doesn't exist (see
`docs/warehouse/core-model.md`). Building the map before the dimension it
maps into exists would leave it with nothing to key against and no way to
be validated by a foreign key constraint. The pattern is decided here
precisely so the next phase doesn't need to re-litigate it -- only build
`core.dim_asset` first, then this table against it.

## What "designing asset conformance deliberately" means, concretely

Before `core.asset_source_map` (or `core.dim_asset`) is built, the following
needs an actual answer, grounded in the real identifier shapes already
documented per source (see `docs/sources/`):

- Trackunit: asset `id` is a UUID-shaped string; PIN/serial
  (`externalReference`/`serialNumber`) is alphanumeric, e.g.
  `MAN00000V00883846`.
- Sendem: `asset_id` is a large signed `BIGINT`.
- EzyTrack: `assetId` is a `BIGINT`.
- Evolution: no asset identifier at all today -- `fleet_number` is a free-text
  field on project-report rows, not a governed identifier.
- FieldOps: unknown -- no source exists yet (`docs/sources/fieldops.md`).

None of these can be assumed to be the same value, or even the same type,
for "the same" physical asset across two systems -- confirming that
requires either an existing cross-reference (if GE already tracks a
fleet/asset number that ties these together operationally) or a matching
exercise against real data. This is exactly the work explicitly deferred to
the next phase.
