# Migrations

**Two entirely separate, independent migration sequences exist in this
repository.** Confusing them is the single most likely migration mistake --
read this document before touching either.

## `sql/migrations/` -- the new GE migration sequence

Targets `ge_warehouse` only. Starts fresh at `001`; four files exist today:

| File | Creates |
|---|---|
| `001_create_platform_schemas.sql` | All 18 platform schemas, plus `ops.schema_version` |
| `002_create_ops_metadata.sql` | `ops.pipeline_run`, `ops.table_load`, `ops.source_watermark`, `ops.data_quality_result`, `ops.alert_event` |
| `003_create_platform_roles.sql` | `ge_platform_admin`, `ge_etl`, `ge_bi_readonly` roles and grants |
| `004_create_core_dim_date.sql` | `core.dim_date`, seeded 2015-01-01..2035-12-31 |

**Naming**: `NNN_verb_object.sql`, exactly 3 zero-padded digits. Enforced
mechanically, not just by convention --
`ge_data_platform.common.migrations.discover_migrations` raises on a
non-3-digit prefix or a duplicate prefix (the exact class of collision that
motivated *renaming*, not renumbering, two old telemetry validation files --
see `docs/architecture/naming-conventions.md`'s legacy-naming table).

**Idempotency**: every file uses `CREATE SCHEMA/TABLE IF NOT EXISTS`, `DO $$
... IF NOT EXISTS ... $$` guards for roles, and `INSERT ... ON CONFLICT DO
NOTHING` for seed data (`core.dim_date`) and self-registration. Safe to run
against an empty database or one that already has some/all migrations
applied.

**Transactions**: each file is one `BEGIN...COMMIT` block -- a failure
partway through leaves the database exactly as it was before the file ran.

**Tracking**: the last statement in every migration inserts its own
filename into `ops.schema_version` (`ON CONFLICT DO NOTHING`) -- "what has
been applied" is always a live query against `ge_warehouse`, not tribal
knowledge or a deployment log.

**Applying them**:

```powershell
python -m scripts.setup_ge_warehouse --all       # create db + migrate + validate
python -m scripts.setup_ge_warehouse --create-db  # or step by step
python -m scripts.setup_ge_warehouse --migrate
python -m scripts.setup_ge_warehouse --validate
```

`--create-db` connects only to the server's `postgres` maintenance database
to issue `CREATE DATABASE` if `ge_warehouse` doesn't exist yet (`CREATE
DATABASE` cannot run inside a migration's own transaction). `--validate`
runs `sql/validation/validate_ge_warehouse_baseline.sql` and prints every
`PASS`/`FAIL` check -- all 15 currently pass against the local database.

## `sql/legacy/telemetry_migrations/` -- frozen, historical

The original, already-applied `telemetry_warehouse` migrations (numbered
`001`-`029`, plus a standalone `sendem_tables.sql`), moved here verbatim
from the old flat `sql/migrations/` layout, git history preserved. **Do not
edit these for content** -- they are the historical record of exactly what
was run against the live production database.

**These are not "the same 001."** `sql/legacy/telemetry_migrations/001_create_sendem_schema.sql`
and `sql/migrations/001_create_platform_schemas.sql` share a filename
prefix by coincidence of both starting a sequence -- they target different
databases, were written at different times, and do not compose. Never treat
the legacy sequence as continuing into the new one, and never apply a
legacy file against `ge_warehouse` or a new file against
`telemetry_warehouse`.

Applying one (unchanged from before the repository move, other than the
file's location):

```powershell
psql -X -v ON_ERROR_STOP=1 -d telemetry_warehouse -f .\sql\legacy\telemetry_migrations\027_add_trackunit_counter_quality.sql
```

For a from-empty `telemetry_warehouse` (rare -- normally this database
already has most of the legacy migrations applied), the object-creation
order matters because some legacy migrations depend on objects from others
out of numeric order (e.g. `022` depends on tables created by `025` and on
the legacy `clean.sendem_*` tables) -- see
`docs/migration/legacy-to-platform-migration.md`'s "Bootstrapping an empty
legacy database" section for the exact sequence, and do not assume a blind
numeric-order runner is sufficient for this specific sequence.

## Why two sequences, not one

`sql/migrations/`'s own convention (`NNN_verb_object.sql`, starting at
`001`) collides by construction with the legacy sequence's own `001`-`029`
numbering. Reusing the same directory and continuing the numbering would
have meant either renumbering already-applied production migration history
(explicitly disallowed -- it would misrepresent what actually ran, when,
against production) or awkwardly starting the new sequence at `030`,
falsely implying it continues the old one. Two clearly-separated sequences,
targeting two clearly-separated databases, avoids both problems. See
`docs/architecture/architecture-decisions.md#adr-007`.
