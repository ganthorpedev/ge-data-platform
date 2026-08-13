# Secrets and access

## `.env` is never committed

`.gitignore` excludes every `.env*` file except `.env.example` (the safe,
value-free template). Copy `.env.example` to `.env` locally and fill in
real values -- see `docs/development/local-setup.md`. If you ever see a
real credential in a diff, stop and treat it as an incident, not a typo to
quietly fix.

## Secrets belong outside Git, always

No credential, API key, connection password, or webhook URL is ever
hardcoded in source, committed in `.env`, or placed in a migration `.sql`
file (see "No passwords in migration SQL" below). Configuration is read
exclusively through `ge_data_platform.config.settings`'s `get_*_settings()`
functions, sourced from environment variables /`.env` at runtime -- see
`docs/operations/pipeline-operations.md#configuration-and-env-precedence`.

## Historical incident: prior secret exposure

The predecessor `telemetry_etl` repository's git history contained
committed `.env` secrets. When this repository (`ge-data-platform`) was
split out from it, that history was scrubbed before the split -- the
standalone repository does not carry that exposure forward. This is
recorded here as institutional memory, not as an active issue: **any
credential that was ever committed to git history anywhere, in any repository,
must be treated as compromised and rotated**, regardless of whether the
commit was later removed or the repository was later scrubbed. Git history
rewriting removes a secret from a *specific* repository's future clones; it
does not un-expose a secret that may have already been fetched, mirrored,
or cached elsewhere.

## No passwords in migration SQL

Every `sql/migrations/*.sql` and `sql/legacy/telemetry_migrations/*.sql`
file that creates a role does so `NOLOGIN` (`ge_platform_admin`, `ge_etl`,
`ge_bi_readonly` -- see below) or, for the one legacy exception
(`sql/legacy/telemetry_migrations/024_create_powerbi_reader_role.sql`,
which creates a `LOGIN` role), with an explicit placeholder password
(`CHANGE_ME_BEFORE_RUNNING`) and a header stating it must never be
committed with a real credential and is not to be run without review. Real
login accounts and their passwords are created by an operator, out of band,
never via a committed migration.

## Role separation

Three `NOLOGIN` group roles exist in `ge_warehouse`
(`sql/migrations/003_create_platform_roles.sql`):

| Role | Scope |
|---|---|
| `ge_platform_admin` | `ALL` on every platform schema, current and future objects |
| `ge_etl` | `USAGE`/`CREATE` on every schema; `SELECT`/`INSERT`/`UPDATE`/`DELETE` on current and future tables everywhere |
| `ge_bi_readonly` | `USAGE`/`SELECT` on `mart_*` schemas **only** -- no access whatsoever to `raw_*`, `stg_*`, `core`, or `ops` |

No actual login account has been created or granted membership in any of
these roles yet -- that's deliberately an operator action taken outside
version control (`GRANT ge_etl TO <login_role>;`, run manually), not a
migration. See `docs/architecture/database-architecture.md#roles` for how
this compares to the legacy database's single `excel_reader` read-only
role, which this design generalizes but does not replace or touch.

## Two databases, one set of credentials -- don't confuse them

`POSTGRES_HOST`/`_PORT`/`_USER`/`_PASSWORD` are shared between the legacy
connection (`Settings`, targeting `POSTGRES_DB` = `telemetry_warehouse`) and
the platform connection (`PlatformSettings`, targeting `GE_WAREHOUSE_DB` =
`ge_warehouse`). The credentials are the same; the database name is not.
Double-check which settings object (and therefore which database) a script
or manual `psql` command is actually targeting before running anything
destructive -- see `docs/development/local-setup.md`.

## Development vs. production Postgres

The local development Postgres instance used to build and validate
`ge_warehouse` was explicitly confirmed, before any database was created,
to be a development-only instance separate from whatever serves
production. If that ambiguity is ever unclear again (a new environment, a
new machine, a shared server), stop and confirm before running any
database-creating command -- do not assume.
