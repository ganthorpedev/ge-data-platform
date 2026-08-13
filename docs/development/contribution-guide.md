# Contribution guide

**Status: no CI pipeline exists yet** (no `.github/workflows`, no other CI
config in this repository) -- everything below is run locally before a
change is committed, not enforced automatically. Treat that as a gap to
close, not a reason to skip these steps.

## Before committing

```powershell
python -m compileall src tests scripts
python -m pytest
python -m pip check
```

All three must pass. If a change touches `ge_warehouse` structure, also run
`python -m scripts.setup_ge_warehouse --validate` against your local
database and confirm every check still `PASS`es. If a change touches
Dagster definitions, also confirm the job/schedule/sensor counts documented
in `docs/operations/pipeline-operations.md#dagster-jobs-and-schedules`
still match.

## Code conventions (observed in this codebase; follow them)

- Every module, class, and non-trivial function has a docstring explaining
  *why*, not just *what* -- match that density in new code rather than
  leaving new logic unexplained.
- Settings are frozen `@dataclass` objects loaded through a `get_*_settings()`
  function in `ge_data_platform.config.settings`, never read from
  `os.environ` ad hoc elsewhere in the codebase.
- A source's client (`client.py`) never writes to the database; a source's
  transform never makes an HTTP/SQL call; a source's sync/entry-point module
  orchestrates both. Keep that separation when adding to an existing source
  or building a new one.
- Retries, backoff, and overlap protection are provider-owned where the
  provider's failure modes are provider-specific (see
  `docs/operations/retries-and-recovery.md`) -- don't add a second, competing
  retry layer around an existing client.

## Documentation conventions (this effort's own rules -- keep following them)

- Every claim about what exists must be labeled **IMPLEMENTED**,
  **PLANNED**, **DEFERRED**, or **LEGACY** -- see
  `docs/architecture/platform-overview.md` for what each means in this
  repository. Never describe planned architecture as if it already exists.
- Prefer linking to the single authoritative document over repeating an
  explanation (e.g. link to `docs/operations/retries-and-recovery.md` for
  retry mechanics rather than re-describing them in a source doc).
- If you're documenting behavior, read the code first. Do not infer a
  database object from a filename, or a job's behavior from its docstring
  alone if the implementation disagrees.

## Commits

Logical, reviewable commits -- one concern per commit, not one commit per
file and not the whole change squashed into one. Nothing in this repository
is pushed to a remote without being explicitly asked for; treat that as the
default posture, not an exception.

## Tests for new work

New behavior needs a new or extended test, following the existing
per-source (or per-concern, for `orchestration`/`platform`) directory
layout under `tests/`. Prefer exercising real logic over mocking it away
where that's safe (see `docs/development/testing.md`'s note on
`tests/platform/test_migrations_discovery.py`'s real-filesystem test) --
reach for a mock when the alternative is a live API call or a live
database, not by default.
