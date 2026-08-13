"""Discovery for the ge_warehouse migration framework (sql/migrations/*.sql).

Pure filesystem logic only -- no database connection is opened here. See
scripts/setup_ge_warehouse.py for the code that actually applies discovered
migrations, and docs/ge_warehouse_architecture.md for the NNN_verb_object.sql
convention this enforces.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

# Exactly three digits, an underscore, a non-empty descriptive slug, ".sql".
# Anything else in sql/migrations/ (a README, a stray file) is ignored by
# discover_migrations rather than rejected -- only files that *look* like a
# migration (start with digits followed by an underscore) are validated
# strictly and can raise.
_MIGRATION_NAME_PATTERN = re.compile(r"^(?P<prefix>\d+)_(?P<slug>[a-z0-9_]+)\.sql$")


@dataclass(frozen=True)
class Migration:
    """One discovered migration file."""

    prefix: str
    slug: str
    path: Path

    @property
    def name(self) -> str:
        """The filename, matching what self-registers into ops.schema_version."""
        return self.path.name


def discover_migrations(directory: Path) -> list[Migration]:
    """Return every `NNN_description.sql` file under `directory`, in numeric order.

    Raises `ValueError` if:
    * the numeric prefix is not exactly 3 digits (zero-padded), or
    * two files share the same numeric prefix (the exact collision that
      motivated renaming, not renumbering, the old sendem validation files --
      see docs/ge_warehouse_architecture.md).

    Files that don't match `NNN_description.sql` at all (a README, a
    non-.sql file) are silently skipped, not rejected.
    """
    if not directory.is_dir():
        raise ValueError(f"Migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    seen_prefixes: dict[str, Path] = {}

    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue
        match = _MIGRATION_NAME_PATTERN.match(path.name)
        if match is None:
            continue

        prefix = match.group("prefix")
        if len(prefix) != 3:
            raise ValueError(
                f"Migration filename {path.name!r} has a {len(prefix)}-digit prefix; "
                "the convention is exactly 3 digits, zero-padded (e.g. 001_create_x.sql)"
            )

        if prefix in seen_prefixes:
            raise ValueError(
                f"Duplicate migration prefix {prefix!r}: {seen_prefixes[prefix].name!r} "
                f"and {path.name!r} both claim it. Rename one -- do not renumber an "
                "already-applied migration."
            )
        seen_prefixes[prefix] = path

        migrations.append(Migration(prefix=prefix, slug=match.group("slug"), path=path))

    migrations.sort(key=lambda m: m.prefix)
    return migrations
