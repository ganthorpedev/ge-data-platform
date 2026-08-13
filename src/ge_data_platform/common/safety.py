"""Guards for local-only development tooling.

Some scripts in this repository (historical backfills, one-off migration
tooling) are only ever meant to run against a developer's local Postgres
instance. This module provides one small, reusable check so those scripts
fail loudly instead of silently connecting to whatever POSTGRES_HOST happens
to be configured.
"""

from __future__ import annotations

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class NonLocalHostError(RuntimeError):
    """Raised when a local-only operation is pointed at a non-local Postgres host."""


def assert_local_host(host: str, *, context: str) -> None:
    """Raise `NonLocalHostError` unless `host` is a recognised local host.

    `context` is included in the error message so a failure clearly names
    which operation refused to run (e.g. "Trackunit historical backfill").
    This is a development-machine safety net for scripts that must never be
    pointed at a shared or production Postgres instance -- it is a
    convenience check against misconfiguration, not a substitute for real
    network-level access control.
    """
    if host not in LOCAL_HOSTS:
        raise NonLocalHostError(
            f"{context} refuses to run against non-local POSTGRES_HOST={host!r}. "
            f"Only {sorted(LOCAL_HOSTS)} are accepted."
        )
