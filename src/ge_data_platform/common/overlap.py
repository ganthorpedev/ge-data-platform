"""Cross-process provider locks shared by CLI jobs and Dagster wrappers.

The lock lives under the project directory rather than ``DAGSTER_HOME`` or a
per-user temporary directory.  That is important on the production Windows
host: Dagster runs as SYSTEM while a manual recovery command may run as an
operator, and both processes must contend for the same lock file.

Lock files are retained after use.  The operating-system lock on the open file
handle is the source of truth and is released when that handle closes, so a
crashed process cannot leave a permanently held logical lock.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import BinaryIO, ParamSpec, TypeVar


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LOCK_DIRECTORY = PROJECT_ROOT / ".ge_data_platform_locks"

# Private process-contract variable set only in the environment copy passed to
# a Dagster-launched child.  It is never written to the parent process's
# environment, so it cannot leak into a later direct CLI invocation.
INHERITED_OVERLAP_GROUP_ENV = "_TELEMETRY_ETL_INHERITED_OVERLAP_GROUP"

SENDEM_OVERLAP_GROUP = "sendem"
EZYTRACK_OVERLAP_GROUP = "ezytrack"
TRACKUNIT_OVERLAP_GROUP = "trackunit"
ACCOUNTS_EVOLUTION_OVERLAP_GROUP = "accounts_evolution"

_VALID_GROUP = re.compile(r"^[A-Za-z0-9_.-]+$")
_P = ParamSpec("_P")
_R = TypeVar("_R")


class OverlapLockUnavailable(RuntimeError):
    """Raised when another process already owns a provider overlap lock."""

    def __init__(self, overlap_group: str, lock_path: Path) -> None:
        self.overlap_group = overlap_group
        self.lock_path = lock_path
        super().__init__(
            f"Cannot start overlap group {overlap_group!r}: another process "
            f"already holds {lock_path}."
        )


def _lock_path(overlap_group: str) -> Path:
    """Return the stable lock path for a validated overlap-group name."""
    if not overlap_group or _VALID_GROUP.fullmatch(overlap_group) is None:
        raise ValueError(f"Invalid overlap group name: {overlap_group!r}")
    return LOCK_DIRECTORY / f"{overlap_group}.lock"


def _lock_file(handle: BinaryIO) -> None:
    """Acquire a non-blocking exclusive lock on the first byte."""
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    handle.seek(0)

    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: BinaryIO) -> None:
    """Release a lock acquired by :func:`_lock_file`."""
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _close_safely(handle: BinaryIO, overlap_group: str) -> None:
    """Close a lock handle without masking an active provider exception."""
    try:
        handle.close()
    except OSError:
        logger.exception(
            "Could not close overlap-lock handle for group %s; the original "
            "provider outcome is preserved",
            overlap_group,
        )


@contextmanager
def provider_overlap_guard(
    overlap_group: str,
    *,
    accept_inherited: bool = False,
) -> Iterator[Path | None]:
    """Hold one cross-process provider lock for the enclosed work.

    Direct provider jobs use ``accept_inherited=True``.  A Dagster wrapper
    already holding this exact group places a private marker only in its child
    process environment; that child may then enter without reacquiring the
    non-reentrant parent lock.  A normal CLI process has no marker and always
    acquires the operating-system lock itself.
    """
    if (
        accept_inherited
        and os.getenv(INHERITED_OVERLAP_GROUP_ENV) == overlap_group
    ):
        logger.info(
            "Using overlap guard %r inherited from the supervising Dagster process",
            overlap_group,
        )
        yield None
        return

    lock_path = _lock_path(overlap_group)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    acquired = False

    try:
        try:
            _lock_file(handle)
            acquired = True
        except (OSError, BlockingIOError) as error:
            raise OverlapLockUnavailable(overlap_group, lock_path) from error

        yield lock_path
    finally:
        if acquired:
            try:
                _unlock_file(handle)
            except OSError:
                # Closing the handle also releases an OS lock.  Never replace
                # the provider's exception with an explicit-unlock error.
                logger.exception(
                    "Explicit unlock failed for overlap group %s; closing the "
                    "handle to release it",
                    overlap_group,
                )
        _close_safely(handle, overlap_group)


def provider_job_lock(
    overlap_group: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Decorate a provider entry point with direct-CLI overlap protection."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def locked(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            with provider_overlap_guard(overlap_group, accept_inherited=True):
                return function(*args, **kwargs)

        return locked

    return decorate
