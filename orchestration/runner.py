"""Run provider modules as bounded, observable subprocesses.

Provider code remains executable with ``python -m`` outside Dagster.  Dagster
uses this module to add three operational guarantees around those commands:

* stdout and stderr are streamed into the Dagster run log while also being
  retained for a useful failure message;
* every provider process has a configurable wall-clock timeout; and
* provider overlap groups use an OS file lock, including manual Dagster runs.

The lock file is intentionally retained after a run.  The operating-system
lock on its file handle is the source of truth and is released automatically
when the process exits, so a timeout, force-kill, or Dagster code-server crash
cannot leave a permanently held logical lock.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from time import monotonic
from typing import TextIO

import dagster as dg

from config.settings import load_project_env
from utils.overlap_lock import (
    ACCOUNTS_EVOLUTION_OVERLAP_GROUP,
    EZYTRACK_OVERLAP_GROUP,
    INHERITED_OVERLAP_GROUP_ENV,
    SENDEM_OVERLAP_GROUP,
    TRACKUNIT_OVERLAP_GROUP,
    OverlapLockUnavailable,
    provider_overlap_guard,
)


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SENDEM_JOB_TIMEOUT_MINUTES = 60
DEFAULT_EZYTRACK_JOB_TIMEOUT_MINUTES = 60
DEFAULT_TRACKUNIT_JOB_TIMEOUT_MINUTES = 360
DEFAULT_ACCOUNTS_EVOLUTION_PROJECT_REPORTS_JOB_TIMEOUT_MINUTES = 60
DEFAULT_TERMINATE_GRACE_SECONDS = 10.0
MAX_CAPTURED_OUTPUT_CHARS = 20_000
MAX_CAPTURED_LINES = 1_000
MAX_CAPTURED_LINE_CHARS = 4_000

_TIMEOUT_ENV_BY_MODULE_PREFIX: tuple[tuple[str, str, int], ...] = (
    ("jobs.sync_sendem", "SENDEM_JOB_TIMEOUT_MINUTES", DEFAULT_SENDEM_JOB_TIMEOUT_MINUTES),
    ("jobs.sync_ezytrack", "EZYTRACK_JOB_TIMEOUT_MINUTES", DEFAULT_EZYTRACK_JOB_TIMEOUT_MINUTES),
    (
        "jobs.sync_trackunit",
        "TRACKUNIT_JOB_TIMEOUT_MINUTES",
        DEFAULT_TRACKUNIT_JOB_TIMEOUT_MINUTES,
    ),
    (
        "jobs.accounts.evolution.sync_project_reports",
        "ACCOUNTS_EVOLUTION_PROJECT_REPORTS_JOB_TIMEOUT_MINUTES",
        DEFAULT_ACCOUNTS_EVOLUTION_PROJECT_REPORTS_JOB_TIMEOUT_MINUTES,
    ),
)


def get_module_timeout_minutes(module: str) -> float:
    """Return the configured positive timeout for a provider module.

    Values are deliberately parsed here instead of widening the shared
    provider settings dataclasses: orchestration is the only consumer and a
    bad operational value should fall back safely without preventing the
    provider's own settings from loading.
    """
    load_project_env()

    for module_prefix, env_name, default in _TIMEOUT_ENV_BY_MODULE_PREFIX:
        if module.startswith(module_prefix):
            raw_value = os.getenv(env_name)
            if raw_value is None or not raw_value.strip():
                return float(default)
            try:
                value = float(raw_value)
            except ValueError:
                logger.warning(
                    "%s=%r is not a number; using the default %s minutes",
                    env_name,
                    raw_value,
                    default,
                )
                return float(default)
            if value <= 0:
                logger.warning(
                    "%s=%r must be greater than zero; using the default %s minutes",
                    env_name,
                    raw_value,
                    default,
                )
                return float(default)
            return value

    raise ValueError(f"No subprocess timeout is registered for module {module!r}")


def get_terminate_grace_seconds() -> float:
    """Return the configured positive terminate-before-kill grace period."""
    load_project_env()
    env_name = "ETL_SUBPROCESS_TERMINATE_GRACE_SECONDS"
    raw_value = os.getenv(env_name)
    if raw_value is None or not raw_value.strip():
        return DEFAULT_TERMINATE_GRACE_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        logger.warning(
            "%s=%r is not a number; using the default %.1f seconds",
            env_name,
            raw_value,
            DEFAULT_TERMINATE_GRACE_SECONDS,
        )
        return DEFAULT_TERMINATE_GRACE_SECONDS
    if value <= 0:
        logger.warning(
            "%s=%r must be greater than zero; using the default %.1f seconds",
            env_name,
            raw_value,
            DEFAULT_TERMINATE_GRACE_SECONDS,
        )
        return DEFAULT_TERMINATE_GRACE_SECONDS
    return value


@contextmanager
def job_overlap_guard(
    context: dg.OpExecutionContext,
    overlap_group: str,
) -> Iterator[None]:
    """Hold a cross-process provider lock for the enclosed work.

    Schedule-time checks make expected overlaps a clean ``SkipReason``.  This
    second layer closes the race between evaluation and launch and also
    protects manually launched Dagster jobs.
    """
    lock_context = provider_overlap_guard(overlap_group)
    try:
        lock_path = lock_context.__enter__()
    except OverlapLockUnavailable as error:
        raise dg.Failure(
            description=str(error),
            allow_retries=False,
        ) from error

    context.log.info(
        "Acquired overlap guard '%s' at %s",
        overlap_group,
        lock_path,
    )
    try:
        yield
    finally:
        # Pass the active exception through the context manager so cleanup can
        # never replace the provider/timeout failure being propagated.
        lock_context.__exit__(*sys.exc_info())
        context.log.info(
            "Released overlap guard '%s'",
            overlap_group,
        )


def _stream_pipe(
    pipe: TextIO,
    captured_lines: deque[str],
    log_line: object,
) -> None:
    """Drain one child pipe, streaming all lines and retaining a bounded tail."""
    try:
        for raw_line in iter(pipe.readline, ""):
            line = raw_line.rstrip("\r\n")
            if len(line) > MAX_CAPTURED_LINE_CHARS:
                omitted = len(line) - MAX_CAPTURED_LINE_CHARS
                captured_lines.append(
                    f"<truncated {omitted} earlier character(s) in line>"
                    f"{line[-MAX_CAPTURED_LINE_CHARS:]}"
                )
            else:
                captured_lines.append(line)
            if line:
                try:
                    log_line(line)  # type: ignore[operator]
                except Exception:
                    # A logging-handler failure must not stop pipe draining;
                    # otherwise a noisy child could fill the pipe and hang.
                    logger.exception("Could not stream a provider subprocess log line")
    finally:
        pipe.close()


def _terminate_process(
    process: subprocess.Popen[str],
    grace_seconds: float,
) -> bool:
    """Terminate, then force-kill if needed; return whether kill was used."""
    if process.poll() is not None:
        return False

    try:
        process.terminate()
    except ProcessLookupError:
        return False

    try:
        process.wait(timeout=grace_seconds)
        return False
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except ProcessLookupError:
            return False
        process.wait()
        return True


def _captured_output(label: str, lines: Sequence[str]) -> str:
    """Format a bounded tail of captured output for a Dagster failure."""
    output = "\n".join(lines).strip()
    if not output:
        return f"{label}: <empty>"
    if len(output) > MAX_CAPTURED_OUTPUT_CHARS:
        omitted = len(output) - MAX_CAPTURED_OUTPUT_CHARS
        output = f"<truncated {omitted} earlier character(s)>\n{output[-MAX_CAPTURED_OUTPUT_CHARS:]}"
    return f"{label}:\n{output}"


def _run_module_process(
    context: dg.OpExecutionContext,
    module: str,
    args: Sequence[str],
    timeout_minutes: float,
    terminate_grace_seconds: float,
    inherited_overlap_group: str | None = None,
) -> None:
    """Launch and supervise one provider subprocess."""
    if timeout_minutes <= 0:
        raise ValueError("timeout_minutes must be greater than zero")
    if terminate_grace_seconds < 0:
        raise ValueError("terminate_grace_seconds cannot be negative")

    command = [sys.executable, "-m", module, *args]
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    # Clear any stale marker inherited by the code-server itself, then set it
    # only when this supervising process genuinely owns the matching lock.
    environment.pop(INHERITED_OVERLAP_GROUP_ENV, None)
    if inherited_overlap_group is not None:
        environment[INHERITED_OVERLAP_GROUP_ENV] = inherited_overlap_group

    context.log.info("Working directory: %s", PROJECT_ROOT)
    context.log.info("Running module: %s", " ".join(command))
    context.log.info("Subprocess timeout: %.2f minute(s)", timeout_minutes)

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    if process.stdout is None or process.stderr is None:
        _terminate_process(process, terminate_grace_seconds)
        raise dg.Failure(
            description=f"Could not capture stdout/stderr for module {module}",
            allow_retries=False,
        )

    stdout_lines: deque[str] = deque(maxlen=MAX_CAPTURED_LINES)
    stderr_lines: deque[str] = deque(maxlen=MAX_CAPTURED_LINES)
    stdout_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stdout, stdout_lines, context.log.info),
        name=f"{module}-stdout",
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stderr, stderr_lines, context.log.warning),
        name=f"{module}-stderr",
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    started_at = monotonic()
    timeout_seconds = timeout_minutes * 60.0
    try:
        return_code = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as timeout_error:
        context.log.error(
            "Module %s exceeded its %.2f-minute timeout; terminating it",
            module,
            timeout_minutes,
        )
        force_killed = _terminate_process(process, terminate_grace_seconds)
        stdout_thread.join(timeout=max(terminate_grace_seconds, 1.0))
        stderr_thread.join(timeout=max(terminate_grace_seconds, 1.0))
        elapsed_seconds = monotonic() - started_at
        action = "terminated, then force-killed" if force_killed else "terminated gracefully"
        raise dg.Failure(
            description=(
                f"Module {module} timed out after {timeout_minutes:.2f} minute(s) "
                f"({elapsed_seconds:.1f}s elapsed) and was {action}.\n"
                f"{_captured_output('Captured stdout', stdout_lines)}\n"
                f"{_captured_output('Captured stderr', stderr_lines)}"
            ),
            allow_retries=False,
        ) from timeout_error
    except BaseException:
        _terminate_process(process, terminate_grace_seconds)
        raise
    finally:
        if process.poll() is not None:
            stdout_thread.join(timeout=max(terminate_grace_seconds, 1.0))
            stderr_thread.join(timeout=max(terminate_grace_seconds, 1.0))

    if return_code != 0:
        raise dg.Failure(
            description=(
                f"Module {module} failed with exit code {return_code}.\n"
                f"{_captured_output('Captured stdout', stdout_lines)}\n"
                f"{_captured_output('Captured stderr', stderr_lines)}"
            ),
            allow_retries=False,
        )

    context.log.info("Module %s completed successfully", module)


def run_module(
    context: dg.OpExecutionContext,
    module: str,
    args: Sequence[str] = (),
    *,
    overlap_group: str | None = None,
    inherited_overlap_group: str | None = None,
    timeout_minutes: float | None = None,
    terminate_grace_seconds: float | None = None,
) -> None:
    """Run one provider module with timeout, streaming, and optional overlap guard."""
    if (
        overlap_group is not None
        and inherited_overlap_group is not None
        and overlap_group != inherited_overlap_group
    ):
        raise ValueError(
            "overlap_group and inherited_overlap_group must match when both are supplied"
        )

    resolved_timeout = (
        get_module_timeout_minutes(module) if timeout_minutes is None else timeout_minutes
    )
    resolved_grace = (
        get_terminate_grace_seconds()
        if terminate_grace_seconds is None
        else terminate_grace_seconds
    )

    if overlap_group is None:
        _run_module_process(
            context,
            module,
            args,
            resolved_timeout,
            resolved_grace,
            inherited_overlap_group,
        )
        return

    with job_overlap_guard(context, overlap_group):
        _run_module_process(
            context,
            module,
            args,
            resolved_timeout,
            resolved_grace,
            overlap_group,
        )
