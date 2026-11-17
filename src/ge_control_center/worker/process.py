"""Child-process supervision helpers.

Ported from the subprocess handling in the legacy
``ge_data_platform/orchestration/runner.py`` (bounded output capture,
terminate-then-kill), without any orchestration-framework dependency.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from collections import deque
from collections.abc import Callable
from typing import TextIO

logger = logging.getLogger(__name__)

MAX_CAPTURED_LINE_CHARS = 4_000
MAX_TAIL_LINES = 200


def popen_kwargs() -> dict:
    """Start the child in its own process group/session so terminal signals aimed at the
    worker (Ctrl-C) do not hit the child directly; the worker decides how to stop it."""
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}  # type: ignore[attr-defined]
    return {"start_new_session": True}


def stream_pipe(pipe: TextIO, on_line: Callable[[str], None], tail: deque[str]) -> None:
    """Drain one child pipe line by line; never let a callback failure stop draining."""
    try:
        for raw_line in iter(pipe.readline, ""):
            line = raw_line.rstrip("\r\n")
            if len(line) > MAX_CAPTURED_LINE_CHARS:
                line = f"<truncated {len(line) - MAX_CAPTURED_LINE_CHARS} chars>{line[-MAX_CAPTURED_LINE_CHARS:]}"
            tail.append(line)
            try:
                on_line(line)
            except Exception:  # noqa: BLE001
                # A failure here must not stop draining, or a chatty child fills the pipe and hangs.
                logger.exception("could not handle a child output line")
    finally:
        try:
            pipe.close()
        except OSError:
            pass


def terminate_process(process: subprocess.Popen, grace_seconds: float) -> bool:
    """SIGTERM (or CTRL_BREAK on Windows), wait, then kill. Returns True if kill was needed."""
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
        pass
    kill_process(process)
    return True


def kill_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        process.kill()
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logger.error("child process %s did not exit after SIGKILL", process.pid)


def python_executable() -> str:
    return sys.executable
