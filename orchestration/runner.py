from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import dagster as dg


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_module(
    context: dg.OpExecutionContext,
    module: str,
    args: Sequence[str] = (),
) -> None:
    command = [sys.executable, "-m", module, *args]

    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"

    context.log.info(f"Working directory: {PROJECT_ROOT}")
    context.log.info(f"Running module: {' '.join(command)}")

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    if process.stdout is None:
        process.kill()
        raise dg.Failure(f"Could not capture output for module {module}")

    for line in process.stdout:
        message = line.rstrip()
        if message:
            context.log.info(message)

    return_code = process.wait()

    if return_code != 0:
        raise dg.Failure(
            description=(
                f"Module {module} failed with exit code {return_code}"
            )
        )

    context.log.info(f"Module {module} completed successfully")