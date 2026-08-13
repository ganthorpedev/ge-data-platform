"""Shared logging configuration for all sync jobs.

Jobs call `configure_logging()` once at startup and then use
`logging.getLogger(__name__)` in their own modules. Output goes to stdout so
Dagster's subprocess runner (orchestration/runner.py) streams it into the
run log exactly like the existing print() output; informational print()s in
jobs are intentionally left alone -- this logger is for structured
warning/error paths (retries, bookkeeping failures, validation findings).
"""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED_FLAG = "_telemetry_etl_configured"


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the root logger used by sync jobs.

    Idempotent: calling it more than once (e.g. a job invoked from a script
    that already configured logging) never adds duplicate handlers.
    """
    root = logging.getLogger()

    if getattr(root, _CONFIGURED_FLAG, False):
        root.setLevel(level)
        return root

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(handler)
    root.setLevel(level)
    setattr(root, _CONFIGURED_FLAG, True)
    return root
