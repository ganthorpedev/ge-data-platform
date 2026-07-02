"""Shared logging configuration for all sync jobs."""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the root logger used by sync jobs.

    Jobs should call this once at startup and then use `logging.getLogger(__name__)`
    in their own modules.
    """
    raise NotImplementedError
