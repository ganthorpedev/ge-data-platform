"""Date helpers shared across telemetry provider jobs.

Provider APIs in this project use integer dates in YYYYMMDD format
(for example 20260630).
"""

from __future__ import annotations

from datetime import date


def rolling_window(lookback_days: int, as_of: date | None = None) -> tuple[int, int]:
    """Return an (start_date, end_date) pair as YYYYMMDD integers.

    The window covers the last `lookback_days` days up to and including `as_of`
    (defaults to today).
    """
    raise NotImplementedError


def to_date_key(value: date) -> int:
    """Convert a `date` to its YYYYMMDD integer representation."""
    raise NotImplementedError


def from_date_key(value: int) -> date:
    """Convert a YYYYMMDD integer representation back to a `date`."""
    raise NotImplementedError
