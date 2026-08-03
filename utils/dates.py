"""Date helpers shared across telemetry provider jobs.

Provider APIs in this project use integer dates in YYYYMMDD format
(for example 20260630). Timestamps are always handled timezone-aware:
UTC for API windows and load bookkeeping, an explicit operational timezone
(default Africa/Harare) for provider-local report days. Nothing here reads
the machine's local clock without an explicit timezone.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

DEFAULT_OPERATIONAL_TIMEZONE = "Africa/Harare"


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def local_today(timezone_name: str = DEFAULT_OPERATIONAL_TIMEZONE) -> date:
    """Return today's calendar date in the given operational timezone.

    Use this instead of `date.today()` so results do not depend on the
    Windows machine's local clock timezone.
    """
    return datetime.now(ZoneInfo(timezone_name)).date()


def to_date_key(value: date) -> int:
    """Convert a `date` to its YYYYMMDD integer representation."""
    return value.year * 10000 + value.month * 100 + value.day


def from_date_key(value: int) -> date:
    """Convert a YYYYMMDD integer representation back to a `date`."""
    return date(value // 10000, (value // 100) % 100, value % 100)


def rolling_window(lookback_days: int, as_of: date | None = None) -> tuple[int, int]:
    """Return a (start_date, end_date) pair as YYYYMMDD integers.

    The window covers the last `lookback_days` days up to and including `as_of`
    (defaults to today in the default operational timezone).
    """
    end = as_of or local_today()
    start = end - timedelta(days=lookback_days)
    return to_date_key(start), to_date_key(end)


def format_utc_iso(value: datetime) -> str:
    """Format a datetime as the "YYYY-MM-DDTHH:MM:SSZ" style the provider APIs use.

    Naive datetimes are treated as already-UTC; aware datetimes are converted.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_day_to_utc_window(
    report_date: date, timezone_name: str = DEFAULT_OPERATIONAL_TIMEZONE
) -> tuple[str, str]:
    """Convert a local calendar day into its UTC [start, end) window.

    Returns (start_utc, end_utc) as ISO-8601 UTC strings (e.g.
    "2026-06-30T22:00:00Z"): start is inclusive (local midnight), end is
    exclusive (the next local midnight). This is the single source of truth
    for report-day windows -- transforms.trackunit_transform delegates here.
    """
    tz = ZoneInfo(timezone_name)
    local_start = datetime.combine(report_date, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    return format_utc_iso(local_start), format_utc_iso(local_end)
