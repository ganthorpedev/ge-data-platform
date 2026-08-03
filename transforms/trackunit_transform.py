"""Transforms raw Trackunit/AEMP API records into clean, database-ready tables.

This module owns dataframe construction and the daily-activity calculations
proven against the manual Activity Report in
Manitou/manitou_trackunit_exploration.ipynb:

- Work day hours: floor(last operating-hours boundary - first operating-hours
  boundary), to the minute.
- Operating hours: CumulativeOperatingHours delta (last point - first point),
  rounded to the minute, or NULL if the cumulative counter decreases.
- Active driving: CumulativeMovingHours delta, rounded to the minute, or NULL
  if the cumulative counter decreases.
- Distance: Distance.Odometer delta where available, or NULL if the
  cumulative counter decreases.

Any consecutive decrease in operating, moving, or distance readings marks
the row COUNTER_RESET and nulls only that counter's derived metric. Raw
readings and unaffected business metrics are preserved.

It must not perform any I/O (no HTTP calls, no database access, no
environment reads).
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from utils.dates import local_day_to_utc_window

DATA_QUALITY_LIVE = "live"
DATA_QUALITY_COUNTER_RESET = "COUNTER_RESET"

RAW_ASSETS_COLUMNS = [
    "asset_id",
    "name",
    "external_reference",
    "serial_number",
    "asset_type",
    "type",
    "brand",
    "model",
    "production_year",
    "owner_account_id",
    "created_at",
    "telematics_device_id",
    "telematics_device_serial",
]

RAW_METRIC_COLUMNS = [
    "asset_id",
    "metric_name",
    "metric_timestamp_utc",
    "metric_value",
    "metric_unit",
]

DAILY_ACTIVITY_COLUMNS = [
    "report_date",
    "asset_id",
    "machine",
    "pin",
    "machine_serial_number",
    "brand",
    "machine_type",
    "model",
    "start_time_utc",
    "stop_time_utc",
    "start_time_local",
    "stop_time_local",
    "work_day_minutes",
    "work_day_hhmm",
    "operating_minutes",
    "operating_hhmm",
    "active_driving_minutes",
    "active_driving_hhmm",
    "distance_km",
    "operating_points",
    "moving_points",
    "distance_points",
    "source_provider",
    "counter_reset_detected",
    "data_quality_status",
]

_VALUE_COLUMN_CANDIDATES = ["Hour", "hour", "Odometer", "odometer", "value", "Value"]
_TIMESTAMP_COLUMN_CANDIDATES = ["datetime", "DateTime", "timestamp", "Timestamp", "dateTime"]


def local_report_day_to_utc_window(report_date: date, timezone: str = "Africa/Harare") -> tuple[str, str]:
    """Convert a local calendar day into its UTC [start, end) window.

    Returns (start_utc, end_utc) as ISO-8601 UTC strings (e.g.
    "2026-06-30T22:00:00Z") suitable for the AEMP time-series endpoints.
    """
    return local_day_to_utc_window(report_date, timezone)


def minutes_to_hhmm(total_minutes: float | int | None, pad_hours: bool = True) -> str | None:
    """Format a minute count as "H:MM" (or "HH:MM" if `pad_hours`). None passes through."""
    if total_minutes is None:
        return None

    sign = "-" if total_minutes < 0 else ""
    whole_minutes = abs(int(round(total_minutes)))
    hours, minutes = divmod(whole_minutes, 60)
    hours_str = f"{hours:02d}" if pad_hours else str(hours)

    return f"{sign}{hours_str}:{minutes:02d}"


def extract_metric_points(response_json: Any, metric_key: str) -> list[dict]:
    """Return the list of data points from a raw AEMP response, wherever they live.

    Checks `response_json[metric_key]` first (a list, or a single dict wrapped
    in a list), then falls back to common envelope keys, then a direct list.
    """
    if isinstance(response_json, list):
        return response_json

    if isinstance(response_json, dict):
        metric_value = response_json.get(metric_key)
        if isinstance(metric_value, list):
            return metric_value
        if isinstance(metric_value, dict):
            return [metric_value]

        for key in ("data", "values", "Values", "content"):
            if isinstance(response_json.get(key), list):
                return response_json[key]

    return []


def detect_value_and_timestamp(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """Detect which columns hold the metric value and the point timestamp."""
    value_column = next((c for c in _VALUE_COLUMN_CANDIDATES if c in df.columns), None)
    timestamp_column = next((c for c in _TIMESTAMP_COLUMN_CANDIDATES if c in df.columns), None)
    return value_column, timestamp_column


def cumulative_counter_reset_detected(points: list[dict]) -> bool:
    """Return True when a cumulative metric decreases between readings.

    Detection is chronological and checks every consecutive pair, rather
    than only comparing the first and last values. This catches a reset that
    later climbs above its pre-reset starting value. Duplicate timestamps are
    collapsed (last value wins), matching the raw-table UPSERT grain.

    Missing, malformed, and single-reading series are not resets.
    """
    if len(points) < 2:
        return False

    df = pd.json_normalize(points)
    value_column, timestamp_column = detect_value_and_timestamp(df)
    if value_column is None or timestamp_column is None:
        return False

    df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True, errors="coerce")
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    df = (
        df.dropna(subset=[timestamp_column, value_column])
        .sort_values(timestamp_column)
        .drop_duplicates(subset=[timestamp_column], keep="last")
    )
    if len(df) < 2:
        return False

    return bool(df[value_column].diff().lt(0).any())


def cumulative_hours_delta_minutes(
    points: list[dict],
) -> tuple[float | None, pd.Timestamp | None, pd.Timestamp | None, int]:
    """Compute (delta_minutes, first_timestamp, last_timestamp, point_count) for an hours metric.

    `delta_minutes` is (last point's value - first point's value) * 60,
    i.e. the raw hours delta converted to minutes. It is None if any
    consecutive reading decreases. Returns Nones for the value/timestamps if
    `points` is empty or the columns can't be detected.
    """
    if not points:
        return None, None, None, 0

    df = pd.json_normalize(points)
    value_column, timestamp_column = detect_value_and_timestamp(df)
    if value_column is None or timestamp_column is None:
        return None, None, None, len(points)

    df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    df = df.sort_values(timestamp_column)

    first_timestamp = df[timestamp_column].iloc[0]
    last_timestamp = df[timestamp_column].iloc[-1]
    if cumulative_counter_reset_detected(points):
        return None, first_timestamp, last_timestamp, len(df)
    delta_hours = df[value_column].iloc[-1] - df[value_column].iloc[0]

    return delta_hours * 60, first_timestamp, last_timestamp, len(df)


def cumulative_distance_delta_km(
    points: list[dict],
) -> tuple[float | None, pd.Timestamp | None, pd.Timestamp | None, int]:
    """Compute distance delta metadata, returning a NULL delta on a counter reset."""
    if not points:
        return None, None, None, 0

    df = pd.json_normalize(points)
    value_column, timestamp_column = detect_value_and_timestamp(df)
    if value_column is None or timestamp_column is None:
        return None, None, None, len(points)

    df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    df = df.sort_values(timestamp_column)

    first_timestamp = df[timestamp_column].iloc[0]
    last_timestamp = df[timestamp_column].iloc[-1]
    if cumulative_counter_reset_detected(points):
        return None, first_timestamp, last_timestamp, len(df)
    delta_km = df[value_column].iloc[-1] - df[value_column].iloc[0]

    return delta_km, first_timestamp, last_timestamp, len(df)


def derive_activity_boundaries_from_operating_points(
    points: list[dict],
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int | None]:
    """Derive (start_time_utc, stop_time_utc, work_day_minutes) from operating-hours points.

    Trackunit polls at a slow, sparse "heartbeat" cadence (roughly hourly)
    whenever CumulativeOperatingHours is not changing, and switches to a much
    denser cadence while it IS changing (i.e. while the machine is actually
    running). Using the raw first/last point in the day's data therefore
    just measures the polling window (close to 24h), not the work day.

    Instead, "boundary" is the first/last VALUE TRANSITION in the series:
      - start_time_utc = the timestamp of the last stable reading immediately
        BEFORE the first value change (the last "resting" sample before
        activity begins).
      - stop_time_utc = the timestamp of the LAST value change (the last
        sample where the value differs from the one before it).
    work_day_minutes is (stop - start) floored to whole minutes.

    Validated exactly against 4 machines' manual Activity Report figures
    (20:39, 02:43, 08:43, 19:22) in
    Manitou/manitou_trackunit_exploration.ipynb-derived acceptance testing.

    If the value never changes across the day (no transitions), returns
    (None, None, 0) -- there was no detectable work period.
    """
    if not points:
        return None, None, None

    df = pd.json_normalize(points)
    value_column, timestamp_column = detect_value_and_timestamp(df)
    if value_column is None or timestamp_column is None:
        return None, None, None

    df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    df = df.sort_values(timestamp_column).reset_index(drop=True)

    value_diffs = df[value_column].diff()
    transition_positions = [i for i in range(1, len(df)) if value_diffs.iloc[i] != 0]

    if not transition_positions:
        return None, None, 0

    first_transition = min(transition_positions)
    last_transition = max(transition_positions)

    start_time_utc = df[timestamp_column].iloc[first_transition - 1]
    stop_time_utc = df[timestamp_column].iloc[last_transition]
    work_day_minutes = math.floor((stop_time_utc - start_time_utc).total_seconds() / 60)

    return start_time_utc, stop_time_utc, work_day_minutes


def _extract_raw_asset(asset: dict) -> dict:
    """Flatten one raw Trackunit asset record into raw.trackunit_assets columns."""
    devices = asset.get("telematicsDevices") or []
    device = devices[0] if devices else {}

    return {
        "asset_id": asset.get("id"),
        "name": asset.get("name"),
        "external_reference": asset.get("externalReference"),
        "serial_number": asset.get("serialNumber"),
        "asset_type": asset.get("assetType"),
        "type": asset.get("type"),
        "brand": asset.get("brand"),
        "model": asset.get("model"),
        "production_year": asset.get("productionYear"),
        "owner_account_id": asset.get("ownerAccountId"),
        "created_at": asset.get("createdAt"),
        "telematics_device_id": device.get("id"),
        "telematics_device_serial": device.get("serialNumber"),
    }


def _build_metric_rows(asset_id: str, points: list[dict], metric_name: str) -> list[dict]:
    """Flatten one asset's metric points into raw metric-table rows.

    The AEMP API has been observed to return two points with the exact same
    timestamp for the same asset/metric within a single page. Since
    (asset_id, metric_timestamp_utc, metric_name) is the raw table's primary
    key, exact-timestamp duplicates are dropped here (keeping the last one)
    so the upsert never tries to touch the same row twice in one statement.
    This does not affect the hours/distance delta calculations, which only
    read the first/last sorted values and are unaffected by which duplicate
    survives.
    """
    if not points:
        return []

    df = pd.json_normalize(points)
    value_column, timestamp_column = detect_value_and_timestamp(df)
    if value_column is None or timestamp_column is None:
        return []

    df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    df = df.drop_duplicates(subset=[timestamp_column], keep="last")

    unit_column = next((c for c in df.columns if "unit" in c.lower()), None)

    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "asset_id": asset_id,
                "metric_name": metric_name,
                "metric_timestamp_utc": pd.to_datetime(row[timestamp_column], utc=True),
                "metric_value": row[value_column],
                "metric_unit": row[unit_column] if unit_column else None,
            }
        )
    return rows


def build_daily_activity_rows(
    assets: list[dict],
    metric_results_by_asset: dict[str, dict[str, list[dict]]],
    report_date: date,
    timezone: str = "Africa/Harare",
) -> dict[str, pd.DataFrame]:
    """Build every Trackunit raw/staging DataFrame for one report day.

    `metric_results_by_asset` maps asset_id -> {"operating_points": [...],
    "moving_points": [...], "distance_points": [...]} (already extracted via
    `extract_metric_points()`).

    If both operating and moving points are empty for an asset, produces a
    valid zero row (work_day_minutes/operating_minutes/active_driving_minutes
    = 0, start/stop_time_utc = None) rather than treating it as a failure.

    Counter decreases null only the affected derived value and set
    counter_reset_detected/COUNTER_RESET on the daily row; raw points remain
    present. Returns a dict with: raw_assets_df, raw_operating_hours_df,
    raw_moving_hours_df, raw_distance_df, dim_assets_df, daily_activity_df.
    """
    tz = ZoneInfo(timezone)

    raw_asset_rows: list[dict] = []
    raw_operating_rows: list[dict] = []
    raw_moving_rows: list[dict] = []
    raw_distance_rows: list[dict] = []
    activity_rows: list[dict] = []

    for asset in assets:
        asset_id = asset.get("id")
        raw_asset_rows.append(_extract_raw_asset(asset))

        machine = asset.get("name")
        pin = asset.get("externalReference") or asset.get("serialNumber")
        devices = asset.get("telematicsDevices") or []
        machine_serial_number = devices[0].get("serialNumber") if devices else None
        brand = asset.get("brand")
        machine_type = asset.get("type") or asset.get("assetType")
        model = asset.get("model")

        metrics = metric_results_by_asset.get(asset_id, {})
        operating_points = metrics.get("operating_points") or []
        moving_points = metrics.get("moving_points") or []
        distance_points = metrics.get("distance_points") or []

        operating_counter_reset = cumulative_counter_reset_detected(operating_points)
        moving_counter_reset = cumulative_counter_reset_detected(moving_points)
        distance_counter_reset = cumulative_counter_reset_detected(distance_points)
        counter_reset_detected = (
            operating_counter_reset or moving_counter_reset or distance_counter_reset
        )

        raw_operating_rows.extend(_build_metric_rows(asset_id, operating_points, "CumulativeOperatingHours"))
        raw_moving_rows.extend(_build_metric_rows(asset_id, moving_points, "CumulativeMovingHours"))
        raw_distance_rows.extend(_build_metric_rows(asset_id, distance_points, "Distance"))

        if not operating_points and not moving_points:
            start_time_utc = None
            stop_time_utc = None
            work_day_minutes = 0
            operating_minutes = 0
            active_driving_minutes = 0
        else:
            start_time_utc, stop_time_utc, work_day_minutes = derive_activity_boundaries_from_operating_points(
                operating_points
            )
            work_day_minutes = work_day_minutes if work_day_minutes is not None else 0

            operating_delta_minutes, _, _, _ = cumulative_hours_delta_minutes(operating_points)
            if operating_counter_reset:
                operating_minutes = None
            else:
                operating_minutes = round(operating_delta_minutes) if operating_delta_minutes is not None else 0

            moving_delta_minutes, _, _, _ = cumulative_hours_delta_minutes(moving_points)
            if moving_counter_reset:
                active_driving_minutes = None
            else:
                active_driving_minutes = round(moving_delta_minutes) if moving_delta_minutes is not None else 0

        distance_km, _, _, _ = cumulative_distance_delta_km(distance_points)
        if distance_counter_reset:
            distance_km = None

        start_time_local = start_time_utc.astimezone(tz).replace(tzinfo=None) if start_time_utc is not None else None
        stop_time_local = stop_time_utc.astimezone(tz).replace(tzinfo=None) if stop_time_utc is not None else None

        activity_rows.append(
            {
                "report_date": report_date,
                "asset_id": asset_id,
                "machine": machine,
                "pin": pin,
                "machine_serial_number": machine_serial_number,
                "brand": brand,
                "machine_type": machine_type,
                "model": model,
                "start_time_utc": start_time_utc,
                "stop_time_utc": stop_time_utc,
                "start_time_local": start_time_local,
                "stop_time_local": stop_time_local,
                "work_day_minutes": work_day_minutes,
                "work_day_hhmm": minutes_to_hhmm(work_day_minutes),
                "operating_minutes": operating_minutes,
                "operating_hhmm": minutes_to_hhmm(operating_minutes),
                "active_driving_minutes": active_driving_minutes,
                "active_driving_hhmm": minutes_to_hhmm(active_driving_minutes),
                "distance_km": distance_km,
                "operating_points": len(operating_points),
                "moving_points": len(moving_points),
                "distance_points": len(distance_points),
                "source_provider": "trackunit",
                "counter_reset_detected": counter_reset_detected,
                "data_quality_status": (
                    DATA_QUALITY_COUNTER_RESET if counter_reset_detected else DATA_QUALITY_LIVE
                ),
            }
        )

    return {
        "raw_assets_df": pd.DataFrame(raw_asset_rows, columns=RAW_ASSETS_COLUMNS),
        "raw_operating_hours_df": pd.DataFrame(raw_operating_rows, columns=RAW_METRIC_COLUMNS),
        "raw_moving_hours_df": pd.DataFrame(raw_moving_rows, columns=RAW_METRIC_COLUMNS),
        "raw_distance_df": pd.DataFrame(raw_distance_rows, columns=RAW_METRIC_COLUMNS),
        "dim_assets_df": pd.DataFrame(raw_asset_rows, columns=RAW_ASSETS_COLUMNS),
        "daily_activity_df": pd.DataFrame(activity_rows, columns=DAILY_ACTIVITY_COLUMNS),
    }
