"""Transforms raw EzyTrack / Telematics Guru GraphQL records into clean,
database-ready tables.

This module owns dataframe construction and derived-metric calculation for
EzyTrack. Column sets match sql/010_create_ezytrack_schema.sql exactly
(excluding `loaded_at`, which is a database-side default column added by the
loader per upsert batch, the same convention transforms/sendem_transform.py
and loaders/postgres_loader.py already use -- it is not produced here).

It must not perform any I/O (no HTTP calls, no database access, no
environment reads), and it must not mutate the raw API records passed in.
"""

from __future__ import annotations

import pandas as pd

RAW_ASSETS_COLUMNS = [
    "asset_id",
    "asset_code",
    "asset_name",
    "asset_description",
    "department_name",
    "project_name",
    "allocated_driver_name",
    "allocated_driver_code",
    "current_geofence_name",
    "is_enabled",
    "last_connected_utc",
]

DIM_ASSETS_COLUMNS = list(RAW_ASSETS_COLUMNS)

RAW_TRIPS_COLUMNS = [
    "trip_id",
    "asset_id",
    "start_time_utc",
    "end_time_utc",
    "duration_seconds",
    "distance_meters",
    "stop_time_seconds",
    "idle_time_seconds",
    "start_odometer_meters",
    "start_run_seconds",
    "driver_name",
    "driver_code",
    "start_geofence_name",
]

FACT_TRIPS_COLUMNS = [
    "trip_id",
    "asset_id",
    "start_time_utc",
    "end_time_utc",
    "duration_seconds",
    "distance_meters",
    "distance_km",
    "stop_time_seconds",
    "idle_time_seconds",
    "time_in_motion_seconds",
    "start_odometer_meters",
    "start_odometer_km",
    "end_odometer_meters",
    "end_odometer_km",
    "start_run_seconds",
    "end_run_seconds",
    "runtime_start_hrs",
    "runtime_end_hrs",
    "driver_name",
    "driver_code",
    "start_geofence_name",
]


def _sub(obj: dict | None) -> dict:
    """Return `obj` if it's a dict, otherwise an empty dict.

    Makes nested GraphQL objects (which may be null, e.g. `department: null`)
    always safe to call `.get()` on.
    """
    return obj if isinstance(obj, dict) else {}


def _safe_div(numerator: float | None, denominator: float) -> float | None:
    """Divide `numerator` by `denominator`, returning None if numerator is None."""
    return numerator / denominator if numerator is not None else None


def _extract_raw_asset(asset: dict) -> dict:
    """Flatten one raw EzyTrack asset record into raw.ezytrack_assets columns."""
    department = _sub(asset.get("department"))
    project = _sub(asset.get("project"))
    allocated_driver = _sub(asset.get("allocatedDriver"))
    geofence = _sub(asset.get("geoFence"))

    return {
        "asset_id": asset.get("assetId"),
        "asset_code": asset.get("assetCode"),
        "asset_name": asset.get("name"),
        "asset_description": asset.get("description"),
        "department_name": department.get("name"),
        "project_name": project.get("name"),
        "allocated_driver_name": allocated_driver.get("name"),
        "allocated_driver_code": allocated_driver.get("driverCode"),
        "current_geofence_name": geofence.get("name"),
        "is_enabled": asset.get("isEnabled"),
        "last_connected_utc": asset.get("lastConnectedUtc"),
    }


def _extract_raw_trip(trip: dict) -> dict:
    """Flatten one raw EzyTrack trip record into raw.ezytrack_trips columns."""
    asset = _sub(trip.get("asset"))
    driver = _sub(trip.get("driver"))
    start_geofence = _sub(trip.get("startGeoFence"))

    return {
        "trip_id": trip.get("tripId"),
        "asset_id": asset.get("assetId"),
        "start_time_utc": trip.get("startTimeUtc"),
        "end_time_utc": trip.get("endTimeUtc"),
        "duration_seconds": trip.get("durationSeconds"),
        "distance_meters": trip.get("distanceMeters"),
        "stop_time_seconds": trip.get("stopTimeSeconds"),
        "idle_time_seconds": trip.get("idleTimeSeconds"),
        "start_odometer_meters": trip.get("startOdometerReadingMeters"),
        "start_run_seconds": trip.get("startRunSeconds"),
        "driver_name": driver.get("name"),
        "driver_code": driver.get("driverCode"),
        "start_geofence_name": start_geofence.get("name"),
    }


def _compute_fact_trip(raw_trip_row: dict) -> dict:
    """Compute staging.ezytrack_fact_trips columns from one raw trip row.

    Mirrors the prototype notebook's format_trip_for_db() calculations
    exactly: distance_km, start/end_odometer_km, runtime_start/end_hrs, and
    time_in_motion_seconds.
    """
    duration_seconds = raw_trip_row["duration_seconds"]
    stop_time_seconds = raw_trip_row["stop_time_seconds"]
    idle_time_seconds = raw_trip_row["idle_time_seconds"]
    distance_meters = raw_trip_row["distance_meters"]
    start_odometer_meters = raw_trip_row["start_odometer_meters"]
    start_run_seconds = raw_trip_row["start_run_seconds"]

    if None not in (duration_seconds, stop_time_seconds, idle_time_seconds):
        time_in_motion_seconds = max(duration_seconds - stop_time_seconds - idle_time_seconds, 0)
    else:
        time_in_motion_seconds = None

    end_odometer_meters = None
    if start_odometer_meters is not None and distance_meters is not None:
        end_odometer_meters = start_odometer_meters + distance_meters

    end_run_seconds = None
    if start_run_seconds is not None and duration_seconds is not None:
        end_run_seconds = start_run_seconds + duration_seconds

    return {
        "trip_id": raw_trip_row["trip_id"],
        "asset_id": raw_trip_row["asset_id"],
        "start_time_utc": raw_trip_row["start_time_utc"],
        "end_time_utc": raw_trip_row["end_time_utc"],
        "duration_seconds": duration_seconds,
        "distance_meters": distance_meters,
        "distance_km": _safe_div(distance_meters, 1000),
        "stop_time_seconds": stop_time_seconds,
        "idle_time_seconds": idle_time_seconds,
        "time_in_motion_seconds": time_in_motion_seconds,
        "start_odometer_meters": start_odometer_meters,
        "start_odometer_km": _safe_div(start_odometer_meters, 1000),
        "end_odometer_meters": end_odometer_meters,
        "end_odometer_km": _safe_div(end_odometer_meters, 1000),
        "start_run_seconds": start_run_seconds,
        "end_run_seconds": end_run_seconds,
        "runtime_start_hrs": _safe_div(start_run_seconds, 3600),
        "runtime_end_hrs": _safe_div(end_run_seconds, 3600),
        "driver_name": raw_trip_row["driver_name"],
        "driver_code": raw_trip_row["driver_code"],
        "start_geofence_name": raw_trip_row["start_geofence_name"],
    }


def build_raw_assets_df(assets_records: list[dict]) -> pd.DataFrame:
    """Build raw.ezytrack_assets rows from raw EzyTrack asset records."""
    rows = [_extract_raw_asset(asset) for asset in assets_records]
    return pd.DataFrame(rows, columns=RAW_ASSETS_COLUMNS)


def build_dim_assets_df(raw_assets_df: pd.DataFrame) -> pd.DataFrame:
    """Build staging.ezytrack_dim_assets from raw.ezytrack_assets rows.

    Phase 1 has only one asset source, so the staging asset master is
    currently identical to raw -- same relationship as
    staging.sendem_dim_assets to raw.sendem_assets.
    """
    return raw_assets_df.copy()


def build_raw_trips_df(trips_records: list[dict]) -> pd.DataFrame:
    """Build raw.ezytrack_trips rows from raw EzyTrack trip records."""
    rows = [_extract_raw_trip(trip) for trip in trips_records]
    return pd.DataFrame(rows, columns=RAW_TRIPS_COLUMNS)


def build_fact_trips_df(raw_trips_df: pd.DataFrame) -> pd.DataFrame:
    """Build staging.ezytrack_fact_trips rows from raw.ezytrack_trips rows."""
    rows = [_compute_fact_trip(row) for row in raw_trips_df.to_dict(orient="records")]
    return pd.DataFrame(rows, columns=FACT_TRIPS_COLUMNS)


def build_all(assets_records: list[dict], trips_records: list[dict]) -> dict[str, pd.DataFrame]:
    """Build every EzyTrack DataFrame from raw API records.

    `assets_records` and `trips_records` are the raw lists returned by
    connectors.ezytrack_client.fetch_ezytrack_assets() /
    fetch_ezytrack_trips() -- untouched GraphQL node dicts.
    """
    raw_assets_df = build_raw_assets_df(assets_records)
    raw_trips_df = build_raw_trips_df(trips_records)
    dim_assets_df = build_dim_assets_df(raw_assets_df)
    fact_trips_df = build_fact_trips_df(raw_trips_df)

    return {
        "raw_assets_df": raw_assets_df,
        "raw_trips_df": raw_trips_df,
        "dim_assets_df": dim_assets_df,
        "fact_trips_df": fact_trips_df,
    }


def _smoke_test() -> None:
    """Fetch a tiny live window and run build_all() over it, printing shapes only.

    Not called on import -- only runs via `python -m transforms.ezytrack_transform`.
    Makes live API calls (via the connector) but never writes to PostgreSQL.
    """
    from datetime import timedelta

    from utils.dates import format_utc_iso, utc_now

    from connectors.ezytrack_client import fetch_ezytrack_assets, fetch_ezytrack_trips

    assets_records = fetch_ezytrack_assets()

    end_time = utc_now()
    start_time = end_time - timedelta(hours=1)
    trips_records = fetch_ezytrack_trips(
        format_utc_iso(start_time),
        format_utc_iso(end_time),
        page_size=50,
    )

    dataframes = build_all(assets_records, trips_records)

    print()
    for name, df in dataframes.items():
        print(f"{name}: shape={df.shape}")
        print(f"  columns={list(df.columns)}")


if __name__ == "__main__":
    _smoke_test()
