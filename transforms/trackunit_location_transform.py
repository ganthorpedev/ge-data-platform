"""Trackunit location enrichment V1 -- pure transform functions only.

Separate module from transforms/trackunit_transform.py (the proven metric
ETL) on purpose: nothing here is imported by, or changes the behavior of,
the existing work-day/operating/moving/distance calculation.

Proven in Manitou/manitou_trackunit_exploration.ipynb (machine 5986, report
date 2026-07-05): the AEMP historical Locations time-series returns a
lowercase `"location"` key -- a list of {Latitude, Longitude, Altitude,
AltitudeUnits, datetime} -- with NO address/zip/city/country fields at all.
Site History resolves a zone *name* (via a separate site-detail lookup) that
is text and can be compared directly, but a site's own address is not the
same thing as the activity's location and must not be treated as such.

Address/zip/city/country columns exist in staging.trackunit_location_enrichment
but this module never populates them -- they are always None in V1. Do not
add reverse geocoding here; do not fill them from the current-location
endpoint (which reflects "now", not the historical report date).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

RAW_LOCATION_COLUMNS = [
    "asset_id", "location_timestamp_utc", "latitude", "longitude",
    "altitude", "altitude_units", "boundary_type", "report_date",
]
RAW_SITE_HISTORY_COLUMNS = ["asset_id", "site_id", "entered_at", "left_at"]
RAW_SITE_COLUMNS = ["site_id", "site_name", "city", "country", "polygon_geojson"]
ENRICHMENT_COLUMNS = [
    "report_date", "asset_id",
    "zone_name_start", "zone_name_stop",
    "last_known_start_location_timestamp_utc", "last_known_start_location_timestamp_local",
    "start_latitude", "start_longitude", "start_altitude",
    "last_known_stop_location_timestamp_utc", "last_known_stop_location_timestamp_local",
    "stop_latitude", "stop_longitude", "stop_altitude",
    "address_start", "zip_start", "city_start", "country_start",
    "address_stop", "zip_stop", "city_stop", "country_stop",
    "location_enrichment_status",
]

STATUS_NOT_APPLICABLE = "NOT_APPLICABLE_ZERO_ACTIVITY"
STATUS_ENRICHED = "ENRICHED"
STATUS_PARTIAL = "PARTIAL"
STATUS_NOT_FOUND = "NOT_FOUND"


def extract_location_points(aemp_response: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the raw location point list from an AEMP Locations response.

    The confirmed response shape (see the exploration notebook, section 42D)
    is `{"links": [...], "location": [{...}, ...]}` -- lowercase `"location"`,
    not the `"Locations"` metric-name path segment used in the URL.
    """
    if isinstance(aemp_response, list):
        return aemp_response
    if isinstance(aemp_response, dict):
        return aemp_response.get("location") or []
    return []


def latest_point_at_or_before(points: list[dict[str, Any]], boundary_utc: datetime) -> dict[str, Any] | None:
    """Return the latest point with datetime <= boundary_utc, or None if none qualify.

    Points are expected to already come from a window ending at boundary_utc
    (see jobs/sync_trackunit_location_enrichment.py), so this filter is a
    defensive double-check, not the primary mechanism.
    """
    if not points:
        return None

    boundary_ts = pd.Timestamp(boundary_utc)
    if boundary_ts.tzinfo is None:
        boundary_ts = boundary_ts.tz_localize("UTC")

    qualifying = []
    for point in points:
        point_ts = pd.Timestamp(point["datetime"])
        if point_ts.tzinfo is None:
            point_ts = point_ts.tz_localize("UTC")
        if point_ts <= boundary_ts:
            qualifying.append((point_ts, point))

    if not qualifying:
        return None

    qualifying.sort(key=lambda pair: pair[0])
    return qualifying[-1][1]


def find_active_site_id(site_history_intervals: list[dict[str, Any]], boundary_utc: datetime) -> str | None:
    """Return the siteId whose [enteredAt, leftAt) interval contains boundary_utc.

    `leftAt` of None means the assignment is still open (treated as
    extending to +infinity). Returns None if no interval contains the
    boundary.
    """
    boundary_ts = pd.Timestamp(boundary_utc)
    if boundary_ts.tzinfo is None:
        boundary_ts = boundary_ts.tz_localize("UTC")

    for interval in site_history_intervals:
        entered_at = pd.Timestamp(interval["enteredAt"])
        if entered_at.tzinfo is None:
            entered_at = entered_at.tz_localize("UTC")

        left_at_raw = interval.get("leftAt")
        left_at = pd.Timestamp(left_at_raw) if left_at_raw else None
        if left_at is not None and left_at.tzinfo is None:
            left_at = left_at.tz_localize("UTC")

        if entered_at <= boundary_ts and (left_at is None or boundary_ts <= left_at):
            return interval["siteId"]

    return None


def build_raw_location_rows(
    asset_id: str, report_date: date, boundary_type: str, points: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Shape raw AEMP location points for raw.trackunit_aemp_locations."""
    rows = []
    for point in points:
        rows.append({
            "asset_id": asset_id,
            "location_timestamp_utc": point["datetime"],
            "latitude": point.get("Latitude"),
            "longitude": point.get("Longitude"),
            "altitude": point.get("Altitude"),
            "altitude_units": point.get("AltitudeUnits"),
            "boundary_type": boundary_type,
            "report_date": report_date,
        })
    return rows


def build_raw_site_history_rows(asset_id: str, site_history_intervals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shape raw Site History intervals for raw.trackunit_site_history."""
    return [
        {
            "asset_id": asset_id,
            "site_id": interval["siteId"],
            "entered_at": interval["enteredAt"],
            "left_at": interval.get("leftAt"),
        }
        for interval in site_history_intervals
    ]


def build_raw_site_row(site_id: str, site_detail: dict[str, Any]) -> dict[str, Any]:
    """Shape one resolved site detail for raw.trackunit_sites."""
    polygon = site_detail.get("polygon") or {}
    return {
        "site_id": site_id,
        "site_name": site_detail.get("name"),
        "city": site_detail.get("city"),
        "country": site_detail.get("country"),
        "polygon_geojson": str(polygon.get("geometry")) if polygon.get("geometry") else None,
    }


def point_in_polygon(longitude: float, latitude: float, polygon_coordinates: list[list[float]]) -> bool:
    """Ray-casting point-in-polygon test, proven in the exploration notebook (42G).

    `polygon_coordinates` is a list of [lon, lat] pairs (GeoJSON winding).
    Not required for the enrichment row itself -- available for validation/
    spot-checking a resolved zone against the site's actual boundary.
    """
    inside = False
    n = len(polygon_coordinates)
    j = n - 1
    for i in range(n):
        xi, yi = polygon_coordinates[i]
        xj, yj = polygon_coordinates[j]
        if ((yi > latitude) != (yj > latitude)) and (
            longitude < (xj - xi) * (latitude - yi) / (yj - yi) + xi
        ):
            inside = not inside
        j = i
    return inside


def _to_local(utc_value: Any, tz_name: str) -> datetime | None:
    if utc_value is None:
        return None
    ts = pd.Timestamp(utc_value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(ZoneInfo(tz_name)).to_pydatetime().replace(tzinfo=None)


def compute_enrichment_status(
    had_boundaries: bool,
    start_point_found: bool,
    stop_point_found: bool,
    start_zone_found: bool,
    stop_zone_found: bool,
) -> str:
    """Decide the one location_enrichment_status value for a row.

    NOT_APPLICABLE_ZERO_ACTIVITY: the daily activity row itself had no
    start/stop boundaries (a valid zero-activity day, not an error -- see
    transforms/trackunit_transform.py's zero-row rule).
    ENRICHED: both a location point and a zone were found for both boundaries.
    NOT_FOUND: neither a location point nor a zone was found for either boundary.
    PARTIAL: anything in between (e.g. location found but zone missing, or
    only one boundary resolved).
    """
    if not had_boundaries:
        return STATUS_NOT_APPLICABLE

    found_flags = [start_point_found, stop_point_found, start_zone_found, stop_zone_found]
    if all(found_flags):
        return STATUS_ENRICHED
    if not any(found_flags):
        return STATUS_NOT_FOUND
    return STATUS_PARTIAL


def build_enrichment_row(
    report_date: date,
    asset_id: str,
    timezone_name: str,
    had_boundaries: bool,
    start_point: dict[str, Any] | None,
    stop_point: dict[str, Any] | None,
    start_zone_name: str | None,
    stop_zone_name: str | None,
) -> dict[str, Any]:
    """Build one staging.trackunit_location_enrichment row.

    Address/zip/city/country are always None -- see module docstring.
    """
    status = compute_enrichment_status(
        had_boundaries=had_boundaries,
        start_point_found=start_point is not None,
        stop_point_found=stop_point is not None,
        start_zone_found=start_zone_name is not None,
        stop_zone_found=stop_zone_name is not None,
    )

    start_ts_utc = start_point["datetime"] if start_point else None
    stop_ts_utc = stop_point["datetime"] if stop_point else None

    return {
        "report_date": report_date,
        "asset_id": asset_id,
        "zone_name_start": start_zone_name,
        "zone_name_stop": stop_zone_name,
        "last_known_start_location_timestamp_utc": start_ts_utc,
        "last_known_start_location_timestamp_local": _to_local(start_ts_utc, timezone_name),
        "start_latitude": start_point.get("Latitude") if start_point else None,
        "start_longitude": start_point.get("Longitude") if start_point else None,
        "start_altitude": start_point.get("Altitude") if start_point else None,
        "last_known_stop_location_timestamp_utc": stop_ts_utc,
        "last_known_stop_location_timestamp_local": _to_local(stop_ts_utc, timezone_name),
        "stop_latitude": stop_point.get("Latitude") if stop_point else None,
        "stop_longitude": stop_point.get("Longitude") if stop_point else None,
        "stop_altitude": stop_point.get("Altitude") if stop_point else None,
        "address_start": None,
        "zip_start": None,
        "city_start": None,
        "country_start": None,
        "address_stop": None,
        "zip_stop": None,
        "city_stop": None,
        "country_stop": None,
        "location_enrichment_status": status,
    }
