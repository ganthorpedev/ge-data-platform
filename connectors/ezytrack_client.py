"""GraphQL client for the EzyTrack / Telematics Guru API.

Auth style, query shape, and pagination pattern are taken directly from the
working prototype notebook (EzyTrack/telematics_data.ipynb: run_graphql(),
ASSETS_QUERY, TRIPS_QUERY, fetch_trips()). This module is phase-1 scope only
(assets + trips) and intentionally omits richer fields (fuel, events,
geofence history, driver master, per-asset deep queries) to keep GraphQL
queries cheap against the API's cost-based rate limit.

This module returns raw API records only. It does not shape data into
database columns (that belongs in transforms/ezytrack_transform.py), does
not convert timestamps, and does not write to PostgreSQL.

Unlike the prototype notebook, fetch_ezytrack_trips() does not swallow a
GraphQL cost-limit hit and return partial data -- it raises RateLimitError
with the failed window attached, so callers can never mistake a partial
fetch for a complete one. Callers are expected to fetch trips in small
time chunks (see jobs/sync_ezytrack.py) to keep each individual request
cheap and to bound how much progress is lost if one chunk fails.
"""

from __future__ import annotations

from typing import Any

import requests

from config.settings import EzytrackSettings, get_ezytrack_settings

ASSETS_QUERY = """
query GetAssets($organisationId: Int!) {
  assets(organisationId: $organisationId) {
    nodes {
      assetId
      assetCode
      name
      description
      isEnabled
      lastConnectedUtc
      department {
        name
      }
      project {
        name
      }
      allocatedDriver {
        name
        driverCode
      }
      geoFence {
        name
      }
    }
  }
}
"""

TRIPS_QUERY = """
query GetTripsForReport(
  $organisationId: Int!,
  $startDateUtc: DateTime,
  $endDateUtc: DateTime,
  $first: Int,
  $after: String
) {
  trips(
    organisationId: $organisationId,
    startDateUtc: $startDateUtc,
    endDateUtc: $endDateUtc,
    first: $first,
    after: $after,
    order: [{ startTimeUtc: ASC }]
  ) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      tripId
      startTimeUtc
      endTimeUtc
      durationSeconds
      distanceMeters
      stopTimeSeconds
      idleTimeSeconds
      startOdometerReadingMeters
      startRunSeconds
      asset {
        assetId
      }
      driver {
        name
        driverCode
      }
      startGeoFence {
        name
      }
    }
  }
}
"""


class RateLimitError(Exception):
    """Raised when the EzyTrack API rejects a request for exceeding its GraphQL cost limit.

    Carries the failed chunk's window, page size, and how many records were
    already fetched before the failure, so callers get a precise, actionable
    error instead of a bare message.
    """

    def __init__(
        self,
        message: str,
        *,
        chunk_start: str | None = None,
        chunk_end: str | None = None,
        page_size: int | None = None,
        records_fetched: int | None = None,
    ) -> None:
        details = []
        if chunk_start is not None:
            details.append(f"chunk_start={chunk_start}")
        if chunk_end is not None:
            details.append(f"chunk_end={chunk_end}")
        if page_size is not None:
            details.append(f"page_size={page_size}")
        if records_fetched is not None:
            details.append(f"records_fetched_before_failure={records_fetched}")

        full_message = f"{message} ({', '.join(details)})" if details else message
        super().__init__(full_message)

        self.chunk_start = chunk_start
        self.chunk_end = chunk_end
        self.page_size = page_size
        self.records_fetched = records_fetched


def _run_graphql(query: str, variables: dict[str, Any], settings: EzytrackSettings) -> dict[str, Any]:
    """POST a GraphQL query to EzyTrack and return the parsed response body.

    Raises `RateLimitError` if the API reports a cost-limit error, `RuntimeError`
    for any other GraphQL error, and `requests.HTTPError` for a failed HTTP
    request (via `raise_for_status`).
    """
    headers = {
        "Authorization": f"Bearer {settings.token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.post(
        settings.graphql_url,
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("errors"):
        for error in data["errors"]:
            error_code = (error.get("extensions") or {}).get("code")
            if error_code == "GRAPHQL_COST_RATE_LIMIT_EXCEEDED":
                raise RateLimitError(error.get("message", "EzyTrack GraphQL cost limit reached."))
        raise RuntimeError(f"EzyTrack GraphQL error(s): {data['errors']}")

    return data


def fetch_ezytrack_assets(settings: EzytrackSettings | None = None) -> list[dict[str, Any]]:
    """Fetch all EzyTrack assets for the configured organisation.

    Returns the raw `assets.nodes` list exactly as returned by the API.
    """
    settings = settings or get_ezytrack_settings()

    result = _run_graphql(ASSETS_QUERY, {"organisationId": settings.organisation_id}, settings)
    assets = result["data"]["assets"]["nodes"] or []

    print(f"Fetched {len(assets)} EzyTrack asset(s).")
    return assets


def fetch_ezytrack_trips(
    start_time_utc: str,
    end_time_utc: str,
    page_size: int = 50,
    settings: EzytrackSettings | None = None,
) -> list[dict[str, Any]]:
    """Fetch EzyTrack trips between `start_time_utc` and `end_time_utc`.

    `start_time_utc`/`end_time_utc` are ISO-8601 UTC strings (e.g.
    "2026-06-05T04:00:00Z"), passed straight through to the API -- no
    timestamp conversion happens here.

    Pages through the API using its cursor (`pageInfo.endCursor`) exactly as
    the prototype's fetch_trips() does. Unlike the prototype, this does NOT
    swallow a cost-limit hit: if `RateLimitError` occurs mid-pagination, it
    is re-raised (enriched with this window, page_size, and how many records
    were already fetched) rather than returning partial data silently.
    Callers must not treat a partial fetch as success.

    Returns the raw `trips.nodes` records exactly as returned by the API.
    """
    settings = settings or get_ezytrack_settings()

    all_trips: list[dict[str, Any]] = []
    after: str | None = None

    while True:
        variables = {
            "organisationId": settings.organisation_id,
            "startDateUtc": start_time_utc,
            "endDateUtc": end_time_utc,
            "first": page_size,
            "after": after,
        }

        try:
            result = _run_graphql(TRIPS_QUERY, variables, settings)
        except RateLimitError as error:
            raise RateLimitError(
                str(error),
                chunk_start=start_time_utc,
                chunk_end=end_time_utc,
                page_size=page_size,
                records_fetched=len(all_trips),
            ) from error

        page = result["data"]["trips"]
        nodes = page["nodes"] or []
        all_trips.extend(nodes)
        print(f"Fetched {len(nodes)} trip(s). Total so far: {len(all_trips)}")

        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]

    return all_trips
