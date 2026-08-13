"""GraphQL client for the EzyTrack / Telematics Guru API.

Auth style, query shape, and pagination pattern are taken directly from the
working prototype notebook (EzyTrack/telematics_data.ipynb: run_graphql(),
ASSETS_QUERY, TRIPS_QUERY, fetch_trips()). This module is phase-1 scope only
(assets + trips) and intentionally omits richer fields (fuel, events,
geofence history, driver master, per-asset deep queries) to keep GraphQL
queries cheap against the API's cost-based rate limit.

This module returns raw API records only. It does not shape data into
database columns (that belongs in ge_data_platform.sources.ezytrack.
transform), does not convert timestamps, and does not write to PostgreSQL.

Unlike the prototype notebook, fetch_ezytrack_trips() does not swallow a
GraphQL cost-limit hit and return partial data -- it raises RateLimitError
with the failed window attached, so callers can never mistake a partial
fetch for a complete one. Callers are expected to fetch trips in small
time chunks (see ge_data_platform.sources.ezytrack.sync) to keep each
individual request cheap and to bound how much progress is lost if one
chunk fails.

Authentication (EzytrackClient.authenticate) is dynamic: Telematics Guru
access tokens expire after roughly 24 hours, so a token fetched once and
pasted into `.env` inevitably goes stale. EzytrackClient fetches a fresh
token at the start of every run and holds it in memory only -- never on
disk, never in the database, never printed or logged. If a request comes
back unauthorized (HTTP 401, or a GraphQL AUTH_NOT_AUTHENTICATED /
UNAUTHENTICATED error -- this API returns the latter, as an HTTP 200 with an
`errors` array, not a raw 401), the client re-authenticates once and retries
the request once. If that retry also fails, the error propagates -- there is
no endless retry loop. This is a distinct failure mode from RateLimitError
below (a GraphQL cost-limit rejection): re-authenticating does not help a
rate limit, and a rate limit is never treated as an auth failure.
"""

from __future__ import annotations

from typing import Any

import requests

from ge_data_platform.common.http import build_retrying_session
from ge_data_platform.config.settings import EzytrackSettings, get_ezytrack_settings, get_http_settings

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


_AUTH_ERROR_CODES = {"AUTH_NOT_AUTHENTICATED", "UNAUTHENTICATED", "UNAUTHORIZED"}


class EzytrackClient:
    """Stateful GraphQL client for EzyTrack / Telematics Guru with dynamic auth.

    Construct once per ETL run, call `authenticate()` once at the start, then
    reuse the same instance for every `fetch_assets()`/`fetch_trips()` call
    in that run. The access token lives only in `self._access_token` (never
    written to disk or the database, never printed/logged); on an
    unauthorized response it is refreshed and the single failed request is
    retried exactly once before giving up.
    """

    def __init__(self, settings: EzytrackSettings | None = None) -> None:
        self.settings = settings or get_ezytrack_settings()
        self._access_token: str | None = None
        self._token_type: str = "Bearer"
        self._expires_in: int | None = None
        # Shared bounded transient-retry session (connection errors, timeouts,
        # 500/502/503/504 only -- never a rate-limit or auth response). POST is
        # explicitly allowed because these GraphQL POSTs are read-only queries,
        # safe to repeat.
        self._http_settings = get_http_settings()
        self._session = build_retrying_session(self._http_settings, allowed_methods=("GET", "POST"))

    @property
    def token_type(self) -> str | None:
        """The token type returned by the auth endpoint (safe to log; not a secret)."""
        return self._token_type

    @property
    def expires_in(self) -> int | None:
        """Seconds until the current token expires, per the auth endpoint (safe to log)."""
        return self._expires_in

    def has_valid_token(self) -> bool:
        """Return True if an access token is currently held in memory."""
        return self._access_token is not None

    def authenticate(self) -> None:
        """Fetch a fresh access token from the Telematics Guru auth endpoint.

        Per the confirmed API contract: GET request, x-www-form-urlencoded
        body of username/password/grant_type. Stores the token in memory
        only. Never logs the username, password, or the token itself --
        only the non-secret token_type/expires_in fields are printed.
        """
        try:
            response = self._session.request(
                "GET",
                self.settings.auth_url,
                data={
                    "username": self.settings.username,
                    "password": self.settings.password,
                    "grant_type": self.settings.grant_type,
                },
                timeout=self._http_settings.timeout,
            )
        except requests.RequestException as error:
            raise RuntimeError(f"EzyTrack authentication request failed: {type(error).__name__}") from None

        if not response.ok:
            raise RuntimeError(f"EzyTrack authentication failed: HTTP {response.status_code}")

        try:
            body = response.json()
        except ValueError:
            raise RuntimeError("EzyTrack authentication failed: response was not valid JSON.") from None

        access_token = body.get("access_token")
        if not access_token:
            raise RuntimeError("EzyTrack authentication failed: no access_token in response.")

        self._access_token = access_token
        self._token_type = body.get("token_type") or "Bearer"
        self._expires_in = body.get("expires_in")
        print(
            f"EzyTrack authentication succeeded (token_type={self._token_type}, "
            f"expires_in={self._expires_in}s)."
        )

    def _auth_headers(self) -> dict[str, str]:
        if not self.has_valid_token():
            self.authenticate()
        return {
            "Authorization": f"{self._token_type} {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _run_graphql(
        self, query: str, variables: dict[str, Any], *, _retried: bool = False
    ) -> dict[str, Any]:
        """POST a GraphQL query to EzyTrack and return the parsed response body.

        Raises `RateLimitError` if the API reports a cost-limit error (never
        retried -- see module docstring), `RuntimeError` for any other
        GraphQL error or an auth failure that survives one retry, and
        `requests.HTTPError` for any other failed HTTP request.
        """
        response = self._session.post(
            self.settings.graphql_url,
            headers=self._auth_headers(),
            json={"query": query, "variables": variables},
            timeout=self._http_settings.timeout,
        )

        if response.status_code == 401:
            if _retried:
                raise RuntimeError("EzyTrack authentication failed after token refresh retry.")
            print("Token refreshed after authorization failure.")
            self.authenticate()
            return self._run_graphql(query, variables, _retried=True)

        response.raise_for_status()
        data = response.json()

        if data.get("errors"):
            for error in data["errors"]:
                error_code = (error.get("extensions") or {}).get("code")
                if error_code == "GRAPHQL_COST_RATE_LIMIT_EXCEEDED":
                    raise RateLimitError(error.get("message", "EzyTrack GraphQL cost limit reached."))
                if error_code in _AUTH_ERROR_CODES:
                    if _retried:
                        raise RuntimeError("EzyTrack authentication failed after token refresh retry.")
                    print("Token refreshed after authorization failure.")
                    self.authenticate()
                    return self._run_graphql(query, variables, _retried=True)
            raise RuntimeError(f"EzyTrack GraphQL error(s): {data['errors']}")

        return data

    def fetch_assets(self) -> list[dict[str, Any]]:
        """Fetch all EzyTrack assets for the configured organisation.

        Returns the raw `assets.nodes` list exactly as returned by the API.
        """
        result = self._run_graphql(ASSETS_QUERY, {"organisationId": self.settings.organisation_id})
        assets = result["data"]["assets"]["nodes"] or []

        print(f"Fetched {len(assets)} EzyTrack asset(s).")
        return assets

    def fetch_trips(
        self, start_time_utc: str, end_time_utc: str, page_size: int = 50
    ) -> list[dict[str, Any]]:
        """Fetch EzyTrack trips between `start_time_utc` and `end_time_utc`.

        `start_time_utc`/`end_time_utc` are ISO-8601 UTC strings (e.g.
        "2026-06-05T04:00:00Z"), passed straight through to the API -- no
        timestamp conversion happens here.

        Pages through the API using its cursor (`pageInfo.endCursor`) exactly
        as the prototype's fetch_trips() does. Does NOT swallow a cost-limit
        hit: if `RateLimitError` occurs mid-pagination, it is re-raised
        (enriched with this window, page_size, and how many records were
        already fetched) rather than returning partial data silently.
        Callers must not treat a partial fetch as success.

        Cursor protection: a repeated `endCursor` or exceeding
        `settings.max_pages` pages (default 500) raises a loud RuntimeError
        instead of looping forever on a misbehaving API.

        Returns the raw `trips.nodes` records exactly as returned by the API.
        """
        if page_size < 1:
            raise ValueError(f"EzyTrack page_size must be at least 1, got {page_size}")
        if self.settings.max_pages < 1:
            raise ValueError(
                f"EzyTrack max_pages must be at least 1, got {self.settings.max_pages}"
            )

        all_trips: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        pages_fetched = 0

        while True:
            if pages_fetched >= self.settings.max_pages:
                raise RuntimeError(
                    f"EzyTrack trips pagination exceeded max_pages={self.settings.max_pages} "
                    f"for window {start_time_utc} to {end_time_utc} "
                    f"({len(all_trips)} record(s) fetched so far). Refusing to loop further -- "
                    "raise EZYTRACK_MAX_PAGES only if this window genuinely has more pages."
                )

            variables = {
                "organisationId": self.settings.organisation_id,
                "startDateUtc": start_time_utc,
                "endDateUtc": end_time_utc,
                "first": page_size,
                "after": after,
            }

            try:
                result = self._run_graphql(TRIPS_QUERY, variables)
            except RateLimitError as error:
                raise RateLimitError(
                    str(error),
                    chunk_start=start_time_utc,
                    chunk_end=end_time_utc,
                    page_size=page_size,
                    records_fetched=len(all_trips),
                ) from error

            pages_fetched += 1
            page = result["data"]["trips"]
            nodes = page["nodes"] or []
            all_trips.extend(nodes)
            print(f"Fetched {len(nodes)} trip(s). Total so far: {len(all_trips)}")

            page_info = page["pageInfo"]
            end_cursor = page_info.get("endCursor")
            if end_cursor is not None and end_cursor in seen_cursors:
                raise RuntimeError(
                    f"EzyTrack trips pagination returned a repeated endCursor "
                    f"({end_cursor!r}) for window {start_time_utc} to {end_time_utc} after "
                    f"{pages_fetched} page(s) -- aborting instead of looping forever."
                )
            if end_cursor is not None:
                seen_cursors.add(end_cursor)

            if not page_info["hasNextPage"]:
                break

            if not end_cursor:
                raise RuntimeError(
                    f"EzyTrack trips pagination returned a null or empty endCursor while "
                    f"hasNextPage=true for window {start_time_utc} to {end_time_utc} after "
                    f"{pages_fetched} page(s) -- aborting instead of looping forever."
                )
            after = end_cursor

        return all_trips


def fetch_ezytrack_assets(settings: EzytrackSettings | None = None) -> list[dict[str, Any]]:
    """Backward-compatible wrapper: authenticate and fetch assets in one call.

    Prefer constructing an `EzytrackClient` directly and reusing it across
    multiple calls within one ETL run (see jobs/sync_ezytrack.py) so
    authentication happens once per run, not once per call.
    """
    client = EzytrackClient(settings)
    client.authenticate()
    return client.fetch_assets()


def fetch_ezytrack_trips(
    start_time_utc: str,
    end_time_utc: str,
    page_size: int = 50,
    settings: EzytrackSettings | None = None,
) -> list[dict[str, Any]]:
    """Backward-compatible wrapper: authenticate and fetch trips in one call.

    Prefer constructing an `EzytrackClient` directly and reusing it across
    multiple calls within one ETL run (see jobs/sync_ezytrack.py) so
    authentication happens once per run, not once per call.
    """
    client = EzytrackClient(settings)
    client.authenticate()
    return client.fetch_trips(start_time_utc, end_time_utc, page_size=page_size)
