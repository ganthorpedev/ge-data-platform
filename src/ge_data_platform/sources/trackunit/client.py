"""HTTP client for the Trackunit / Manitou IRIS + AEMP APIs.

Auth style, asset pagination, and AEMP URL pattern are taken directly from
the proven exploration notebook (Manitou/manitou_trackunit_exploration.ipynb).

This module is the only place that should know how to talk to Trackunit.
Transform and loader modules must not make HTTP calls directly. It returns
raw API records/JSON only -- it does not shape data into database columns
(that belongs in transforms/trackunit_transform.py) and does not write to
PostgreSQL.

Retry policy (all handled here, so callers never sleep or retry themselves):

- 401 on any authenticated request: refresh the OAuth token and retry that
  request exactly once. A second 401 raises a descriptive RuntimeError --
  there is no unlimited auth loop.
- 429 on any authenticated request: wait using the server's Retry-After
  header (seconds form) when present and valid, otherwise exponential
  backoff from `rate_limit_base_delay_seconds`; either way capped at
  `rate_limit_max_delay_seconds` plus 0-3s random jitter. `max_retries` is
  treated as the maximum total attempts (default 7), including the first.
- Transient failures (connection errors, timeouts, HTTP 500/502/503/504):
  up to HTTP_MAX_RETRIES total attempts (default 3) with exponential
  backoff from HTTP_BACKOFF_SECONDS. This client deliberately does NOT use
  ge_data_platform.common.http's adapter-level retries, so retries are never
  doubled up.
- Any other non-200 (403 on get_site excepted) raises immediately. Ordinary
  4xx responses are never retried.
"""

from __future__ import annotations

import base64
import logging
import random
import time
from typing import Any

import requests

from ge_data_platform.config.settings import (
    HttpSettings,
    TrackunitSettings,
    get_http_settings,
    get_trackunit_settings,
)

TRANSIENT_STATUS_CODES = (500, 502, 503, 504)

logger = logging.getLogger(__name__)

# Maps the AEMP path segment (metric_name) to the JSON key it returns under.
SUPPORTED_METRICS = {
    "CumulativeOperatingHours": "cumulativeOperatingHours",
    "CumulativeMovingHours": "cumulativeMovingHours",
    "Distance": "distance",
}


def _parse_retry_after_seconds(header_value: str | None) -> float | None:
    """Parse an HTTP `Retry-After` header as a non-negative number of seconds.

    Returns None if the header is missing, not a plain number (e.g. an
    HTTP-date), or negative -- callers should fall back to exponential
    backoff in that case.
    """
    if not header_value:
        return None
    try:
        seconds = float(header_value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def _rate_limit_wait_seconds(
    retry_after_header: str | None,
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> float:
    """Compute how long to wait before retrying a 429, in seconds.

    Prefers the server's `Retry-After` header when present and valid;
    otherwise falls back to exponential backoff from `base_delay_seconds`
    (attempt 1 -> base, attempt 2 -> base*2, ...). Either way the wait is
    capped at `max_delay_seconds` and then given 0-3 seconds of random
    jitter, so concurrent callers don't retry in lockstep.
    """
    retry_after = _parse_retry_after_seconds(retry_after_header)
    if retry_after is not None:
        wait_seconds = retry_after
    else:
        wait_seconds = base_delay_seconds * (2 ** (attempt - 1))

    wait_seconds = min(wait_seconds, max_delay_seconds)
    return wait_seconds + random.uniform(0, 3)


class TrackunitSiteAccessDeniedError(RuntimeError):
    """Raised only for a 403 on GET /sites/{site_id} -- non-fatal to callers.

    A 403 here means this account cannot see that specific site, not that
    auth is broken (that's a 401, still raised as a plain RuntimeError).
    Callers should catch this specifically and continue enrichment without
    failing the whole run -- see jobs/sync_trackunit_location_enrichment.py.
    """

    def __init__(self, site_id: str) -> None:
        self.site_id = site_id
        super().__init__(f"Trackunit get_site access denied (403) for site_id={site_id}")


class TrackunitClient:
    """Thin wrapper around the Trackunit IRIS asset API and AEMP time-series API."""

    def __init__(
        self,
        settings: TrackunitSettings | None = None,
        http_settings: HttpSettings | None = None,
    ) -> None:
        """Store settings and prepare a `requests.Session` for all calls."""
        self.settings = settings or get_trackunit_settings()
        self.http_settings = http_settings or get_http_settings()
        self.session = requests.Session()
        self._access_token: str | None = None

    def _request_transient_retry(self, method: str, url: str, *, context: str, **kwargs: Any) -> requests.Response:
        """Issue one HTTP request, retrying only transient failures.

        Transient means: connection errors, timeouts, and HTTP 500/502/503/504.
        Up to `http_settings.max_retries` total attempts (default 3) with
        exponential backoff from `http_settings.backoff_seconds`. Anything
        else (including every ordinary 4xx) is returned to the caller
        untouched -- 401/429 policy lives in `_authorized_get`, not here.
        """
        kwargs.setdefault("timeout", self.http_settings.timeout)
        max_attempts = max(1, self.http_settings.max_retries)
        attempt = 0

        while True:
            attempt += 1
            try:
                response = self.session.request(method, url, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as error:
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Trackunit request failed after {attempt} attempt(s) ({context}): "
                        f"{type(error).__name__}"
                    ) from error
                wait_seconds = self.http_settings.backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Trackunit transient network failure type=%s context=%s "
                    "attempt=%s/%s wait_seconds=%.1f",
                    type(error).__name__,
                    context,
                    attempt,
                    max_attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            if response.status_code in TRANSIENT_STATUS_CODES:
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Trackunit request failed with HTTP {response.status_code} after "
                        f"{attempt} attempt(s) ({context}): {response.text[:500]}"
                    )
                wait_seconds = self.http_settings.backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Trackunit transient HTTP failure status=%s context=%s "
                    "attempt=%s/%s wait_seconds=%.1f",
                    response.status_code,
                    context,
                    attempt,
                    max_attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            return response

    def authenticate(self) -> str:
        """Request a new OAuth token via the Trackunit password flow and store it.

        Returns the access token. Raises `RuntimeError` on a failed auth request.
        """
        basic_credentials = base64.b64encode(
            f"{self.settings.client_id}:{self.settings.client_secret}".encode()
        ).decode()

        response = self._request_transient_retry(
            "POST",
            self.settings.token_url,
            context="authenticate",
            headers={
                "Authorization": f"Basic {basic_credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "password",
                "username": self.settings.username,
                "password": self.settings.password,
                "scope": self.settings.scope,
            },
        )

        if not response.ok:
            raise RuntimeError(f"Trackunit authentication failed: {response.status_code} {response.text[:500]}")

        self._access_token = response.json()["access_token"]
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        """Return Bearer auth headers, authenticating first if no token is cached yet."""
        if self._access_token is None:
            self.authenticate()
        return {"Authorization": f"Bearer {self._access_token}", "Accept": "application/json"}

    def _authorized_get(
        self,
        url: str,
        *,
        context: str,
        params: dict[str, Any] | None = None,
        metric: str | None = None,
        pin: str | None = None,
    ) -> requests.Response:
        """GET `url` with auth, refreshing the token once on a 401.

        Every authenticated Trackunit GET goes through here. On a 401 the
        OAuth token is refreshed and the request retried exactly once; a
        second 401 raises a descriptive error (no unlimited auth loop). Every
        429 is retried with Retry-After / exponential backoff, with
        `settings.max_retries` interpreted as the maximum TOTAL number of 429
        attempts (not retries after the first request). Transient failures are
        retried by `_request_transient_retry` underneath.
        """
        token_refreshed = False
        rate_limit_attempt = 0

        while True:
            response = self._request_transient_retry(
                "GET", url, context=context, headers=self._auth_headers(), params=params
            )

            if response.status_code == 401:
                if token_refreshed:
                    raise RuntimeError(
                        f"Trackunit request still unauthorized (401) after a token refresh ({context}). "
                        "Check TRACKUNIT_* credentials."
                    )
                logger.warning(
                    "Trackunit token expired status=401 context=%s; refreshing token and retrying once",
                    context,
                )
                self.authenticate()
                token_refreshed = True
                continue

            if response.status_code == 429:
                rate_limit_attempt += 1
                max_attempts = max(1, self.settings.max_retries)
                if rate_limit_attempt >= max_attempts:
                    raise RuntimeError(
                        f"Trackunit rate limited (429) after {rate_limit_attempt} attempt(s) "
                        f"(metric={metric or '-'}, pin={pin or '-'}, context={context}): "
                        f"{response.text[:500]}"
                    )
                wait_seconds = _rate_limit_wait_seconds(
                    response.headers.get("Retry-After"),
                    rate_limit_attempt,
                    self.settings.rate_limit_base_delay_seconds,
                    self.settings.rate_limit_max_delay_seconds,
                )
                logger.warning(
                    "Trackunit rate limited status=429 metric=%s pin=%s context=%s "
                    "attempt=%s/%s wait_seconds=%.1f",
                    metric or "-",
                    pin or "-",
                    context,
                    rate_limit_attempt,
                    max_attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            return response

    def get_assets(self, page: int = 0, size: int = 100) -> dict[str, Any]:
        """Fetch one page of assets from GET /v1/assets. Returns the raw paginated envelope.

        Refreshes the token once and retries on a 401 (expired token) before
        raising. Raises `RuntimeError` on any other non-200 response.
        """
        url = f"{self.settings.asset_base_url}/v1/assets"
        response = self._authorized_get(
            url, context=f"get_assets page={page}", params={"page": page, "size": size}
        )

        if not response.ok:
            raise RuntimeError(f"Trackunit get_assets failed: {response.status_code} {response.text[:500]}")

        return response.json()

    def get_all_assets(self) -> list[dict[str, Any]]:
        """Fetch every asset page-by-page (size 100) and return the combined list."""
        all_assets: list[dict[str, Any]] = []
        page = 0

        while True:
            page_json = self.get_assets(page=page, size=100)
            page_assets = page_json.get("content") or []

            if not page_assets:
                break

            all_assets.extend(page_assets)
            print(f"Fetched Trackunit assets page {page}: {len(page_assets)} asset(s), total so far: {len(all_assets)}")

            total_pages = page_json.get("totalPages")
            if total_pages is not None and page >= total_pages - 1:
                break
            page += 1

        return all_assets

    def get_aemp_series(
        self,
        pin: str,
        metric_name: str,
        metric_key: str,
        start_utc: str,
        end_utc: str,
        page: int = 1,
    ) -> dict[str, Any]:
        """Fetch one AEMP time-series metric page for `pin` between start_utc/end_utc.

        `metric_name` is the AEMP URL path segment (e.g. "CumulativeOperatingHours");
        `metric_key` is the JSON key expected in the response (e.g.
        "cumulativeOperatingHours") -- see `SUPPORTED_METRICS`. `start_utc`/
        `end_utc` are ISO-8601 UTC strings, passed straight through -- no
        timestamp conversion happens here. AEMP pagination starts at page 1.

        Paces itself using `TrackunitSettings.request_delay_seconds` (default
        1s) before the request. On a 429, waits using the server's
        `Retry-After` header when present and valid, or exponential backoff
        starting at `rate_limit_base_delay_seconds` (default 30s) otherwise
        -- either way capped at `rate_limit_max_delay_seconds` (default 300s)
        plus 0-3s jitter -- with at most `max_retries` total 429 attempts
        (default 7) before raising a descriptive `RuntimeError`. A 401 mid-run refreshes
        the token and retries once; transient failures (connection errors,
        timeouts, 5xx) are retried up to HTTP_MAX_RETRIES times -- all inside
        `_authorized_get`.

        Raises `RuntimeError` on a 429 that exhausts all retries, or on any
        other non-200 response -- never returns partial/failed data silently.
        """
        url = (
            f"{self.settings.aemp_base_url}/{self.settings.account_id}/Fleet/Equipment/ID/{pin}"
            f"/{metric_name}/{start_utc}/{end_utc}/{page}"
        )


        if self.settings.request_delay_seconds > 0:
            time.sleep(self.settings.request_delay_seconds)

        response = self._authorized_get(
            url,
            context=f"AEMP metric={metric_name} pin={pin}",
            metric=metric_name,
            pin=pin,
        )

        if not response.ok:
            raise RuntimeError(
                f"Trackunit AEMP request failed ({response.status_code}) for {metric_name} "
                f"(pin={pin}): {response.text[:500]}"
            )

        response_json = response.json()
        if isinstance(response_json, dict) and metric_key not in response_json:
            logger.warning(
                "Expected key %s not found in Trackunit AEMP response metric=%s pin=%s keys=%s",
                metric_key,
                metric_name,
                pin,
                list(response_json.keys()),
            )

        return response_json

    def get_site_history(self, asset_id: str, from_time_utc: str, to_time_utc: str) -> dict[str, Any]:
        """Fetch Site History for one asset between from_time_utc/to_time_utc.

        GET {site_base_url}/sites/history?assetIds=...&fromTime=...&toTime=...
        Proven in Manitou/manitou_trackunit_exploration.ipynb (section 42G).
        Returns the raw paginated envelope (`content` is a list of
        {siteId, assetId, enteredAt, leftAt} -- `leftAt` NULL means the
        assignment is still open). Raises `RuntimeError` on any non-200
        response.
        """
        url = f"{self.settings.site_base_url}/sites/history"
        response = self._authorized_get(
            url,
            context=f"get_site_history asset_id={asset_id}",
            params={"assetIds": asset_id, "fromTime": from_time_utc, "toTime": to_time_utc, "page": 0, "size": 50},
        )

        if not response.ok:
            raise RuntimeError(f"Trackunit get_site_history failed: {response.status_code} {response.text[:500]}")

        return response.json()

    def get_site(self, site_id: str) -> dict[str, Any]:
        """Fetch one site's detail (name, city, country, polygon) by id.

        GET {site_base_url}/sites/{site_id}. Site History only returns a
        siteId, not a name, so this resolves it -- proven in the exploration
        notebook (section 42G). Raises `TrackunitSiteAccessDeniedError` on a
        403 (this account cannot see that site -- non-fatal, do not retry)
        and `RuntimeError` on any other non-200 response.
        """
        url = f"{self.settings.site_base_url}/sites/{site_id}"
        response = self._authorized_get(url, context=f"get_site site_id={site_id}")

        if response.status_code == 403:
            raise TrackunitSiteAccessDeniedError(site_id)
        if not response.ok:
            raise RuntimeError(f"Trackunit get_site failed: {response.status_code} {response.text[:500]}")

        return response.json()
