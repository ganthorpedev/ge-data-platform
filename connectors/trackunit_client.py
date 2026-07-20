"""HTTP client for the Trackunit / Manitou IRIS + AEMP APIs.

Auth style, asset pagination, and AEMP URL pattern are taken directly from
the proven exploration notebook (Manitou/manitou_trackunit_exploration.ipynb).

This module is the only place that should know how to talk to Trackunit.
Transform and loader modules must not make HTTP calls directly. It returns
raw API records/JSON only -- it does not shape data into database columns
(that belongs in transforms/trackunit_transform.py) and does not write to
PostgreSQL.

Failures are never swallowed: a 429 or any other non-200 response raises a
clear `RuntimeError` immediately. Callers (jobs/sync_trackunit_daily_activity.py)
are responsible for pacing calls (sleeping between AEMP requests) -- this
client makes one request per call and does not retry or rate-limit itself.
"""

from __future__ import annotations

import base64
from typing import Any

import requests

from config.settings import TrackunitSettings, get_trackunit_settings

# Maps the AEMP path segment (metric_name) to the JSON key it returns under.
SUPPORTED_METRICS = {
    "CumulativeOperatingHours": "cumulativeOperatingHours",
    "CumulativeMovingHours": "cumulativeMovingHours",
    "Distance": "distance",
}


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

    def __init__(self, settings: TrackunitSettings | None = None) -> None:
        """Store settings and prepare a `requests.Session` for all calls."""
        self.settings = settings or get_trackunit_settings()
        self.session = requests.Session()
        self._access_token: str | None = None

    def authenticate(self) -> str:
        """Request a new OAuth token via the Trackunit password flow and store it.

        Returns the access token. Raises `RuntimeError` on a failed auth request.
        """
        basic_credentials = base64.b64encode(
            f"{self.settings.client_id}:{self.settings.client_secret}".encode()
        ).decode()

        response = self.session.post(
            self.settings.token_url,
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
            timeout=30,
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

    def get_assets(self, page: int = 0, size: int = 100) -> dict[str, Any]:
        """Fetch one page of assets from GET /v1/assets. Returns the raw paginated envelope.

        Refreshes the token once and retries on a 401 (expired token) before
        raising. Raises `RuntimeError` on any other non-200 response.
        """
        url = f"{self.settings.asset_base_url}/v1/assets"
        response = self.session.get(url, headers=self._auth_headers(), params={"page": page, "size": size}, timeout=30)

        if response.status_code == 401:
            self.authenticate()
            response = self.session.get(url, headers=self._auth_headers(), params={"page": page, "size": size}, timeout=30)

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

        Raises `RuntimeError` on a 429 (rate limited) or any other non-200
        response -- never returns partial/failed data silently.
        """
        url = (
            f"{self.settings.aemp_base_url}/{self.settings.account_id}/Fleet/Equipment/ID/{pin}"
            f"/{metric_name}/{start_utc}/{end_utc}/{page}"
        )

        response = self.session.get(url, headers=self._auth_headers(), timeout=30)

        if response.status_code == 429:
            raise RuntimeError(
                f"Trackunit AEMP rate limited (429) for {metric_name} (pin={pin}): {response.text[:500]}"
            )
        if not response.ok:
            raise RuntimeError(
                f"Trackunit AEMP request failed ({response.status_code}) for {metric_name} "
                f"(pin={pin}): {response.text[:500]}"
            )

        response_json = response.json()
        if isinstance(response_json, dict) and metric_key not in response_json:
            print(
                f"  Warning: expected key '{metric_key}' not found in AEMP response for "
                f"{metric_name} (pin={pin}). Keys present: {list(response_json.keys())}"
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
        response = self.session.get(
            url,
            headers=self._auth_headers(),
            params={"assetIds": asset_id, "fromTime": from_time_utc, "toTime": to_time_utc, "page": 0, "size": 50},
            timeout=30,
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
        response = self.session.get(url, headers=self._auth_headers(), timeout=30)

        if response.status_code == 403:
            raise TrackunitSiteAccessDeniedError(site_id)
        if not response.ok:
            raise RuntimeError(f"Trackunit get_site failed: {response.status_code} {response.text[:500]}")

        return response.json()
