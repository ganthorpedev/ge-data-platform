"""HTTP client for the Sendem/MiX Customer Insights API.

This module is the only place that should know how to talk to the Sendem API.
Transform and loader modules must not make HTTP calls directly.
"""

from __future__ import annotations

import requests

from config.settings import Settings, get_settings


class SendemClient:
    """Thin wrapper around the Sendem/MiX Customer Insights API."""

    def __init__(self, settings: Settings) -> None:
        """Store settings and prepare a `requests.Session` for all calls."""
        self.settings = settings
        self.base_url = settings.insights_api_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-API-KEY": settings.sendem_api_key,
                "Accept": "application/json",
            }
        )

    def get_json(self, endpoint: str, params: dict | None = None) -> list | dict:
        """Perform a GET request against `endpoint` and return the parsed JSON body.

        Raises `RuntimeError` (including the status code and response text) if
        the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params, timeout=60)

        if not response.ok:
            raise RuntimeError(
                f"Sendem API request failed: {response.status_code} {response.url}\n{response.text}"
            )

        return response.json()

    def get_swagger(self) -> dict:
        """Fetch the API's swagger/OpenAPI document."""
        return self.get_json("/swagger/v1/swagger.json")

    def get_assets(self) -> list:
        """Fetch the assets dimension for the configured group."""
        return self.get_json(f"/api/dimensions/assets/{self.settings.sendem_group_id}")

    def get_sites(self) -> list:
        """Fetch the sites dimension for the configured group."""
        return self.get_json(f"/api/dimensions/sites/{self.settings.sendem_group_id}")

    def get_people(self) -> list:
        """Fetch the people dimension for the configured group."""
        return self.get_json(f"/api/dimensions/people/{self.settings.sendem_group_id}")

    def get_organisations(self) -> list:
        """Fetch the organisations dimension for the configured group."""
        return self.get_json(f"/api/dimensions/organisations/{self.settings.sendem_group_id}")

    def get_event_descriptions(self) -> list:
        """Fetch the event descriptions dimension for the configured group."""
        return self.get_json(f"/api/dimensions/event-descriptions/{self.settings.sendem_group_id}")

    def get_trips_assets_daily(self, start_date: int, end_date: int) -> list:
        """Fetch daily trip aggregates for assets within a date range."""
        return self.get_json(
            f"/api/aggregates/trips/assets/{self.settings.sendem_group_id}",
            params={"startDate": start_date, "endDate": end_date},
        )

    def get_events_assets_daily(self, start_date: int, end_date: int) -> list:
        """Fetch daily event aggregates for assets within a date range."""
        return self.get_json(
            f"/api/aggregates/events/assets/{self.settings.sendem_group_id}",
            params={"startDate": start_date, "endDate": end_date},
        )


if __name__ == "__main__":
    settings = get_settings()
    client = SendemClient(settings)
    swagger = client.get_swagger()
    print("Title:", swagger["info"]["title"])
    print("Version:", swagger["info"]["version"])
