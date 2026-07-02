"""Centralized environment configuration for the telemetry ETL project.

All secrets and environment-specific values must be read here via environment
variables (loaded from a `.env` file). Never hardcode credentials in code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

DEFAULT_INSIGHTS_API_URL = "https://insights.za.mixtelematics.com"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_SYNC_LOOKBACK_DAYS = 7
DEFAULT_TELEMATICS_LOOKBACK_DAYS = 7
DEFAULT_TELEMATICS_PAGE_SIZE = 50
DEFAULT_TELEMATICS_LOOKBACK_HOURS = 6
DEFAULT_TELEMATICS_CHUNK_HOURS = 1

REQUIRED_ENV_VARS = (
    "SENDEM_API_KEY",
    "SENDEM_GROUP_ID",
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)

REQUIRED_EZYTRACK_ENV_VARS = (
    "TELEMATICS_GRAPHQL_URL",
    "TELEMATICS_TOKEN",
    "TELEMATICS_ORGANISATION_ID",
)


@dataclass(frozen=True)
class Settings:
    """All configuration values required to run the Sendem/MiX sync job."""

    insights_api_url: str
    sendem_api_key: str
    sendem_group_id: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    sync_lookback_days: int


def get_settings() -> Settings:
    """Load, validate, and return all settings from environment variables.

    Loads `.env` via `python-dotenv`, applies defaults for optional values,
    and raises `ValueError` if any required value is missing.
    """
    load_dotenv()

    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return Settings(
        insights_api_url=os.getenv("INSIGHTS_API_URL", DEFAULT_INSIGHTS_API_URL),
        sendem_api_key=os.environ["SENDEM_API_KEY"],
        sendem_group_id=int(os.environ["SENDEM_GROUP_ID"]),
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.getenv("POSTGRES_PORT", DEFAULT_POSTGRES_PORT)),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        sync_lookback_days=int(os.getenv("SYNC_LOOKBACK_DAYS", DEFAULT_SYNC_LOOKBACK_DAYS)),
    )


@dataclass(frozen=True)
class EzytrackSettings:
    """Configuration required to call the EzyTrack / Telematics Guru GraphQL API."""

    graphql_url: str
    token: str
    organisation_id: int
    lookback_days: int
    page_size: int
    lookback_hours: int
    chunk_hours: int


def get_ezytrack_settings() -> EzytrackSettings:
    """Load, validate, and return EzyTrack settings from environment variables.

    Loads `.env` via `python-dotenv` and raises `ValueError` if any required
    value is missing. The GraphQL URL, token, and organisation id have no
    defaults (environment/tenant specific).

    lookback_days (default 7) is the older day-granularity window, currently
    unused now that jobs/sync_ezytrack.py runs in conservative chunked mode.
    It's kept rather than removed since it's expected to be used again once
    the provider confirms how its GraphQL cost limit works.

    lookback_hours (default 6) and chunk_hours (default 1) drive the current
    conservative sync mode: fetch the last `lookback_hours` hours of trips in
    `chunk_hours`-sized windows, one GraphQL request window at a time.

    page_size (default 50) is shared by both modes.
    """
    load_dotenv()

    missing = [name for name in REQUIRED_EZYTRACK_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return EzytrackSettings(
        graphql_url=os.environ["TELEMATICS_GRAPHQL_URL"],
        token=os.environ["TELEMATICS_TOKEN"],
        organisation_id=int(os.environ["TELEMATICS_ORGANISATION_ID"]),
        lookback_days=int(os.getenv("TELEMATICS_LOOKBACK_DAYS", DEFAULT_TELEMATICS_LOOKBACK_DAYS)),
        page_size=int(os.getenv("TELEMATICS_PAGE_SIZE", DEFAULT_TELEMATICS_PAGE_SIZE)),
        lookback_hours=int(os.getenv("TELEMATICS_LOOKBACK_HOURS", DEFAULT_TELEMATICS_LOOKBACK_HOURS)),
        chunk_hours=int(os.getenv("TELEMATICS_CHUNK_HOURS", DEFAULT_TELEMATICS_CHUNK_HOURS)),
    )
