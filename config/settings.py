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

REQUIRED_ENV_VARS = (
    "SENDEM_API_KEY",
    "SENDEM_GROUP_ID",
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
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
