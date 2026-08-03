"""Centralized environment configuration for the telemetry ETL project.

All secrets and environment-specific values must be read here via environment
variables (loaded from a `.env` file). Never hardcode credentials in code.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# The canonical .env lives at the telemetry_etl project root (next to this
# package). It is loaded by absolute path so behaviour does not depend on the
# current working directory. Precedence order (highest wins):
#   1. Real environment variables already set on the process.
#   2. The canonical <project root>/.env.
#   3. A legacy .env in the current working directory (warned about, temporary
#      backward compatibility only).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_ENV_PATH = PROJECT_ROOT / ".env"

_env_loaded = False


def load_project_env() -> None:
    """Load the canonical project .env (and warn about legacy nested ones).

    Idempotent: only the first call does any work. `load_dotenv` never
    overrides real environment variables, and the canonical file is loaded
    before any legacy file, so the documented precedence order holds.
    """
    global _env_loaded
    if _env_loaded:
        return
    _env_loaded = True

    load_dotenv(CANONICAL_ENV_PATH)

    legacy_env = Path.cwd() / ".env"
    try:
        is_legacy = legacy_env.exists() and legacy_env.resolve() != CANONICAL_ENV_PATH.resolve()
    except OSError:
        is_legacy = False
    if is_legacy:
        logger.warning(
            "Loading legacy .env from %s -- please migrate its values into the canonical %s",
            legacy_env,
            CANONICAL_ENV_PATH,
        )
        load_dotenv(legacy_env)


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to `default` on absence or junk."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Environment variable %s=%r is not an integer; using default %s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    """Read a float environment variable, falling back to `default` on absence or junk."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Environment variable %s=%r is not a number; using default %s", name, raw, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable ("true"/"1"/"yes" style)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


DEFAULT_INSIGHTS_API_URL = "https://insights.za.mixtelematics.com"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_SYNC_LOOKBACK_DAYS = 7
DEFAULT_TELEMATICS_LOOKBACK_DAYS = 7
DEFAULT_TELEMATICS_PAGE_SIZE = 50
DEFAULT_TELEMATICS_LOOKBACK_HOURS = 6
DEFAULT_TELEMATICS_CHUNK_HOURS = 1
DEFAULT_TELEMATICS_GRANT_TYPE = "password"
DEFAULT_TRACKUNIT_TOKEN_URL = "https://auth.trackunit.com/token"
DEFAULT_TRACKUNIT_ASSET_BASE_URL = "https://iris.trackunit.com/api/asset"
DEFAULT_TRACKUNIT_AEMP_BASE_URL = "https://iris.trackunit.com/public/api/aemp/v2"
DEFAULT_TRACKUNIT_SITE_BASE_URL = "https://iris.trackunit.com/api/site/v1"
DEFAULT_TRACKUNIT_SCOPE = "api"
DEFAULT_TRACKUNIT_TIMEZONE = "Africa/Harare"
DEFAULT_TRACKUNIT_REQUEST_DELAY_SECONDS = 1
DEFAULT_TRACKUNIT_MAX_RETRIES = 7
DEFAULT_TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS = 30
DEFAULT_TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS = 300
DEFAULT_HTTP_MAX_RETRIES = 3
DEFAULT_HTTP_BACKOFF_SECONDS = 2.0
DEFAULT_HTTP_CONNECT_TIMEOUT_SECONDS = 30.0
DEFAULT_HTTP_READ_TIMEOUT_SECONDS = 120.0
DEFAULT_EZYTRACK_MAX_CATCHUP_HOURS = 168
DEFAULT_EZYTRACK_CATCHUP_OVERLAP_MINUTES = 30
DEFAULT_EZYTRACK_MAX_PAGES = 500
DEFAULT_EZYTRACK_RECONCILIATION_LOOKBACK_HOURS = 48
DEFAULT_ETL_ABANDONED_RUN_HOURS = 12
DEFAULT_ETL_VALIDATION_MODE = "warn"
DEFAULT_ETL_VALIDATION_LOOKBACK_HOURS = 24
DEFAULT_ALERT_COOLDOWN_MINUTES = 360
DEFAULT_SENDEM_MAX_SUCCESS_AGE_HOURS = 12
DEFAULT_EZYTRACK_MAX_SUCCESS_AGE_HOURS = 12
DEFAULT_TRACKUNIT_MAX_SUCCESS_AGE_HOURS = 30

REQUIRED_ENV_VARS = (
    "SENDEM_API_KEY",
    "SENDEM_GROUP_ID",
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)

REQUIRED_EZYTRACK_ENV_VARS = (
    "TELEMATICS_AUTH_URL",
    "TELEMATICS_GRAPHQL_URL",
    "TELEMATICS_USERNAME",
    "TELEMATICS_PASSWORD",
    "TELEMATICS_ORGANISATION_ID",
)

REQUIRED_TRACKUNIT_ENV_VARS = (
    "TRACKUNIT_CLIENT_ID",
    "TRACKUNIT_CLIENT_SECRET",
    "TRACKUNIT_USERNAME",
    "TRACKUNIT_PASSWORD",
    "TRACKUNIT_ACCOUNT_ID",
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
    postgres_pool_timeout_seconds: int = 30


def get_settings() -> Settings:
    """Load, validate, and return all settings from environment variables.

    Loads the canonical project `.env` via `load_project_env()`, applies
    defaults for optional values, and raises `ValueError` if any required
    value is missing.
    """
    load_project_env()

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
        postgres_pool_timeout_seconds=_env_int("POSTGRES_POOL_TIMEOUT_SECONDS", 30),
    )


@dataclass(frozen=True)
class HttpSettings:
    """Shared bounded-retry and timeout policy for provider HTTP calls.

    Applies only to transient failures (connection errors, timeouts, HTTP
    500/502/503/504). Ordinary 4xx responses are never retried here;
    Trackunit's 401 refresh and 429 backoff stay provider-specific in
    connectors/trackunit_client.py.
    """

    max_retries: int
    backoff_seconds: float
    connect_timeout_seconds: float
    read_timeout_seconds: float

    @property
    def timeout(self) -> tuple[float, float]:
        """(connect, read) timeout tuple for `requests` calls."""
        return (self.connect_timeout_seconds, self.read_timeout_seconds)


def get_http_settings() -> HttpSettings:
    """Load the shared HTTP retry/timeout settings (all optional, all defaulted)."""
    load_project_env()
    return HttpSettings(
        max_retries=_env_int("HTTP_MAX_RETRIES", DEFAULT_HTTP_MAX_RETRIES),
        backoff_seconds=_env_float("HTTP_BACKOFF_SECONDS", DEFAULT_HTTP_BACKOFF_SECONDS),
        connect_timeout_seconds=_env_float("HTTP_CONNECT_TIMEOUT_SECONDS", DEFAULT_HTTP_CONNECT_TIMEOUT_SECONDS),
        read_timeout_seconds=_env_float("HTTP_READ_TIMEOUT_SECONDS", DEFAULT_HTTP_READ_TIMEOUT_SECONDS),
    )


@dataclass(frozen=True)
class EtlOpsSettings:
    """Operational settings: stale-run cleanup, validation, alerting, freshness."""

    abandoned_run_hours: int
    validation_mode: str  # "off" | "warn" | "strict"
    validation_lookback_hours: int
    alerts_enabled: bool
    alert_webhook_url: str | None
    alert_cooldown_minutes: int
    sendem_max_success_age_hours: int
    ezytrack_max_success_age_hours: int
    trackunit_max_success_age_hours: int


def get_etl_ops_settings() -> EtlOpsSettings:
    """Load operational settings (all optional, all defaulted).

    `validation_mode` is normalised to one of off/warn/strict -- anything
    unrecognised falls back to "warn" with a logged warning rather than
    silently disabling validation.
    """
    load_project_env()

    validation_mode = (os.getenv("ETL_VALIDATION_MODE") or DEFAULT_ETL_VALIDATION_MODE).strip().lower()
    if validation_mode not in ("off", "warn", "strict"):
        logger.warning(
            "ETL_VALIDATION_MODE=%r is not one of off/warn/strict; using %r",
            validation_mode,
            DEFAULT_ETL_VALIDATION_MODE,
        )
        validation_mode = DEFAULT_ETL_VALIDATION_MODE

    webhook_url = (os.getenv("TELEMETRY_ALERT_WEBHOOK_URL") or "").strip() or None

    return EtlOpsSettings(
        abandoned_run_hours=_env_int("ETL_ABANDONED_RUN_HOURS", DEFAULT_ETL_ABANDONED_RUN_HOURS),
        validation_mode=validation_mode,
        validation_lookback_hours=_env_int(
            "ETL_VALIDATION_LOOKBACK_HOURS", DEFAULT_ETL_VALIDATION_LOOKBACK_HOURS
        ),
        alerts_enabled=_env_bool("TELEMETRY_ALERTS_ENABLED", False),
        alert_webhook_url=webhook_url,
        alert_cooldown_minutes=_env_int("TELEMETRY_ALERT_COOLDOWN_MINUTES", DEFAULT_ALERT_COOLDOWN_MINUTES),
        sendem_max_success_age_hours=_env_int("SENDEM_MAX_SUCCESS_AGE_HOURS", DEFAULT_SENDEM_MAX_SUCCESS_AGE_HOURS),
        ezytrack_max_success_age_hours=_env_int(
            "EZYTRACK_MAX_SUCCESS_AGE_HOURS", DEFAULT_EZYTRACK_MAX_SUCCESS_AGE_HOURS
        ),
        trackunit_max_success_age_hours=_env_int(
            "TRACKUNIT_MAX_SUCCESS_AGE_HOURS", DEFAULT_TRACKUNIT_MAX_SUCCESS_AGE_HOURS
        ),
    )


@dataclass(frozen=True)
class EzytrackSettings:
    """Configuration required to call the EzyTrack / Telematics Guru GraphQL API.

    Authentication is dynamic, not a static token: `auth_url`/`username`/
    `password`/`grant_type` are used by connectors.ezytrack_client.EzytrackClient
    to fetch a fresh bearer token at the start of every ETL run, because
    Telematics Guru access tokens expire after roughly 24 hours. A
    previously-used static TELEMATICS_TOKEN is deprecated and intentionally
    not read here -- see docs/ezytrack_telematics_auth.md.
    """

    auth_url: str
    graphql_url: str
    username: str
    password: str
    grant_type: str
    organisation_id: int
    lookback_days: int
    page_size: int
    lookback_hours: int
    chunk_hours: int
    max_catchup_hours: int
    catchup_overlap_minutes: int
    max_pages: int
    reconciliation_lookback_hours: int


def get_ezytrack_settings() -> EzytrackSettings:
    """Load, validate, and return EzyTrack settings from environment variables.

    Loads `.env` via `python-dotenv` and raises `ValueError` if any required
    value is missing. The auth URL, GraphQL URL, username, password, and
    organisation id have no defaults (environment/tenant specific).

    lookback_days (default 7) is the older day-granularity window, currently
    unused now that jobs/sync_ezytrack.py runs in conservative chunked mode.
    It's kept rather than removed since it's expected to be used again once
    the provider confirms how its GraphQL cost limit works.

    lookback_hours (default 6) and chunk_hours (default 1) drive the current
    conservative sync mode: fetch the last `lookback_hours` hours of trips in
    `chunk_hours`-sized windows, one GraphQL request window at a time.

    page_size (default 50) is shared by both modes.

    max_catchup_hours (default 168) caps how far back a normal run will
    automatically extend its window to heal a gap after downtime;
    catchup_overlap_minutes (default 30) is the safety overlap subtracted
    from the last successful sync time. max_pages (default 500) bounds
    cursor pagination per chunk. reconciliation_lookback_hours (default 48)
    is the window used by the daily reconciliation run (--reconcile).
    """
    load_project_env()

    missing = [name for name in REQUIRED_EZYTRACK_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return EzytrackSettings(
        auth_url=os.environ["TELEMATICS_AUTH_URL"],
        graphql_url=os.environ["TELEMATICS_GRAPHQL_URL"],
        username=os.environ["TELEMATICS_USERNAME"],
        password=os.environ["TELEMATICS_PASSWORD"],
        grant_type=os.getenv("TELEMATICS_GRANT_TYPE", DEFAULT_TELEMATICS_GRANT_TYPE),
        organisation_id=int(os.environ["TELEMATICS_ORGANISATION_ID"]),
        lookback_days=int(os.getenv("TELEMATICS_LOOKBACK_DAYS", DEFAULT_TELEMATICS_LOOKBACK_DAYS)),
        page_size=int(os.getenv("TELEMATICS_PAGE_SIZE", DEFAULT_TELEMATICS_PAGE_SIZE)),
        lookback_hours=int(os.getenv("TELEMATICS_LOOKBACK_HOURS", DEFAULT_TELEMATICS_LOOKBACK_HOURS)),
        chunk_hours=int(os.getenv("TELEMATICS_CHUNK_HOURS", DEFAULT_TELEMATICS_CHUNK_HOURS)),
        max_catchup_hours=_env_int("EZYTRACK_MAX_CATCHUP_HOURS", DEFAULT_EZYTRACK_MAX_CATCHUP_HOURS),
        catchup_overlap_minutes=_env_int(
            "EZYTRACK_CATCHUP_OVERLAP_MINUTES", DEFAULT_EZYTRACK_CATCHUP_OVERLAP_MINUTES
        ),
        max_pages=_env_int("EZYTRACK_MAX_PAGES", DEFAULT_EZYTRACK_MAX_PAGES),
        reconciliation_lookback_hours=_env_int(
            "EZYTRACK_RECONCILIATION_LOOKBACK_HOURS", DEFAULT_EZYTRACK_RECONCILIATION_LOOKBACK_HOURS
        ),
    )


@dataclass(frozen=True)
class TrackunitSettings:
    """Configuration required to call the Trackunit / Manitou IRIS + AEMP APIs."""

    token_url: str
    asset_base_url: str
    aemp_base_url: str
    site_base_url: str
    client_id: str
    client_secret: str
    username: str
    password: str
    scope: str
    account_id: str
    timezone: str
    request_delay_seconds: float
    max_retries: int
    rate_limit_base_delay_seconds: float
    rate_limit_max_delay_seconds: float


def get_trackunit_settings() -> TrackunitSettings:
    """Load, validate, and return Trackunit settings from environment variables.

    Loads `.env` via `python-dotenv` and raises `ValueError` if any required
    value is missing. `token_url`/`asset_base_url`/`aemp_base_url` default to
    Trackunit's public endpoints (not secrets); `scope` defaults to "api";
    `timezone` defaults to "Africa/Harare" (this provider's report-day
    timezone). `account_id` is the AEMP account/fleet path segment (e.g.
    "15143/-3") and is tenant-specific, so it has no default.

    `request_delay_seconds` paces every AEMP request (default 1s).
    `max_retries`/`rate_limit_base_delay_seconds`/`rate_limit_max_delay_seconds`
    govern the AEMP 429 backoff in connectors.trackunit_client.
    """
    load_project_env()

    missing = [name for name in REQUIRED_TRACKUNIT_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return TrackunitSettings(
        token_url=os.getenv("TRACKUNIT_TOKEN_URL", DEFAULT_TRACKUNIT_TOKEN_URL),
        asset_base_url=os.getenv("TRACKUNIT_ASSET_BASE_URL", DEFAULT_TRACKUNIT_ASSET_BASE_URL),
        aemp_base_url=os.getenv("TRACKUNIT_AEMP_BASE_URL", DEFAULT_TRACKUNIT_AEMP_BASE_URL),
        site_base_url=os.getenv("TRACKUNIT_SITE_BASE_URL", DEFAULT_TRACKUNIT_SITE_BASE_URL),
        client_id=os.environ["TRACKUNIT_CLIENT_ID"],
        client_secret=os.environ["TRACKUNIT_CLIENT_SECRET"],
        username=os.environ["TRACKUNIT_USERNAME"],
        password=os.environ["TRACKUNIT_PASSWORD"],
        scope=os.getenv("TRACKUNIT_SCOPE", DEFAULT_TRACKUNIT_SCOPE),
        account_id=os.environ["TRACKUNIT_ACCOUNT_ID"],
        timezone=os.getenv("TRACKUNIT_TIMEZONE", DEFAULT_TRACKUNIT_TIMEZONE),
        request_delay_seconds=float(
            os.getenv("TRACKUNIT_REQUEST_DELAY_SECONDS", DEFAULT_TRACKUNIT_REQUEST_DELAY_SECONDS)
        ),
        max_retries=int(os.getenv("TRACKUNIT_MAX_RETRIES", DEFAULT_TRACKUNIT_MAX_RETRIES)),
        rate_limit_base_delay_seconds=float(
            os.getenv("TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS", DEFAULT_TRACKUNIT_RATE_LIMIT_BASE_DELAY_SECONDS)
        ),
        rate_limit_max_delay_seconds=float(
            os.getenv("TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS", DEFAULT_TRACKUNIT_RATE_LIMIT_MAX_DELAY_SECONDS)
        ),
    )
