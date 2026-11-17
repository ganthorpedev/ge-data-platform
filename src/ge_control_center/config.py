"""Control Center settings, read from environment variables.

Precedence: real process environment first, then an optional
``<project root>/.env.control-center`` file. The legacy ``.env`` (which
holds POSTGRES_* for the production telemetry database) is deliberately NOT
read here, so the Control Center can never pick up legacy production
credentials by accident.

M1 is local-only: ``GECC_ENVIRONMENT`` must be ``local`` and every database
URL must point at a local host or a local Compose service.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env.control-center"

SUPPORTED_ENVIRONMENTS = frozenset({"local"})
LOCAL_DB_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "control-db", "warehouse-db"})


class ConfigurationError(ValueError):
    """Raised for missing or unsafe Control Center configuration."""


def _load_env_file() -> None:
    if ENV_FILE.is_file():
        from dotenv import load_dotenv

        load_dotenv(ENV_FILE, override=False)


def _int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name}={raw!r} is not an integer") from error
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name}={value} must be between {minimum} and {maximum}")
    return value


def _float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name}={raw!r} is not a number") from error
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name}={value} must be between {minimum} and {maximum}")
    return value


def assert_local_database(url: str, *, setting: str) -> None:
    """Refuse any database URL whose host is not local (M1 safety guard)."""
    try:
        host = make_url(url).host
    except Exception as error:  # noqa: BLE001 - re-raised with context
        raise ConfigurationError(f"{setting} is not a valid SQLAlchemy URL") from error
    if host not in LOCAL_DB_HOSTS:
        raise ConfigurationError(
            f"{setting} points at host {host!r}. GE Data Control Center M1 runs against local "
            f"databases only ({sorted(LOCAL_DB_HOSTS)}); production use is not enabled yet."
        )


@dataclass(frozen=True)
class Settings:
    environment: str
    control_db_url: str
    warehouse_db_url: str | None
    api_token: str | None
    actor: str | None
    lease_seconds: int
    heartbeat_seconds: int
    lease_safety_seconds: int
    poll_seconds: float
    reap_seconds: int
    drain_seconds: int
    worker_lost_after_seconds: int


def load_settings(*, require_control_db: bool = True) -> Settings:
    """Load and validate settings from the environment."""
    _load_env_file()

    environment = os.getenv("GECC_ENVIRONMENT", "local").strip().lower()
    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ConfigurationError(
            f"GECC_ENVIRONMENT={environment!r} is not supported in this milestone; only 'local' is."
        )

    control_db_url = os.getenv("GECC_CONTROL_DB_URL", "").strip()
    if require_control_db and not control_db_url:
        raise ConfigurationError("GECC_CONTROL_DB_URL is required")
    if control_db_url:
        assert_local_database(control_db_url, setting="GECC_CONTROL_DB_URL")

    warehouse_db_url = os.getenv("GECC_WAREHOUSE_DB_URL", "").strip() or None
    if warehouse_db_url:
        assert_local_database(warehouse_db_url, setting="GECC_WAREHOUSE_DB_URL")

    lease_seconds = _int("GECC_LEASE_SECONDS", 30, minimum=5, maximum=3600)
    heartbeat_seconds = _int("GECC_HEARTBEAT_SECONDS", 10, minimum=1, maximum=600)
    lease_safety_seconds = _int("GECC_LEASE_SAFETY_SECONDS", max(2, lease_seconds // 4), minimum=1, maximum=600)
    if heartbeat_seconds * 2 > lease_seconds - lease_safety_seconds:
        raise ConfigurationError(
            "GECC_HEARTBEAT_SECONDS is too large for the lease: at least two renewals must fit in "
            "GECC_LEASE_SECONDS - GECC_LEASE_SAFETY_SECONDS "
            f"(lease={lease_seconds}, safety={lease_safety_seconds}, heartbeat={heartbeat_seconds})"
        )

    return Settings(
        environment=environment,
        control_db_url=control_db_url,
        warehouse_db_url=warehouse_db_url,
        api_token=os.getenv("GECC_API_TOKEN") or None,
        actor=os.getenv("GECC_ACTOR") or None,
        lease_seconds=lease_seconds,
        heartbeat_seconds=heartbeat_seconds,
        lease_safety_seconds=lease_safety_seconds,
        poll_seconds=_float("GECC_POLL_SECONDS", 2.0, minimum=0.1, maximum=300),
        reap_seconds=_int("GECC_REAP_SECONDS", 15, minimum=1, maximum=3600),
        drain_seconds=_int("GECC_DRAIN_SECONDS", 20, minimum=0, maximum=3600),
        worker_lost_after_seconds=_int("GECC_WORKER_LOST_AFTER_SECONDS", 120, minimum=10, maximum=86400),
    )
