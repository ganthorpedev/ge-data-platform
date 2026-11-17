from __future__ import annotations

import pytest

from ge_control_center import config
from ge_control_center.config import ConfigurationError, load_settings

LOCAL_URL = "postgresql+psycopg2://u:p@127.0.0.1:55432/ge_control"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for key in list(__import__("os").environ):
        if key.startswith("GECC_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(config, "ENV_FILE", tmp_path / "absent")


def test_local_settings_load(monkeypatch) -> None:
    monkeypatch.setenv("GECC_CONTROL_DB_URL", LOCAL_URL)
    settings = load_settings()
    assert settings.environment == "local" and settings.lease_seconds == 30


@pytest.mark.parametrize("host", ["prod-db.internal", "10.0.0.5", "telemetry-server"])
def test_non_local_databases_are_refused(monkeypatch, host) -> None:
    monkeypatch.setenv("GECC_CONTROL_DB_URL", f"postgresql+psycopg2://u:p@{host}:5432/ge_control")
    with pytest.raises(ConfigurationError, match="local databases only"):
        load_settings()


def test_non_local_warehouse_is_refused(monkeypatch) -> None:
    monkeypatch.setenv("GECC_CONTROL_DB_URL", LOCAL_URL)
    monkeypatch.setenv("GECC_WAREHOUSE_DB_URL", "postgresql+psycopg2://u:p@prod:5432/ge_warehouse")
    with pytest.raises(ConfigurationError, match="GECC_WAREHOUSE_DB_URL"):
        load_settings()


def test_only_local_environment_is_supported(monkeypatch) -> None:
    monkeypatch.setenv("GECC_CONTROL_DB_URL", LOCAL_URL)
    monkeypatch.setenv("GECC_ENVIRONMENT", "production")
    with pytest.raises(ConfigurationError, match="only 'local'"):
        load_settings()


def test_heartbeat_must_fit_inside_lease(monkeypatch) -> None:
    monkeypatch.setenv("GECC_CONTROL_DB_URL", LOCAL_URL)
    monkeypatch.setenv("GECC_LEASE_SECONDS", "20")
    monkeypatch.setenv("GECC_HEARTBEAT_SECONDS", "10")
    with pytest.raises(ConfigurationError, match="two renewals"):
        load_settings()
