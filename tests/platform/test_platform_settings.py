from __future__ import annotations

import pytest

from ge_data_platform.config.settings import (
    DEFAULT_GE_WAREHOUSE_DB,
    PlatformSettings,
    get_platform_settings,
)

REQUIRED_SHARED_VARS = ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD")


def _set_required_shared_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")


def test_default_ge_warehouse_db_name_is_not_the_legacy_database() -> None:
    # The whole point of PlatformSettings/GE_WAREHOUSE_DB is that new code
    # never hardcodes -- or accidentally defaults to -- the legacy database
    # name.
    assert DEFAULT_GE_WAREHOUSE_DB == "ge_warehouse"
    assert DEFAULT_GE_WAREHOUSE_DB != "telemetry_warehouse"


def test_get_platform_settings_defaults_db_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_shared_vars(monkeypatch)
    monkeypatch.delenv("GE_WAREHOUSE_DB", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)

    settings = get_platform_settings()

    assert settings.ge_warehouse_db == "ge_warehouse"
    assert settings.postgres_port == 5432
    assert settings.postgres_host == "localhost"
    assert settings.postgres_user == "test_user"
    assert settings.postgres_password == "test_password"
    assert settings.postgres_pool_timeout_seconds == 30


def test_get_platform_settings_respects_ge_warehouse_db_override(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_shared_vars(monkeypatch)
    monkeypatch.setenv("GE_WAREHOUSE_DB", "ge_warehouse_test")

    settings = get_platform_settings()

    assert settings.ge_warehouse_db == "ge_warehouse_test"


@pytest.mark.parametrize("missing_var", REQUIRED_SHARED_VARS)
def test_get_platform_settings_raises_on_missing_required_var(
    monkeypatch: pytest.MonkeyPatch, missing_var: str
) -> None:
    _set_required_shared_vars(monkeypatch)
    monkeypatch.delenv(missing_var, raising=False)

    with pytest.raises(ValueError, match="Missing required environment variables"):
        get_platform_settings()


def test_platform_settings_is_frozen() -> None:
    settings = PlatformSettings(
        postgres_host="localhost",
        postgres_port=5432,
        ge_warehouse_db="ge_warehouse",
        postgres_user="user",
        postgres_password="password",
    )
    with pytest.raises(AttributeError):
        settings.ge_warehouse_db = "other"  # type: ignore[misc]
