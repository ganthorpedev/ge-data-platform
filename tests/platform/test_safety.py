from __future__ import annotations

import pytest

from ge_data_platform.common.safety import NonLocalHostError, assert_local_host


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_assert_local_host_accepts_recognised_local_hosts(host: str) -> None:
    assert_local_host(host, context="test operation")  # must not raise


@pytest.mark.parametrize(
    "host",
    ["prod-db.internal", "10.0.0.5", "some-remote-host", "", "LOCALHOST"],
)
def test_assert_local_host_rejects_everything_else(host: str) -> None:
    with pytest.raises(NonLocalHostError, match="test operation"):
        assert_local_host(host, context="test operation")


def test_assert_local_host_error_names_the_offending_host() -> None:
    with pytest.raises(NonLocalHostError, match="prod-db.internal"):
        assert_local_host("prod-db.internal", context="Trackunit historical backfill")
