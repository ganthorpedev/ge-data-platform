"""Focused, offline reliability tests for Trackunit fetch, transform, and jobs."""

from __future__ import annotations

from datetime import date
from unittest.mock import Mock

import pytest

from config.settings import HttpSettings, TrackunitSettings
from connectors.trackunit_client import TrackunitClient
from jobs import sync_trackunit_daily_activity, sync_trackunit_location_enrichment
from transforms.trackunit_transform import (
    DATA_QUALITY_COUNTER_RESET,
    DATA_QUALITY_LIVE,
    build_daily_activity_rows,
    cumulative_counter_reset_detected,
    cumulative_distance_delta_km,
    cumulative_hours_delta_minutes,
)
from utils import overlap_lock


def _settings(
    *,
    request_delay_seconds: float = 0,
    max_retries: int = 7,
) -> TrackunitSettings:
    return TrackunitSettings(
        token_url="https://example.invalid/token",
        asset_base_url="https://example.invalid/assets",
        aemp_base_url="https://example.invalid/aemp",
        site_base_url="https://example.invalid/sites",
        client_id="client-id",
        client_secret="client-secret",
        username="test-user",
        password="test-password",
        scope="api",
        account_id="123/-1",
        timezone="Africa/Harare",
        request_delay_seconds=request_delay_seconds,
        max_retries=max_retries,
        rate_limit_base_delay_seconds=30,
        rate_limit_max_delay_seconds=300,
    )


def _http_settings(*, max_attempts: int = 3) -> HttpSettings:
    return HttpSettings(
        max_retries=max_attempts,
        backoff_seconds=2,
        connect_timeout_seconds=30,
        read_timeout_seconds=120,
    )


def _response(
    status_code: int,
    payload: object | None = None,
    *,
    headers: dict[str, str] | None = None,
    text: str = "response body",
) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 400
    response.headers = headers or {}
    response.text = text
    response.json.return_value = {} if payload is None else payload
    return response


def _client(
    *,
    request_delay_seconds: float = 0,
    rate_limit_attempts: int = 7,
    transient_attempts: int = 3,
) -> TrackunitClient:
    client = TrackunitClient(
        _settings(
            request_delay_seconds=request_delay_seconds,
            max_retries=rate_limit_attempts,
        ),
        _http_settings(max_attempts=transient_attempts),
    )
    client._access_token = "old-token"
    return client


def test_401_refreshes_once_and_retries_authenticated_get(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            _response(401),
            _response(200, {"content": [{"id": "asset-1"}], "totalPages": 1}),
        ]
    )

    refresh = Mock()

    def _refresh_token() -> str:
        refresh()
        client._access_token = "refreshed-token"
        return client._access_token

    monkeypatch.setattr(client, "authenticate", _refresh_token)

    payload = client.get_assets()

    assert payload["content"][0]["id"] == "asset-1"
    assert refresh.call_count == 1
    assert client.session.request.call_count == 2
    assert client.session.request.call_args_list[1].kwargs["headers"]["Authorization"] == "Bearer refreshed-token"


def test_second_401_raises_without_an_authentication_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[_response(401), _response(401)]
    )
    refresh = Mock(return_value="refreshed-token")
    monkeypatch.setattr(client, "authenticate", refresh)

    with pytest.raises(RuntimeError, match="still unauthorized.*after a token refresh"):
        client.get_assets()

    assert refresh.call_count == 1
    assert client.session.request.call_count == 2


def test_retry_after_429_is_respected_and_logs_metric_and_pin(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client()
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            _response(429, headers={"Retry-After": "12"}),
            _response(200, {"distance": []}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)
    monkeypatch.setattr("connectors.trackunit_client.random.uniform", lambda _low, _high: 1.25)

    with caplog.at_level("WARNING", logger="connectors.trackunit_client"):
        client.get_aemp_series(
            "PIN-9",
            "Distance",
            "distance",
            "2026-08-01T00:00:00Z",
            "2026-08-02T00:00:00Z",
        )

    sleep.assert_called_once_with(13.25)
    assert "metric=Distance" in caplog.text
    assert "pin=PIN-9" in caplog.text
    assert "attempt=1/7" in caplog.text
    assert "wait_seconds=13.2" in caplog.text


def test_every_authenticated_get_retries_429(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            _response(429, headers={"Retry-After": "4"}),
            _response(200, {"content": [], "totalPages": 0}),
            _response(429, headers={"Retry-After": "4"}),
            _response(200, {"distance": []}),
            _response(429, headers={"Retry-After": "4"}),
            _response(200, {"content": []}),
            _response(429, headers={"Retry-After": "4"}),
            _response(200, {"id": "site-1"}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)
    monkeypatch.setattr("connectors.trackunit_client.random.uniform", lambda _low, _high: 0)

    client.get_assets()
    client.get_aemp_series(
        "PIN-1",
        "Distance",
        "distance",
        "2026-08-01T00:00:00Z",
        "2026-08-02T00:00:00Z",
    )
    client.get_site_history(
        "asset-1",
        "2026-08-01T00:00:00Z",
        "2026-08-02T00:00:00Z",
    )
    client.get_site("site-1")

    assert [call.args[0] for call in sleep.call_args_list] == [4, 4, 4, 4]
    assert client.session.request.call_count == 8


def test_invalid_retry_after_uses_exponential_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            _response(429, headers={"Retry-After": "not-seconds"}),
            _response(429),
            _response(200, {"cumulativeOperatingHours": []}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)
    monkeypatch.setattr("connectors.trackunit_client.random.uniform", lambda _low, _high: 0.5)

    client.get_aemp_series(
        "PIN-1",
        "CumulativeOperatingHours",
        "cumulativeOperatingHours",
        "2026-08-01T00:00:00Z",
        "2026-08-02T00:00:00Z",
    )

    assert [call.args[0] for call in sleep.call_args_list] == [30.5, 60.5]


def test_persistent_429_stops_at_seven_total_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(rate_limit_attempts=7)
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[_response(429) for _ in range(7)]
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)
    monkeypatch.setattr("connectors.trackunit_client.random.uniform", lambda _low, _high: 0)

    with pytest.raises(RuntimeError, match=r"429.*after 7 attempt\(s\).+metric=Distance.+pin=PIN-7"):
        client.get_aemp_series(
            "PIN-7",
            "Distance",
            "distance",
            "2026-08-01T00:00:00Z",
            "2026-08-02T00:00:00Z",
        )

    assert client.session.request.call_count == 7
    assert [call.args[0] for call in sleep.call_args_list] == [30, 60, 120, 240, 300, 300]


def test_transient_5xx_retries_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(transient_attempts=3)
    client.session.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            _response(500),
            _response(502),
            _response(200, {"content": [], "totalPages": 0}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)

    client.get_assets()

    assert client.session.request.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4]


def test_ordinary_4xx_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    client.session.request = Mock(return_value=_response(400))  # type: ignore[method-assign]
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)

    with pytest.raises(RuntimeError, match="get_assets failed: 400"):
        client.get_assets()

    assert client.session.request.call_count == 1
    sleep.assert_not_called()


def test_configured_aemp_request_pacing_is_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(request_delay_seconds=1.5)
    client.session.request = Mock(  # type: ignore[method-assign]
        return_value=_response(200, {"distance": []})
    )
    sleep = Mock()
    monkeypatch.setattr("connectors.trackunit_client.time.sleep", sleep)

    client.get_aemp_series(
        "PIN-1",
        "Distance",
        "distance",
        "2026-08-01T00:00:00Z",
        "2026-08-02T00:00:00Z",
    )

    sleep.assert_called_once_with(1.5)


def _points(values: list[float], value_key: str) -> list[dict]:
    return [
        {
            "datetime": f"2026-08-01T{index:02d}:00:00Z",
            value_key: value,
        }
        for index, value in enumerate(values)
    ]


def _asset() -> dict:
    return {
        "id": "asset-1",
        "name": "Machine 1",
        "externalReference": "PIN-1",
        "serialNumber": "SERIAL-1",
        "brand": "Manitou",
        "type": "Telehandler",
        "model": "M-1",
        "telematicsDevices": [{"id": "device-1", "serialNumber": "DEVICE-SERIAL-1"}],
    }


def _activity(
    *,
    operating_values: list[float],
    moving_values: list[float],
    distance_values: list[float],
) -> dict:
    result = build_daily_activity_rows(
        [_asset()],
        {
            "asset-1": {
                "operating_points": _points(operating_values, "Hour"),
                "moving_points": _points(moving_values, "Hour"),
                "distance_points": _points(distance_values, "Odometer"),
            }
        },
        date(2026, 8, 1),
    )
    return result["daily_activity_df"].iloc[0].to_dict()


def test_valid_cumulative_deltas_are_preserved() -> None:
    row = _activity(
        operating_values=[100, 101],
        moving_values=[20, 20.5],
        distance_values=[500, 505],
    )

    assert row["operating_minutes"] == 60
    assert row["active_driving_minutes"] == 30
    assert row["distance_km"] == 5
    assert bool(row["counter_reset_detected"]) is False
    assert row["data_quality_status"] == DATA_QUALITY_LIVE


@pytest.mark.parametrize(
    ("operating_values", "moving_values", "distance_values", "nulled_column", "hhmm_column"),
    [
        ([100, 101, 2, 3], [20, 20.5], [500, 505], "operating_minutes", "operating_hhmm"),
        ([100, 101], [20, 20.5, 1, 2], [500, 505], "active_driving_minutes", "active_driving_hhmm"),
        ([100, 101], [20, 20.5], [500, 505, 3, 6], "distance_km", None),
    ],
)
def test_each_counter_reset_nulls_only_its_derived_metric(
    operating_values: list[float],
    moving_values: list[float],
    distance_values: list[float],
    nulled_column: str,
    hhmm_column: str | None,
) -> None:
    row = _activity(
        operating_values=operating_values,
        moving_values=moving_values,
        distance_values=distance_values,
    )

    assert row[nulled_column] is None
    if hhmm_column is not None:
        assert row[hhmm_column] is None
    assert bool(row["counter_reset_detected"]) is True
    assert row["data_quality_status"] == DATA_QUALITY_COUNTER_RESET

    if nulled_column != "operating_minutes":
        assert row["operating_minutes"] == 60
    if nulled_column != "active_driving_minutes":
        assert row["active_driving_minutes"] == 30
    if nulled_column != "distance_km":
        assert row["distance_km"] == 5


def test_mid_series_reset_is_detected_even_if_final_value_recovers() -> None:
    points = _points([100, 110, 2, 120], "Hour")

    delta, _, _, count = cumulative_hours_delta_minutes(points)

    assert cumulative_counter_reset_detected(points) is True
    assert delta is None
    assert count == 4


def test_empty_and_single_reading_behaviour_is_preserved() -> None:
    empty = _activity(operating_values=[], moving_values=[], distance_values=[])
    single = _activity(operating_values=[100], moving_values=[20], distance_values=[500])

    assert empty["operating_minutes"] == 0
    assert empty["active_driving_minutes"] == 0
    assert empty["distance_km"] is None
    assert bool(empty["counter_reset_detected"]) is False

    assert single["operating_minutes"] == 0
    assert single["active_driving_minutes"] == 0
    assert single["distance_km"] == 0
    assert bool(single["counter_reset_detected"]) is False


def test_reset_helpers_never_return_negative_derived_values() -> None:
    hours_delta, *_ = cumulative_hours_delta_minutes(_points([20, 3], "Hour"))
    distance_delta, *_ = cumulative_distance_delta_km(_points([100, 10], "Odometer"))

    assert hours_delta is None
    assert distance_delta is None


def test_daily_job_preserves_original_error_when_failed_bookkeeping_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_error = RuntimeError("Trackunit provider root cause")

    class FakeLoader:
        def __init__(self, settings: object) -> None:
            self.settings = settings

        def start_sync_run(self, **kwargs: object) -> str:
            return "trackunit-run-1"

        def finish_sync_run(self, **kwargs: object) -> None:
            raise OSError("database unavailable during FAILED update")

    class FakeClient:
        def __init__(self, settings: TrackunitSettings) -> None:
            self.settings = settings

        def authenticate(self) -> None:
            raise original_error

    monkeypatch.setattr(sync_trackunit_daily_activity, "get_settings", lambda: object())
    monkeypatch.setattr(sync_trackunit_daily_activity, "get_trackunit_settings", _settings)
    monkeypatch.setattr(sync_trackunit_daily_activity, "PostgresLoader", FakeLoader)
    monkeypatch.setattr(sync_trackunit_daily_activity, "TrackunitClient", FakeClient)

    with pytest.raises(RuntimeError) as raised:
        sync_trackunit_daily_activity.run(date_arg="2026-08-01")

    assert raised.value is original_error


def test_location_job_preserves_original_error_when_failed_bookkeeping_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_error = RuntimeError("Trackunit location provider root cause")

    class FakeLoader:
        def __init__(self, settings: object) -> None:
            self.settings = settings

        def start_sync_run(self, **kwargs: object) -> str:
            return "trackunit-location-run-1"

        def finish_sync_run(self, **kwargs: object) -> None:
            raise OSError("database unavailable during FAILED update")

    class FakeClient:
        def __init__(self, settings: TrackunitSettings) -> None:
            self.settings = settings

        def authenticate(self) -> None:
            raise original_error

    monkeypatch.setattr(sync_trackunit_location_enrichment, "get_settings", lambda: object())
    monkeypatch.setattr(sync_trackunit_location_enrichment, "get_trackunit_settings", _settings)
    monkeypatch.setattr(sync_trackunit_location_enrichment, "PostgresLoader", FakeLoader)
    monkeypatch.setattr(sync_trackunit_location_enrichment, "TrackunitClient", FakeClient)
    monkeypatch.setattr(sync_trackunit_location_enrichment, "_fetch_activity_rows", lambda *_args: [])

    with pytest.raises(RuntimeError) as raised:
        sync_trackunit_location_enrichment.run(date(2026, 8, 1))

    assert raised.value is original_error


@pytest.mark.parametrize(
    ("run_job", "kwargs"),
    [
        (
            sync_trackunit_daily_activity.run,
            {"date_arg": "2026-08-01"},
        ),
        (
            sync_trackunit_location_enrichment.run,
            {"report_date": date(2026, 8, 1)},
        ),
    ],
)
def test_direct_trackunit_entry_points_refuse_a_shared_active_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: object,
    run_job: object,
    kwargs: dict[str, object],
) -> None:
    lock_directory = tmp_path / "locks"  # type: ignore[operator]
    monkeypatch.setattr(overlap_lock, "LOCK_DIRECTORY", lock_directory)
    monkeypatch.delenv(overlap_lock.INHERITED_OVERLAP_GROUP_ENV, raising=False)

    with overlap_lock.provider_overlap_guard(overlap_lock.TRACKUNIT_OVERLAP_GROUP):
        with pytest.raises(
            overlap_lock.OverlapLockUnavailable,
            match="another process already holds",
        ):
            run_job(**kwargs)  # type: ignore[operator]
