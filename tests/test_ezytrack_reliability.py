"""Focused, offline reliability tests for the EzyTrack connector and job."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from config.settings import EzytrackSettings
from connectors.ezytrack_client import EzytrackClient
from jobs import sync_ezytrack


def _settings(*, max_pages: int = 500) -> EzytrackSettings:
    return EzytrackSettings(
        auth_url="https://example.invalid/auth",
        graphql_url="https://example.invalid/graphql",
        username="test-user",
        password="test-password",
        grant_type="password",
        organisation_id=123,
        lookback_days=7,
        page_size=50,
        lookback_hours=6,
        chunk_hours=1,
        max_catchup_hours=168,
        catchup_overlap_minutes=30,
        max_pages=max_pages,
        reconciliation_lookback_hours=48,
    )


def _trips_page(*, cursor: str | None, has_next: bool, trip_id: int) -> dict:
    return {
        "data": {
            "trips": {
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                "nodes": [{"tripId": trip_id}],
            }
        }
    }


def test_repeated_cursor_is_detected_without_a_live_api_call() -> None:
    client = EzytrackClient(_settings())
    client._run_graphql = Mock(  # type: ignore[method-assign]
        side_effect=[
            _trips_page(cursor="same-cursor", has_next=True, trip_id=1),
            _trips_page(cursor="same-cursor", has_next=True, trip_id=2),
        ]
    )

    with pytest.raises(RuntimeError, match="repeated endCursor"):
        client.fetch_trips("2026-08-03T00:00:00Z", "2026-08-03T01:00:00Z")

    assert client._run_graphql.call_count == 2


def test_max_page_count_stops_pagination() -> None:
    client = EzytrackClient(_settings(max_pages=2))
    client._run_graphql = Mock(  # type: ignore[method-assign]
        side_effect=[
            _trips_page(cursor="cursor-1", has_next=True, trip_id=1),
            _trips_page(cursor="cursor-2", has_next=True, trip_id=2),
        ]
    )

    with pytest.raises(RuntimeError, match="exceeded max_pages=2"):
        client.fetch_trips("2026-08-03T00:00:00Z", "2026-08-03T01:00:00Z")

    assert client._run_graphql.call_count == 2


def test_first_run_preserves_existing_default_lookback() -> None:
    end = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)

    start, resolved_end = sync_ezytrack.calculate_sync_window(
        end_time=end,
        default_lookback_hours=6,
        max_catchup_hours=168,
        overlap_minutes=30,
    )

    assert start == end - timedelta(hours=6)
    assert resolved_end == end


def test_success_cursor_uses_overlap() -> None:
    end = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    last_window_end = end - timedelta(hours=4)

    start, _ = sync_ezytrack.calculate_sync_window(
        end_time=end,
        default_lookback_hours=6,
        max_catchup_hours=168,
        overlap_minutes=30,
        last_success_window_end=last_window_end,
    )

    assert start == last_window_end - timedelta(minutes=30)


def test_automatic_catchup_is_capped() -> None:
    end = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    very_old_success = end - timedelta(days=30)

    start, _ = sync_ezytrack.calculate_sync_window(
        end_time=end,
        default_lookback_hours=6,
        max_catchup_hours=168,
        overlap_minutes=30,
        last_success_window_end=very_old_success,
    )

    assert start == end - timedelta(hours=168)


def test_reconciliation_uses_its_configured_lookback() -> None:
    end = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)

    start, _ = sync_ezytrack.calculate_sync_window(
        end_time=end,
        default_lookback_hours=6,
        max_catchup_hours=168,
        overlap_minutes=30,
        last_success_window_end=end - timedelta(minutes=5),
        reconciliation_lookback_hours=48,
    )

    assert start == end - timedelta(hours=48)


def test_job_preserves_original_error_when_failed_bookkeeping_also_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    original_error = RuntimeError("provider root cause")

    class FakeLoader:
        def __init__(self, settings: object) -> None:
            self.settings = settings

        def get_last_successful_run(self, source_system: str) -> None:
            return None

        def start_sync_run(self, **kwargs: object) -> str:
            return "sync-run-1"

        def finish_sync_run(self, **kwargs: object) -> None:
            raise OSError("database unavailable during FAILED update")

    class FakeClient:
        def __init__(self, settings: EzytrackSettings) -> None:
            self.settings = settings

        def authenticate(self) -> None:
            raise original_error

    monkeypatch.setattr(sync_ezytrack, "get_settings", lambda: object())
    monkeypatch.setattr(sync_ezytrack, "get_ezytrack_settings", _settings)
    monkeypatch.setattr(sync_ezytrack, "PostgresLoader", FakeLoader)
    monkeypatch.setattr(sync_ezytrack, "EzytrackClient", FakeClient)

    with pytest.raises(RuntimeError) as raised:
        sync_ezytrack.run(now_utc=datetime(2026, 8, 3, 12, tzinfo=timezone.utc))

    assert raised.value is original_error

