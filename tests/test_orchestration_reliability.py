"""Offline tests for Dagster orchestration reliability behavior."""

from __future__ import annotations

import io
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest

from orchestration import alerts, definitions, monitoring, runner
from orchestration import schedules as schedules_module
from orchestration.overlap import (
    ACTIVE_RUN_STATUSES,
    EZYTRACK_DAGSTER_JOB_NAMES,
    TRACKUNIT_DAGSTER_JOB_NAMES,
    dagster_jobs_for_sync_run,
)
from utils import overlap_lock


class CapturingLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def _write(self, level: str, message: object, *args: object) -> None:
        rendered = str(message) % args if args else str(message)
        self.entries.append((level, rendered))

    def info(self, message: object, *args: object) -> None:
        self._write("info", message, *args)

    def warning(self, message: object, *args: object) -> None:
        self._write("warning", message, *args)

    def error(self, message: object, *args: object) -> None:
        self._write("error", message, *args)


class FakeContext:
    def __init__(self, instance: object | None = None) -> None:
        self.log = CapturingLog()
        self.instance = instance


class HungProcess:
    """Popen double that ignores terminate and exits only after kill."""

    def __init__(self) -> None:
        self.stdout = io.StringIO("provider stdout before timeout\n")
        self.stderr = io.StringIO("provider stderr before timeout\n")
        self.return_code: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.return_code

    def wait(self, timeout: float | None = None) -> int:
        if self.killed:
            self.return_code = -9
            return self.return_code
        raise subprocess.TimeoutExpired(cmd="provider", timeout=timeout)

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self.return_code = -9


def _ops_settings(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "abandoned_run_hours": 12,
        "alerts_enabled": False,
        "alert_webhook_url": None,
        "alert_cooldown_minutes": 360,
        "sendem_max_success_age_hours": 12,
        "ezytrack_max_success_age_hours": 12,
        "trackunit_max_success_age_hours": 30,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_subprocess_timeout_force_kills_captures_both_streams_and_releases_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    process = HungProcess()
    context = FakeContext()
    popen_call: dict[str, object] = {}
    monkeypatch.setattr(overlap_lock, "LOCK_DIRECTORY", tmp_path / "locks")

    def fake_popen(*args: object, **kwargs: object) -> HungProcess:
        popen_call.update(kwargs)
        return process

    monkeypatch.setattr(runner.subprocess, "Popen", fake_popen)

    with pytest.raises(dg.Failure) as raised:
        runner.run_module(
            context,  # type: ignore[arg-type]
            "jobs.sync_trackunit_daily_activity",
            overlap_group=runner.TRACKUNIT_OVERLAP_GROUP,
            timeout_minutes=0.001,
            terminate_grace_seconds=0,
        )

    message = str(raised.value)
    assert "timed out after 0.00 minute(s)" in message
    assert "provider stdout before timeout" in message
    assert "provider stderr before timeout" in message
    assert process.terminated is True
    assert process.killed is True
    child_environment = popen_call["env"]
    assert isinstance(child_environment, dict)
    assert (
        child_environment[overlap_lock.INHERITED_OVERLAP_GROUP_ENV]
        == runner.TRACKUNIT_OVERLAP_GROUP
    )

    # The same OS lock can be acquired immediately after the timeout failure.
    with runner.job_overlap_guard(  # type: ignore[arg-type]
        context,
        runner.TRACKUNIT_OVERLAP_GROUP,
    ):
        pass


def test_timeout_and_terminate_grace_environment_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SENDEM_JOB_TIMEOUT_MINUTES", "17.5")
    monkeypatch.setenv("EZYTRACK_JOB_TIMEOUT_MINUTES", "23")
    monkeypatch.setenv("TRACKUNIT_JOB_TIMEOUT_MINUTES", "180")
    monkeypatch.setenv("ETL_SUBPROCESS_TERMINATE_GRACE_SECONDS", "4.5")

    assert runner.get_module_timeout_minutes("jobs.sync_sendem") == 17.5
    assert runner.get_module_timeout_minutes("jobs.sync_ezytrack") == 23
    assert runner.get_module_timeout_minutes("jobs.sync_trackunit_location_enrichment") == 180
    assert runner.get_terminate_grace_seconds() == 4.5


def test_definitions_are_loadable_and_schedules_have_intended_cadence() -> None:
    dg.Definitions.validate_loadable(definitions.defs)
    repository = definitions.defs.get_repository_def()
    schedule_by_name = {schedule.name: schedule for schedule in repository.schedule_defs}

    assert set(schedule_by_name) == {
        "sendem_sync_schedule",
        "ezytrack_sync_schedule",
        "ezytrack_daily_reconciliation_schedule",
        "trackunit_daily_refresh_schedule",
        "trackunit_intraday_refresh_schedule",
        "trackunit_rolling_7_days_schedule",
        "stale_started_run_cleanup_schedule",
    }

    assert schedule_by_name["trackunit_daily_refresh_schedule"].cron_schedule == "5 2 * * *"
    assert schedule_by_name["trackunit_intraday_refresh_schedule"].cron_schedule == "20 */3 * * *"
    assert schedule_by_name["trackunit_rolling_7_days_schedule"].cron_schedule == "45 1 * * 0"
    assert schedule_by_name["ezytrack_daily_reconciliation_schedule"].cron_schedule == "15 1 * * *"
    assert schedule_by_name["ezytrack_sync_schedule"].cron_schedule == "45 */3 * * *"
    assert all(schedule.execution_timezone == "Africa/Harare" for schedule in repository.schedule_defs)
    assert not any("3_hour" in schedule_name for schedule_name in schedule_by_name)

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    assert sensor_names == {
        "telemetry_provider_freshness_sensor",
        "telemetry_run_failure_sensor",
    }


def test_trackunit_jobs_are_tagged_for_run_level_concurrency() -> None:
    repository = definitions.defs.get_repository_def()
    for job_name in TRACKUNIT_DAGSTER_JOB_NAMES:
        assert repository.get_job(job_name).tags["telemetry/provider"] == "trackunit"


def test_intraday_schedule_skips_with_a_clear_reason_when_trackunit_group_is_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        schedules_module,
        "find_active_run_job",
        lambda instance, job_names: "trackunit_daily_refresh",
    )
    context = FakeContext(instance=object())

    result = schedules_module._skip_if_overlapping(
        context,  # type: ignore[arg-type]
        job_name="trackunit_intraday_refresh",
        overlap_with=schedules_module.TRACKUNIT_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "trackunit"},
    )

    assert isinstance(result, dg.SkipReason)
    message = str(result)
    assert "trackunit_intraday_refresh" in message
    assert "trackunit_daily_refresh" in message


def test_intraday_schedule_requests_a_run_when_trackunit_group_is_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        schedules_module,
        "find_active_run_job",
        lambda instance, job_names: None,
    )
    context = FakeContext(instance=object())
    context.scheduled_execution_time = datetime(2026, 8, 6, 5, 20, tzinfo=timezone.utc)  # type: ignore[attr-defined]

    result = schedules_module._skip_if_overlapping(
        context,  # type: ignore[arg-type]
        job_name="trackunit_intraday_refresh",
        overlap_with=schedules_module.TRACKUNIT_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "trackunit"},
    )

    assert isinstance(result, dg.RunRequest)
    assert result.tags == {"telemetry/provider": "trackunit"}


def test_reconciliation_and_trackunit_jobs_pass_exact_cli_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[
        tuple[str, tuple[str, ...], str | None, str | None, float | None]
    ] = []

    def fake_run_module(
        context: object,
        module: str,
        args: tuple[str, ...] | list[str] = (),
        *,
        overlap_group: str | None = None,
        inherited_overlap_group: str | None = None,
        timeout_minutes: float | None = None,
        **_: object,
    ) -> None:
        calls.append(
            (
                module,
                tuple(args),
                overlap_group,
                inherited_overlap_group,
                timeout_minutes,
            )
        )

    @contextmanager
    def fake_guard(context: object, overlap_group: str):
        yield

    monkeypatch.setattr(definitions, "run_module", fake_run_module)
    monkeypatch.setattr(definitions, "job_overlap_guard", fake_guard)

    assert definitions.ezytrack_daily_reconciliation.execute_in_process().success
    assert calls.pop() == (
        "jobs.sync_ezytrack",
        ("--reconcile",),
        runner.EZYTRACK_OVERLAP_GROUP,
        None,
        None,
    )

    assert definitions.trackunit_rolling_7_days.execute_in_process().success
    assert calls.pop() == (
        "jobs.sync_trackunit_daily_activity",
        ("--rolling-days", "7"),
        runner.TRACKUNIT_OVERLAP_GROUP,
        None,
        None,
    )

    assert definitions.trackunit_intraday_refresh.execute_in_process().success
    assert calls.pop() == (
        "jobs.sync_trackunit_daily_activity",
        ("--rolling-days", "1"),
        runner.TRACKUNIT_OVERLAP_GROUP,
        None,
        None,
    )

    monotonic_values = iter([100.0, 160.0])
    monkeypatch.setattr(definitions, "get_module_timeout_minutes", lambda module: 120.0)
    monkeypatch.setattr(definitions, "monotonic", lambda: next(monotonic_values))
    assert definitions.trackunit_daily_refresh.execute_in_process().success
    assert calls == [
        (
            "jobs.sync_trackunit_daily_activity",
            ("--rolling-days", "2"),
            None,
            runner.TRACKUNIT_OVERLAP_GROUP,
            120.0,
        ),
        (
            "jobs.sync_trackunit_location_enrichment",
            (),
            None,
            runner.TRACKUNIT_OVERLAP_GROUP,
            119.0,
        ),
    ]


def test_inherited_marker_bypasses_only_the_matching_parent_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    monkeypatch.setattr(overlap_lock, "LOCK_DIRECTORY", tmp_path / "locks")

    with overlap_lock.provider_overlap_guard(overlap_lock.TRACKUNIT_OVERLAP_GROUP):
        monkeypatch.setenv(
            overlap_lock.INHERITED_OVERLAP_GROUP_ENV,
            overlap_lock.TRACKUNIT_OVERLAP_GROUP,
        )
        # This models the child-side job decorator while Dagster retains the
        # real lock in its supervising process.
        with overlap_lock.provider_overlap_guard(
            overlap_lock.TRACKUNIT_OVERLAP_GROUP,
            accept_inherited=True,
        ) as inherited_path:
            assert inherited_path is None

        # A marker for Trackunit must not bypass a different provider lock.
        with overlap_lock.provider_overlap_guard(
            overlap_lock.SENDEM_OVERLAP_GROUP,
            accept_inherited=True,
        ) as sendem_path:
            assert sendem_path is not None


def test_overlap_mapping_covers_ezytrack_reconciliation_and_both_trackunit_sources() -> None:
    assert dagster_jobs_for_sync_run("ezytrack", "ezytrack_hourly_sync") == EZYTRACK_DAGSTER_JOB_NAMES
    assert dagster_jobs_for_sync_run("ezytrack", "ezytrack_daily_reconciliation") == EZYTRACK_DAGSTER_JOB_NAMES
    assert dagster_jobs_for_sync_run("trackunit", "trackunit_daily_activity_sync") == TRACKUNIT_DAGSTER_JOB_NAMES
    assert dagster_jobs_for_sync_run(
        "trackunit_location", "trackunit_location_enrichment_sync"
    ) == TRACKUNIT_DAGSTER_JOB_NAMES
    assert dg.DagsterRunStatus.NOT_STARTED in ACTIVE_RUN_STATUSES
    assert dg.DagsterRunStatus.MANAGED in ACTIVE_RUN_STATUSES


def test_stale_cleanup_fails_closed_for_an_unmapped_sync_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = {
        "sync_run_id": "unknown-stale-run",
        "source_system": "future_provider",
        "job_name": "renamed_job",
        "started_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
    }

    class FakeLoader:
        mark_calls = 0

        def mark_abandoned_runs(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            self.mark_calls += 1
            return [candidate]

    loader = FakeLoader()
    monkeypatch.setattr(monitoring, "list_stale_started_runs", lambda *args, **kwargs: [candidate])

    with pytest.raises(dg.Failure, match="could not verify active Dagster work"):
        monitoring.cleanup_stale_started_runs(
            FakeContext(instance=object()),  # type: ignore[arg-type]
            loader,  # type: ignore[arg-type]
            _ops_settings(),  # type: ignore[arg-type]
            now_utc=datetime(2026, 8, 3, tzinfo=timezone.utc),
        )

    assert loader.mark_calls == 0


def test_stale_cleanup_defers_every_update_while_corresponding_dagster_work_is_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = {
        "sync_run_id": "stale-run",
        "source_system": "trackunit_location",
        "job_name": "trackunit_location_enrichment_sync",
        "started_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
    }

    class FakeLoader:
        mark_calls = 0

        def mark_abandoned_runs(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            self.mark_calls += 1
            return [candidate]

    loader = FakeLoader()
    monkeypatch.setattr(monitoring, "list_stale_started_runs", lambda *args, **kwargs: [candidate])
    monkeypatch.setattr(
        monitoring,
        "stale_run_blocking_jobs",
        lambda *args, **kwargs: {"trackunit_daily_refresh"},
    )

    marked = monitoring.cleanup_stale_started_runs(
        FakeContext(instance=object()),  # type: ignore[arg-type]
        loader,  # type: ignore[arg-type]
        _ops_settings(),  # type: ignore[arg-type]
        now_utc=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )

    assert marked == []
    assert loader.mark_calls == 0


def test_stale_cleanup_marks_and_alerts_only_after_active_check_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = {
        "sync_run_id": "stale-run",
        "source_system": "sendem",
        "job_name": "sendem_hourly_sync",
        "started_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
    }
    alert_calls: list[dict[str, object]] = []

    class FakeLoader:
        def mark_abandoned_runs(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            return [candidate]

        def get_last_successful_run(self, source_system: str) -> None:
            return None

    monkeypatch.setattr(monitoring, "list_stale_started_runs", lambda *args, **kwargs: [candidate])
    monkeypatch.setattr(monitoring, "stale_run_blocking_jobs", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        monitoring,
        "send_operational_alert",
        lambda **kwargs: alert_calls.append(kwargs),
    )

    marked = monitoring.cleanup_stale_started_runs(
        FakeContext(instance=object()),  # type: ignore[arg-type]
        FakeLoader(),  # type: ignore[arg-type]
        _ops_settings(),  # type: ignore[arg-type]
        now_utc=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )

    assert marked == [candidate]
    assert alert_calls[0]["event_type"] == "abandoned"
    assert alert_calls[0]["run_id"] == "stale-run"
    assert alert_calls[0]["dedupe_key"] == "abandoned:stale-run"


def test_freshness_cursor_cooldown_and_recovery_are_deterministic() -> None:
    now = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    cursor = alerts.encode_freshness_cursor({"freshness:sendem": now - timedelta(minutes=30)})
    decoded = alerts.decode_freshness_cursor(cursor)

    assert not alerts.alert_due_after_cooldown(
        decoded["freshness:sendem"],
        now_utc=now,
        cooldown_minutes=60,
    )
    assert alerts.alert_due_after_cooldown(
        decoded["freshness:sendem"],
        now_utc=now + timedelta(minutes=31),
        cooldown_minutes=60,
    )
    assert alerts.is_freshness_stale(
        now - timedelta(hours=13),
        now_utc=now,
        max_success_age_hours=12,
    )
    assert not alerts.is_freshness_stale(
        now - timedelta(hours=1),
        now_utc=now,
        max_success_age_hours=12,
    )


def test_disabled_webhook_is_logged_and_never_called(monkeypatch: pytest.MonkeyPatch) -> None:
    context = FakeContext()
    monkeypatch.setattr(
        alerts.requests,
        "post",
        lambda *args, **kwargs: pytest.fail("disabled alerts must not call a webhook"),
    )

    delivery = alerts.send_operational_alert(
        event_type="run_failure",
        provider="sendem",
        job_name="sendem_sync",
        run_id="dagster-run-id",
        failure_message="root cause",
        timestamp=datetime(2026, 8, 3, tzinfo=timezone.utc),
        last_success_at=None,
        dedupe_key="run_failure:dagster-run-id",
        settings=_ops_settings(),  # type: ignore[arg-type]
        log=context.log,
    )

    assert delivery.delivered is False
    assert delivery.reason == "alerts_disabled"
    assert "TELEMETRY_ALERTS_ENABLED=false" in context.log.entries[0][1]


def test_generic_webhook_payload_contains_required_operational_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request: dict[str, object] = {}

    class SuccessfulResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: object) -> SuccessfulResponse:
        captured_request.update({"url": url, **kwargs})
        return SuccessfulResponse()

    monkeypatch.setattr(alerts.requests, "post", fake_post)
    occurred_at = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    last_success = occurred_at - timedelta(hours=13)
    delivery = alerts.send_operational_alert(
        event_type="freshness",
        provider="ezytrack",
        job_name="ezytrack_hourly_sync",
        run_id="sync-run-id",
        failure_message="last success is too old",
        timestamp=occurred_at,
        last_success_at=last_success,
        dedupe_key="freshness:ezytrack",
        settings=_ops_settings(  # type: ignore[arg-type]
            alerts_enabled=True,
            alert_webhook_url="https://alerts.example.invalid/telemetry",
        ),
        log=CapturingLog(),
    )

    payload = captured_request["json"]
    assert delivery.delivered is True
    assert captured_request["url"] == "https://alerts.example.invalid/telemetry"
    assert isinstance(payload, dict)
    assert payload == {
        "event_type": "freshness",
        "provider": "ezytrack",
        "job_name": "ezytrack_hourly_sync",
        "run_id": "sync-run-id",
        "failure_message": "last success is too old",
        "timestamp": occurred_at.isoformat(),
        "last_success_at": last_success.isoformat(),
        "dedupe_key": "freshness:ezytrack",
        "text": (
            "Telemetry alert [freshness] provider=ezytrack job=ezytrack_hourly_sync "
            "run_id=sync-run-id failure=last success is too old "
            f"timestamp={occurred_at.isoformat()} last_success={last_success.isoformat()}"
        ),
    }


@pytest.mark.parametrize("cursor", ["not-json", "[]", '{"version":1,"last_alerted_at":[]}'])
def test_malformed_freshness_cursor_resets_safely(cursor: str) -> None:
    assert alerts.decode_freshness_cursor(cursor) == {}
