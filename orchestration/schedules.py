"""Cron schedules for the telemetry ETL jobs defined in orchestration/definitions.py.

Every schedule here is created with default_status=STOPPED -- they must be
turned on manually in the Dagster UI (Automation tab) after review. This is
intentional: do not flip any of these to RUNNING as part of a code change.

Overlap protection: each schedule's evaluation function checks the Dagster
instance for runs of the relevant job(s) already in QUEUED, STARTING,
STARTED, or CANCELING before requesting a new run, and returns a SkipReason
(logged by Dagster) instead of a RunRequest when one is found. This is a
simple in-instance check, not a distributed lock -- it is sufficient here
because these jobs are all launched by the same Dagster instance/daemon.

trackunit_3_hour_refresh and trackunit_rolling_7_days both touch
staging.trackunit_daily_activity, so they additionally guard against each
other (not just against themselves) to avoid a concurrent write conflict.
"""

from __future__ import annotations

import dagster as dg

from orchestration.definitions import (
    ezytrack_sync,
    sendem_sync,
    trackunit_3_hour_refresh,
    trackunit_rolling_7_days,
)

TIMEZONE = "Africa/Harare"

_ACTIVE_RUN_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


def _find_active_run_job(instance: dg.DagsterInstance, job_names: list[str]) -> str | None:
    """Return the first job name in job_names with an active run, or None."""
    for job_name in job_names:
        runs = instance.get_runs(
            filters=dg.RunsFilter(job_name=job_name, statuses=_ACTIVE_RUN_STATUSES),
            limit=1,
        )
        if runs:
            return job_name
    return None


def _skip_if_overlapping(
    context: dg.ScheduleEvaluationContext,
    job_name: str,
    overlap_with: list[str],
) -> dg.SkipReason | dg.RunRequest:
    """Shared overlap-protection logic for every schedule below.

    `overlap_with` is the full set of job names (including `job_name` itself)
    that must have no active run before this schedule is allowed to launch.
    """
    blocking_job = _find_active_run_job(context.instance, overlap_with)
    if blocking_job is not None:
        reason = (
            f"Skipping scheduled launch of '{job_name}': job '{blocking_job}' already has a "
            "run in QUEUED, STARTING, STARTED, or CANCELING."
        )
        context.log.warning(reason)
        return dg.SkipReason(reason)

    return dg.RunRequest(run_key=context.scheduled_execution_time.isoformat())


@dg.schedule(
    job=trackunit_3_hour_refresh,
    cron_schedule="5 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def trackunit_3_hour_refresh_schedule(context: dg.ScheduleEvaluationContext) -> dg.SkipReason | dg.RunRequest:
    """Rolling 2-day activity refresh, then location enrichment, every 3 hours."""
    return _skip_if_overlapping(
        context,
        job_name="trackunit_3_hour_refresh",
        overlap_with=["trackunit_3_hour_refresh", "trackunit_rolling_7_days"],
    )


@dg.schedule(
    job=sendem_sync,
    cron_schedule="35 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def sendem_sync_schedule(context: dg.ScheduleEvaluationContext) -> dg.SkipReason | dg.RunRequest:
    return _skip_if_overlapping(context, job_name="sendem_sync", overlap_with=["sendem_sync"])


@dg.schedule(
    job=ezytrack_sync,
    cron_schedule="45 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def ezytrack_sync_schedule(context: dg.ScheduleEvaluationContext) -> dg.SkipReason | dg.RunRequest:
    # No retry policy attached here on purpose: a rate-limit (429) failure
    # should fail this run clearly and wait for the next scheduled launch,
    # not retry within the run.
    return _skip_if_overlapping(context, job_name="ezytrack_sync", overlap_with=["ezytrack_sync"])


@dg.schedule(
    job=trackunit_rolling_7_days,
    cron_schedule="45 1 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def trackunit_rolling_7_days_schedule(context: dg.ScheduleEvaluationContext) -> dg.SkipReason | dg.RunRequest:
    """Daily reconciliation/backfill run."""
    return _skip_if_overlapping(
        context,
        job_name="trackunit_rolling_7_days",
        overlap_with=["trackunit_rolling_7_days", "trackunit_3_hour_refresh"],
    )


schedules = [
    trackunit_3_hour_refresh_schedule,
    sendem_sync_schedule,
    ezytrack_sync_schedule,
    trackunit_rolling_7_days_schedule,
]
