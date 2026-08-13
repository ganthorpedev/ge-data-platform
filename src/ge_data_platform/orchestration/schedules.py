"""Cron schedules and schedule-time overlap protection.

Provider schedules keep ``default_status=STOPPED`` to preserve this project's
deployment convention: each must be reviewed and enabled in the Dagster UI.
The two operational sensors have their own explicit status in
``ge_data_platform.orchestration.monitoring``.

Every provider schedule checks all Dagster jobs in its overlap group, not
only its own job.  The OS lock in ``ge_data_platform.orchestration.runner``
and the optional Dagster run-tag concurrency rule in
``dagster.yaml.example`` close the race after schedule evaluation and
protect manual launches too.
"""

from collections.abc import Sequence

import dagster as dg

from ge_data_platform.orchestration.definitions import (
    ezytrack_daily_reconciliation,
    ezytrack_sync,
    sendem_sync,
    stale_started_run_cleanup,
    trackunit_daily_refresh,
    trackunit_intraday_refresh,
    trackunit_rolling_7_days,
)
from ge_data_platform.orchestration.overlap import (
    EZYTRACK_DAGSTER_JOB_NAMES,
    HOUSEKEEPING_DAGSTER_JOB_NAMES,
    SENDEM_DAGSTER_JOB_NAMES,
    TRACKUNIT_DAGSTER_JOB_NAMES,
    find_active_run_job,
)


TIMEZONE = "Africa/Harare"


def _skip_if_overlapping(
    context: dg.ScheduleEvaluationContext,
    job_name: str,
    overlap_with: Sequence[str],
    *,
    run_tags: dict[str, str] | None = None,
) -> dg.SkipReason | dg.RunRequest:
    """Skip when any non-terminal run exists in the supplied overlap group."""
    blocking_job = find_active_run_job(context.instance, overlap_with)
    if blocking_job is not None:
        reason = (
            f"Skipping scheduled launch of {job_name!r}: {blocking_job!r} already has "
            "a run in a non-terminal Dagster status."
        )
        context.log.warning(reason)
        return dg.SkipReason(reason)

    scheduled_time = context.scheduled_execution_time
    return dg.RunRequest(
        run_key=scheduled_time.isoformat() if scheduled_time is not None else None,
        tags=run_tags,
    )


@dg.schedule(
    job=trackunit_daily_refresh,
    cron_schedule="5 2 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def trackunit_daily_refresh_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Daily rolling two-day activity refresh followed by enrichment."""
    return _skip_if_overlapping(
        context,
        job_name="trackunit_daily_refresh",
        overlap_with=TRACKUNIT_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "trackunit"},
    )


@dg.schedule(
    job=trackunit_intraday_refresh,
    cron_schedule="20 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def trackunit_intraday_refresh_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Safe intraday rolling one-day activity refresh; no enrichment.

    Shares the Trackunit overlap group with the daily and weekly jobs, so an
    active daily refresh or weekly reconciliation causes this evaluation to
    skip with a clear reason instead of launching a concurrent run.
    """
    return _skip_if_overlapping(
        context,
        job_name="trackunit_intraday_refresh",
        overlap_with=TRACKUNIT_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "trackunit"},
    )


@dg.schedule(
    job=sendem_sync,
    cron_schedule="35 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def sendem_sync_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Existing three-hour Sendem schedule (unchanged)."""
    return _skip_if_overlapping(
        context,
        job_name="sendem_sync",
        overlap_with=SENDEM_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "sendem"},
    )


@dg.schedule(
    job=ezytrack_sync,
    cron_schedule="45 */3 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def ezytrack_sync_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Existing three-hour EzyTrack incremental/catch-up schedule."""
    return _skip_if_overlapping(
        context,
        job_name="ezytrack_sync",
        overlap_with=EZYTRACK_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "ezytrack"},
    )


@dg.schedule(
    job=ezytrack_daily_reconciliation,
    cron_schedule="15 1 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def ezytrack_daily_reconciliation_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Daily long-lookback reconciliation using ``--reconcile``."""
    return _skip_if_overlapping(
        context,
        job_name="ezytrack_daily_reconciliation",
        overlap_with=EZYTRACK_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "ezytrack"},
    )


@dg.schedule(
    job=trackunit_rolling_7_days,
    cron_schedule="45 1 * * 0",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def trackunit_rolling_7_days_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Weekly rolling seven-day Trackunit reconciliation (Sunday)."""
    return _skip_if_overlapping(
        context,
        job_name="trackunit_rolling_7_days",
        overlap_with=TRACKUNIT_DAGSTER_JOB_NAMES,
        run_tags={"telemetry/provider": "trackunit"},
    )


@dg.schedule(
    job=stale_started_run_cleanup,
    cron_schedule="20 * * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def stale_started_run_cleanup_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.SkipReason | dg.RunRequest:
    """Hourly conservative STARTED-to-ABANDONED housekeeping."""
    return _skip_if_overlapping(
        context,
        job_name="stale_started_run_cleanup",
        overlap_with=HOUSEKEEPING_DAGSTER_JOB_NAMES,
    )


schedules = [
    trackunit_daily_refresh_schedule,
    trackunit_intraday_refresh_schedule,
    sendem_sync_schedule,
    ezytrack_sync_schedule,
    ezytrack_daily_reconciliation_schedule,
    trackunit_rolling_7_days_schedule,
    stale_started_run_cleanup_schedule,
]
