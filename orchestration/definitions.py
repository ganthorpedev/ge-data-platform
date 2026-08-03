"""Dagster 1.13.14 definitions for telemetry provider and operations jobs."""

from time import monotonic

import dagster as dg

from orchestration.monitoring import sensors, stale_started_run_cleanup
from orchestration.runner import (
    EZYTRACK_OVERLAP_GROUP,
    SENDEM_OVERLAP_GROUP,
    TRACKUNIT_OVERLAP_GROUP,
    get_module_timeout_minutes,
    job_overlap_guard,
    run_module,
)


SENDEM_RUN_TAGS = {"telemetry/provider": "sendem"}
EZYTRACK_RUN_TAGS = {"telemetry/provider": "ezytrack"}
TRACKUNIT_RUN_TAGS = {"telemetry/provider": "trackunit"}


@dg.op
def smoke_test_op(context: dg.OpExecutionContext) -> None:
    context.log.info("Dagster smoke test succeeded: orchestration wiring is healthy.")


@dg.job
def dagster_smoke_test() -> None:
    smoke_test_op()


@dg.op
def sendem_sync_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_sendem",
        overlap_group=SENDEM_OVERLAP_GROUP,
    )


@dg.job(tags=SENDEM_RUN_TAGS)
def sendem_sync() -> None:
    sendem_sync_op()


@dg.op
def ezytrack_sync_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_ezytrack",
        overlap_group=EZYTRACK_OVERLAP_GROUP,
    )


@dg.job(tags=EZYTRACK_RUN_TAGS)
def ezytrack_sync() -> None:
    ezytrack_sync_op()


@dg.op
def ezytrack_daily_reconciliation_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_ezytrack",
        ["--reconcile"],
        overlap_group=EZYTRACK_OVERLAP_GROUP,
    )


@dg.job(tags=EZYTRACK_RUN_TAGS)
def ezytrack_daily_reconciliation() -> None:
    ezytrack_daily_reconciliation_op()


@dg.op
def trackunit_rolling_2_days_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_daily_activity",
        ["--rolling-days", "2"],
        overlap_group=TRACKUNIT_OVERLAP_GROUP,
    )


@dg.job(tags=TRACKUNIT_RUN_TAGS)
def trackunit_rolling_2_days() -> None:
    trackunit_rolling_2_days_op()


@dg.op
def trackunit_rolling_7_days_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_daily_activity",
        ["--rolling-days", "7"],
        overlap_group=TRACKUNIT_OVERLAP_GROUP,
    )


@dg.job(tags=TRACKUNIT_RUN_TAGS)
def trackunit_rolling_7_days() -> None:
    trackunit_rolling_7_days_op()


@dg.op
def trackunit_location_enrichment_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_location_enrichment",
        overlap_group=TRACKUNIT_OVERLAP_GROUP,
    )


@dg.job(tags=TRACKUNIT_RUN_TAGS)
def trackunit_location_enrichment() -> None:
    trackunit_location_enrichment_op()


@dg.op
def trackunit_daily_refresh_op(context: dg.OpExecutionContext) -> None:
    """Hold one lock and one timeout budget across activity plus enrichment."""
    timeout_budget_minutes = get_module_timeout_minutes(
        "jobs.sync_trackunit_daily_activity"
    )
    started_at = monotonic()
    with job_overlap_guard(context, TRACKUNIT_OVERLAP_GROUP):
        run_module(
            context,
            "jobs.sync_trackunit_daily_activity",
            ["--rolling-days", "2"],
            inherited_overlap_group=TRACKUNIT_OVERLAP_GROUP,
            timeout_minutes=timeout_budget_minutes,
        )
        # This second command only runs if daily activity completed.  Keeping
        # both under one outer lock prevents a manual Trackunit run from
        # entering between the two subprocesses.  It receives only the
        # remainder of the provider's six-hour default budget, so Dagster's
        # seven-hour run-monitor ceiling cannot preempt a still-valid child.
        elapsed_minutes = (monotonic() - started_at) / 60.0
        remaining_minutes = timeout_budget_minutes - elapsed_minutes
        if remaining_minutes <= 0:
            raise dg.Failure(
                description=(
                    "Trackunit daily activity consumed the complete "
                    f"{timeout_budget_minutes:.2f}-minute daily refresh timeout; "
                    "location enrichment was not started."
                ),
                allow_retries=False,
            )
        run_module(
            context,
            "jobs.sync_trackunit_location_enrichment",
            inherited_overlap_group=TRACKUNIT_OVERLAP_GROUP,
            timeout_minutes=remaining_minutes,
        )


@dg.job(tags=TRACKUNIT_RUN_TAGS)
def trackunit_daily_refresh() -> None:
    trackunit_daily_refresh_op()


defs_jobs = [
    dagster_smoke_test,
    sendem_sync,
    ezytrack_sync,
    ezytrack_daily_reconciliation,
    trackunit_rolling_2_days,
    trackunit_rolling_7_days,
    trackunit_location_enrichment,
    trackunit_daily_refresh,
    stale_started_run_cleanup,
]

# Imported after job construction because orchestration/schedules.py imports
# these JobDefinition objects.  This preserves the project's established
# single public Definitions entry point while remaining loadable on 1.13.14.
from orchestration.schedules import schedules  # noqa: E402

defs = dg.Definitions(jobs=defs_jobs, schedules=schedules, sensors=sensors)
