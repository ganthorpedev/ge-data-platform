"""Shared Dagster active-run and provider-overlap definitions."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import dagster as dg


# Include every non-terminal Dagster 1.13.14 status.  In particular,
# NOT_STARTED and MANAGED can exist before STARTING and must still prevent a
# second run from being launched.
ACTIVE_RUN_STATUSES: tuple[dg.DagsterRunStatus, ...] = (
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.MANAGED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
)

SENDEM_DAGSTER_JOB_NAMES: tuple[str, ...] = ("sendem_sync",)
EZYTRACK_DAGSTER_JOB_NAMES: tuple[str, ...] = (
    "ezytrack_sync",
    "ezytrack_daily_reconciliation",
)
TRACKUNIT_DAGSTER_JOB_NAMES: tuple[str, ...] = (
    "trackunit_daily_refresh",
    "trackunit_rolling_2_days",
    "trackunit_rolling_7_days",
    "trackunit_location_enrichment",
)
HOUSEKEEPING_DAGSTER_JOB_NAMES: tuple[str, ...] = ("stale_started_run_cleanup",)

ALL_PROVIDER_DAGSTER_JOB_NAMES: tuple[str, ...] = (
    *SENDEM_DAGSTER_JOB_NAMES,
    *EZYTRACK_DAGSTER_JOB_NAMES,
    *TRACKUNIT_DAGSTER_JOB_NAMES,
)


def find_active_run_job(
    instance: dg.DagsterInstance,
    job_names: Sequence[str],
) -> str | None:
    """Return the first requested job name with a non-terminal Dagster run."""
    for job_name in job_names:
        runs = instance.get_runs(
            filters=dg.RunsFilter(job_name=job_name, statuses=list(ACTIVE_RUN_STATUSES)),
            limit=1,
        )
        if runs:
            return job_name
    return None


def find_active_run_jobs(
    instance: dg.DagsterInstance,
    job_names: Iterable[str],
) -> set[str]:
    """Return all requested job names with at least one non-terminal run."""
    return {
        job_name
        for job_name in job_names
        if find_active_run_job(instance, [job_name]) is not None
    }


def dagster_jobs_for_sync_run(source_system: str, job_name: str) -> tuple[str, ...]:
    """Map an ``etl.sync_runs`` row to Dagster jobs that may own its work.

    The warehouse predates Dagster and does not store a Dagster run ID.  A
    conservative provider/job mapping is therefore used: if any plausible
    owning Dagster job is active, housekeeping defers that stale row.
    """
    normalized_source = source_system.strip().lower()
    normalized_job = job_name.strip().lower()

    if normalized_source == "sendem" or normalized_job == "sendem_hourly_sync":
        return SENDEM_DAGSTER_JOB_NAMES
    if normalized_source == "ezytrack" or normalized_job in {
        "ezytrack_hourly_sync",
        "ezytrack_daily_reconciliation",
    }:
        return EZYTRACK_DAGSTER_JOB_NAMES
    if normalized_source in {"trackunit", "trackunit_location"}:
        return TRACKUNIT_DAGSTER_JOB_NAMES
    if normalized_job == "trackunit_daily_activity_sync":
        return TRACKUNIT_DAGSTER_JOB_NAMES
    if normalized_job == "trackunit_location_enrichment_sync":
        return TRACKUNIT_DAGSTER_JOB_NAMES
    return ()
