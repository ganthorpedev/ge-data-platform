import dagster as dg

from orchestration.runner import run_module


@dg.op
def smoke_test_op(context: dg.OpExecutionContext) -> None:
    context.log.info("Dagster smoke test succeeded: orchestration wiring is healthy.")


@dg.job
def dagster_smoke_test() -> None:
    smoke_test_op()


@dg.op
def sendem_sync_op(context: dg.OpExecutionContext) -> None:
    run_module(context, "jobs.sync_sendem")


@dg.job
def sendem_sync() -> None:
    sendem_sync_op()


@dg.op
def ezytrack_sync_op(context: dg.OpExecutionContext) -> None:
    run_module(context, "jobs.sync_ezytrack")


@dg.job
def ezytrack_sync() -> None:
    ezytrack_sync_op()


@dg.op
def trackunit_rolling_2_days_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_daily_activity",
        ["--rolling-days", "2"],
    )


@dg.job
def trackunit_rolling_2_days() -> None:
    trackunit_rolling_2_days_op()


@dg.op
def trackunit_rolling_7_days_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_daily_activity",
        ["--rolling-days", "7"],
    )


@dg.job
def trackunit_rolling_7_days() -> None:
    trackunit_rolling_7_days_op()


@dg.op
def trackunit_location_enrichment_op(context: dg.OpExecutionContext) -> None:
    run_module(context, "jobs.sync_trackunit_location_enrichment")


@dg.job
def trackunit_location_enrichment() -> None:
    trackunit_location_enrichment_op()


# Ops for the combined 3-hour refresh job. Kept separate from
# trackunit_rolling_2_days_op / trackunit_location_enrichment_op above (rather
# than reused directly) so the standalone jobs are untouched; the `Nothing`
# in/out pair below only exists to force Dagster to run activity to
# completion before enrichment starts, with no data actually passed between
# them -- if activity fails, the job plan never reaches enrichment.
@dg.op(out=dg.Out(dagster_type=dg.Nothing))
def trackunit_3_hour_activity_op(context: dg.OpExecutionContext) -> None:
    run_module(
        context,
        "jobs.sync_trackunit_daily_activity",
        ["--rolling-days", "2"],
    )


@dg.op(ins={"start_after": dg.In(dagster_type=dg.Nothing)})
def trackunit_3_hour_enrichment_op(context: dg.OpExecutionContext) -> None:
    run_module(context, "jobs.sync_trackunit_location_enrichment")


@dg.job
def trackunit_3_hour_refresh() -> None:
    trackunit_3_hour_enrichment_op(start_after=trackunit_3_hour_activity_op())


defs_jobs = [
    dagster_smoke_test,
    sendem_sync,
    ezytrack_sync,
    trackunit_rolling_2_days,
    trackunit_rolling_7_days,
    trackunit_location_enrichment,
    trackunit_3_hour_refresh,
]

# Imported at the bottom on purpose: orchestration/schedules.py imports the
# job objects defined above from this module, so this module must finish
# defining them first.
from orchestration.schedules import schedules  # noqa: E402

defs = dg.Definitions(jobs=defs_jobs, schedules=schedules)
