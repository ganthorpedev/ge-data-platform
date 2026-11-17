"""Built-in diagnostic steps used to exercise the engine (DAGs, retries, timeouts, kills, cancellation).

None of these touch any database or external system.
"""

from __future__ import annotations

import time

from pydantic import Field

from ge_control_center.registry import PermanentStepError, StepContext, StepParams, step


class NoopParams(StepParams):
    message: str = Field("noop", max_length=500)


@step(
    key="core.noop",
    version="1",
    display_name="No-op",
    description="Does nothing except log a message. Used for wiring and DAG-ordering checks.",
    params=NoopParams,
    idempotency="read_only",
    max_attempts=1,
    retry_backoff_seconds=0,
    retry_backoff_max_seconds=0,
    timeout_seconds=60,
)
def noop(ctx: StepContext, params: NoopParams) -> dict:
    ctx.log.info("noop: %s", params.message)
    return {"message": params.message}


class SleepParams(StepParams):
    seconds: float = Field(ge=0, le=86400)
    progress_every_seconds: float = Field(5.0, gt=0, le=3600)


@step(
    key="core.sleep",
    version="1",
    display_name="Sleep",
    description="Sleeps for a fixed time, logging progress. Used to test kills, lease recovery, timeouts and cancellation.",
    params=SleepParams,
    idempotency="read_only",
    max_attempts=3,
    retry_backoff_seconds=5,
    retry_backoff_max_seconds=60,
    timeout_seconds=86400,
)
def sleep(ctx: StepContext, params: SleepParams) -> dict:
    started = time.monotonic()
    next_report = params.progress_every_seconds
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= params.seconds:
            break
        if elapsed >= next_report:
            ctx.log.info("slept %.1f / %.1f s (attempt %d)", elapsed, params.seconds, ctx.attempt_no)
            next_report += params.progress_every_seconds
        time.sleep(min(0.2, params.seconds - elapsed))
    ctx.log.info("done sleeping %.1f s", params.seconds)
    return {"slept_seconds": params.seconds}


class FailNTimesParams(StepParams):
    fail_times: int = Field(ge=0, le=19, description="Fail this many attempts, then succeed.")
    permanent: bool = Field(False, description="Raise a permanent (non-retryable) error instead.")


@step(
    key="core.fail_n_times",
    version="1",
    display_name="Fail N times",
    description="Fails its first N attempts (based on the attempt number), then succeeds. Used to test retries and backoff.",
    params=FailNTimesParams,
    idempotency="read_only",
    max_attempts=3,
    retry_backoff_seconds=2,
    retry_backoff_max_seconds=30,
    timeout_seconds=60,
)
def fail_n_times(ctx: StepContext, params: FailNTimesParams) -> dict:
    if ctx.attempt_no <= params.fail_times:
        message = f"deliberate failure on attempt {ctx.attempt_no} of {params.fail_times} planned"
        if params.permanent:
            raise PermanentStepError(message)
        raise RuntimeError(message)
    ctx.log.info("succeeding on attempt %d", ctx.attempt_no)
    return {"succeeded_on_attempt": ctx.attempt_no}
