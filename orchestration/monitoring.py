"""Dagster failure alerts, freshness checks, and stale-run housekeeping."""

import logging
from datetime import datetime

import dagster as dg
from sqlalchemy import text

from config.settings import EtlOpsSettings, get_etl_ops_settings, get_settings
from loaders.postgres_loader import (
    PostgresLoader,
    compute_abandoned_cutoff,
)
from orchestration.alerts import (
    alert_due_after_cooldown,
    decode_freshness_cursor,
    encode_freshness_cursor,
    is_freshness_stale,
    latest_success_at,
    provider_freshness_targets,
    send_operational_alert,
)
from orchestration.overlap import (
    dagster_jobs_for_sync_run,
    find_active_run_jobs,
)
from utils.dates import utc_now


logger = logging.getLogger(__name__)
FRESHNESS_CHECK_INTERVAL_SECONDS = 15 * 60


def _dispose_loader_safely(loader: PostgresLoader, log: object) -> None:
    """Dispose monitoring connections without masking the monitored outcome."""
    try:
        loader.engine.dispose()
    except Exception as error:
        log.warning(  # type: ignore[attr-defined]
            "Could not dispose the monitoring PostgreSQL engine cleanly: %s",
            error,
        )


def _provider_and_source_for_dagster_job(job_name: str) -> tuple[str, str | None]:
    """Return alert provider plus warehouse source for a Dagster job."""
    if job_name.startswith("sendem"):
        return "sendem", "sendem"
    if job_name.startswith("ezytrack"):
        return "ezytrack", "ezytrack"
    if job_name == "trackunit_location_enrichment":
        return "trackunit_location", "trackunit_location"
    if job_name.startswith("trackunit"):
        return "trackunit", "trackunit"
    return "orchestration", None


def _failure_message(context: dg.RunFailureSensorContext) -> str:
    """Extract the clearest available Dagster failure text."""
    message = getattr(context.failure_event, "message", None)
    if isinstance(message, str) and message.strip():
        return message.strip()
    return "Dagster run failed without a failure message"


def _lookup_latest_success(source_system: str | None) -> dict[str, object] | None:
    """Best-effort lookup used by failure alerts.

    A provider failure may itself be caused by PostgreSQL being unavailable;
    this lookup must therefore never replace or suppress its alert.
    """
    if source_system is None:
        return None
    loader: PostgresLoader | None = None
    try:
        loader = PostgresLoader(get_settings())
        return loader.get_last_successful_run(source_system)
    except Exception:
        logger.exception(
            "Could not look up the latest successful %s sync for a failure alert",
            source_system,
        )
        return None
    finally:
        if loader is not None:
            _dispose_loader_safely(loader, logger)


@dg.run_failure_sensor(
    name="telemetry_run_failure_sensor",
    description=(
        "Sends a provider-neutral webhook alert for any failed run in this repository. "
        "Dagster's persisted run-status cursor deduplicates each failed run ID."
    ),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def telemetry_run_failure_sensor(context: dg.RunFailureSensorContext) -> None:
    """Alert once for each Dagster run that transitions to failure."""
    dagster_run = context.dagster_run
    provider, source_system = _provider_and_source_for_dagster_job(dagster_run.job_name)
    latest_success = _lookup_latest_success(source_system)
    run_id = dagster_run.run_id
    send_operational_alert(
        event_type="run_failure",
        provider=provider,
        job_name=dagster_run.job_name,
        run_id=run_id,
        failure_message=_failure_message(context),
        timestamp=utc_now(),
        last_success_at=latest_success_at(latest_success),
        dedupe_key=f"run_failure:{run_id}",
        log=context.log,
    )


@dg.sensor(
    name="telemetry_provider_freshness_sensor",
    minimum_interval_seconds=FRESHNESS_CHECK_INTERVAL_SECONDS,
    description=(
        "Checks latest successful warehouse syncs and emits cooldown-deduplicated "
        "provider freshness alerts."
    ),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def telemetry_provider_freshness_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    """Check provider success ages and persist alert cooldowns in the cursor."""
    settings = get_etl_ops_settings()
    now_utc = utc_now()
    last_alerted_at = decode_freshness_cursor(context.cursor)
    cooldown_minutes = settings.alert_cooldown_minutes
    if cooldown_minutes < 1:
        context.log.error(
            "TELEMETRY_ALERT_COOLDOWN_MINUTES must be at least 1; using 360 minutes "
            "for this freshness evaluation"
        )
        cooldown_minutes = 360

    try:
        loader = PostgresLoader(get_settings())
    except Exception as error:
        message = f"Freshness check could not initialize PostgreSQL: {error}"
        context.log.error(message)
        return dg.SensorResult(
            cursor=encode_freshness_cursor(last_alerted_at),
            skip_reason=message,
        )

    alert_count = 0
    stale_count = 0
    try:
        for target in provider_freshness_targets(settings):
            if target.max_success_age_hours < 1:
                context.log.error(
                    "Skipping %s freshness check: configured threshold must be at least 1 hour, got %s",
                    target.provider,
                    target.max_success_age_hours,
                )
                continue

            try:
                latest_success = loader.get_last_successful_run(target.source_system)
            except Exception as error:
                context.log.error(
                    "Could not query latest successful %s sync: %s",
                    target.provider,
                    error,
                )
                continue

            success_at = latest_success_at(latest_success)
            stale = is_freshness_stale(
                success_at,
                now_utc=now_utc,
                max_success_age_hours=target.max_success_age_hours,
            )
            dedupe_key = f"freshness:{target.source_system}"
            if not stale:
                if dedupe_key in last_alerted_at:
                    context.log.info(
                        "%s freshness recovered; clearing its alert cooldown",
                        target.provider,
                    )
                    last_alerted_at.pop(dedupe_key, None)
                continue

            stale_count += 1
            if not alert_due_after_cooldown(
                last_alerted_at.get(dedupe_key),
                now_utc=now_utc,
                cooldown_minutes=cooldown_minutes,
            ):
                context.log.warning(
                    "%s remains stale; suppressing a duplicate alert within the %s-minute cooldown",
                    target.provider,
                    cooldown_minutes,
                )
                continue

            if success_at is None:
                failure_message = (
                    f"No successful {target.provider} sync exists; maximum allowed age is "
                    f"{target.max_success_age_hours} hour(s)"
                )
            else:
                age_hours = (now_utc - success_at).total_seconds() / 3600
                failure_message = (
                    f"Latest successful {target.provider} sync is {age_hours:.1f} hour(s) old; "
                    f"maximum allowed age is {target.max_success_age_hours} hour(s)"
                )

            run_id_value = latest_success.get("sync_run_id") if latest_success else None
            latest_job = latest_success.get("job_name") if latest_success else None
            send_operational_alert(
                event_type="freshness",
                provider=target.provider,
                job_name=str(latest_job or f"{target.provider}_freshness"),
                run_id=str(run_id_value) if run_id_value is not None else None,
                failure_message=failure_message,
                timestamp=now_utc,
                last_success_at=success_at,
                dedupe_key=dedupe_key,
                settings=settings,
                log=context.log,
            )
            # Cool down log-only and failed delivery attempts too.  A broken
            # endpoint must not create a tight retry/spam loop every 15 min.
            last_alerted_at[dedupe_key] = now_utc
            alert_count += 1
    finally:
        _dispose_loader_safely(loader, context.log)

    summary = (
        f"Freshness check complete: stale_providers={stale_count}, "
        f"alerts_attempted={alert_count}"
    )
    context.log.info(summary)
    return dg.SensorResult(
        cursor=encode_freshness_cursor(last_alerted_at),
        skip_reason=summary,
    )


def list_stale_started_runs(
    loader: PostgresLoader,
    threshold_hours: int,
    *,
    now_utc: datetime | None = None,
) -> list[dict[str, object]]:
    """List STARTED rows eligible for housekeeping without changing them."""
    cutoff = compute_abandoned_cutoff(threshold_hours, now_utc)
    statement = text("""
        SELECT sync_run_id, source_system, job_name, started_at
        FROM etl.sync_runs
        WHERE status = 'STARTED' AND started_at < :cutoff
        ORDER BY started_at
    """)
    with loader.engine.connect() as connection:
        rows = connection.execute(statement, {"cutoff": cutoff}).mappings().all()
    return [dict(row) for row in rows]


def stale_run_blocking_jobs(
    instance: dg.DagsterInstance,
    stale_runs: list[dict[str, object]],
) -> set[str]:
    """Return active Dagster jobs that could correspond to stale rows."""
    plausible_jobs: set[str] = set()
    for run in stale_runs:
        mapped_jobs = dagster_jobs_for_sync_run(
            str(run.get("source_system") or ""),
            str(run.get("job_name") or ""),
        )
        if not mapped_jobs:
            raise ValueError(
                "Cannot safely map stale sync run to a Dagster job: "
                f"sync_run_id={run.get('sync_run_id')!s}, "
                f"source_system={run.get('source_system')!r}, "
                f"job_name={run.get('job_name')!r}"
            )
        plausible_jobs.update(mapped_jobs)
    return find_active_run_jobs(instance, sorted(plausible_jobs))


def cleanup_stale_started_runs(
    context: dg.OpExecutionContext,
    loader: PostgresLoader,
    settings: EtlOpsSettings,
    *,
    now_utc: datetime | None = None,
) -> list[dict[str, object]]:
    """Safely mark stale STARTED rows and alert once for each transition.

    ``PostgresLoader.mark_abandoned_runs`` updates every eligible row in one
    statement.  Therefore, if even one eligible row could belong to an active
    Dagster provider job, this evaluation defers the entire batch.  That is
    intentionally conservative and guarantees housekeeping never abandons
    corresponding live Dagster work.
    """
    effective_now = now_utc or utc_now()
    candidates = list_stale_started_runs(
        loader,
        settings.abandoned_run_hours,
        now_utc=effective_now,
    )
    if not candidates:
        context.log.info("No stale STARTED sync runs are eligible for cleanup")
        return []

    try:
        blocking_jobs = stale_run_blocking_jobs(context.instance, candidates)
    except Exception as error:
        # Failing closed is required: if Dagster activity cannot be checked,
        # no warehouse run may be declared abandoned.
        raise dg.Failure(
            description=(
                "Stale-run cleanup could not verify active Dagster work; no STARTED "
                f"rows were changed: {error}"
            ),
            allow_retries=False,
        ) from error

    if blocking_jobs:
        context.log.warning(
            "Deferring cleanup of %s stale STARTED row(s) because corresponding Dagster "
            "work is active: %s",
            len(candidates),
            ", ".join(sorted(blocking_jobs)),
        )
        return []

    marked = loader.mark_abandoned_runs(
        settings.abandoned_run_hours,
        now_utc=effective_now,
    )
    for run in marked:
        source_system = str(run.get("source_system") or "unknown")
        try:
            latest_success = loader.get_last_successful_run(source_system)
        except Exception as error:
            context.log.error(
                "Could not look up last successful %s sync for ABANDONED alert: %s",
                source_system,
                error,
            )
            latest_success = None
        run_id = str(run.get("sync_run_id") or "unknown")
        send_operational_alert(
            event_type="abandoned",
            provider=source_system,
            job_name=str(run.get("job_name") or "unknown"),
            run_id=run_id,
            failure_message=(
                f"STARTED sync exceeded ETL_ABANDONED_RUN_HOURS="
                f"{settings.abandoned_run_hours} and had no active corresponding Dagster run"
            ),
            timestamp=effective_now,
            last_success_at=latest_success_at(latest_success),
            dedupe_key=f"abandoned:{run_id}",
            settings=settings,
            log=context.log,
        )
    return marked


@dg.op
def stale_started_run_cleanup_op(context: dg.OpExecutionContext) -> None:
    """Dagster op for conservative stale ``etl.sync_runs`` cleanup."""
    settings = get_etl_ops_settings()
    loader = PostgresLoader(get_settings())
    try:
        marked = cleanup_stale_started_runs(context, loader, settings)
        context.log.info("Stale-run cleanup marked %s row(s) ABANDONED", len(marked))
    finally:
        _dispose_loader_safely(loader, context.log)


@dg.job
def stale_started_run_cleanup() -> None:
    """Small housekeeping job scheduled independently of provider syncs."""
    stale_started_run_cleanup_op()


sensors = [
    telemetry_run_failure_sensor,
    telemetry_provider_freshness_sensor,
]
