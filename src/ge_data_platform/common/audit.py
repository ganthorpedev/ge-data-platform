"""Shared GE Data Platform pipeline audit/run-tracking against ge_warehouse's ops schema.

This is the platform successor to legacy telemetry_warehouse's
etl.sync_runs / etl.sync_table_loads bookkeeping, targeting
ops.pipeline_run / ops.table_load instead (sql/migrations/002_create_ops_metadata.sql).
One implementation, reused by every migrated source's platform-target run
(Trackunit, Sendem, EzyTrack, Evolution Project Reports) through
PostgresLoader's dispatch in ge_data_platform.common.database -- see that
module's start_sync_run/finish_sync_run/start_table_load/finish_table_load
for the legacy/platform dispatch point. Every function here takes an
explicit `engine` (always the caller's ge_warehouse engine) rather than
owning a connection itself, matching PostgresLoader's own style.

Lifecycle mirrors the legacy tables exactly: STARTED -> SUCCESS | FAILED |
ABANDONED. Column semantics are the same too (see the migration file's
column comments for the legacy -> platform rename justification) -- this
module does not invent new bookkeeping shape, only a new destination.

AUDIT HISTORY IS NOT WATERMARK STATE.
ops.pipeline_run records what happened (for observability/troubleshooting).
It is deliberately never read back as a catch-up/reconciliation cursor --
that is ops.source_watermark's job (not populated by any job yet; see
docs/operations/pipeline-operations.md). PostgresLoader.get_last_successful_run
keeps returning None unconditionally for a platform-target loader specifically
so a platform run never infers a catch-up window from ops.pipeline_run
history, even now that this module populates it -- see that method's
docstring for the full safety rationale (this was the EzyTrack
legacy-state/catch-up defect this migration must not reintroduce).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def start_pipeline_run(
    engine: Engine,
    *,
    source_system: str,
    job_name: str,
    start_date: int | None = None,
    end_date: int | None = None,
) -> str:
    """Insert a STARTED ops.pipeline_run row and return the new pipeline_run_id.

    started_at is left to the column's own DEFAULT now() (matching how
    legacy start_sync_run leaves etl.sync_runs.started_at to its default)
    rather than being computed here, so the persisted timestamp is always
    the database's clock, not the application's.
    """
    pipeline_run_id = str(uuid.uuid4())
    statement = text("""
        INSERT INTO ops.pipeline_run
            (pipeline_run_id, source_system, job_name, start_date, end_date, status)
        VALUES
            (:pipeline_run_id, :source_system, :job_name, :start_date, :end_date, 'STARTED')
    """)

    with engine.begin() as conn:
        conn.execute(
            statement,
            {
                "pipeline_run_id": pipeline_run_id,
                "source_system": source_system,
                "job_name": job_name,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

    return pipeline_run_id


def finish_pipeline_run(
    engine: Engine,
    pipeline_run_id: str,
    *,
    status: str,
    rows_fetched: int = 0,
    rows_loaded: int = 0,
    error_message: str | None = None,
) -> None:
    """Update an ops.pipeline_run row with its final status and counts.

    General-purpose terminal-status writer -- SUCCESS, FAILED, or ABANDONED
    are all just a `status` value; the same statement handles all three.
    complete_pipeline_run/fail_pipeline_run below are the two convenience
    wrappers actual call sites use.
    """
    statement = text("""
        UPDATE ops.pipeline_run
        SET status = :status,
            finished_at = CURRENT_TIMESTAMP,
            rows_fetched = :rows_fetched,
            rows_loaded = :rows_loaded,
            error_message = :error_message
        WHERE pipeline_run_id = :pipeline_run_id
    """)

    with engine.begin() as conn:
        conn.execute(
            statement,
            {
                "pipeline_run_id": pipeline_run_id,
                "status": status,
                "rows_fetched": rows_fetched,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
            },
        )


def complete_pipeline_run(
    engine: Engine,
    pipeline_run_id: str,
    *,
    rows_fetched: int = 0,
    rows_loaded: int = 0,
) -> None:
    """Mark an ops.pipeline_run row SUCCESS. Zero rows_loaded is a valid, real outcome."""
    finish_pipeline_run(
        engine,
        pipeline_run_id,
        status="SUCCESS",
        rows_fetched=rows_fetched,
        rows_loaded=rows_loaded,
    )


def fail_pipeline_run(
    engine: Engine,
    pipeline_run_id: str,
    *,
    error_message: str,
    rows_fetched: int = 0,
    rows_loaded: int = 0,
) -> None:
    """Mark an ops.pipeline_run row FAILED with the given error message."""
    finish_pipeline_run(
        engine,
        pipeline_run_id,
        status="FAILED",
        rows_fetched=rows_fetched,
        rows_loaded=rows_loaded,
        error_message=error_message,
    )


def fail_pipeline_run_safe(engine: Engine, pipeline_run_id: str, error: BaseException) -> None:
    """Mark a pipeline run FAILED without ever masking the original job error.

    Platform-target mirror of database.finish_sync_run_failed_safe: if the
    FAILED bookkeeping update itself fails (e.g. the database is the thing
    that broke), the bookkeeping error is logged and swallowed so the
    caller's `raise` re-raises the ORIGINAL exception. The pipeline_run row
    is left in STARTED; see mark_abandoned_pipeline_runs for cleanup.
    """
    try:
        fail_pipeline_run(engine, pipeline_run_id, error_message=str(error))
    except Exception as bookkeeping_error:
        logger.error(
            "Could not mark pipeline run %s as FAILED (bookkeeping error: %s). "
            "The original job error is preserved and re-raised; this run will "
            "remain STARTED until ops.pipeline_run stale-run cleanup marks it "
            "ABANDONED (see docs/operations/pipeline-operations.md).",
            pipeline_run_id,
            bookkeeping_error,
        )


def start_table_load(
    engine: Engine,
    pipeline_run_id: str,
    *,
    source_system: str,
    schema_name: str,
    table_name: str,
    rows_input: int,
) -> int:
    """Insert a STARTED ops.table_load row and return its table_load_id."""
    statement = text("""
        INSERT INTO ops.table_load
            (pipeline_run_id, source_system, schema_name, table_name, rows_input, started_at, status)
        VALUES
            (:pipeline_run_id, :source_system, :schema_name, :table_name, :rows_input, CURRENT_TIMESTAMP, 'STARTED')
        RETURNING table_load_id
    """)

    with engine.begin() as conn:
        table_load_id = conn.execute(
            statement,
            {
                "pipeline_run_id": pipeline_run_id,
                "source_system": source_system,
                "schema_name": schema_name,
                "table_name": table_name,
                "rows_input": rows_input,
            },
        ).scalar_one()

    return table_load_id


def finish_table_load(
    engine: Engine,
    table_load_id: int,
    *,
    status: str,
    rows_loaded: int = 0,
    error_message: str | None = None,
) -> None:
    """Update an ops.table_load row with its final status and counts."""
    statement = text("""
        UPDATE ops.table_load
        SET status = :status,
            rows_loaded = :rows_loaded,
            finished_at = CURRENT_TIMESTAMP,
            error_message = :error_message
        WHERE table_load_id = :table_load_id
    """)

    with engine.begin() as conn:
        conn.execute(
            statement,
            {
                "table_load_id": table_load_id,
                "status": status,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
            },
        )


def record_table_load(
    engine: Engine,
    pipeline_run_id: str,
    *,
    source_system: str,
    schema_name: str,
    table_name: str,
    rows_input: int,
    rows_loaded: int,
    status: str = "SUCCESS",
    error_message: str | None = None,
) -> int:
    """Convenience one-shot record for a load whose outcome is already known.

    Equivalent to start_table_load immediately followed by finish_table_load.
    PostgresLoader itself always uses the two-phase start/finish pair (so a
    failure mid-load-plan still leaves a STARTED-then-FAILED row for the
    table that broke); this wrapper is provided for simpler callers that
    only need to record a completed load in one call. Returns the
    table_load_id.
    """
    table_load_id = start_table_load(
        engine,
        pipeline_run_id,
        source_system=source_system,
        schema_name=schema_name,
        table_name=table_name,
        rows_input=rows_input,
    )
    finish_table_load(engine, table_load_id, status=status, rows_loaded=rows_loaded, error_message=error_message)
    return table_load_id


def mark_abandoned_pipeline_runs(
    engine: Engine,
    cutoff: datetime,
    *,
    threshold_hours: int,
) -> list[dict]:
    """Mark STARTED ops.pipeline_run rows older than `cutoff` as ABANDONED.

    Platform-target mirror of PostgresLoader.mark_abandoned_runs (same
    single-statement, unconditional-per-row semantics). `cutoff` is computed
    by the caller via database.compute_abandoned_cutoff so this module has
    no dependency on ge_data_platform.common.database (avoiding a circular
    import) and so both backends share the exact same cutoff arithmetic.

    Returns the runs that were marked (pipeline_run_id, source_system,
    job_name, started_at). Unlike the legacy Dagster housekeeping job
    (orchestration/monitoring.py's stale_started_run_cleanup), nothing
    currently schedules this for platform -- see
    docs/operations/pipeline-operations.md "ops metadata wiring status".
    Callers are responsible for making sure no marked run is still
    genuinely active before calling this, same caveat as the legacy
    function.
    """
    statement = text("""
        UPDATE ops.pipeline_run
        SET status = 'ABANDONED',
            finished_at = CURRENT_TIMESTAMP,
            error_message = COALESCE(error_message, '') || :note
        WHERE status = 'STARTED' AND started_at < :cutoff
        RETURNING pipeline_run_id, source_system, job_name, started_at
    """)

    note = f" [Marked ABANDONED by housekeeping: STARTED for more than {threshold_hours}h with no finish]"

    with engine.begin() as conn:
        rows = conn.execute(statement, {"cutoff": cutoff, "note": note}).mappings().all()

    marked = [dict(row) for row in rows]
    for run in marked:
        logger.warning(
            "Marked pipeline run %s (%s / %s, started %s) as ABANDONED",
            run["pipeline_run_id"],
            run["source_system"],
            run["job_name"],
            run["started_at"],
        )
    return marked
