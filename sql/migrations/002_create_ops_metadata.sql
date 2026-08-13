-- ge_warehouse ops metadata model.
--
-- Replaces the concepts in telemetry_warehouse's etl.sync_runs /
-- etl.sync_table_loads with ops.pipeline_run / ops.table_load (every column
-- preserved; only the renames forced by the table rename itself, or that fix
-- a confirmed pre-existing inconsistency, are applied -- see
-- docs/ge_warehouse_architecture.md's "Ops metadata: current -> target
-- mapping" table for the full column-by-column justification). Also adds
-- three new, additive tables that formalize things the legacy code only did
-- ad hoc or logged and discarded: ops.source_watermark,
-- ops.data_quality_result, ops.alert_event.
--
-- Nothing in this file is written to by application code yet -- PostgresLoader
-- and orchestration/alerts.py are unchanged and still operate against
-- telemetry_warehouse. This is structure only, ahead of the ingestion port.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS throughout) and transactional.

BEGIN;

-- =============================================================================
-- ops.pipeline_run
-- One row per pipeline run. Same lifecycle as legacy etl.sync_runs:
-- STARTED -> SUCCESS | FAILED | ABANDONED.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    pipeline_run_id UUID PRIMARY KEY,
    source_system TEXT NOT NULL,
    job_name TEXT NOT NULL,
    start_date INTEGER,
    end_date INTEGER,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_fetched INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    error_message TEXT
);

COMMENT ON TABLE ops.pipeline_run IS
    'Platform equivalent of legacy etl.sync_runs. status is one of STARTED, SUCCESS, FAILED, ABANDONED -- unrestricted TEXT, matching the legacy table, so housekeeping can add values without a migration.';
COMMENT ON COLUMN ops.pipeline_run.pipeline_run_id IS
    'Was etl.sync_runs.sync_run_id; renamed only because the table itself was renamed.';

CREATE INDEX IF NOT EXISTS ix_pipeline_run_started_at_when_started
    ON ops.pipeline_run (started_at)
    WHERE status = 'STARTED';

-- =============================================================================
-- ops.table_load
-- One row per per-table load attempt within a pipeline_run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops.table_load (
    table_load_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id UUID REFERENCES ops.pipeline_run (pipeline_run_id),
    source_system TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    rows_input INTEGER,
    rows_loaded INTEGER,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    status TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.table_load IS
    'Platform equivalent of legacy etl.sync_table_loads.';
COMMENT ON COLUMN ops.table_load.table_load_id IS
    'Was etl.sync_table_loads.id; renamed only because the table itself was renamed.';
COMMENT ON COLUMN ops.table_load.pipeline_run_id IS
    'Was etl.sync_table_loads.sync_run_id; renamed to match the renamed parent key it references.';
COMMENT ON COLUMN ops.table_load.source_system IS
    'Was etl.sync_table_loads.provider. Deliberately renamed to match ops.pipeline_run.source_system -- the legacy tables used two different names (provider vs source_system) for the same concept; this is the one place that inconsistency is fixed, not a blind rename.';

-- =============================================================================
-- ops.source_watermark  (new, additive)
-- Formalizes the last-successful-run lookup that
-- PostgresLoader.get_last_successful_run() currently derives ad hoc from
-- etl.sync_runs on every call. Not written to by any job yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops.source_watermark (
    source_system TEXT NOT NULL,
    job_name TEXT NOT NULL,
    watermark_value TEXT,
    watermark_type TEXT NOT NULL DEFAULT 'timestamp',
    last_pipeline_run_id UUID REFERENCES ops.pipeline_run (pipeline_run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_system, job_name)
);

COMMENT ON TABLE ops.source_watermark IS
    'Explicit last-successful-cursor per (source_system, job_name). Not populated by any job yet -- see docs/ge_warehouse_architecture.md "Deferred to the next phase".';

-- =============================================================================
-- ops.data_quality_result  (new, additive)
-- Formalizes the POST_LOAD_VALIDATION_QUERIES results that
-- PostgresLoader.run_post_load_validation() currently only logs and
-- discards. Not written to by any job yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops.data_quality_result (
    data_quality_result_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id UUID REFERENCES ops.pipeline_run (pipeline_run_id),
    source_system TEXT NOT NULL,
    check_name TEXT NOT NULL,
    critical BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL,
    issue_count INTEGER,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.data_quality_result IS
    'Persisted form of the existing bounded post-load validation checks (PASS/FAIL/ERROR per check). Not populated by any job yet.';

-- =============================================================================
-- ops.alert_event  (new, additive)
-- Formalizes the webhook alerts orchestration/alerts.py currently fires and
-- forgets (no persisted record today). Not written to by any job yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops.alert_event (
    alert_event_id BIGSERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    job_name TEXT,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    cooldown_key TEXT,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.alert_event IS
    'Persisted record of alerts fired via the webhook in orchestration/alerts.py. Not populated by any job yet.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('002_create_ops_metadata.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
