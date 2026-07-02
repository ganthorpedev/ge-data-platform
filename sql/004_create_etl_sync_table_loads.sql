-- Per-table load log. One row per table load attempt within a sync run, so
-- a partial failure (e.g. table 6 of 10 fails) is visible: which tables
-- already succeeded, which one failed, and why.
--
-- Rerunnable: CREATE TABLE IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS etl.sync_table_loads (
    id BIGSERIAL PRIMARY KEY,
    sync_run_id UUID REFERENCES etl.sync_runs (sync_run_id),
    provider TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    rows_input INTEGER,
    rows_loaded INTEGER,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    status TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
