-- Registry: registered steps, pipelines and immutable pipeline versions
-- (ADR-013, ADR-014).
--
-- Steps are Python callables registered in code (ge_control_center.registry)
-- and synced here by `gecc registry sync`. Pipelines may only reference
-- registered (step_key, code_version) pairs -- there is no column anywhere
-- in this schema that can hold executable code.

CREATE TABLE IF NOT EXISTS registry.step_definition (
    step_key                          TEXT NOT NULL,
    code_version                      TEXT NOT NULL,
    display_name                      TEXT NOT NULL,
    description                       TEXT NOT NULL DEFAULT '',
    param_schema                      JSONB NOT NULL,
    definition_hash                   TEXT NOT NULL,
    idempotency_mode                  TEXT NOT NULL,
    concurrency_keys                  TEXT[] NOT NULL DEFAULT '{}',
    default_queue                     TEXT NOT NULL DEFAULT 'default',
    default_max_attempts              INTEGER NOT NULL,
    default_retry_backoff_seconds     INTEGER NOT NULL,
    default_retry_backoff_max_seconds INTEGER NOT NULL,
    default_timeout_seconds           INTEGER NOT NULL,
    is_active                         BOOLEAN NOT NULL DEFAULT TRUE,
    registered_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at                    TIMESTAMPTZ,
    PRIMARY KEY (step_key, code_version),
    CONSTRAINT step_key_format CHECK (step_key ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'),
    CONSTRAINT step_code_version_format CHECK (code_version ~ '^[A-Za-z0-9._-]{1,32}$'),
    CONSTRAINT step_definition_hash_format CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT step_idempotency_mode_valid CHECK (
        idempotency_mode IN ('read_only', 'upsert', 'replace_partition', 'full_replace', 'append_dedup')
    ),
    CONSTRAINT step_param_schema_is_object CHECK (jsonb_typeof(param_schema) = 'object'),
    CONSTRAINT step_default_max_attempts_range CHECK (default_max_attempts BETWEEN 1 AND 20),
    CONSTRAINT step_default_backoff_range CHECK (default_retry_backoff_seconds BETWEEN 0 AND 86400),
    CONSTRAINT step_default_backoff_max_range CHECK (
        default_retry_backoff_max_seconds BETWEEN default_retry_backoff_seconds AND 86400
    ),
    CONSTRAINT step_default_timeout_range CHECK (default_timeout_seconds BETWEEN 1 AND 172800)
);

-- At most one active code version per step key.
CREATE UNIQUE INDEX IF NOT EXISTS ux_step_definition_one_active
    ON registry.step_definition (step_key) WHERE is_active;

COMMENT ON TABLE registry.step_definition IS
    'Registered Python steps, synced from code. A changed parameter schema or execution contract requires a new code_version.';

CREATE TABLE IF NOT EXISTS registry.pipeline (
    pipeline_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_key     TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    description      TEXT NOT NULL DEFAULT '',
    owner            TEXT,
    is_paused        BOOLEAN NOT NULL DEFAULT FALSE,
    max_active_runs  INTEGER NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by       TEXT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at      TIMESTAMPTZ,
    CONSTRAINT pipeline_key_format CHECK (pipeline_key ~ '^[a-z][a-z0-9_]{2,62}$'),
    CONSTRAINT pipeline_max_active_runs_range CHECK (max_active_runs BETWEEN 1 AND 50)
);

COMMENT ON TABLE registry.pipeline IS
    'Mutable pipeline header. The DAG itself lives in immutable registry.pipeline_version rows.';
COMMENT ON COLUMN registry.pipeline.is_paused IS 'Reserved for the scheduler (M2): paused pipelines are not scheduled.';

CREATE TABLE IF NOT EXISTS registry.pipeline_version (
    pipeline_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id         UUID NOT NULL REFERENCES registry.pipeline (pipeline_id),
    version_no          INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'PUBLISHED',
    spec                JSONB NOT NULL,
    spec_hash           TEXT NOT NULL,
    ui_layout           JSONB NOT NULL DEFAULT '{}'::jsonb,
    published_by        TEXT NOT NULL,
    published_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at          TIMESTAMPTZ,
    UNIQUE (pipeline_id, version_no),
    CONSTRAINT pipeline_version_no_positive CHECK (version_no > 0),
    CONSTRAINT pipeline_version_status_valid CHECK (status IN ('PUBLISHED', 'RETIRED')),
    CONSTRAINT pipeline_version_spec_hash_format CHECK (spec_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT pipeline_version_spec_is_object CHECK (jsonb_typeof(spec) = 'object'),
    CONSTRAINT pipeline_version_layout_is_object CHECK (jsonb_typeof(ui_layout) = 'object')
);

COMMENT ON TABLE registry.pipeline_version IS
    'Immutable, content-hashed DAG definition. Only PUBLISHED->RETIRED and ui_layout (cosmetic) may change.';

CREATE TABLE IF NOT EXISTS registry.pipeline_node (
    pipeline_version_id       UUID NOT NULL REFERENCES registry.pipeline_version (pipeline_version_id),
    node_key                  TEXT NOT NULL,
    step_key                  TEXT NOT NULL,
    step_code_version         TEXT NOT NULL,
    params                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_attempts              INTEGER NOT NULL,
    retry_backoff_seconds     INTEGER NOT NULL,
    retry_backoff_max_seconds INTEGER NOT NULL,
    timeout_seconds           INTEGER NOT NULL,
    queue                     TEXT NOT NULL,
    concurrency_keys          TEXT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (pipeline_version_id, node_key),
    FOREIGN KEY (step_key, step_code_version) REFERENCES registry.step_definition (step_key, code_version),
    CONSTRAINT node_key_format CHECK (node_key ~ '^[a-z][a-z0-9_]{0,62}$'),
    CONSTRAINT node_params_is_object CHECK (jsonb_typeof(params) = 'object'),
    CONSTRAINT node_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 20),
    CONSTRAINT node_backoff_range CHECK (retry_backoff_seconds BETWEEN 0 AND 86400),
    CONSTRAINT node_backoff_max_range CHECK (retry_backoff_max_seconds BETWEEN retry_backoff_seconds AND 86400),
    CONSTRAINT node_timeout_range CHECK (timeout_seconds BETWEEN 1 AND 172800),
    CONSTRAINT node_queue_format CHECK (queue ~ '^[a-z][a-z0-9_-]{0,62}$')
);

CREATE TABLE IF NOT EXISTS registry.pipeline_edge (
    pipeline_version_id UUID NOT NULL,
    upstream_node_key   TEXT NOT NULL,
    downstream_node_key TEXT NOT NULL,
    PRIMARY KEY (pipeline_version_id, upstream_node_key, downstream_node_key),
    FOREIGN KEY (pipeline_version_id, upstream_node_key)
        REFERENCES registry.pipeline_node (pipeline_version_id, node_key),
    FOREIGN KEY (pipeline_version_id, downstream_node_key)
        REFERENCES registry.pipeline_node (pipeline_version_id, node_key),
    CONSTRAINT edge_not_self_loop CHECK (upstream_node_key <> downstream_node_key)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_edge_downstream
    ON registry.pipeline_edge (pipeline_version_id, downstream_node_key);

CREATE TABLE IF NOT EXISTS registry.concurrency_limit (
    concurrency_key TEXT PRIMARY KEY,
    max_slots       INTEGER NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by      TEXT NOT NULL,
    CONSTRAINT concurrency_key_format CHECK (concurrency_key ~ '^[a-z][a-z0-9_.:-]{0,127}$'),
    CONSTRAINT concurrency_max_slots_range CHECK (max_slots BETWEEN 1 AND 1000)
);

COMMENT ON TABLE registry.concurrency_limit IS
    'Max simultaneously RUNNING attempts per concurrency key (e.g. provider:trackunit = 1). Keys without a row get an automatic limit of 1.';

-- ---------------------------------------------------------------------------
-- Immutability guards
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION registry.guard_pipeline_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'pipeline_version % is immutable and cannot be deleted (retire it instead)',
            OLD.pipeline_version_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    IF NEW.pipeline_version_id IS DISTINCT FROM OLD.pipeline_version_id
       OR NEW.pipeline_id IS DISTINCT FROM OLD.pipeline_id
       OR NEW.version_no IS DISTINCT FROM OLD.version_no
       OR NEW.spec IS DISTINCT FROM OLD.spec
       OR NEW.spec_hash IS DISTINCT FROM OLD.spec_hash
       OR NEW.published_by IS DISTINCT FROM OLD.published_by
       OR NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'pipeline_version % is immutable: only status (PUBLISHED->RETIRED) and ui_layout may change',
            OLD.pipeline_version_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status
       AND NOT (OLD.status = 'PUBLISHED' AND NEW.status = 'RETIRED') THEN
        RAISE EXCEPTION 'pipeline_version % status change % -> % is not allowed',
            OLD.pipeline_version_id, OLD.status, NEW.status USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pipeline_version_immutable ON registry.pipeline_version;
CREATE TRIGGER pipeline_version_immutable
    BEFORE UPDATE OR DELETE ON registry.pipeline_version
    FOR EACH ROW EXECUTE FUNCTION registry.guard_pipeline_version();

CREATE OR REPLACE FUNCTION registry.reject_published_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '%.% rows belong to an immutable pipeline version: % is not allowed',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

DROP TRIGGER IF EXISTS pipeline_node_immutable ON registry.pipeline_node;
CREATE TRIGGER pipeline_node_immutable
    BEFORE UPDATE OR DELETE ON registry.pipeline_node
    FOR EACH ROW EXECUTE FUNCTION registry.reject_published_mutation();

DROP TRIGGER IF EXISTS pipeline_edge_immutable ON registry.pipeline_edge;
CREATE TRIGGER pipeline_edge_immutable
    BEFORE UPDATE OR DELETE ON registry.pipeline_edge
    FOR EACH ROW EXECUTE FUNCTION registry.reject_published_mutation();
