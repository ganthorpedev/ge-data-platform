-- Orchestration state: workers, runs, tasks, attempts, concurrency slots,
-- checkpoints and logs (ADR-011, ADR-012, ADR-015, ADR-017).
--
-- The execution state machine is enforced HERE, by triggers, not only in
-- application code: an UPDATE that performs a transition not listed in
-- orchestration.allowed_transition fails, and every transition (including
-- the initial insert) is written to audit.state_transition automatically.

CREATE TABLE IF NOT EXISTS orchestration.worker (
    worker_id         UUID PRIMARY KEY,
    worker_name       TEXT NOT NULL,
    hostname          TEXT NOT NULL,
    pid               INTEGER NOT NULL,
    queues            TEXT[] NOT NULL,
    capacity          INTEGER NOT NULL,
    code_version      TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'ACTIVE',
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at        TIMESTAMPTZ,
    CONSTRAINT worker_status_valid CHECK (status IN ('ACTIVE', 'DRAINING', 'STOPPED', 'LOST')),
    CONSTRAINT worker_capacity_range CHECK (capacity BETWEEN 1 AND 64),
    CONSTRAINT worker_queues_not_empty CHECK (cardinality(queues) > 0)
);

CREATE TABLE IF NOT EXISTS orchestration.run (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id         UUID NOT NULL REFERENCES registry.pipeline (pipeline_id),
    pipeline_version_id UUID NOT NULL REFERENCES registry.pipeline_version (pipeline_version_id),
    trigger_type        TEXT NOT NULL,
    triggered_by        TEXT NOT NULL,
    idempotency_key     TEXT NOT NULL UNIQUE,
    params              JSONB NOT NULL DEFAULT '{}'::jsonb,
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    state               TEXT NOT NULL DEFAULT 'QUEUED',
    priority            INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    cancel_requested_by TEXT,
    CONSTRAINT run_state_valid CHECK (
        state IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLING', 'CANCELLED')
    ),
    CONSTRAINT run_trigger_type_valid CHECK (trigger_type IN ('MANUAL', 'API', 'SCHEDULE', 'BACKFILL', 'RESUME')),
    CONSTRAINT run_idempotency_key_length CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    CONSTRAINT run_params_is_object CHECK (jsonb_typeof(params) = 'object'),
    CONSTRAINT run_window_both_or_neither CHECK ((window_start IS NULL) = (window_end IS NULL)),
    CONSTRAINT run_window_ordered CHECK (window_start IS NULL OR window_start < window_end),
    CONSTRAINT run_priority_range CHECK (priority BETWEEN -100 AND 100)
);

CREATE INDEX IF NOT EXISTS ix_run_pipeline_active
    ON orchestration.run (pipeline_id, state, priority DESC, created_at)
    WHERE state IN ('QUEUED', 'RUNNING', 'CANCELLING');
CREATE INDEX IF NOT EXISTS ix_run_created_at ON orchestration.run (created_at DESC);

COMMENT ON COLUMN orchestration.run.window_start IS
    'Logical data window. Fixed at run creation so every retry reprocesses the same window (ADR-012).';

CREATE TABLE IF NOT EXISTS orchestration.task (
    task_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                    UUID NOT NULL REFERENCES orchestration.run (run_id),
    node_key                  TEXT NOT NULL,
    step_key                  TEXT NOT NULL,
    step_code_version         TEXT NOT NULL,
    params                    JSONB NOT NULL,
    state                     TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count             INTEGER NOT NULL DEFAULT 0,
    max_attempts              INTEGER NOT NULL,
    retry_backoff_seconds     INTEGER NOT NULL,
    retry_backoff_max_seconds INTEGER NOT NULL,
    timeout_seconds           INTEGER NOT NULL,
    queue                     TEXT NOT NULL,
    priority                  INTEGER NOT NULL DEFAULT 0,
    concurrency_keys          TEXT[] NOT NULL DEFAULT '{}',
    next_eligible_at          TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    ready_at                  TIMESTAMPTZ,
    started_at                TIMESTAMPTZ,
    finished_at               TIMESTAMPTZ,
    UNIQUE (run_id, node_key),
    FOREIGN KEY (step_key, step_code_version) REFERENCES registry.step_definition (step_key, code_version),
    CONSTRAINT task_state_valid CHECK (
        state IN ('PENDING', 'READY', 'RUNNING', 'UP_FOR_RETRY', 'SUCCEEDED', 'FAILED', 'UPSTREAM_FAILED', 'CANCELLED')
    ),
    CONSTRAINT task_attempt_count_bounded CHECK (attempt_count BETWEEN 0 AND max_attempts)
);

CREATE INDEX IF NOT EXISTS ix_task_claimable
    ON orchestration.task (queue, priority DESC, next_eligible_at, created_at)
    WHERE state IN ('READY', 'UP_FOR_RETRY');

CREATE TABLE IF NOT EXISTS orchestration.task_attempt (
    attempt_id       UUID PRIMARY KEY,
    task_id          UUID NOT NULL REFERENCES orchestration.task (task_id),
    run_id           UUID NOT NULL REFERENCES orchestration.run (run_id),
    attempt_no       INTEGER NOT NULL,
    worker_id        UUID NOT NULL REFERENCES orchestration.worker (worker_id),
    lease_token      UUID NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at     TIMESTAMPTZ NOT NULL,
    state            TEXT NOT NULL DEFAULT 'RUNNING',
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    exit_code        INTEGER,
    error_class      TEXT,
    error_message    TEXT,
    result           JSONB,
    UNIQUE (task_id, attempt_no),
    CONSTRAINT attempt_state_valid CHECK (
        state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED', 'LOST')
    ),
    CONSTRAINT attempt_no_positive CHECK (attempt_no > 0)
);

-- The control plane never has two RUNNING attempts for the same task.
CREATE UNIQUE INDEX IF NOT EXISTS ux_task_attempt_one_running
    ON orchestration.task_attempt (task_id) WHERE state = 'RUNNING';
CREATE INDEX IF NOT EXISTS ix_task_attempt_lease
    ON orchestration.task_attempt (lease_expires_at) WHERE state = 'RUNNING';
CREATE INDEX IF NOT EXISTS ix_task_attempt_run ON orchestration.task_attempt (run_id);

CREATE TABLE IF NOT EXISTS orchestration.concurrency_slot (
    concurrency_key   TEXT NOT NULL REFERENCES registry.concurrency_limit (concurrency_key),
    slot_no           INTEGER NOT NULL,
    holder_attempt_id UUID REFERENCES orchestration.task_attempt (attempt_id),
    acquired_at       TIMESTAMPTZ,
    PRIMARY KEY (concurrency_key, slot_no),
    CONSTRAINT slot_no_positive CHECK (slot_no > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_concurrency_slot_holder
    ON orchestration.concurrency_slot (concurrency_key, holder_attempt_id) WHERE holder_attempt_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS orchestration.task_checkpoint (
    task_id        UUID NOT NULL REFERENCES orchestration.task (task_id),
    checkpoint_key TEXT NOT NULL,
    value          JSONB NOT NULL,
    attempt_id     UUID NOT NULL REFERENCES orchestration.task_attempt (attempt_id),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, checkpoint_key)
);

COMMENT ON TABLE orchestration.task_checkpoint IS 'Step progress checkpoints (ADR-018). Structure only in M1; wired in M3.';

CREATE TABLE IF NOT EXISTS orchestration.task_log (
    attempt_id UUID NOT NULL REFERENCES orchestration.task_attempt (attempt_id),
    seq        BIGINT NOT NULL,
    logged_at  TIMESTAMPTZ NOT NULL,
    stream     TEXT NOT NULL,
    line       TEXT NOT NULL,
    PRIMARY KEY (attempt_id, seq),
    CONSTRAINT task_log_stream_valid CHECK (stream IN ('stdout', 'stderr', 'system'))
);

-- ---------------------------------------------------------------------------
-- Concurrency slots follow concurrency_limit.max_slots.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION registry.sync_concurrency_slots()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO orchestration.concurrency_slot (concurrency_key, slot_no)
    SELECT NEW.concurrency_key, g
    FROM generate_series(1, NEW.max_slots) AS g
    ON CONFLICT DO NOTHING;

    -- Shrinking: free surplus slots go now; held surplus slots are removed
    -- when released (see orchestration._release_slots).
    DELETE FROM orchestration.concurrency_slot
    WHERE concurrency_key = NEW.concurrency_key
      AND slot_no > NEW.max_slots
      AND holder_attempt_id IS NULL;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS concurrency_limit_sync_slots ON registry.concurrency_limit;
CREATE TRIGGER concurrency_limit_sync_slots
    AFTER INSERT OR UPDATE OF max_slots ON registry.concurrency_limit
    FOR EACH ROW EXECUTE FUNCTION registry.sync_concurrency_slots();

-- ---------------------------------------------------------------------------
-- State machine (ADR-015): allowed transitions, enforced by trigger.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orchestration.allowed_transition (
    entity_type TEXT NOT NULL,
    from_state  TEXT NOT NULL,
    to_state    TEXT NOT NULL,
    PRIMARY KEY (entity_type, from_state, to_state),
    CONSTRAINT allowed_transition_entity_valid CHECK (entity_type IN ('RUN', 'TASK', 'ATTEMPT'))
);

INSERT INTO orchestration.allowed_transition (entity_type, from_state, to_state) VALUES
    -- initial states (from_state '' = row creation)
    ('RUN', '', 'QUEUED'),
    ('TASK', '', 'PENDING'),
    ('ATTEMPT', '', 'RUNNING'),
    -- run
    ('RUN', 'QUEUED', 'RUNNING'),
    ('RUN', 'QUEUED', 'CANCELLED'),
    ('RUN', 'RUNNING', 'SUCCEEDED'),
    ('RUN', 'RUNNING', 'FAILED'),
    ('RUN', 'RUNNING', 'CANCELLING'),
    ('RUN', 'CANCELLING', 'CANCELLED'),
    -- task
    ('TASK', 'PENDING', 'READY'),
    ('TASK', 'PENDING', 'UPSTREAM_FAILED'),
    ('TASK', 'PENDING', 'CANCELLED'),
    ('TASK', 'READY', 'RUNNING'),
    ('TASK', 'READY', 'CANCELLED'),
    ('TASK', 'UP_FOR_RETRY', 'RUNNING'),
    ('TASK', 'UP_FOR_RETRY', 'CANCELLED'),
    ('TASK', 'RUNNING', 'SUCCEEDED'),
    ('TASK', 'RUNNING', 'FAILED'),
    ('TASK', 'RUNNING', 'UP_FOR_RETRY'),
    ('TASK', 'RUNNING', 'CANCELLED'),
    -- attempt
    ('ATTEMPT', 'RUNNING', 'SUCCEEDED'),
    ('ATTEMPT', 'RUNNING', 'FAILED'),
    ('ATTEMPT', 'RUNNING', 'TIMED_OUT'),
    ('ATTEMPT', 'RUNNING', 'CANCELLED'),
    ('ATTEMPT', 'RUNNING', 'LOST')
ON CONFLICT DO NOTHING;

-- TG_ARGV[0] = entity type (RUN/TASK/ATTEMPT), TG_ARGV[1] = id column name.
CREATE OR REPLACE FUNCTION orchestration.enforce_state_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_from TEXT := CASE WHEN TG_OP = 'INSERT' THEN '' ELSE OLD.state END;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.state IS NOT DISTINCT FROM OLD.state THEN
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM orchestration.allowed_transition
        WHERE entity_type = TG_ARGV[0] AND from_state = v_from AND to_state = NEW.state
    ) THEN
        RAISE EXCEPTION 'illegal % state transition for %: % -> %',
            TG_ARGV[0], to_jsonb(NEW) ->> TG_ARGV[1], COALESCE(NULLIF(v_from, ''), '(new)'), NEW.state
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION orchestration.record_state_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.state IS NOT DISTINCT FROM OLD.state THEN
        RETURN NULL;
    END IF;

    INSERT INTO audit.state_transition (entity_type, entity_id, run_id, from_state, to_state, actor, reason)
    VALUES (
        TG_ARGV[0],
        (to_jsonb(NEW) ->> TG_ARGV[1])::uuid,
        (to_jsonb(NEW) ->> 'run_id')::uuid,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE OLD.state END,
        NEW.state,
        COALESCE(NULLIF(current_setting('gecc.actor', true), ''), session_user::text),
        NULLIF(current_setting('gecc.reason', true), '')
    );
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS run_enforce_transition ON orchestration.run;
CREATE TRIGGER run_enforce_transition
    BEFORE INSERT OR UPDATE OF state ON orchestration.run
    FOR EACH ROW EXECUTE FUNCTION orchestration.enforce_state_transition('RUN', 'run_id');
DROP TRIGGER IF EXISTS run_record_transition ON orchestration.run;
CREATE TRIGGER run_record_transition
    AFTER INSERT OR UPDATE OF state ON orchestration.run
    FOR EACH ROW EXECUTE FUNCTION orchestration.record_state_transition('RUN', 'run_id');

DROP TRIGGER IF EXISTS task_enforce_transition ON orchestration.task;
CREATE TRIGGER task_enforce_transition
    BEFORE INSERT OR UPDATE OF state ON orchestration.task
    FOR EACH ROW EXECUTE FUNCTION orchestration.enforce_state_transition('TASK', 'task_id');
DROP TRIGGER IF EXISTS task_record_transition ON orchestration.task;
CREATE TRIGGER task_record_transition
    AFTER INSERT OR UPDATE OF state ON orchestration.task
    FOR EACH ROW EXECUTE FUNCTION orchestration.record_state_transition('TASK', 'task_id');

DROP TRIGGER IF EXISTS attempt_enforce_transition ON orchestration.task_attempt;
CREATE TRIGGER attempt_enforce_transition
    BEFORE INSERT OR UPDATE OF state ON orchestration.task_attempt
    FOR EACH ROW EXECUTE FUNCTION orchestration.enforce_state_transition('ATTEMPT', 'attempt_id');
DROP TRIGGER IF EXISTS attempt_record_transition ON orchestration.task_attempt;
CREATE TRIGGER attempt_record_transition
    AFTER INSERT OR UPDATE OF state ON orchestration.task_attempt
    FOR EACH ROW EXECUTE FUNCTION orchestration.record_state_transition('ATTEMPT', 'attempt_id');
