-- Run lifecycle and worker protocol (ADR-011, ADR-012, ADR-016, ADR-017, ADR-021).
--
-- Every state-changing operation is one transaction inside one of these
-- functions. Workers and the API never UPDATE orchestration tables directly;
-- they only EXECUTE the functions granted to their role (see 006).
--
-- Delivery semantics: AT-LEAST-ONCE. A task attempt holds a time-bound lease
-- identified by (attempt_id, lease_token). If the lease expires (worker
-- crash, kill -9, network partition), the reaper marks the attempt LOST and
-- the task becomes eligible for retry; any later write carrying the old
-- lease token is rejected (fencing). Steps must therefore be idempotent.
--
-- Row locks use FOR NO KEY UPDATE so they never conflict with the KEY SHARE
-- locks taken by foreign-key checks (e.g. inserting an attempt references
-- its run). Lock ordering: attempt -> run -> task -> slots ->
-- pipeline. Admission locks pipeline then QUEUED runs with SKIP LOCKED.
--
-- All SECURITY DEFINER functions pin search_path and fully qualify objects.

-- ---------------------------------------------------------------------------
-- Internal helpers (not granted to any role)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION orchestration._set_context(p_actor TEXT, p_reason TEXT)
RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT set_config('gecc.actor', COALESCE(p_actor, ''), true),
           set_config('gecc.reason', COALESCE(p_reason, ''), true);
$$;

CREATE OR REPLACE FUNCTION orchestration._promote_ready_tasks(p_run_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    PERFORM set_config('gecc.reason', 'dependencies satisfied', true);

    UPDATE orchestration.task t
    SET state = 'READY', ready_at = clock_timestamp(), next_eligible_at = clock_timestamp()
    WHERE t.run_id = p_run_id
      AND t.state = 'PENDING'
      AND NOT EXISTS (
          SELECT 1
          FROM orchestration.run r
          JOIN registry.pipeline_edge e
            ON e.pipeline_version_id = r.pipeline_version_id
           AND e.downstream_node_key = t.node_key
          JOIN orchestration.task up
            ON up.run_id = t.run_id
           AND up.node_key = e.upstream_node_key
          WHERE r.run_id = p_run_id
            AND up.state <> 'SUCCEEDED'
      );
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION orchestration._mark_upstream_failed(p_run_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_step  INTEGER;
    v_total INTEGER := 0;
BEGIN
    PERFORM set_config('gecc.reason', 'an upstream task failed', true);
    LOOP
        UPDATE orchestration.task t
        SET state = 'UPSTREAM_FAILED', finished_at = clock_timestamp()
        WHERE t.run_id = p_run_id
          AND t.state = 'PENDING'
          AND EXISTS (
              SELECT 1
              FROM orchestration.run r
              JOIN registry.pipeline_edge e
                ON e.pipeline_version_id = r.pipeline_version_id
               AND e.downstream_node_key = t.node_key
              JOIN orchestration.task up
                ON up.run_id = t.run_id
               AND up.node_key = e.upstream_node_key
              WHERE r.run_id = p_run_id
                AND up.state IN ('FAILED', 'UPSTREAM_FAILED')
          );
        GET DIAGNOSTICS v_step = ROW_COUNT;
        EXIT WHEN v_step = 0;
        v_total := v_total + v_step;
    END LOOP;
    RETURN v_total;
END;
$$;

CREATE OR REPLACE FUNCTION orchestration._release_slots(p_attempt_id UUID)
RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, pg_temp
AS $$
    UPDATE orchestration.concurrency_slot
    SET holder_attempt_id = NULL, acquired_at = NULL
    WHERE holder_attempt_id = p_attempt_id;

    DELETE FROM orchestration.concurrency_slot s
    USING registry.concurrency_limit l
    WHERE s.concurrency_key = l.concurrency_key
      AND s.holder_attempt_id IS NULL
      AND s.slot_no > l.max_slots;
$$;

-- Admit QUEUED runs of a pipeline up to max_active_runs, promoting their root tasks.
CREATE OR REPLACE FUNCTION orchestration._admit_runs(p_pipeline_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_max      INTEGER;
    v_active   INTEGER;
    v_admitted INTEGER := 0;
    v_run      RECORD;
BEGIN
    SELECT max_active_runs INTO v_max
    FROM registry.pipeline WHERE pipeline_id = p_pipeline_id
    FOR NO KEY UPDATE;

    SELECT count(*) INTO v_active
    FROM orchestration.run
    WHERE pipeline_id = p_pipeline_id AND state IN ('RUNNING', 'CANCELLING');

    IF v_active >= v_max THEN
        RETURN 0;
    END IF;

    FOR v_run IN
        SELECT run_id FROM orchestration.run
        WHERE pipeline_id = p_pipeline_id AND state = 'QUEUED'
        ORDER BY priority DESC, created_at, run_id
        LIMIT (v_max - v_active)
        FOR NO KEY UPDATE SKIP LOCKED
    LOOP
        PERFORM set_config('gecc.reason', 'admitted: pipeline below max_active_runs', true);
        UPDATE orchestration.run
        SET state = 'RUNNING', started_at = clock_timestamp()
        WHERE run_id = v_run.run_id;
        PERFORM orchestration._promote_ready_tasks(v_run.run_id);
        v_admitted := v_admitted + 1;
    END LOOP;

    RETURN v_admitted;
END;
$$;

-- Settle a run whose tasks may all be terminal. Caller holds (or may take) the run lock.
CREATE OR REPLACE FUNCTION orchestration._finalize_run(p_run_id UUID)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_run   orchestration.run%ROWTYPE;
    v_final TEXT;
BEGIN
    SELECT * INTO v_run FROM orchestration.run WHERE run_id = p_run_id FOR NO KEY UPDATE;

    IF v_run.state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'QUEUED') THEN
        RETURN v_run.state;
    END IF;

    IF v_run.state = 'CANCELLING' THEN
        PERFORM set_config('gecc.reason', 'run cancelled', true);
        UPDATE orchestration.task
        SET state = 'CANCELLED', finished_at = clock_timestamp()
        WHERE run_id = p_run_id AND state IN ('PENDING', 'READY', 'UP_FOR_RETRY');

        IF EXISTS (SELECT 1 FROM orchestration.task WHERE run_id = p_run_id AND state = 'RUNNING') THEN
            RETURN 'CANCELLING';
        END IF;

        UPDATE orchestration.run SET state = 'CANCELLED', finished_at = clock_timestamp() WHERE run_id = p_run_id;
        PERFORM orchestration._admit_runs(v_run.pipeline_id);
        RETURN 'CANCELLED';
    END IF;

    -- RUNNING
    IF EXISTS (
        SELECT 1 FROM orchestration.task
        WHERE run_id = p_run_id AND state IN ('PENDING', 'READY', 'RUNNING', 'UP_FOR_RETRY')
    ) THEN
        RETURN 'RUNNING';
    END IF;

    v_final := CASE
        WHEN EXISTS (SELECT 1 FROM orchestration.task WHERE run_id = p_run_id AND state <> 'SUCCEEDED')
        THEN 'FAILED' ELSE 'SUCCEEDED'
    END;

    PERFORM set_config('gecc.reason', 'all tasks terminal', true);
    UPDATE orchestration.run SET state = v_final, finished_at = clock_timestamp() WHERE run_id = p_run_id;
    PERFORM orchestration._admit_runs(v_run.pipeline_id);
    RETURN v_final;
END;
$$;

-- Apply a terminal attempt outcome. Caller must hold the attempt row lock and
-- have verified it is RUNNING. Returns the task's resulting state.
CREATE OR REPLACE FUNCTION orchestration._apply_attempt_outcome(
    p_attempt_id    UUID,
    p_outcome       TEXT,
    p_retryable     BOOLEAN,
    p_exit_code     INTEGER,
    p_error_class   TEXT,
    p_error_message TEXT,
    p_result        JSONB
)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_attempt orchestration.task_attempt%ROWTYPE;
    v_run     orchestration.run%ROWTYPE;
    v_task    orchestration.task%ROWTYPE;
    v_new     TEXT;
    v_delay   DOUBLE PRECISION;
BEGIN
    IF p_outcome NOT IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED', 'LOST') THEN
        RAISE EXCEPTION 'invalid attempt outcome %', p_outcome USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_attempt FROM orchestration.task_attempt WHERE attempt_id = p_attempt_id;
    SELECT * INTO v_run FROM orchestration.run WHERE run_id = v_attempt.run_id FOR NO KEY UPDATE;
    SELECT * INTO v_task FROM orchestration.task WHERE task_id = v_attempt.task_id FOR NO KEY UPDATE;

    UPDATE orchestration.task_attempt
    SET state = p_outcome,
        finished_at = clock_timestamp(),
        exit_code = p_exit_code,
        error_class = left(p_error_class, 200),
        error_message = left(p_error_message, 8000),
        result = p_result
    WHERE attempt_id = p_attempt_id;

    PERFORM orchestration._release_slots(p_attempt_id);

    v_new := CASE
        WHEN p_outcome = 'SUCCEEDED' THEN 'SUCCEEDED'
        WHEN p_outcome = 'CANCELLED' OR v_run.state = 'CANCELLING' THEN 'CANCELLED'
        WHEN p_retryable AND v_task.attempt_count < v_task.max_attempts THEN 'UP_FOR_RETRY'
        ELSE 'FAILED'
    END;

    IF v_new = 'UP_FOR_RETRY' THEN
        -- Exponential backoff with +/-20% jitter, capped.
        v_delay := LEAST(
            v_task.retry_backoff_seconds * power(2, GREATEST(v_task.attempt_count - 1, 0)),
            v_task.retry_backoff_max_seconds
        ) * (0.8 + random() * 0.4);
        UPDATE orchestration.task
        SET state = 'UP_FOR_RETRY',
            next_eligible_at = clock_timestamp() + make_interval(secs => v_delay)
        WHERE task_id = v_task.task_id;
    ELSE
        UPDATE orchestration.task
        SET state = v_new, finished_at = clock_timestamp()
        WHERE task_id = v_task.task_id;
    END IF;

    IF v_new = 'SUCCEEDED' AND v_run.state = 'RUNNING' THEN
        PERFORM orchestration._promote_ready_tasks(v_run.run_id);
    ELSIF v_new = 'FAILED' THEN
        PERFORM orchestration._mark_upstream_failed(v_run.run_id);
    END IF;

    PERFORM orchestration._finalize_run(v_run.run_id);
    RETURN v_new;
END;
$$;

-- ---------------------------------------------------------------------------
-- Run lifecycle (API / CLI / future scheduler)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION orchestration.create_run(
    p_pipeline_key    TEXT,
    p_trigger_type    TEXT,
    p_triggered_by    TEXT,
    p_idempotency_key TEXT,
    p_params          JSONB DEFAULT '{}'::jsonb,
    p_window_start    TIMESTAMPTZ DEFAULT NULL,
    p_window_end      TIMESTAMPTZ DEFAULT NULL,
    p_priority        INTEGER DEFAULT 0
)
RETURNS TABLE (run_id UUID, created BOOLEAN, state TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_pipeline registry.pipeline%ROWTYPE;
    v_version  registry.pipeline_version%ROWTYPE;
    v_existing orchestration.run%ROWTYPE;
    v_run_id   UUID;
    v_inactive TEXT;
BEGIN
    IF p_triggered_by IS NULL OR length(p_triggered_by) = 0 THEN
        RAISE EXCEPTION 'triggered_by is required' USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_pipeline FROM registry.pipeline WHERE pipeline_key = p_pipeline_key;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown pipeline %', p_pipeline_key USING ERRCODE = 'no_data_found';
    END IF;

    -- Idempotent create: the same key always resolves to the same run.
    SELECT * INTO v_existing FROM orchestration.run r WHERE r.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.pipeline_id <> v_pipeline.pipeline_id THEN
            RAISE EXCEPTION 'idempotency key % already used by a run of another pipeline', p_idempotency_key
                USING ERRCODE = 'unique_violation';
        END IF;
        run_id := v_existing.run_id; created := FALSE; state := v_existing.state;
        RETURN NEXT;
        RETURN;
    END IF;

    IF v_pipeline.archived_at IS NOT NULL THEN
        RAISE EXCEPTION 'pipeline % is archived', p_pipeline_key USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT * INTO v_version FROM registry.pipeline_version pv
    WHERE pv.pipeline_id = v_pipeline.pipeline_id AND pv.status = 'PUBLISHED'
    ORDER BY pv.version_no DESC LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pipeline % has no published version', p_pipeline_key
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT string_agg(n.node_key || ' (' || n.step_key || '@' || n.step_code_version || ')', ', ')
    INTO v_inactive
    FROM registry.pipeline_node n
    JOIN registry.step_definition s
      ON s.step_key = n.step_key AND s.code_version = n.step_code_version
    WHERE n.pipeline_version_id = v_version.pipeline_version_id AND NOT s.is_active;
    IF v_inactive IS NOT NULL THEN
        RAISE EXCEPTION 'pipeline % version % references inactive step versions: %. Republish it against the current registry.',
            p_pipeline_key, v_version.version_no, v_inactive USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM orchestration._set_context(p_triggered_by, 'run created (' || p_trigger_type || ')');

    INSERT INTO orchestration.run AS r (
        pipeline_id, pipeline_version_id, trigger_type, triggered_by, idempotency_key,
        params, window_start, window_end, priority, state
    ) VALUES (
        v_pipeline.pipeline_id, v_version.pipeline_version_id, p_trigger_type, p_triggered_by, p_idempotency_key,
        COALESCE(p_params, '{}'::jsonb), p_window_start, p_window_end, COALESCE(p_priority, 0), 'QUEUED'
    )
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING r.run_id INTO v_run_id;

    IF v_run_id IS NULL THEN
        -- Lost a race with a concurrent create using the same key.
        SELECT * INTO v_existing FROM orchestration.run r WHERE r.idempotency_key = p_idempotency_key;
        IF v_existing.pipeline_id <> v_pipeline.pipeline_id THEN
            RAISE EXCEPTION 'idempotency key % already used by a run of another pipeline', p_idempotency_key
                USING ERRCODE = 'unique_violation';
        END IF;
        run_id := v_existing.run_id; created := FALSE; state := v_existing.state;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Concurrency keys without an explicit limit default to 1 (safe default).
    INSERT INTO registry.concurrency_limit (concurrency_key, max_slots, updated_by)
    SELECT DISTINCT k, 1, 'system:auto-default'
    FROM registry.pipeline_node n, unnest(n.concurrency_keys) AS k
    WHERE n.pipeline_version_id = v_version.pipeline_version_id
    ON CONFLICT DO NOTHING;

    INSERT INTO orchestration.task (
        run_id, node_key, step_key, step_code_version, params, max_attempts,
        retry_backoff_seconds, retry_backoff_max_seconds, timeout_seconds,
        queue, priority, concurrency_keys, state
    )
    SELECT v_run_id, n.node_key, n.step_key, n.step_code_version, n.params, n.max_attempts,
           n.retry_backoff_seconds, n.retry_backoff_max_seconds, n.timeout_seconds,
           n.queue, COALESCE(p_priority, 0), n.concurrency_keys, 'PENDING'
    FROM registry.pipeline_node n
    WHERE n.pipeline_version_id = v_version.pipeline_version_id;

    PERFORM orchestration._admit_runs(v_pipeline.pipeline_id);

    SELECT r.state INTO state FROM orchestration.run r WHERE r.run_id = v_run_id;
    run_id := v_run_id; created := TRUE;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION orchestration.request_cancel(p_run_id UUID, p_actor TEXT, p_reason TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_run orchestration.run%ROWTYPE;
BEGIN
    SELECT * INTO v_run FROM orchestration.run WHERE run_id = p_run_id FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown run %', p_run_id USING ERRCODE = 'no_data_found';
    END IF;

    IF v_run.state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'CANCELLING') THEN
        RETURN v_run.state;  -- idempotent
    END IF;

    PERFORM orchestration._set_context(p_actor, COALESCE(p_reason, 'cancel requested'));

    IF v_run.state = 'QUEUED' THEN
        UPDATE orchestration.task SET state = 'CANCELLED', finished_at = clock_timestamp()
        WHERE run_id = p_run_id AND state = 'PENDING';
        UPDATE orchestration.run
        SET state = 'CANCELLED', cancel_requested_at = clock_timestamp(),
            cancel_requested_by = p_actor, finished_at = clock_timestamp()
        WHERE run_id = p_run_id;
        RETURN 'CANCELLED';
    END IF;

    UPDATE orchestration.run
    SET state = 'CANCELLING', cancel_requested_at = clock_timestamp(), cancel_requested_by = p_actor
    WHERE run_id = p_run_id;

    -- RUNNING tasks are stopped by their workers on the next lease renewal.
    RETURN orchestration._finalize_run(p_run_id);
END;
$$;

-- ---------------------------------------------------------------------------
-- Worker protocol
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION orchestration.register_worker(
    p_worker_id    UUID,
    p_worker_name  TEXT,
    p_hostname     TEXT,
    p_pid          INTEGER,
    p_queues       TEXT[],
    p_capacity     INTEGER,
    p_code_version TEXT
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    INSERT INTO orchestration.worker
        (worker_id, worker_name, hostname, pid, queues, capacity, code_version, status, started_at, last_heartbeat_at)
    VALUES
        (p_worker_id, p_worker_name, p_hostname, p_pid, p_queues, p_capacity, p_code_version, 'ACTIVE', now(), now())
    ON CONFLICT (worker_id) DO UPDATE
    SET status = 'ACTIVE', last_heartbeat_at = now(), stopped_at = NULL;
$$;

CREATE OR REPLACE FUNCTION orchestration.worker_heartbeat(p_worker_id UUID, p_status TEXT DEFAULT 'ACTIVE')
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF p_status NOT IN ('ACTIVE', 'DRAINING') THEN
        RAISE EXCEPTION 'invalid heartbeat status %', p_status USING ERRCODE = 'invalid_parameter_value';
    END IF;
    UPDATE orchestration.worker
    SET last_heartbeat_at = now(), status = p_status
    WHERE worker_id = p_worker_id AND status IN ('ACTIVE', 'DRAINING', 'LOST');
    RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION orchestration.stop_worker(p_worker_id UUID)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    UPDATE orchestration.worker
    SET status = 'STOPPED', stopped_at = now(), last_heartbeat_at = now()
    WHERE worker_id = p_worker_id;
$$;

CREATE OR REPLACE FUNCTION orchestration.claim_task(
    p_worker_id     UUID,
    p_queues        TEXT[],
    p_lease_seconds INTEGER
)
RETURNS TABLE (
    attempt_id        UUID,
    lease_token       UUID,
    attempt_no        INTEGER,
    task_id           UUID,
    run_id            UUID,
    node_key          TEXT,
    step_key          TEXT,
    step_code_version TEXT,
    params            JSONB,
    run_params        JSONB,
    window_start      TIMESTAMPTZ,
    window_end        TIMESTAMPTZ,
    max_attempts      INTEGER,
    timeout_seconds   INTEGER,
    lease_expires_at  TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_task       RECORD;
    v_key        TEXT;
    v_slot       INTEGER;
    v_slot_keys  TEXT[];
    v_slot_nos   INTEGER[];
    v_ok         BOOLEAN;
    v_attempt_id UUID;
    v_token      UUID;
    v_no         INTEGER;
    v_expires    TIMESTAMPTZ;
    v_skipped    UUID[] := ARRAY[]::UUID[];
BEGIN
    IF p_lease_seconds NOT BETWEEN 5 AND 3600 THEN
        RAISE EXCEPTION 'lease seconds must be between 5 and 3600' USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE orchestration.worker w SET last_heartbeat_at = now()
    WHERE w.worker_id = p_worker_id AND w.status = 'ACTIVE';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'worker % is not registered or not ACTIVE', p_worker_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Lock ONE candidate per iteration. (A FOR-over-query loop would fetch and
    -- lock rows in batches, starving concurrent claimers of tasks this call
    -- never takes.) Candidates blocked by concurrency limits are skipped.
    FOR v_try IN 1..50 LOOP
        SELECT t.task_id, t.concurrency_keys INTO v_task
        FROM orchestration.task t
        JOIN orchestration.run r ON r.run_id = t.run_id
        WHERE t.state IN ('READY', 'UP_FOR_RETRY')
          AND t.next_eligible_at <= clock_timestamp()
          AND t.queue = ANY (p_queues)
          AND r.state = 'RUNNING'
          AND NOT (t.task_id = ANY (v_skipped))
        ORDER BY t.priority DESC, t.next_eligible_at, t.created_at
        LIMIT 1
        FOR NO KEY UPDATE OF t SKIP LOCKED;
        EXIT WHEN NOT FOUND;
        v_skipped := v_skipped || v_task.task_id;

        -- Acquire one free slot per concurrency key (keys are sorted/deduped at publish).
        v_ok := TRUE;
        v_slot_keys := ARRAY[]::TEXT[];
        v_slot_nos := ARRAY[]::INTEGER[];
        FOREACH v_key IN ARRAY v_task.concurrency_keys LOOP
            SELECT s.slot_no INTO v_slot
            FROM orchestration.concurrency_slot s
            JOIN registry.concurrency_limit l ON l.concurrency_key = s.concurrency_key
            WHERE s.concurrency_key = v_key
              AND s.holder_attempt_id IS NULL
              AND s.slot_no <= l.max_slots
            ORDER BY s.slot_no
            LIMIT 1
            FOR NO KEY UPDATE OF s SKIP LOCKED;
            IF NOT FOUND THEN
                v_ok := FALSE;
                EXIT;
            END IF;
            v_slot_keys := v_slot_keys || v_key;
            v_slot_nos := v_slot_nos || v_slot;
        END LOOP;
        CONTINUE WHEN NOT v_ok;

        PERFORM orchestration._set_context('worker:' || p_worker_id::text, 'claimed by worker');

        v_attempt_id := gen_random_uuid();
        v_token := gen_random_uuid();
        v_expires := clock_timestamp() + make_interval(secs => p_lease_seconds);

        UPDATE orchestration.task t
        SET state = 'RUNNING',
            attempt_count = t.attempt_count + 1,
            started_at = COALESCE(t.started_at, clock_timestamp())
        WHERE t.task_id = v_task.task_id
        RETURNING t.attempt_count INTO v_no;

        INSERT INTO orchestration.task_attempt AS a
            (attempt_id, task_id, run_id, attempt_no, worker_id, lease_token, lease_expires_at, heartbeat_at, state)
        SELECT v_attempt_id, t.task_id, t.run_id, v_no, p_worker_id, v_token, v_expires, clock_timestamp(), 'RUNNING'
        FROM orchestration.task t WHERE t.task_id = v_task.task_id;

        UPDATE orchestration.concurrency_slot s
        SET holder_attempt_id = v_attempt_id, acquired_at = clock_timestamp()
        FROM unnest(v_slot_keys, v_slot_nos) AS acquired(k, n)
        WHERE s.concurrency_key = acquired.k AND s.slot_no = acquired.n;

        RETURN QUERY
        SELECT v_attempt_id, v_token, v_no, t.task_id, t.run_id, t.node_key, t.step_key, t.step_code_version,
               t.params, r.params, r.window_start, r.window_end, t.max_attempts, t.timeout_seconds, v_expires
        FROM orchestration.task t
        JOIN orchestration.run r ON r.run_id = t.run_id
        WHERE t.task_id = v_task.task_id;
        RETURN;
    END LOOP;

    RETURN;  -- nothing claimable
END;
$$;

-- Renew a lease. Expired leases cannot be revived: once expired, the
-- reaper may hand the task to another worker at any moment.
CREATE OR REPLACE FUNCTION orchestration.renew_lease(
    p_attempt_id    UUID,
    p_lease_token   UUID,
    p_lease_seconds INTEGER
)
RETURNS TABLE (lease_ok BOOLEAN, cancel_requested BOOLEAN, lease_expires_at TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_expires TIMESTAMPTZ;
    v_run_id  UUID;
BEGIN
    IF p_lease_seconds NOT BETWEEN 5 AND 3600 THEN
        RAISE EXCEPTION 'lease seconds must be between 5 and 3600' USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE orchestration.task_attempt a
    SET lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
        heartbeat_at = clock_timestamp()
    WHERE a.attempt_id = p_attempt_id
      AND a.lease_token = p_lease_token
      AND a.state = 'RUNNING'
      AND a.lease_expires_at > clock_timestamp()
    RETURNING a.lease_expires_at, a.run_id INTO v_expires, v_run_id;

    IF NOT FOUND THEN
        lease_ok := FALSE; cancel_requested := FALSE; lease_expires_at := NULL;
        RETURN NEXT;
        RETURN;
    END IF;

    lease_ok := TRUE;
    lease_expires_at := v_expires;
    SELECT r.cancel_requested_at IS NOT NULL INTO cancel_requested FROM orchestration.run r WHERE r.run_id = v_run_id;
    RETURN NEXT;
END;
$$;

-- Report an attempt's outcome. Returns the task's new state, or NULL when the
-- write is fenced (wrong token, or the attempt is no longer RUNNING).
CREATE OR REPLACE FUNCTION orchestration.finish_attempt(
    p_attempt_id    UUID,
    p_lease_token   UUID,
    p_outcome       TEXT,
    p_retryable     BOOLEAN,
    p_exit_code     INTEGER DEFAULT NULL,
    p_error_class   TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_result        JSONB DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_attempt orchestration.task_attempt%ROWTYPE;
BEGIN
    IF p_outcome NOT IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED') THEN
        RAISE EXCEPTION 'workers may report SUCCEEDED, FAILED, TIMED_OUT or CANCELLED, not %', p_outcome
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_attempt FROM orchestration.task_attempt WHERE attempt_id = p_attempt_id FOR NO KEY UPDATE;
    IF NOT FOUND OR v_attempt.lease_token <> p_lease_token OR v_attempt.state <> 'RUNNING' THEN
        RETURN NULL;
    END IF;

    PERFORM orchestration._set_context('worker:' || v_attempt.worker_id::text, 'attempt ' || lower(p_outcome));
    RETURN orchestration._apply_attempt_outcome(
        p_attempt_id, p_outcome, COALESCE(p_retryable, FALSE), p_exit_code, p_error_class, p_error_message, p_result
    );
END;
$$;

CREATE OR REPLACE FUNCTION orchestration.append_task_logs(
    p_attempt_id  UUID,
    p_lease_token UUID,
    p_seqs        BIGINT[],
    p_logged_at   TIMESTAMPTZ[],
    p_streams     TEXT[],
    p_lines       TEXT[]
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Token check only (not state): trailing lines may flush just after finish.
    PERFORM 1 FROM orchestration.task_attempt WHERE attempt_id = p_attempt_id AND lease_token = p_lease_token;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    INSERT INTO orchestration.task_log (attempt_id, seq, logged_at, stream, line)
    SELECT p_attempt_id, l.seq, l.logged_at, l.stream, left(l.line, 4000)
    FROM unnest(p_seqs, p_logged_at, p_streams, p_lines) AS l(seq, logged_at, stream, line)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

-- Recover from crashed / partitioned workers. Safe to run concurrently from
-- any number of processes (SKIP LOCKED). Returns the number of attempts reaped.
CREATE OR REPLACE FUNCTION orchestration.reap_expired_leases(
    p_limit                     INTEGER DEFAULT 100,
    p_worker_lost_after_seconds INTEGER DEFAULT 120
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_attempt RECORD;
    v_count   INTEGER := 0;
BEGIN
    FOR v_attempt IN
        SELECT a.attempt_id
        FROM orchestration.task_attempt a
        WHERE a.state = 'RUNNING' AND a.lease_expires_at < clock_timestamp()
        ORDER BY a.lease_expires_at
        LIMIT p_limit
        FOR NO KEY UPDATE SKIP LOCKED
    LOOP
        PERFORM orchestration._set_context('system:reaper', 'lease expired without renewal');
        PERFORM orchestration._apply_attempt_outcome(
            v_attempt.attempt_id, 'LOST', TRUE, NULL, 'LeaseExpired',
            'Lease expired: the worker stopped renewing it (crash, kill, or network partition).', NULL
        );
        v_count := v_count + 1;
    END LOOP;

    UPDATE orchestration.worker
    SET status = 'LOST'
    WHERE status IN ('ACTIVE', 'DRAINING')
      AND last_heartbeat_at < clock_timestamp() - make_interval(secs => p_worker_lost_after_seconds);

    RETURN v_count;
END;
$$;
