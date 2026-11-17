-- Append-only audit trail (ADR-019).
--
-- audit.event            who did what to which entity (publish, trigger, cancel, registry sync, ...)
-- audit.state_transition every run/task/attempt state change, written by triggers (see 004)
--
-- Both tables reject UPDATE, DELETE and TRUNCATE via triggers. A superuser
-- can still disable triggers; that is an operator-level break-glass action,
-- not an application path.

CREATE TABLE IF NOT EXISTS audit.event (
    event_id     BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor_type   TEXT NOT NULL,
    actor_id     TEXT NOT NULL,
    action       TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_id    TEXT,
    before       JSONB,
    after        JSONB,
    details      JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_id   TEXT,
    CONSTRAINT event_actor_type_valid CHECK (actor_type IN ('USER', 'CLI', 'API', 'WORKER', 'SYSTEM')),
    CONSTRAINT event_action_format CHECK (action ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$')
);

CREATE INDEX IF NOT EXISTS ix_event_entity ON audit.event (entity_type, entity_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_event_occurred_at ON audit.event (occurred_at);

COMMENT ON TABLE audit.event IS 'Append-only record of control-plane actions (who/what/when, before/after).';

CREATE TABLE IF NOT EXISTS audit.state_transition (
    transition_id BIGSERIAL PRIMARY KEY,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    entity_type   TEXT NOT NULL,
    entity_id     UUID NOT NULL,
    run_id        UUID NOT NULL,
    from_state    TEXT,
    to_state      TEXT NOT NULL,
    actor         TEXT NOT NULL,
    reason        TEXT,
    CONSTRAINT state_transition_entity_type_valid CHECK (entity_type IN ('RUN', 'TASK', 'ATTEMPT'))
);

CREATE INDEX IF NOT EXISTS ix_state_transition_run ON audit.state_transition (run_id, transition_id);
CREATE INDEX IF NOT EXISTS ix_state_transition_entity ON audit.state_transition (entity_id, transition_id);

COMMENT ON TABLE audit.state_transition IS 'Append-only history of every run/task/attempt state change. Written only by triggers on orchestration tables.';

CREATE OR REPLACE FUNCTION audit.reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit table %.% is append-only: % is not allowed', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

DROP TRIGGER IF EXISTS event_append_only ON audit.event;
CREATE TRIGGER event_append_only
    BEFORE UPDATE OR DELETE ON audit.event
    FOR EACH ROW EXECUTE FUNCTION audit.reject_mutation();

DROP TRIGGER IF EXISTS event_no_truncate ON audit.event;
CREATE TRIGGER event_no_truncate
    BEFORE TRUNCATE ON audit.event
    FOR EACH STATEMENT EXECUTE FUNCTION audit.reject_mutation();

DROP TRIGGER IF EXISTS state_transition_append_only ON audit.state_transition;
CREATE TRIGGER state_transition_append_only
    BEFORE UPDATE OR DELETE ON audit.state_transition
    FOR EACH ROW EXECUTE FUNCTION audit.reject_mutation();

DROP TRIGGER IF EXISTS state_transition_no_truncate ON audit.state_transition;
CREATE TRIGGER state_transition_no_truncate
    BEFORE TRUNCATE ON audit.state_transition
    FOR EACH STATEMENT EXECUTE FUNCTION audit.reject_mutation();
