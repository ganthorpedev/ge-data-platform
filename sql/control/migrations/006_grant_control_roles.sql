-- Least-privilege grants (ADR-021).
--
-- cc_worker: EXECUTE on the worker-protocol functions, and SELECT on the
--            step registry (startup compatibility check). No direct DML.
-- cc_api:    SELECT everywhere, EXECUTE create_run/request_cancel, INSERT audit.event.
-- cc_readonly: SELECT everywhere.
--
-- PostgreSQL grants EXECUTE on new functions to PUBLIC by default; that is
-- revoked here so SECURITY DEFINER functions are callable only by the roles
-- explicitly listed.

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA registry, orchestration, audit FROM PUBLIC;

GRANT USAGE ON SCHEMA registry, orchestration, audit TO cc_api, cc_worker, cc_readonly;

GRANT SELECT ON ALL TABLES IN SCHEMA registry, orchestration, audit TO cc_readonly, cc_api;
GRANT SELECT ON registry.step_definition TO cc_worker;

-- meta.schema_version is created by the migration runner before any file runs.
GRANT USAGE ON SCHEMA meta TO cc_api, cc_readonly;
GRANT SELECT ON meta.schema_version TO cc_api, cc_readonly;

GRANT INSERT ON audit.event TO cc_api;
GRANT USAGE ON SEQUENCE audit.event_event_id_seq TO cc_api;

GRANT EXECUTE ON FUNCTION
    orchestration.create_run(TEXT, TEXT, TEXT, TEXT, JSONB, TIMESTAMPTZ, TIMESTAMPTZ, INTEGER),
    orchestration.request_cancel(UUID, TEXT, TEXT)
TO cc_api;

GRANT EXECUTE ON FUNCTION
    orchestration.register_worker(UUID, TEXT, TEXT, INTEGER, TEXT[], INTEGER, TEXT),
    orchestration.worker_heartbeat(UUID, TEXT),
    orchestration.stop_worker(UUID),
    orchestration.claim_task(UUID, TEXT[], INTEGER),
    orchestration.renew_lease(UUID, UUID, INTEGER),
    orchestration.finish_attempt(UUID, UUID, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, JSONB),
    orchestration.append_task_logs(UUID, UUID, BIGINT[], TIMESTAMPTZ[], TEXT[], TEXT[]),
    orchestration.reap_expired_leases(INTEGER, INTEGER)
TO cc_worker;
