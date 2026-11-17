-- GE Data Control Center: control-plane baseline (database: ge_control).
--
-- This is an independent migration sequence for the CONTROL-PLANE database.
-- It has no relationship to sql/migrations/ (the ge_warehouse sequence):
-- the control plane is deliberately kept in its own database so that
-- orchestration state can be backed up, restored, replicated, and failed
-- over independently of warehouse data (ADR-010).
--
-- Applied by `gecc migrate`, which wraps every file in its own transaction
-- and records it in meta.schema_version with a checksum. Files must
-- therefore NOT contain BEGIN/COMMIT.

CREATE SCHEMA IF NOT EXISTS registry;       -- what can run: steps, pipelines, immutable versions
CREATE SCHEMA IF NOT EXISTS orchestration;  -- what is running: runs, tasks, attempts, leases, workers
CREATE SCHEMA IF NOT EXISTS audit;          -- what happened: append-only events and state transitions

COMMENT ON SCHEMA registry IS 'GE Data Control Center: registered steps, pipelines and immutable pipeline versions.';
COMMENT ON SCHEMA orchestration IS 'GE Data Control Center: runs, tasks, attempts, leases, concurrency slots and workers.';
COMMENT ON SCHEMA audit IS 'GE Data Control Center: append-only audit events and state transitions.';

-- Group roles (NOLOGIN). Login accounts are created out-of-band by an
-- operator and granted membership, mirroring ge_warehouse's role model.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_api') THEN
        CREATE ROLE cc_api NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_worker') THEN
        CREATE ROLE cc_worker NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_readonly') THEN
        CREATE ROLE cc_readonly NOLOGIN;
    END IF;
END
$$;

COMMENT ON ROLE cc_api IS 'GE Data Control Center API: reads control-plane state, creates/cancels runs via functions, writes audit events.';
COMMENT ON ROLE cc_worker IS 'GE Data Control Center workers: may only call the worker-protocol functions (claim, lease, finish, logs, reap).';
COMMENT ON ROLE cc_readonly IS 'GE Data Control Center read-only observers.';

REVOKE ALL ON SCHEMA registry, orchestration, audit FROM PUBLIC;
