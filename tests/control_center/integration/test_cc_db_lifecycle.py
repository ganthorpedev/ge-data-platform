"""Control-plane semantics against real PostgreSQL: migrations, publishing, the state machine,
queue claims, retries, leases/reaper/fencing, concurrency, admission, cancellation, audit and grants."""

from __future__ import annotations

import threading
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, ProgrammingError

from ge_control_center.controlplane.client import SqlControlPlaneClient
from ge_control_center.controlplane.migrations import MigrationDriftError, apply_migrations
from ge_control_center.dag.publish import publish_pipeline
from ge_control_center.dag.spec import validate_spec
from ge_control_center.registry.step import load_default_registry


# -- migrations & registry ----------------------------------------------------------------------

def test_migrations_are_idempotent(engine) -> None:
    outcomes = apply_migrations(engine)
    assert outcomes and not any(o.applied for o in outcomes)


def test_migration_drift_is_detected(engine, tmp_path) -> None:
    from ge_control_center.controlplane.migrations import CONTROL_MIGRATIONS_DIR

    for source in CONTROL_MIGRATIONS_DIR.glob("*.sql"):
        (tmp_path / source.name).write_text(source.read_text())
    (tmp_path / "001_create_control_schemas.sql").write_text("-- edited after apply\n")
    with pytest.raises(MigrationDriftError):
        apply_migrations(engine, tmp_path)


def test_control_db_holds_no_warehouse_schemas(engine) -> None:
    with engine.connect() as conn:
        schemas = set(conn.execute(text(
            "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'"
        )).scalars())
    assert schemas == {"public", "meta", "registry", "orchestration", "audit"}


# -- publishing & immutability --------------------------------------------------------------------

def test_publish_versions_and_immutability(engine, make_pipeline) -> None:
    first = make_pipeline([{"key": "a", "step": "core.noop"}])
    assert first["result"].created and first["result"].version_no == 1

    again = publish_pipeline(engine, validate_spec(first["spec"], load_default_registry()), actor="pytest")
    assert not again.created and again.version_no == 1

    changed = dict(first["spec"], nodes=[dict(first["spec"]["nodes"][0], params={"message": "v2"})])
    second = publish_pipeline(engine, validate_spec(changed, load_default_registry()), actor="pytest")
    assert second.created and second.version_no == 2

    for statement in (
        "UPDATE registry.pipeline_version SET spec = '{}'::jsonb WHERE pipeline_version_id = CAST(:v AS uuid)",
        "DELETE FROM registry.pipeline_version WHERE pipeline_version_id = CAST(:v AS uuid)",
        "UPDATE registry.pipeline_node SET params = '{}'::jsonb WHERE pipeline_version_id = CAST(:v AS uuid)",
    ):
        with pytest.raises(DBAPIError, match="immutable"):
            with engine.begin() as conn:
                conn.execute(text(statement), {"v": first["result"].pipeline_version_id})


# -- state machine & audit ------------------------------------------------------------------------

def test_illegal_transitions_are_rejected_by_the_database(engine, make_pipeline, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
    run = h.create_run(engine, pipeline["key"], f"illegal-{uuid.uuid4()}")
    with pytest.raises(DBAPIError, match="illegal RUN state transition"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE orchestration.run SET state = 'QUEUED' WHERE run_id = CAST(:r AS uuid)"),
                         {"r": run["run_id"]})
    with pytest.raises(DBAPIError, match="illegal TASK state transition"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE orchestration.task SET state = 'SUCCEEDED' WHERE run_id = CAST(:r AS uuid)"),
                         {"r": run["run_id"]})


def test_audit_tables_are_append_only(engine) -> None:
    for statement in ("UPDATE audit.state_transition SET reason = 'x'", "DELETE FROM audit.state_transition",
                      "TRUNCATE audit.event", "DELETE FROM audit.event"):
        with pytest.raises(DBAPIError, match="append-only"):
            with engine.begin() as conn:
                conn.execute(text(statement))


# -- runs, claims, DAG order ------------------------------------------------------------------------

def test_create_run_is_idempotent(engine, make_pipeline, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
    key = f"idem-{uuid.uuid4()}"
    first = h.create_run(engine, pipeline["key"], key)
    second = h.create_run(engine, pipeline["key"], key)
    assert first["created"] and not second["created"] and first["run_id"] == second["run_id"]


def test_idempotency_key_cannot_be_reused_across_pipelines(engine, make_pipeline, unique, h) -> None:
    a = make_pipeline([{"key": "a", "step": "core.noop"}], key=f"pa_{unique}")
    b = make_pipeline([{"key": "a", "step": "core.noop"}], key=f"pb_{unique}")
    key = f"shared-{uuid.uuid4()}"
    h.create_run(engine, a["key"], key)
    with pytest.raises(DBAPIError, match="already used by a run of another pipeline"):
        h.create_run(engine, b["key"], key)


def test_dependencies_gate_claims_and_run_completes(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}, {"key": "b", "step": "core.noop"},
                              {"key": "c", "step": "core.noop"}], [("a", "b"), ("a", "c")])
    worker = register_worker(pipeline["queue"])
    run = h.create_run(engine, pipeline["key"], f"dag-{uuid.uuid4()}")
    assert h.task_states(engine, run["run_id"]) == {"a": "READY", "b": "PENDING", "c": "PENDING"}

    claim_a = client.claim(worker, [pipeline["queue"]], 30)
    assert claim_a.node_key == "a"
    assert client.claim(worker, [pipeline["queue"]], 30) is None  # b, c blocked on a
    assert client.finish(claim_a.attempt_id, claim_a.lease_token, outcome="SUCCEEDED", retryable=False) == "SUCCEEDED"

    claimed = [client.claim(worker, [pipeline["queue"]], 30) for _ in range(2)]
    assert {c.node_key for c in claimed} == {"b", "c"}
    for claim in claimed:
        client.finish(claim.attempt_id, claim.lease_token, outcome="SUCCEEDED", retryable=False)
    assert h.run_state(engine, run["run_id"]) == "SUCCEEDED"


def test_concurrent_claims_never_double_assign(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": f"n{i}", "step": "core.noop"} for i in range(5)], max_active_runs=1)
    workers = [register_worker(pipeline["queue"], f"w{i}") for i in range(8)]
    h.create_run(engine, pipeline["key"], f"race-{uuid.uuid4()}")

    claims, barrier = [], threading.Barrier(len(workers) * 2)

    def grab(worker_id: str) -> None:
        barrier.wait()
        claim = client.claim(worker_id, [pipeline["queue"]], 30)
        if claim:
            claims.append(claim.task_id)

    threads = [threading.Thread(target=grab, args=(w,)) for w in workers * 2]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claims) == 5 and len(set(claims)) == 5


# -- retries, leases, fencing ------------------------------------------------------------------------

def test_retry_backoff_then_exhaustion_marks_downstream(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline(
        [{"key": "a", "step": "core.fail_n_times", "params": {"fail_times": 9},
          "retry": {"max_attempts": 2, "backoff_seconds": 60}},
         {"key": "b", "step": "core.noop"}, {"key": "c", "step": "core.noop"}],
        [("a", "b"), ("b", "c")],
    )
    worker = register_worker(pipeline["queue"])
    run = h.create_run(engine, pipeline["key"], f"retry-{uuid.uuid4()}")

    first = client.claim(worker, [pipeline["queue"]], 30)
    assert client.finish(first.attempt_id, first.lease_token, outcome="FAILED", retryable=True) == "UP_FOR_RETRY"
    assert client.claim(worker, [pipeline["queue"]], 30) is None  # backoff (~60s) not elapsed

    h.make_claimable_now(engine, run["run_id"])
    second = client.claim(worker, [pipeline["queue"]], 30)
    assert second.attempt_no == 2
    assert client.finish(second.attempt_id, second.lease_token, outcome="FAILED", retryable=True) == "FAILED"
    assert h.task_states(engine, run["run_id"]) == {"a": "FAILED", "b": "UPSTREAM_FAILED", "c": "UPSTREAM_FAILED"}
    assert h.run_state(engine, run["run_id"]) == "FAILED"


def test_permanent_failure_is_not_retried(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop", "retry": {"max_attempts": 5}}])
    worker = register_worker(pipeline["queue"])
    run = h.create_run(engine, pipeline["key"], f"perm-{uuid.uuid4()}")
    claim = client.claim(worker, [pipeline["queue"]], 30)
    assert client.finish(claim.attempt_id, claim.lease_token, outcome="FAILED", retryable=False) == "FAILED"
    assert h.run_state(engine, run["run_id"]) == "FAILED"


def test_expired_lease_is_reaped_and_old_owner_is_fenced(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop", "retry": {"max_attempts": 3, "backoff_seconds": 0}}])
    worker_1 = register_worker(pipeline["queue"], "w1")
    worker_2 = register_worker(pipeline["queue"], "w2")
    run = h.create_run(engine, pipeline["key"], f"lease-{uuid.uuid4()}")

    stale = client.claim(worker_1, [pipeline["queue"]], 30)
    assert client.renew_lease(stale.attempt_id, stale.lease_token, 30).ok
    assert not client.renew_lease(stale.attempt_id, str(uuid.uuid4()), 30).ok  # wrong token

    h.expire_lease(engine, stale.attempt_id)
    assert not client.renew_lease(stale.attempt_id, stale.lease_token, 30).ok  # expired leases cannot be revived
    assert client.reap_expired_leases() >= 1
    assert h.task_states(engine, run["run_id"]) == {"a": "UP_FOR_RETRY"}

    fresh = client.claim(worker_2, [pipeline["queue"]], 30)
    assert fresh.attempt_no == 2
    # The zombie's late result is rejected; the new owner's is accepted.
    assert client.finish(stale.attempt_id, stale.lease_token, outcome="SUCCEEDED", retryable=False) is None
    assert client.finish(fresh.attempt_id, fresh.lease_token, outcome="SUCCEEDED", retryable=False) == "SUCCEEDED"

    with engine.connect() as conn:
        states = conn.execute(text("SELECT attempt_no, state FROM orchestration.task_attempt "
                                   "WHERE run_id = CAST(:r AS uuid) ORDER BY attempt_no"), {"r": run["run_id"]}).all()
    assert states == [(1, "LOST"), (2, "SUCCEEDED")]


def test_one_running_attempt_per_task_is_enforced(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
    worker = register_worker(pipeline["queue"])
    h.create_run(engine, pipeline["key"], f"uniq-{uuid.uuid4()}")
    claim = client.claim(worker, [pipeline["queue"]], 30)
    with pytest.raises(DBAPIError, match="ux_task_attempt_one_running"):
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO orchestration.task_attempt
                    (attempt_id, task_id, run_id, attempt_no, worker_id, lease_token, lease_expires_at, heartbeat_at)
                SELECT gen_random_uuid(), task_id, run_id, 99, worker_id, gen_random_uuid(), now(), now()
                FROM orchestration.task_attempt WHERE attempt_id = CAST(:a AS uuid)
            """), {"a": claim.attempt_id})


# -- concurrency & admission ------------------------------------------------------------------------------

def test_concurrency_key_limit_serialises_across_runs(engine, client, make_pipeline, register_worker, unique, h) -> None:
    key = f"provider:{unique}"
    pipeline = make_pipeline([{"key": "a", "step": "core.noop", "concurrency_keys": [key]}], max_active_runs=5)
    worker = register_worker(pipeline["queue"])
    h.create_run(engine, pipeline["key"], f"ck1-{uuid.uuid4()}")
    h.create_run(engine, pipeline["key"], f"ck2-{uuid.uuid4()}")

    first = client.claim(worker, [pipeline["queue"]], 30)
    assert first is not None
    assert client.claim(worker, [pipeline["queue"]], 30) is None  # slot held (default limit 1)
    client.finish(first.attempt_id, first.lease_token, outcome="SUCCEEDED", retryable=False)
    assert client.claim(worker, [pipeline["queue"]], 30) is not None

    with engine.begin() as conn:
        conn.execute(text("UPDATE registry.concurrency_limit SET max_slots = 3 WHERE concurrency_key = :k"), {"k": key})
        slots = conn.execute(text("SELECT count(*) FROM orchestration.concurrency_slot WHERE concurrency_key = :k"),
                             {"k": key}).scalar_one()
    assert slots == 3


def test_max_active_runs_queues_extra_runs(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}], max_active_runs=1)
    worker = register_worker(pipeline["queue"])
    first = h.create_run(engine, pipeline["key"], f"adm1-{uuid.uuid4()}")
    second = h.create_run(engine, pipeline["key"], f"adm2-{uuid.uuid4()}")
    assert (first["state"], second["state"]) == ("RUNNING", "QUEUED")

    claim = client.claim(worker, [pipeline["queue"]], 30)
    client.finish(claim.attempt_id, claim.lease_token, outcome="SUCCEEDED", retryable=False)
    assert h.run_state(engine, second["run_id"]) == "RUNNING"  # admitted when the first finished


# -- cancellation ---------------------------------------------------------------------------------------

def test_cancel_queued_and_running_runs(engine, client, make_pipeline, register_worker, h) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}, {"key": "b", "step": "core.noop"}], [("a", "b")])
    worker = register_worker(pipeline["queue"])
    running = h.create_run(engine, pipeline["key"], f"c1-{uuid.uuid4()}")
    queued = h.create_run(engine, pipeline["key"], f"c2-{uuid.uuid4()}")

    with engine.begin() as conn:
        assert conn.execute(text("SELECT orchestration.request_cancel(CAST(:r AS uuid), 'pytest')"),
                            {"r": queued["run_id"]}).scalar_one() == "CANCELLED"

    claim = client.claim(worker, [pipeline["queue"]], 30)
    with engine.begin() as conn:
        assert conn.execute(text("SELECT orchestration.request_cancel(CAST(:r AS uuid), 'pytest')"),
                            {"r": running["run_id"]}).scalar_one() == "CANCELLING"
    assert h.task_states(engine, running["run_id"]) == {"a": "RUNNING", "b": "CANCELLED"}

    status = client.renew_lease(claim.attempt_id, claim.lease_token, 30)
    assert status.ok and status.cancel_requested
    assert client.finish(claim.attempt_id, claim.lease_token, outcome="CANCELLED", retryable=False) == "CANCELLED"
    assert h.run_state(engine, running["run_id"]) == "CANCELLED"


# -- least privilege ---------------------------------------------------------------------------------------

def test_worker_role_can_only_use_protocol_functions(engine, db_url, make_pipeline, h) -> None:
    role, password = f"gecc_test_worker_{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
    with engine.begin() as conn:
        conn.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD '{password}' IN ROLE cc_worker"))
    worker_engine = create_engine(make_url(db_url).set(username=role, password=password))
    try:
        pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
        h.create_run(engine, pipeline["key"], f"priv-{uuid.uuid4()}")
        worker_client = SqlControlPlaneClient(worker_engine)
        worker_id = str(uuid.uuid4())
        worker_client.register_worker(worker_id, "least-priv", "h", 1, [pipeline["queue"]], 1, "t")
        claim = worker_client.claim(worker_id, [pipeline["queue"]], 30)
        assert claim is not None
        assert worker_client.finish(claim.attempt_id, claim.lease_token, outcome="SUCCEEDED", retryable=False) == "SUCCEEDED"

        for statement in ("UPDATE orchestration.task SET priority = 1", "SELECT * FROM orchestration.run",
                          "INSERT INTO audit.event (actor_type, actor_id, action, entity_type) VALUES ('USER','x','a.b','X')",
                          "SELECT orchestration.create_run('x','MANUAL','x','x')"):
            with pytest.raises(ProgrammingError, match="permission denied"):
                with worker_engine.begin() as conn:
                    conn.execute(text(statement))
    finally:
        worker_engine.dispose()
        with engine.begin() as conn:
            conn.execute(text(f"DROP ROLE {role}"))
