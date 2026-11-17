"""Integration fixtures: a throwaway control-plane database on a LOCAL PostgreSQL server.

Set GECC_TEST_ADMIN_DB_URL to an admin connection on the local Compose
control-db, e.g.::

    GECC_TEST_ADMIN_DB_URL=postgresql+psycopg2://gecc_admin:gecc_admin_dev@127.0.0.1:55432/postgres

A database named gecc_test_<random> is created, migrated and registry-synced
for the session, then dropped. Without the variable these tests are skipped.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from ge_control_center.config import assert_local_database
from ge_control_center.controlplane.client import SqlControlPlaneClient
from ge_control_center.controlplane.migrations import apply_migrations
from ge_control_center.dag.publish import publish_pipeline
from ge_control_center.dag.spec import validate_spec
from ge_control_center.registry.step import load_default_registry
from ge_control_center.registry.sync import sync_registry

ADMIN_URL = os.getenv("GECC_TEST_ADMIN_DB_URL")

pytestmark = pytest.mark.skipif(not ADMIN_URL, reason="GECC_TEST_ADMIN_DB_URL not set (local Compose control-db)")


@pytest.fixture(scope="session")
def db_url() -> Iterator[str]:
    if not ADMIN_URL:
        pytest.skip("GECC_TEST_ADMIN_DB_URL not set")
    assert_local_database(ADMIN_URL, setting="GECC_TEST_ADMIN_DB_URL")
    name = f"gecc_test_{uuid.uuid4().hex[:10]}"
    admin = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(ADMIN_URL).set(database=name).render_as_string(hide_password=False)
    try:
        yield url
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture(scope="session")
def engine(db_url: str) -> Iterator[Engine]:
    eng = create_engine(db_url, pool_size=10, max_overflow=10,
                        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=30000"})
    apply_migrations(eng)
    sync_registry(eng, load_default_registry(), actor="pytest")
    yield eng
    eng.dispose()


@pytest.fixture
def client(engine: Engine) -> SqlControlPlaneClient:
    return SqlControlPlaneClient(engine)


@pytest.fixture
def unique() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture
def make_pipeline(engine: Engine, unique: str):
    """Publish a pipeline whose nodes all use a per-test queue, so tests never claim each other's tasks."""

    def _make(nodes: list[dict], edges: list[tuple[str, str]] = (), *, max_active_runs: int = 1,
              key: str | None = None) -> dict:
        queue = f"q{unique}"
        spec = {
            "pipeline_key": key or f"p_{unique}",
            "name": "integration",
            "max_active_runs": max_active_runs,
            "nodes": [{**node, "queue": queue} for node in nodes],
            "edges": [{"from": a, "to": b} for a, b in edges],
        }
        result = publish_pipeline(engine, validate_spec(spec, load_default_registry()), actor="pytest")
        return {"key": spec["pipeline_key"], "queue": queue, "spec": spec, "result": result}

    return _make


@pytest.fixture
def register_worker(client: SqlControlPlaneClient):
    def _register(queue: str, name: str = "pytest-worker") -> str:
        worker_id = str(uuid.uuid4())
        client.register_worker(worker_id, name, "pytest-host", os.getpid(), [queue], 4, "test")
        return worker_id

    return _register


def create_run(engine: Engine, pipeline_key: str, idempotency_key: str, **kwargs) -> dict:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM orchestration.create_run(:k, 'MANUAL', 'pytest', :i, CAST(:p AS jsonb))"),
            {"k": pipeline_key, "i": idempotency_key, "p": json.dumps(kwargs.get("params", {}))},
        ).mappings().one()
    return {"run_id": str(row["run_id"]), "created": row["created"], "state": row["state"]}


def task_states(engine: Engine, run_id: str) -> dict[str, str]:
    with engine.connect() as conn:
        return dict(conn.execute(
            text("SELECT node_key, state FROM orchestration.task WHERE run_id = CAST(:r AS uuid)"), {"r": run_id}
        ).all())


def run_state(engine: Engine, run_id: str) -> str:
    with engine.connect() as conn:
        return conn.execute(text("SELECT state FROM orchestration.run WHERE run_id = CAST(:r AS uuid)"),
                            {"r": run_id}).scalar_one()


def make_claimable_now(engine: Engine, run_id: str) -> None:
    """Skip retry backoff (not a state change, so the state machine allows it)."""
    with engine.begin() as conn:
        conn.execute(text("UPDATE orchestration.task SET next_eligible_at = now() - interval '1 second' "
                          "WHERE run_id = CAST(:r AS uuid) AND state = 'UP_FOR_RETRY'"), {"r": run_id})


def expire_lease(engine: Engine, attempt_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE orchestration.task_attempt SET lease_expires_at = now() - interval '1 second' "
                          "WHERE attempt_id = CAST(:a AS uuid)"), {"a": attempt_id})


@pytest.fixture
def h():
    """Helper functions, exposed as a fixture so test modules never import conftest directly."""
    from types import SimpleNamespace

    return SimpleNamespace(create_run=create_run, task_states=task_states, run_state=run_state,
                           make_claimable_now=make_claimable_now, expire_lease=expire_lease)
