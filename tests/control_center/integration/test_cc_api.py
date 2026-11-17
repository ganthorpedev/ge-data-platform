from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from ge_control_center.api.app import create_app
from ge_control_center.config import Settings

TOKEN = "test-token"


def _settings() -> Settings:
    return Settings(environment="local", control_db_url="unused", warehouse_db_url=None, api_token=TOKEN, actor=None,
                    lease_seconds=30, heartbeat_seconds=5, lease_safety_seconds=5, poll_seconds=1, reap_seconds=5,
                    drain_seconds=5, worker_lost_after_seconds=60)


@pytest.fixture
def api(engine) -> TestClient:
    return TestClient(create_app(_settings(), engine))


AUTH = {"Authorization": f"Bearer {TOKEN}"}


def test_api_refuses_to_start_without_token(engine) -> None:
    settings = _settings()
    with pytest.raises(RuntimeError, match="GECC_API_TOKEN"):
        create_app(Settings(**{**settings.__dict__, "api_token": None}), engine)


def test_health_is_public_everything_else_needs_the_token(api) -> None:
    health = api.get("/health")
    assert health.status_code == 200 and health.json()["latest_migration"].startswith("006")
    assert api.get("/steps").status_code == 401
    assert api.get("/steps", headers={"Authorization": "Bearer wrong"}).status_code == 401
    steps = api.get("/steps", headers=AUTH).json()
    assert {s["step_key"] for s in steps} >= {"core.noop", "core.sleep"}
    assert "properties" in steps[0]["param_schema"]


def test_run_lifecycle_over_http(api, engine, make_pipeline) -> None:
    pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
    body = {"pipeline_key": pipeline["key"], "idempotency_key": f"api-{uuid.uuid4()}"}

    created = api.post("/runs", json=body, headers={**AUTH, "X-Request-ID": "req-123"})
    assert created.status_code == 201 and created.headers["X-Request-ID"] == "req-123"
    run_id = created.json()["run_id"]
    repeat = api.post("/runs", json=body, headers=AUTH)
    assert repeat.status_code == 200 and repeat.json()["run_id"] == run_id

    detail = api.get(f"/runs/{run_id}", headers=AUTH).json()
    assert detail["state"] == "RUNNING" and detail["tasks"][0]["state"] == "READY"

    cancelled = api.post(f"/runs/{run_id}/cancel", json={"reason": "test"}, headers=AUTH)
    assert cancelled.json()["state"] == "CANCELLED"

    from sqlalchemy import text
    with engine.connect() as conn:
        events = conn.execute(text("SELECT action, request_id FROM audit.event WHERE entity_id = :r ORDER BY event_id"),
                              {"r": run_id}).all()
    assert [e.action for e in events] == ["run.created", "run.create_deduplicated", "run.cancel_requested"]
    assert events[0].request_id == "req-123"


def test_api_error_mapping(api) -> None:
    assert api.post("/runs", json={"pipeline_key": "nope_nope", "idempotency_key": "x"}, headers=AUTH).status_code == 404
    assert api.get(f"/runs/{uuid.uuid4()}", headers=AUTH).status_code == 404
    bad_window = {"pipeline_key": "abc", "idempotency_key": "x", "window_start": "2026-01-01T00:00:00"}
    assert api.post("/runs", json=bad_window, headers=AUTH).status_code == 422
    assert api.post("/runs", json={"pipeline_key": "abc", "idempotency_key": "x", "code": "x"},
                    headers=AUTH).status_code == 422


def test_api_works_under_the_least_privilege_api_role(engine, db_url, make_pipeline) -> None:
    """Exercise the API exactly as deployed: connected as a login that is only a member of cc_api."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    role, password = f"gecc_test_api_{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
    with engine.begin() as conn:
        conn.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD '{password}' IN ROLE cc_api"))
    api_engine = create_engine(make_url(db_url).set(username=role, password=password))
    try:
        api = TestClient(create_app(_settings(), api_engine))
        health = api.get("/health")
        assert health.status_code == 200 and health.json()["status"] == "ok", health.json()
        pipeline = make_pipeline([{"key": "a", "step": "core.noop"}])
        created = api.post("/runs", json={"pipeline_key": pipeline["key"], "idempotency_key": f"lp-{uuid.uuid4()}"},
                           headers=AUTH)
        assert created.status_code == 201
        run_id = created.json()["run_id"]
        assert api.get(f"/runs/{run_id}", headers=AUTH).status_code == 200
        assert api.get(f"/runs/{run_id}/logs", headers=AUTH).status_code == 200
        assert api.get("/pipelines", headers=AUTH).status_code == 200
        assert api.post(f"/runs/{run_id}/cancel", headers=AUTH).json()["state"] == "CANCELLED"
    finally:
        api_engine.dispose()
        with engine.begin() as conn:
            conn.execute(text(f"DROP ROLE {role}"))
