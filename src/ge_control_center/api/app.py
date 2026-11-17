"""HTTP API (M1): health, step catalog, pipelines, runs, cancellation, logs.

Authentication in M1 is a single bearer token (``GECC_API_TOKEN``) for local
development; user identity and SSO arrive with the editor API (M4). Every
mutating request writes an audit event carrying the request id.

Publishing pipelines is CLI-only in M1; the editor endpoints (drafts,
validate, publish, diff) arrive in M4 and will reuse ``dag.spec`` and
``dag.publish`` unchanged.
"""

from __future__ import annotations

import hmac
import logging
import uuid
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from ge_control_center import __version__
from ge_control_center.config import Settings
from ge_control_center.controlplane import runs
from ge_control_center.controlplane.migrations import latest_applied_migration

logger = logging.getLogger(__name__)

API_ACTOR = "api:dev-token"

# PostgreSQL SQLSTATE -> HTTP status for errors raised by the control-plane functions.
_SQLSTATE_STATUS = {
    "P0002": 404,  # no_data_found (unknown pipeline / run)
    "23505": 409,  # unique_violation (idempotency key reused for another pipeline)
    "55000": 409,  # object_not_in_prerequisite_state (no published version, archived, inactive steps)
    "22023": 400,  # invalid_parameter_value
    "23514": 400,  # check_violation
}


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_key: str = Field(min_length=3, max_length=63)
    idempotency_key: str = Field(min_length=1, max_length=200)
    params: dict[str, Any] = Field(default_factory=dict)
    window_start: datetime | None = None
    window_end: datetime | None = None
    priority: int = Field(0, ge=-100, le=100)

    @model_validator(mode="after")
    def _check_window(self) -> "CreateRunRequest":
        if (self.window_start is None) != (self.window_end is None):
            raise ValueError("window_start and window_end must be given together")
        for value in (self.window_start, self.window_end):
            if value is not None and value.tzinfo is None:
                raise ValueError("window timestamps must include a timezone offset")
        if self.window_start and self.window_end and self.window_start >= self.window_end:
            raise ValueError("window_start must be before window_end")
        return self


class CancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str | None = Field(None, max_length=500)


def create_app(settings: Settings, engine: Engine) -> FastAPI:
    if not settings.api_token:
        raise RuntimeError("GECC_API_TOKEN must be set; the API refuses to start without authentication")
    expected = settings.api_token.encode("utf-8")

    app = FastAPI(title="GE Data Control Center", version=__version__)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id[:100]
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    def require_token(request: Request) -> None:
        header = request.headers.get("Authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(token.encode("utf-8"), expected):
            raise HTTPException(status_code=401, detail="missing or invalid bearer token",
                                headers={"WWW-Authenticate": "Bearer"})

    @app.exception_handler(DBAPIError)
    async def db_error_handler(request: Request, error: DBAPIError):
        sqlstate = getattr(error.orig, "pgcode", None)
        status = _SQLSTATE_STATUS.get(sqlstate or "")
        if status is None:
            logger.exception("database error", exc_info=error)
            return JSONResponse(status_code=503, content={"detail": "control-plane database error"})
        message = getattr(getattr(error.orig, "diag", None), "message_primary", None) or str(error.orig)
        return JSONResponse(status_code=status, content={"detail": message})

    @app.get("/health")
    def health() -> JSONResponse:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError:
            return JSONResponse(status_code=503, content={"status": "unavailable", "control_db": "unreachable"})
        try:
            migration = latest_applied_migration(engine)
        except SQLAlchemyError:
            logger.warning("health: control DB reachable but meta.schema_version is not readable", exc_info=True)
            return JSONResponse(status_code=503, content={"status": "degraded", "control_db": "ok",
                                                          "latest_migration": None})
        return JSONResponse(content={"status": "ok", "version": __version__, "control_db": "ok",
                                     "latest_migration": migration})

    @app.get("/steps", dependencies=[Depends(require_token)])
    def list_steps(include_inactive: bool = False) -> list[dict[str, Any]]:
        return runs.list_steps(engine, include_inactive=include_inactive)

    @app.get("/pipelines", dependencies=[Depends(require_token)])
    def list_pipelines() -> list[dict[str, Any]]:
        return runs.list_pipelines(engine)

    @app.post("/runs", dependencies=[Depends(require_token)])
    def create_run(body: CreateRunRequest, request: Request, response: Response) -> dict[str, Any]:
        created = runs.create_run(
            engine,
            pipeline_key=body.pipeline_key,
            idempotency_key=body.idempotency_key,
            trigger_type="API",
            actor=API_ACTOR,
            actor_type="API",
            params=body.params,
            window_start=body.window_start,
            window_end=body.window_end,
            priority=body.priority,
            request_id=request.state.request_id,
        )
        response.status_code = 201 if created.created else 200
        return {"run_id": created.run_id, "created": created.created, "state": created.state}

    @app.get("/runs", dependencies=[Depends(require_token)])
    def list_runs(pipeline_key: str | None = None, limit: int = Query(20, ge=1, le=500)) -> list[dict[str, Any]]:
        return runs.list_runs(engine, pipeline_key=pipeline_key, limit=limit)

    @app.get("/runs/{run_id}", dependencies=[Depends(require_token)])
    def get_run(run_id: uuid.UUID) -> dict[str, Any]:
        run = runs.get_run(engine, str(run_id))
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return run

    @app.post("/runs/{run_id}/cancel", dependencies=[Depends(require_token)])
    def cancel_run(run_id: uuid.UUID, request: Request, body: CancelRequest | None = None) -> dict[str, Any]:
        state = runs.request_cancel(engine, str(run_id), actor=API_ACTOR, actor_type="API",
                                    reason=body.reason if body else None, request_id=request.state.request_id)
        return {"run_id": str(run_id), "state": state}

    @app.get("/runs/{run_id}/logs", dependencies=[Depends(require_token)])
    def get_logs(
        run_id: uuid.UUID,
        node_key: str | None = None,
        attempt_no: int | None = Query(None, ge=1),
        after_seq: int = Query(0, ge=0),
        limit: int = Query(1000, ge=1, le=10000),
    ) -> list[dict[str, Any]]:
        return runs.get_logs(engine, str(run_id), node_key=node_key, attempt_no=attempt_no,
                             after_seq=after_seq, limit=limit)

    return app
