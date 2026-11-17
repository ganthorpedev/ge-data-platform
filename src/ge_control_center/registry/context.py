"""Runtime context handed to a step function inside the worker's child process."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class StepContext:
    """Everything a step may know about its execution.

    The context intentionally carries no control-plane credentials: a step
    can reach the warehouse (``warehouse_engine``) but not the orchestration
    database (ADR-015).
    """

    run_id: str
    task_id: str
    attempt_id: str
    attempt_no: int
    max_attempts: int
    node_key: str
    step_key: str
    step_version: str
    run_params: Mapping[str, Any]
    window_start: datetime | None
    window_end: datetime | None
    log: logging.Logger
    warehouse_db_url: str | None = None
    _warehouse_engine: Engine | None = field(default=None, init=False, repr=False)

    @property
    def is_final_attempt(self) -> bool:
        return self.attempt_no >= self.max_attempts

    def warehouse_engine(self) -> Engine:
        """Lazily create (once) an engine for the warehouse database."""
        if self.warehouse_db_url is None:
            raise RuntimeError("GECC_WAREHOUSE_DB_URL is not configured for this worker")
        if self._warehouse_engine is None:
            self._warehouse_engine = create_engine(
                self.warehouse_db_url,
                pool_pre_ping=True,
                pool_size=2,
                connect_args={
                    "connect_timeout": 10,
                    "application_name": f"gecc-step-{self.step_key}"[:63],
                },
            )
        return self._warehouse_engine

    def close(self) -> None:
        if self._warehouse_engine is not None:
            self._warehouse_engine.dispose()
            self._warehouse_engine = None
