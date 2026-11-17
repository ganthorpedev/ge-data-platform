"""Child-process entry point: ``python -m ge_control_center.worker.step_runner``.

Runs exactly one step attempt. Reads the invocation from stdin, resolves the
step from the in-code registry (refusing unknown keys and version
mismatches), validates parameters, calls the step, and reports the outcome
on stdout using the protocol in ``worker/protocol.py``.

This process never receives control-plane credentials.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import traceback

from pydantic import ValidationError

from ge_control_center.registry import PermanentStepError, RegistryError, StepContext
from ge_control_center.registry.step import load_default_registry
from ge_control_center.worker.protocol import (
    EXIT_INVOCATION_ERROR,
    EXIT_OK,
    EXIT_PARENT_GONE,
    EXIT_STEP_FAILED,
    EXIT_TERMINATED,
    decode_invocation,
    encode_message,
)

_write_lock = threading.Lock()


class _Terminated(BaseException):
    """Raised in the main thread on SIGTERM so step cleanup (finally / context managers) runs."""


def _emit(message: dict) -> None:
    with _write_lock:
        sys.stdout.write(encode_message(message) + "\n")
        sys.stdout.flush()


def _watch_parent() -> None:
    """Exit immediately if the supervisor dies (stdin reaches EOF)."""
    try:
        while sys.stdin.read(4096):
            pass
    except (OSError, ValueError):
        pass
    os._exit(EXIT_PARENT_GONE)


def _on_sigterm(signum, frame) -> None:  # noqa: ARG001
    raise _Terminated()


def _fail(error_class: str, message: str, *, retryable: bool, exit_code: int) -> int:
    _emit({"type": "error", "error_class": error_class, "message": message[:8000], "retryable": retryable})
    return exit_code


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(levelname)s %(message)s", force=True)
    log = logging.getLogger("gecc.step")

    first_line = sys.stdin.readline()
    threading.Thread(target=_watch_parent, name="parent-watchdog", daemon=True).start()
    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        invocation = decode_invocation(first_line)
    except (ValueError, json.JSONDecodeError) as error:
        return _fail("InvalidInvocation", str(error), retryable=False, exit_code=EXIT_INVOCATION_ERROR)

    registry = load_default_registry()
    try:
        definition = registry.get(invocation["step_key"], invocation["step_version"])
    except RegistryError as error:
        # The worker's code does not provide this exact step version: deploy/sync mismatch.
        return _fail(type(error).__name__, str(error), retryable=False, exit_code=EXIT_INVOCATION_ERROR)

    try:
        params = definition.params_model.model_validate(invocation.get("params") or {})
    except ValidationError as error:
        return _fail("InvalidParameters", str(error), retryable=False, exit_code=EXIT_INVOCATION_ERROR)

    ctx = StepContext(
        run_id=invocation["run_id"],
        task_id=invocation["task_id"],
        attempt_id=invocation["attempt_id"],
        attempt_no=int(invocation["attempt_no"]),
        max_attempts=int(invocation["max_attempts"]),
        node_key=invocation["node_key"],
        step_key=definition.key,
        step_version=definition.version,
        run_params=invocation.get("run_params") or {},
        window_start=invocation.get("window_start"),
        window_end=invocation.get("window_end"),
        log=log,
        warehouse_db_url=os.getenv("GECC_WAREHOUSE_DB_URL") or None,
    )

    log.info("step %s@%s attempt %d/%d starting", definition.key, definition.version, ctx.attempt_no, ctx.max_attempts)
    try:
        raw_result = definition.func(ctx, params)
    except _Terminated:
        log.warning("terminated by supervisor")
        return _fail("Terminated", "terminated by supervisor", retryable=True, exit_code=EXIT_TERMINATED)
    except PermanentStepError as error:
        log.error("permanent failure: %s", error)
        return _fail(type(error).__name__, str(error), retryable=False, exit_code=EXIT_STEP_FAILED)
    except Exception as error:  # noqa: BLE001 - every other failure is retryable, bounded by max_attempts
        traceback.print_exc()
        return _fail(type(error).__name__, str(error), retryable=True, exit_code=EXIT_STEP_FAILED)
    finally:
        ctx.close()

    try:
        result = {} if raw_result is None else dict(raw_result)
        json.dumps(result)
    except (TypeError, ValueError) as error:
        # A code defect in the step; retrying would produce the same result.
        return _fail("InvalidStepResult", f"step result must be a JSON-serialisable mapping: {error}",
                     retryable=False, exit_code=EXIT_STEP_FAILED)

    _emit({"type": "result", "result": result})
    log.info("step %s attempt %d succeeded", definition.key, ctx.attempt_no)
    return EXIT_OK


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)  # do not wait on the daemon watchdog thread blocked reading stdin
