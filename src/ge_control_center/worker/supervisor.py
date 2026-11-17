"""The worker process (ADR-011, ADR-012, ADR-015, ADR-021).

One worker runs up to ``capacity`` attempts at once. Each attempt executes in
its own child process (``worker/step_runner.py``) so it can be timed out,
cancelled or killed without harming the worker or other attempts.

Safety properties:

* **Leases.** Every claimed attempt carries a lease the heartbeat thread
  renews. If renewal is rejected -- or cannot succeed before the lease would
  expire (database unreachable, network partition) -- the worker kills the
  child itself ("self-fencing") and does not report a result, so a task
  is never knowingly executed by two workers at once. Lease deadlines use
  this process's monotonic clock, measured from *before* the claim/renew
  request, so they are always conservative relative to the database clock.
* **Parent-death detection.** If the worker is killed outright (``kill -9``),
  each child sees EOF on stdin and exits; the reaper (any worker) then marks
  the expired attempt LOST and the task is retried.
* **At-least-once.** Any outcome that cannot be reported is recovered by the
  reaper as LOST -> retry. Steps must be idempotent.
* **Graceful stop.** SIGTERM/SIGINT stops claiming, drains running attempts
  for ``drain_seconds``, then terminates the rest and reports them as
  retryable failures. A second signal skips the drain.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import threading
import uuid
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ge_control_center import __version__
from ge_control_center.config import PROJECT_ROOT
from ge_control_center.controlplane.client import ClaimedAttempt, ControlPlaneClient, LogLine
from ge_control_center.registry.step import StepRegistry
from ge_control_center.worker.process import (
    MAX_TAIL_LINES,
    kill_process,
    popen_kwargs,
    python_executable,
    stream_pipe,
    terminate_process,
)
from ge_control_center.worker.protocol import encode_invocation, parse_message

logger = logging.getLogger(__name__)

_STRIPPED_ENV_PREFIXES = ("GECC_CONTROL_DB", "GECC_API_")


class RegistryMismatchError(RuntimeError):
    """This worker's step code does not match the control database's active step registry."""


@dataclass(frozen=True)
class WorkerOptions:
    name: str
    queues: tuple[str, ...]
    capacity: int
    lease_seconds: int = 30
    heartbeat_seconds: int = 10
    lease_safety_seconds: int = 8
    poll_seconds: float = 2.0
    reap_seconds: int = 15
    drain_seconds: int = 20
    worker_lost_after_seconds: int = 120
    terminate_grace_seconds: float = 10.0
    log_flush_seconds: float = 1.0
    max_log_lines_per_attempt: int = 20_000


def build_child_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Child environment: everything except control-plane and API credentials."""
    source = os.environ if base is None else base
    env = {key: value for key, value in source.items() if not key.startswith(_STRIPPED_ENV_PREFIXES)}
    env["PYTHONUNBUFFERED"] = "1"
    return env


class AttemptExecution:
    """Runs and supervises one claimed attempt in a child process."""

    def __init__(
        self,
        claim: ClaimedAttempt,
        client: ControlPlaneClient,
        options: WorkerOptions,
        *,
        lease_started_at: float,
        child_env: Mapping[str, str],
    ) -> None:
        self.claim = claim
        self.client = client
        self.options = options
        self.child_env = dict(child_env)
        self.lease_deadline = lease_started_at + options.lease_seconds
        self.done = threading.Event()

        self._cancel = threading.Event()
        self._fence = threading.Event()
        self._shutdown = threading.Event()
        self._fence_reason: str | None = None

        self._result: dict[str, Any] | None = None
        self._error: dict[str, Any] | None = None
        self._stderr_tail: deque[str] = deque(maxlen=MAX_TAIL_LINES)
        self._stdout_tail: deque[str] = deque(maxlen=MAX_TAIL_LINES)

        self._log_lock = threading.Lock()
        self._pending_logs: list[LogLine] = []
        self._log_seq = 0
        self._log_dropped = False
        self._last_flush = monotonic()
        self._log_failing = False
        self._log_backoff = 0.0
        self._log_retry_at = 0.0

        self.thread = threading.Thread(target=self._run_safely, name=f"attempt-{claim.label}", daemon=True)

    # -- control from other threads -------------------------------------------------
    def start(self) -> None:
        self.thread.start()

    def request_cancel(self) -> None:
        self._cancel.set()

    def request_shutdown(self) -> None:
        self._shutdown.set()

    def fence(self, reason: str) -> None:
        if not self._fence.is_set() and not self.done.is_set():
            self._fence_reason = reason
            logger.error("[%s] FENCING: %s", self.claim.label, reason)
            self._fence.set()

    # -- logging ------------------------------------------------------------------------
    def _add_log(self, stream: str, line: str) -> None:
        with self._log_lock:
            if self._log_seq >= self.options.max_log_lines_per_attempt:
                if not self._log_dropped:
                    self._log_dropped = True
                    self._log_seq += 1
                    self._pending_logs.append(LogLine(self._log_seq, datetime.now(timezone.utc), "system",
                                                      "log limit reached; further lines are not stored"))
                return
            self._log_seq += 1
            self._pending_logs.append(LogLine(self._log_seq, datetime.now(timezone.utc), stream, line))
            if len(self._pending_logs) > 5_000:  # control DB unreachable for a long time: bound memory
                del self._pending_logs[: len(self._pending_logs) - 5_000]

    def _flush_logs(self, *, force: bool = False) -> bool:
        """Ship pending log lines. Returns True when nothing is left pending.

        On failure the lines are kept and shipping backs off exponentially (1 s .. 30 s) with ONE
        warning per failure streak, so a control-DB outage is not hammered or log-spammed.
        Appends are idempotent (attempt_id, seq), so re-sending a partially shipped batch is safe.
        """
        now = monotonic()
        if not force and (now - self._last_flush < self.options.log_flush_seconds or now < self._log_retry_at):
            return False
        self._last_flush = now
        with self._log_lock:
            batch, self._pending_logs = self._pending_logs, []
        if not batch:
            return True
        try:
            for start in range(0, len(batch), 500):
                self.client.append_logs(self.claim.attempt_id, self.claim.lease_token, batch[start:start + 500])
        except SQLAlchemyError as error:
            with self._log_lock:
                self._pending_logs = batch + self._pending_logs
            self._log_backoff = min(max(self._log_backoff * 2, 1.0), 30.0)
            self._log_retry_at = monotonic() + self._log_backoff
            if not self._log_failing:
                logger.warning("[%s] cannot ship log lines; buffering and retrying with backoff: %s",
                               self.claim.label, error)
            self._log_failing = True
            return False
        if self._log_failing:
            logger.info("[%s] log shipping recovered", self.claim.label)
        self._log_failing, self._log_backoff, self._log_retry_at = False, 0.0, 0.0
        return True

    def _final_flush(self) -> None:
        """Deliver every remaining line before the attempt ends, retrying through a control-DB outage.

        Bounded so a dead control plane cannot hold the worker slot forever. Log appends are fenced
        by lease token, not attempt state, so the lines of a fenced or LOST attempt -- the ones most
        needed for diagnosis -- are still accepted once the database is back.
        """
        give_up_at = monotonic() + max(60.0, self.options.lease_seconds * 4.0)
        delay = 1.0
        while not self._flush_logs(force=True):
            if monotonic() >= give_up_at:
                with self._log_lock:
                    pending = len(self._pending_logs)
                logger.error("[%s] giving up on %d undelivered log line(s)", self.claim.label, pending)
                return
            threading.Event().wait(delay)
            delay = min(delay * 2, 15.0)

    def _on_stdout(self, line: str) -> None:
        message = parse_message(line)
        if message is None:
            logger.info("[%s] %s", self.claim.label, line)
            self._add_log("stdout", line)
        elif message["type"] == "result":
            self._result = message.get("result") or {}
        else:
            self._error = message

    def _on_stderr(self, line: str) -> None:
        logger.info("[%s] stderr: %s", self.claim.label, line)
        self._stderr_tail.append(line)
        self._add_log("stderr", line)

    # -- execution ----------------------------------------------------------------------
    def _run_safely(self) -> None:
        try:
            self._run()
        except Exception as error:  # noqa: BLE001 - a supervisor bug must not kill the worker
            logger.exception("[%s] supervisor error", self.claim.label)
            self._report("FAILED", retryable=True, error_class="SupervisorError", message=str(error))
        finally:
            self.done.set()

    def _run(self) -> None:
        claim = self.claim
        self._add_log("system", f"attempt {claim.attempt_no}/{claim.max_attempts} of {claim.step_key}@{claim.step_version} "
                                f"started on {socket.gethostname()} (timeout {claim.timeout_seconds}s)")
        try:
            process = subprocess.Popen(
                [python_executable(), "-m", "ge_control_center.worker.step_runner"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=PROJECT_ROOT,
                env=self.child_env,
                **popen_kwargs(),
            )
        except OSError as error:
            self._report("FAILED", retryable=True, error_class="SpawnFailed", message=str(error))
            return

        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        process.stdin.write(encode_invocation(
            attempt_id=claim.attempt_id, attempt_no=claim.attempt_no, max_attempts=claim.max_attempts,
            task_id=claim.task_id, run_id=claim.run_id, node_key=claim.node_key, step_key=claim.step_key,
            step_version=claim.step_version, params=claim.params, run_params=claim.run_params,
            window_start=claim.window_start, window_end=claim.window_end,
        ))
        process.stdin.flush()  # stdin stays open: EOF tells the child its supervisor died

        readers = [
            threading.Thread(target=stream_pipe, args=(process.stdout, self._on_stdout, self._stdout_tail), daemon=True),
            threading.Thread(target=stream_pipe, args=(process.stderr, self._on_stderr, deque(maxlen=1)), daemon=True),
        ]
        for reader in readers:
            reader.start()

        deadline = monotonic() + claim.timeout_seconds
        stop_reason: str | None = None
        while True:
            try:
                process.wait(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                pass
            self._flush_logs()
            if self._fence.is_set():
                stop_reason = "fenced"
                kill_process(process)
                break
            if monotonic() >= deadline:
                stop_reason = "timeout"
                self._add_log("system", f"timeout after {claim.timeout_seconds}s; terminating")
                terminate_process(process, self.options.terminate_grace_seconds)
                break
            if self._cancel.is_set():
                stop_reason = "cancel"
                self._add_log("system", "run cancellation requested; terminating")
                terminate_process(process, self.options.terminate_grace_seconds)
                break
            if self._shutdown.is_set():
                stop_reason = "shutdown"
                self._add_log("system", "worker shutting down; terminating")
                terminate_process(process, self.options.terminate_grace_seconds)
                break

        return_code = process.wait()
        for reader in readers:
            reader.join(timeout=5)
        try:
            process.stdin.close()
        except OSError:
            pass

        if stop_reason == "fenced":
            self._add_log("system", f"child killed; result not reported: {self._fence_reason}")
            self._final_flush()
            return

        outcome = self._classify(return_code, stop_reason)
        self._add_log("system", f"exit code {return_code}; outcome {outcome['outcome']}")
        self._final_flush()
        self._report(**outcome)

    def _stderr_excerpt(self) -> str:
        tail = list(self._stderr_tail)[-30:]
        return ("\n--- stderr (tail) ---\n" + "\n".join(tail)) if tail else ""

    def _classify(self, return_code: int, stop_reason: str | None) -> dict[str, Any]:
        timeout = self.claim.timeout_seconds
        if stop_reason == "timeout":
            return {"outcome": "TIMED_OUT", "retryable": True, "exit_code": return_code, "error_class": "Timeout",
                    "message": f"attempt exceeded its {timeout}s timeout" + self._stderr_excerpt()}
        if stop_reason == "cancel":
            return {"outcome": "CANCELLED", "retryable": False, "exit_code": return_code,
                    "error_class": "Cancelled", "message": "run cancelled"}
        if stop_reason == "shutdown":
            return {"outcome": "FAILED", "retryable": True, "exit_code": return_code, "error_class": "WorkerShutdown",
                    "message": "worker shut down before the attempt finished"}
        if return_code == 0 and self._result is not None:
            return {"outcome": "SUCCEEDED", "retryable": False, "exit_code": 0, "result": self._result}
        if self._error is not None:
            return {"outcome": "FAILED", "retryable": bool(self._error.get("retryable", True)), "exit_code": return_code,
                    "error_class": str(self._error.get("error_class") or "StepError"),
                    "message": str(self._error.get("message") or "") + self._stderr_excerpt()}
        if return_code == 0:
            return {"outcome": "FAILED", "retryable": True, "exit_code": 0, "error_class": "ProtocolError",
                    "message": "step runner exited 0 without reporting a result"}
        return {"outcome": "FAILED", "retryable": True, "exit_code": return_code, "error_class": "ChildCrashed",
                "message": f"step process exited with code {return_code} without reporting an error"
                           + self._stderr_excerpt()}

    def _report(self, outcome: str, *, retryable: bool, exit_code: int | None = None, error_class: str | None = None,
                message: str | None = None, result: dict[str, Any] | None = None) -> None:
        give_up_at = monotonic() + max(60, self.options.lease_seconds * 4)
        delay = 1.0
        while True:
            try:
                new_state = self.client.finish(
                    self.claim.attempt_id, self.claim.lease_token, outcome=outcome, retryable=retryable,
                    exit_code=exit_code, error_class=error_class, error_message=message, result=result,
                )
            except SQLAlchemyError as error:
                if monotonic() >= give_up_at:
                    logger.error("[%s] could not report %s; the reaper will recover this attempt as LOST: %s",
                                 self.claim.label, outcome, error)
                    return
                logger.warning("[%s] reporting %s failed (retrying in %.0fs): %s", self.claim.label, outcome, delay, error)
                threading.Event().wait(delay)
                delay = min(delay * 2, 15)
                continue
            if new_state is None:
                logger.warning("[%s] outcome %s rejected: attempt no longer owned by this worker (reaped or already "
                               "finished)", self.claim.label, outcome)
            else:
                logger.info("[%s] %s -> task %s", self.claim.label, outcome, new_state)
            return


class Worker:
    def __init__(
        self,
        client: ControlPlaneClient,
        options: WorkerOptions,
        *,
        registry: StepRegistry,
        child_env: Mapping[str, str] | None = None,
    ) -> None:
        self.client = client
        self.options = options
        self.registry = registry
        self.child_env = build_child_env(child_env)
        self.worker_id = str(uuid.uuid4())
        self.code_version = f"{__version__}+{registry.fingerprint()[:12]}"

        self._executions: dict[str, AttemptExecution] = {}
        self._lock = threading.Lock()
        self._stopping = threading.Event()
        self._skip_drain = threading.Event()
        self._halt = threading.Event()
        self._status = "ACTIVE"

    # -- lifecycle ------------------------------------------------------------------------
    def request_stop(self, *_: object) -> None:
        if self._stopping.is_set():
            logger.warning("second stop request: skipping drain")
            self._skip_drain.set()
        else:
            logger.info("stop requested: no new claims; draining running attempts")
        self._stopping.set()

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)

    def check_registry_compatibility(self) -> list[str]:
        """Return warnings; raise RegistryMismatchError if this worker cannot run the active registry."""
        active = self.client.active_step_versions()
        errors: list[str] = []
        warnings: list[str] = []
        for key, (version, digest) in sorted(active.items()):
            if key not in self.registry:
                errors.append(f"{key}@{version} is active in the control DB but not provided by this worker")
                continue
            local = self.registry.get(key)
            if local.version != version:
                errors.append(f"{key}: control DB has version {version}, this worker has {local.version}")
            elif local.definition_hash() != digest:
                errors.append(f"{key}@{version}: contract differs from the registered definition")
        for definition in self.registry:
            if definition.key not in active:
                warnings.append(f"{definition.key}@{definition.version} is not registered (run `gecc registry sync`)")
        if errors:
            raise RegistryMismatchError("worker code does not match the control-plane registry:\n  - " + "\n  - ".join(errors))
        return warnings

    def _active(self) -> list[AttemptExecution]:
        with self._lock:
            for attempt_id in [a for a, e in self._executions.items() if e.done.is_set()]:
                del self._executions[attempt_id]
            return list(self._executions.values())

    def run(self) -> int:
        for warning in self.check_registry_compatibility():
            logger.warning(warning)

        self.client.register_worker(
            self.worker_id, self.options.name, socket.gethostname(), os.getpid(),
            list(self.options.queues), self.options.capacity, self.code_version,
        )
        logger.info("worker %s (%s) started: queues=%s capacity=%d code=%s", self.options.name, self.worker_id,
                    ",".join(self.options.queues), self.options.capacity, self.code_version)

        background = [
            threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True),
            threading.Thread(target=self._reaper_loop, name="reaper", daemon=True),
        ]
        for thread in background:
            thread.start()

        try:
            self._claim_loop()
            self._drain()
        finally:
            self._halt.set()
            for thread in background:
                thread.join(timeout=5)
            try:
                self.client.stop_worker(self.worker_id)
            except SQLAlchemyError as error:
                logger.warning("could not mark worker stopped: %s", error)
            logger.info("worker %s stopped", self.options.name)
        return 0

    def _claim_loop(self) -> None:
        error_backoff = self.options.poll_seconds
        while not self._stopping.is_set():
            if len(self._active()) >= self.options.capacity:
                self._stopping.wait(0.25)
                continue
            claim_started = monotonic()
            try:
                claim = self.client.claim(self.worker_id, list(self.options.queues), self.options.lease_seconds)
            except SQLAlchemyError as error:
                logger.warning("claim failed (retrying in %.1fs): %s", error_backoff, error)
                self._stopping.wait(error_backoff)
                error_backoff = min(error_backoff * 2, 30)
                continue
            error_backoff = self.options.poll_seconds
            if claim is None:
                self._stopping.wait(self.options.poll_seconds)
                continue
            execution = AttemptExecution(claim, self.client, self.options,
                                         lease_started_at=claim_started, child_env=self.child_env)
            with self._lock:
                self._executions[claim.attempt_id] = execution
            logger.info("[%s] claimed %s@%s", claim.label, claim.step_key, claim.step_version)
            execution.start()

    def _drain(self) -> None:
        self._status = "DRAINING"
        deadline = monotonic() + self.options.drain_seconds
        while self._active() and monotonic() < deadline and not self._skip_drain.is_set():
            self._skip_drain.wait(0.5)
        remaining = self._active()
        for execution in remaining:
            execution.request_shutdown()
        for execution in remaining:
            execution.done.wait(timeout=self.options.terminate_grace_seconds + 30)

    # -- background loops ---------------------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        next_beat = 0.0
        while not self._halt.wait(0.5):
            now = monotonic()
            executions = self._active()
            if now >= next_beat:
                next_beat = now + self.options.heartbeat_seconds
                try:
                    self.client.worker_heartbeat(self.worker_id, self._status)
                except SQLAlchemyError as error:
                    logger.warning("worker heartbeat failed: %s", error)
                for execution in executions:
                    renew_started = monotonic()
                    try:
                        status = self.client.renew_lease(execution.claim.attempt_id, execution.claim.lease_token,
                                                         self.options.lease_seconds)
                    except SQLAlchemyError as error:
                        logger.warning("[%s] lease renewal failed: %s", execution.claim.label, error)
                        continue
                    if status.ok:
                        execution.lease_deadline = renew_started + self.options.lease_seconds
                        if status.cancel_requested:
                            execution.request_cancel()
                    else:
                        execution.fence("lease renewal rejected (lease expired or attempt reaped)")
            for execution in executions:
                if monotonic() >= execution.lease_deadline - self.options.lease_safety_seconds:
                    execution.fence("could not renew the lease before it expires; stopping the child so the task "
                                    "cannot run twice")

    def _reaper_loop(self) -> None:
        while not self._halt.wait(self.options.reap_seconds):
            try:
                reaped = self.client.reap_expired_leases(100, self.options.worker_lost_after_seconds)
            except SQLAlchemyError as error:
                logger.warning("reaper failed: %s", error)
                continue
            if reaped:
                logger.warning("reaper: marked %d expired attempt(s) LOST", reaped)
