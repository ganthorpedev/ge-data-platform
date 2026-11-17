"""``gecc`` -- GE Data Control Center command line.

    gecc migrate                              apply sql/control/migrations to GECC_CONTROL_DB_URL
    gecc registry sync | list                 sync in-code steps to the control DB / list them
    gecc pipeline validate FILE               validate a pipeline spec (no database needed)
    gecc pipeline publish FILE                publish a new immutable version (no-op if unchanged)
    gecc pipeline list
    gecc run trigger KEY --idempotency-key K  create a run (idempotent)
    gecc run show|wait|cancel|logs RUN_ID
    gecc run list [--pipeline KEY]
    gecc worker [--name N --queues a,b --capacity C]
    gecc reap                                 recover expired leases now
    gecc api [--host H --port P]
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.exc import DBAPIError

from ge_control_center.config import ConfigurationError, Settings, load_settings
from ge_control_center.controlplane import runs
from ge_control_center.controlplane.db import create_control_engine

TERMINAL_RUN_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _actor(settings: Settings) -> str:
    if settings.actor:
        return settings.actor
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001
        user = "unknown"
    return f"cli:{user}"


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str, sort_keys=False))


def _read_spec(path: str) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"spec file not found: {path}")
    except json.JSONDecodeError as error:
        raise SystemExit(f"{path} is not valid JSON: {error}")


def _parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise SystemExit(f"{value!r} must include a timezone offset, e.g. 2026-11-17T00:00:00+02:00")
    return parsed


# -- commands -----------------------------------------------------------------------------

def cmd_migrate(args, settings: Settings) -> int:
    from ge_control_center.controlplane.migrations import apply_migrations

    engine = create_control_engine(settings.control_db_url, component="migrate", pool_size=1)
    for outcome in apply_migrations(engine):
        print(f"{'applied ' if outcome.applied else 'present '} {outcome.name}")
    return 0


def cmd_registry_sync(args, settings: Settings) -> int:
    from ge_control_center.registry.step import load_default_registry
    from ge_control_center.registry.sync import sync_registry

    engine = create_control_engine(settings.control_db_url, component="registry-sync", pool_size=1)
    report = sync_registry(engine, load_default_registry(), actor=_actor(settings))
    for label, items in (("inserted", report.inserted), ("updated", report.updated_cosmetic),
                         ("unchanged", report.unchanged), ("deactivated", report.deactivated)):
        for item in items:
            print(f"{label:<12}{item}")
    return 0


def cmd_registry_list(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    for step in runs.list_steps(engine, include_inactive=args.all):
        flag = "" if step["is_active"] else "  (inactive)"
        print(f"{step['step_key']}@{step['code_version']:<4} {step['idempotency_mode']:<12} "
              f"keys={','.join(step['concurrency_keys']) or '-'}  {step['display_name']}{flag}")
    return 0


def cmd_pipeline_validate(args, settings: Settings | None) -> int:
    from ge_control_center.dag.spec import SpecValidationError, validate_spec
    from ge_control_center.registry.step import load_default_registry

    try:
        resolved = validate_spec(_read_spec(args.file), load_default_registry())
    except SpecValidationError as error:
        print(error, file=sys.stderr)
        return 1
    print(f"valid: {resolved.pipeline_key} ({len(resolved.nodes)} nodes, {len(resolved.edges)} edges) "
          f"spec_hash={resolved.spec_hash}")
    return 0


def cmd_pipeline_publish(args, settings: Settings) -> int:
    from ge_control_center.dag.publish import PublishError, publish_pipeline
    from ge_control_center.dag.spec import SpecValidationError, validate_spec
    from ge_control_center.registry.step import load_default_registry

    try:
        resolved = validate_spec(_read_spec(args.file), load_default_registry())
    except SpecValidationError as error:
        print(error, file=sys.stderr)
        return 1
    engine = create_control_engine(settings.control_db_url, component="publish", pool_size=1)
    try:
        result = publish_pipeline(engine, resolved, actor=_actor(settings))
    except PublishError as error:
        print(f"publish refused: {error}", file=sys.stderr)
        return 1
    verb = "published" if result.created else "unchanged"
    print(f"{verb}: {resolved.pipeline_key} v{result.version_no} spec_hash={result.spec_hash}")
    return 0


def cmd_pipeline_list(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    for pipeline in runs.list_pipelines(engine):
        print(f"{pipeline['pipeline_key']:<32} v{pipeline['latest_version']}  max_active_runs="
              f"{pipeline['max_active_runs']}  {pipeline['name']}")
    return 0


def cmd_run_trigger(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    params = json.loads(args.params) if args.params else {}
    if not isinstance(params, dict):
        raise SystemExit("--params must be a JSON object")
    created = runs.create_run(
        engine,
        pipeline_key=args.pipeline_key,
        idempotency_key=args.idempotency_key,
        trigger_type="MANUAL",
        actor=_actor(settings),
        actor_type="CLI",
        params=params,
        window_start=_parse_time(args.window_start),
        window_end=_parse_time(args.window_end),
        priority=args.priority,
    )
    _print_json({"run_id": created.run_id, "created": created.created, "state": created.state})
    return 0


def _summarise(run: dict[str, Any]) -> str:
    lines = [f"run {run['run_id']}  {run['pipeline_key']} v{run['pipeline_version']}  {run['state']}"]
    for task in run["tasks"]:
        lines.append(f"  {task['node_key']:<20} {task['state']:<16} attempts {task['attempt_count']}/{task['max_attempts']}")
        for attempt in task["attempts"]:
            detail = attempt.get("error_class") or ""
            lines.append(f"      #{attempt['attempt_no']} {attempt['state']:<10} {attempt['worker_name']:<16} {detail}")
    return "\n".join(lines)


def cmd_run_show(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    run = runs.get_run(engine, args.run_id)
    if run is None:
        print("run not found", file=sys.stderr)
        return 1
    if args.json:
        _print_json(run)
    else:
        print(_summarise(run))
    return 0


def cmd_run_wait(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    deadline = time.monotonic() + args.timeout
    while True:
        run = runs.get_run(engine, args.run_id)
        if run is None:
            print("run not found", file=sys.stderr)
            return 1
        if run["state"] in TERMINAL_RUN_STATES:
            print(_summarise(run))
            return 0 if run["state"] == "SUCCEEDED" else 2
        if time.monotonic() >= deadline:
            print(_summarise(run))
            print(f"timed out waiting after {args.timeout}s", file=sys.stderr)
            return 3
        time.sleep(1)


def cmd_run_cancel(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    state = runs.request_cancel(engine, args.run_id, actor=_actor(settings), actor_type="CLI", reason=args.reason)
    print(state)
    return 0


def cmd_run_list(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    for run in runs.list_runs(engine, pipeline_key=args.pipeline, limit=args.limit):
        print(f"{run['run_id']}  {run['pipeline_key']:<28} v{run['pipeline_version']:<3} {run['state']:<11} "
              f"{run['trigger_type']:<7} {run['created_at']}")
    return 0


def cmd_run_logs(args, settings: Settings) -> int:
    engine = create_control_engine(settings.control_db_url, component="cli", pool_size=1)
    for line in runs.get_logs(engine, args.run_id, node_key=args.node, attempt_no=args.attempt, limit=args.limit):
        print(f"{line['node_key']}#{line['attempt_no']} {line['logged_at']} [{line['stream']}] {line['line']}")
    return 0


def cmd_worker(args, settings: Settings) -> int:
    from ge_control_center.controlplane.client import SqlControlPlaneClient
    from ge_control_center.registry.step import load_default_registry
    from ge_control_center.worker.supervisor import RegistryMismatchError, Worker, WorkerOptions

    queues = tuple(q.strip() for q in args.queues.split(",") if q.strip())
    options = WorkerOptions(
        name=args.name or f"{socket.gethostname()}-{os.getpid()}",
        queues=queues,
        capacity=args.capacity,
        lease_seconds=settings.lease_seconds,
        heartbeat_seconds=settings.heartbeat_seconds,
        lease_safety_seconds=settings.lease_safety_seconds,
        poll_seconds=settings.poll_seconds,
        reap_seconds=settings.reap_seconds,
        drain_seconds=settings.drain_seconds,
        worker_lost_after_seconds=settings.worker_lost_after_seconds,
    )
    engine = create_control_engine(settings.control_db_url, component=f"worker-{options.name}",
                                   pool_size=max(4, args.capacity + 3))
    worker = Worker(SqlControlPlaneClient(engine), options, registry=load_default_registry())
    worker.install_signal_handlers()
    try:
        return worker.run()
    except RegistryMismatchError as error:
        print(str(error), file=sys.stderr)
        return 4


def cmd_reap(args, settings: Settings) -> int:
    from ge_control_center.controlplane.client import SqlControlPlaneClient

    engine = create_control_engine(settings.control_db_url, component="reaper", pool_size=1)
    print(SqlControlPlaneClient(engine).reap_expired_leases(1000, settings.worker_lost_after_seconds))
    return 0


def cmd_api(args, settings: Settings) -> int:
    import uvicorn

    from ge_control_center.api.app import create_app

    engine = create_control_engine(settings.control_db_url, component="api", pool_size=10)
    uvicorn.run(create_app(settings, engine), host=args.host, port=args.port, log_level="info")
    return 0


# -- parser -------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gecc", description="GE Data Control Center")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply control-plane migrations").set_defaults(func=cmd_migrate)

    registry = sub.add_parser("registry", help="step registry").add_subparsers(dest="registry_command", required=True)
    registry.add_parser("sync").set_defaults(func=cmd_registry_sync)
    reg_list = registry.add_parser("list")
    reg_list.add_argument("--all", action="store_true", help="include inactive step versions")
    reg_list.set_defaults(func=cmd_registry_list)

    pipeline = sub.add_parser("pipeline", help="pipelines").add_subparsers(dest="pipeline_command", required=True)
    validate = pipeline.add_parser("validate")
    validate.add_argument("file")
    validate.set_defaults(func=cmd_pipeline_validate, needs_db=False)
    publish = pipeline.add_parser("publish")
    publish.add_argument("file")
    publish.set_defaults(func=cmd_pipeline_publish)
    pipeline.add_parser("list").set_defaults(func=cmd_pipeline_list)

    run = sub.add_parser("run", help="runs").add_subparsers(dest="run_command", required=True)
    trigger = run.add_parser("trigger")
    trigger.add_argument("pipeline_key")
    trigger.add_argument("--idempotency-key", required=True)
    trigger.add_argument("--params", help="run parameters as a JSON object")
    trigger.add_argument("--window-start")
    trigger.add_argument("--window-end")
    trigger.add_argument("--priority", type=int, default=0)
    trigger.set_defaults(func=cmd_run_trigger)
    show = run.add_parser("show")
    show.add_argument("run_id")
    show.add_argument("--json", action="store_true")
    show.set_defaults(func=cmd_run_show)
    wait = run.add_parser("wait")
    wait.add_argument("run_id")
    wait.add_argument("--timeout", type=int, default=300)
    wait.set_defaults(func=cmd_run_wait)
    cancel = run.add_parser("cancel")
    cancel.add_argument("run_id")
    cancel.add_argument("--reason")
    cancel.set_defaults(func=cmd_run_cancel)
    run_list = run.add_parser("list")
    run_list.add_argument("--pipeline")
    run_list.add_argument("--limit", type=int, default=20)
    run_list.set_defaults(func=cmd_run_list)
    logs = run.add_parser("logs")
    logs.add_argument("run_id")
    logs.add_argument("--node")
    logs.add_argument("--attempt", type=int)
    logs.add_argument("--limit", type=int, default=1000)
    logs.set_defaults(func=cmd_run_logs)

    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument("--name")
    worker.add_argument("--queues", default="default")
    worker.add_argument("--capacity", type=int, default=2, choices=range(1, 65), metavar="1-64")
    worker.set_defaults(func=cmd_worker)

    sub.add_parser("reap", help="recover expired leases now").set_defaults(func=cmd_reap)

    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.set_defaults(func=cmd_api)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        settings = load_settings(require_control_db=getattr(args, "needs_db", True))
        return args.func(args, settings)
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    except DBAPIError as error:
        message = getattr(getattr(error.orig, "diag", None), "message_primary", None) or str(error.orig)
        print(f"control-plane error: {message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
