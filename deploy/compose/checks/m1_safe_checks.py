#!/usr/bin/env python3
"""GE Data Control Center -- automated SAFE M1 checks against the running local stack.

    python3 deploy/compose/checks/m1_safe_checks.py

Safe means:
  * no container is stopped, killed or restarted; no database is restarted, dropped or restored;
  * nothing existing is modified -- new pipelines and runs use check-specific names
    (chk_*_<tag>, idempotency keys checks-<tag>-*); the m1_demo pipeline is never republished;
  * every "this write must be rejected" probe runs inside BEGIN ... ROLLBACK.
Destructive / recovery tests live in m1_recovery_checks.py and require explicit approval.

Exit code 0 when every check passes. Takes about 2.5 minutes (two 60 s sleep runs).
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from _common import (
    ACTOR,
    REPO,
    CheckFailed,
    Runner,
    api,
    attempts,
    compose,
    expect,
    gecc,
    max_overlap,
    preflight,
    psql,
    q,
    q1,
    run_detail,
    sh,
    trigger,
    wait_run,
    wait_until,
)

TAG = time.strftime("%Y%m%d%H%M%S")
SCRATCH = REPO / "pipelines" / "scratch" / f"checks_{TAG}"
IN_CONTAINER = f"pipelines/scratch/checks_{TAG}"
runs: dict[str, object] = {}


def write_spec(name: str, spec: dict) -> str:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    (SCRATCH / f"{name}.json").write_text(json.dumps(spec, indent=2))
    return f"{IN_CONTAINER}/{name}.json"


def rejected(sql: str, expected: str) -> None:
    """Run SQL in a rolled-back transaction and require that it fails with `expected`."""
    rc, out = psql(f"BEGIN; {sql}; ROLLBACK;")
    expect(rc != 0 and expected in out, f"expected rejection containing {expected!r}, got rc={rc}: {out.strip()[:300]}")


# --------------------------------------------------------------------------------- stack & platform

def c_stack() -> str:
    rc, out = compose("ps", "-a", "--format", "json")
    expect(rc == 0, out)
    rows = [json.loads(line) for line in out.splitlines() if line.strip().startswith("{")]
    by = {}
    for row in rows:
        by.setdefault(row["Service"], []).append(row)
    for one_shot in ("bootstrap", "warehouse-setup"):
        expect(one_shot in by and all(r["State"] == "exited" and r.get("ExitCode") == 0 for r in by[one_shot]),
               f"{one_shot} did not exit 0: {by.get(one_shot)}")
    for db in ("control-db", "warehouse-db"):
        expect(by.get(db) and by[db][0].get("Health") == "healthy", f"{db} not healthy")
    expect(by.get("api") and by["api"][0]["State"] == "running", "api not running")
    workers = [r for r in by.get("worker", []) if r["State"] == "running"]
    expect(len(workers) >= 2, f"expected >=2 running worker containers, found {len(workers)}")
    return f"{len(workers)} workers running; databases healthy; one-shot jobs exited 0"


def c_no_dagster() -> str:
    rc, out = sh(["docker", "run", "--rm", "ge-data-control-center:dev", "python", "-c",
                  "import importlib.util as u, subprocess, sys;"
                  "pkgs = subprocess.run([sys.executable,'-m','pip','list'],capture_output=True,text=True).stdout.lower();"
                  "print(bool(u.find_spec('dagster')), pkgs.count('dagster'))"])
    expect(rc == 0 and out.strip().endswith("False 0"), f"dagster present in image: {out.strip()}")
    return "image: dagster not importable, 0 dagster packages installed"


def c_api_health_auth() -> str:
    status, body, _ = api("GET", "/health", token=None)
    expect(status == 200 and body["status"] == "ok", f"/health -> {status} {body}")
    expect(body["latest_migration"] == "006_grant_control_roles.sql", f"latest migration {body['latest_migration']}")
    expect(api("GET", "/steps", token=None)[0] == 401, "no token should be 401")
    expect(api("GET", "/steps", token="wrong")[0] == 401, "wrong token should be 401")
    expect(api("GET", "/steps")[0] == 200, "valid token should be 200")
    return "health ok; 401 without/with wrong token; 200 with token"


def c_workers() -> str:
    rows = q("select worker_name, status, extract(epoch from now()-last_heartbeat_at)::int, code_version "
             "from orchestration.worker where status = 'ACTIVE'")
    expect(len(rows) >= 2, f"expected >=2 ACTIVE workers, got {rows}")
    stale = [r for r in rows if int(r[2]) > 20]
    expect(not stale, f"stale heartbeats: {stale}")
    expect(len({r[3] for r in rows}) == 1, f"workers run different code: {[r[3] for r in rows]}")
    return f"{len(rows)} ACTIVE workers, heartbeats <= {max(int(r[2]) for r in rows)}s old, code {rows[0][3]}"


def c_db_separation() -> str:
    control = {r[0] for r in q("select nspname from pg_namespace where nspname not like 'pg\\_%' "
                               "and nspname <> 'information_schema'")}
    expect(control == {"public", "meta", "registry", "orchestration", "audit"}, f"ge_control schemas: {control}")
    rc, out = psql("select nspname from pg_namespace", db="ge_warehouse", service="warehouse-db", user="ge_wh_admin")
    warehouse = set(out.split())
    expect(rc == 0 and not ({"registry", "orchestration", "audit", "meta"} & warehouse),
           f"control schemas found in ge_warehouse: {warehouse & {'registry', 'orchestration', 'audit', 'meta'}}")
    expect({"core", "ops", "raw_sendem", "stg_trackunit"} <= warehouse, "warehouse layers missing")
    return "ge_control = {audit, meta, orchestration, registry}; ge_warehouse has no control-plane schemas"


def c_migrations() -> str:
    rc, out = gecc("migrate")
    expect(rc == 0, out)
    lines = out.splitlines()
    expect(len(lines) == 6 and all(l.startswith("present") for l in lines), f"migrate output:\n{out}")
    return "6 migrations, all already present (checksums verified)"


def c_registry() -> str:
    rc, out = gecc("registry", "sync")
    expect(rc == 0 and out.count("unchanged") == 4 and "inserted" not in out, f"sync output:\n{out}")
    active = [r[0] for r in q("select step_key from registry.step_definition where is_active order by 1")]
    expect(active == ["core.fail_n_times", "core.noop", "core.sleep", "warehouse.validate_ge_warehouse_baseline"],
           f"active steps {active}")
    return "4 active steps; re-sync unchanged"


# --------------------------------------------------------------------------------- definitions

def c_invalid_specs() -> str:
    before = q1("select count(*) from registry.pipeline")
    rc, out = gecc("pipeline", "validate", "pipelines/examples/invalid/cycle.json")
    expect(rc != 0 and "cycle involving: a, b, c" in out, out)
    rc, out = gecc("pipeline", "validate", "pipelines/examples/invalid/unknown_step_and_bad_params.json")
    for fragment in ("unknown downstream node 'missing'", "unknown step 'core.does_not_exist'",
                     "greater than or equal to 0", "shell: Extra inputs"):
        expect(rc != 0 and fragment in out, f"missing {fragment!r} in:\n{out}")
    rc, out = gecc("pipeline", "publish", "pipelines/examples/invalid/code_injection.json")
    expect(rc != 0 and "code: Extra inputs are not permitted" in out, out)
    expect(q1("select count(*) from registry.pipeline") == before, "an invalid pipeline was written")
    return "cycle, unknown step, bad params, dangling edge and code field all rejected; nothing written"


def c_versioning() -> str:
    key = f"chk_ver_{TAG}"
    base = {"pipeline_key": key, "name": "check: versioning",
            "nodes": [{"key": "a", "step": "core.noop", "params": {"message": "v1"}}], "ui_layout": {"a": {"x": 0, "y": 0}}}
    moved = dict(base, ui_layout={"a": {"x": 500, "y": 80}})
    changed = dict(base, nodes=[{"key": "a", "step": "core.noop", "params": {"message": "v2"}}])
    outputs = [gecc("pipeline", "publish", write_spec(name, spec))[1]
               for name, spec in (("ver_base", base), ("ver_base_again", base), ("ver_moved", moved), ("ver_changed", changed))]
    expect(outputs[0].startswith(f"published: {key} v1"), outputs[0])
    expect(outputs[1].startswith(f"unchanged: {key} v1"), outputs[1])
    expect(outputs[2].startswith(f"unchanged: {key} v1"), f"layout-only change must not version: {outputs[2]}")
    expect(outputs[3].startswith(f"published: {key} v2"), outputs[3])
    expect(outputs[0].split("spec_hash=")[1] != outputs[3].split("spec_hash=")[1], "hash should change")
    runs["versioning_key"] = key
    return f"{key}: v1 -> unchanged -> unchanged (layout) -> v2 (param change)"


def c_immutability() -> str:
    key = runs.get("versioning_key")
    expect(key is not None, "depends on the versioning check")
    vid = q1(f"select pipeline_version_id from registry.pipeline_version v join registry.pipeline p using (pipeline_id) "
             f"where p.pipeline_key = '{key}' and version_no = 1")
    rejected(f"update registry.pipeline_version set spec = '{{}}' where pipeline_version_id = '{vid}'", "immutable")
    rejected(f"delete from registry.pipeline_version where pipeline_version_id = '{vid}'", "immutable")
    rejected(f"update registry.pipeline_node set params = '{{}}' where pipeline_version_id = '{vid}'", "immutable")
    return "UPDATE/DELETE of a published version and its nodes rejected"


def c_audit_append_only() -> str:
    rejected("delete from audit.event where event_id = (select max(event_id) from audit.event)", "append-only")
    rejected("update audit.state_transition set reason = 'x' "
             "where transition_id = (select max(transition_id) from audit.state_transition)", "append-only")
    rejected("truncate audit.event", "append-only")
    return "DELETE, UPDATE and TRUNCATE on audit tables rejected"


def c_least_privilege() -> str:
    probes = [
        ("gecc_worker", "gecc_worker_dev", "select count(*) from orchestration.run", "permission denied"),
        ("gecc_worker", "gecc_worker_dev", "update orchestration.task set priority = 0", "permission denied"),
        ("gecc_worker", "gecc_worker_dev", "select orchestration.create_run('x','MANUAL','x','x')", "permission denied"),
        ("gecc_api", "gecc_api_dev", "update orchestration.task set priority = 0", "permission denied"),
        ("gecc_api", "gecc_api_dev", "delete from audit.event", "permission denied"),
    ]
    for user, password, sql, expected in probes:
        rc, out = psql(f"BEGIN; {sql}; ROLLBACK;", user=user, password=password)
        expect(rc != 0 and expected in out, f"{user}: {sql} -> rc={rc} {out.strip()[:200]}")
    rc, out = psql("select count(*) from registry.step_definition", user="gecc_worker", password="gecc_worker_dev")
    expect(rc == 0, f"worker should read the step registry: {out}")
    return "worker/api logins denied direct table access; worker may only use protocol functions"


# --------------------------------------------------------------------------------- execution

def launch_runs() -> str:
    """Start every run up front so the slow ones overlap; later checks verify them."""
    cancel_key = f"chk_cancel_{TAG}"
    out = gecc("pipeline", "publish", write_spec("cancel", {
        "pipeline_key": cancel_key, "name": "check: cancellation",
        "nodes": [{"key": "long_nap", "step": "core.sleep", "params": {"seconds": 120}}]}))[1]
    expect(out.startswith(("published", "unchanged")), out)
    for name in ("m1_retry", "m1_sleep", "m1_timeout"):
        rc, out = gecc("pipeline", "publish", f"pipelines/examples/{name}.json")
        expect(rc == 0, out)
    runs["sleep_a"] = trigger("m1_sleep", f"checks-{TAG}-sleep-a")
    runs["sleep_b"] = trigger("m1_sleep", f"checks-{TAG}-sleep-b")
    runs["retry"] = trigger("m1_retry", f"checks-{TAG}-retry")
    runs["timeout"] = trigger("m1_timeout", f"checks-{TAG}-timeout")
    runs["cancel"] = trigger(cancel_key, f"checks-{TAG}-cancel")
    return "triggered: 2x m1_sleep, m1_retry, m1_timeout, chk_cancel"


def c_idempotent_trigger_and_demo_run() -> str:
    body = {"pipeline_key": "m1_demo", "idempotency_key": f"checks-{TAG}-demo"}
    s1, p1, h1 = api("POST", "/runs", body, headers={"X-Request-ID": f"checks-{TAG}"})
    s2, p2, _ = api("POST", "/runs", body)
    expect((s1, s2) == (201, 200) and p1["run_id"] == p2["run_id"], f"{s1}/{s2} {p1} {p2}")
    expect(h1.get("x-request-id") == f"checks-{TAG}", "request id not echoed")
    run_id = p1["run_id"]
    runs["demo"] = run_id
    detail = wait_run(run_id, 90)
    expect(detail["state"] == "SUCCEEDED", f"demo run {detail['state']}")
    done = {t["node_key"]: t["finished_at"] for t in detail["tasks"]}
    expect(done["extract"] < done["flaky"] < done["validate_warehouse"] < done["report"], f"order wrong: {done}")
    flaky = [(a["no"], a["state"]) for a in attempts(run_id) if a["node"] == "flaky"]
    expect(flaky == [(1, "FAILED"), (2, "SUCCEEDED")], f"flaky attempts {flaky}")
    validate = next(t for t in detail["tasks"] if t["node_key"] == "validate_warehouse")["attempts"][-1]["result"]
    expect(validate["checks"] == 15 and validate["failed"] == 0, f"validation result {validate}")
    _, logs, _ = api("GET", f"/runs/{run_id}/logs?node_key=validate_warehouse")
    expect(sum(" PASS " in l["line"] or l["line"].startswith("INFO PASS") for l in logs) == 15, "15 PASS log lines")
    unattributed = q1(f"select count(*) from audit.state_transition where run_id = '{run_id}' and coalesce(actor,'') = ''")
    expect(unattributed == "0", "transitions without actor")
    events = [r[0] for r in q(f"select action from audit.event where entity_id = '{run_id}' order by event_id")]
    expect(events == ["run.created", "run.create_deduplicated"], f"audit events {events}")
    return f"201 then 200 (same run); DAG order kept; flaky FAILED->SUCCEEDED; 15/15 warehouse checks; run {run_id}"


def c_retry_backoff() -> str:
    detail = wait_run(runs["retry"], 90)
    expect(detail["state"] == "SUCCEEDED", f"run {detail['state']}")
    att = attempts(runs["retry"])
    expect([(a["no"], a["state"]) for a in att] == [(1, "FAILED"), (2, "FAILED"), (3, "SUCCEEDED")], f"{att}")
    gaps = [att[i]["started"] - att[i - 1]["finished"] for i in (1, 2)]
    # backoff 3 s then 6 s, each with -20% jitter at worst
    expect(gaps[0] >= 2.4 and gaps[1] >= 4.8, f"backoff too short: {gaps}")
    return f"FAILED, FAILED, SUCCEEDED; waits {gaps[0]:.1f}s then {gaps[1]:.1f}s (exponential)"


def c_timeout() -> str:
    detail = wait_run(runs["timeout"], 120)
    att = [(a["state"], a["error"]) for a in attempts(runs["timeout"])]
    expect(detail["state"] == "FAILED" and att == [("TIMED_OUT", "Timeout")] * 2, f"{detail['state']} {att}")
    return "2 attempts TIMED_OUT after 10 s each; task FAILED; run FAILED"


def c_cancel_running() -> str:
    run_id = runs["cancel"]
    wait_until(lambda: run_detail(run_id)["tasks"][0]["state"] == "RUNNING", 60, "cancel-check task RUNNING")
    time.sleep(2)
    requested = time.monotonic()
    status, body, _ = api("POST", f"/runs/{run_id}/cancel", {"reason": "automated safe check"})
    expect(status == 200 and body["state"] == "CANCELLING", f"cancel -> {status} {body}")
    detail = wait_run(run_id, 45)
    took = time.monotonic() - requested
    att = attempts(run_id)
    expect(detail["state"] == "CANCELLED" and [a["state"] for a in att] == ["CANCELLED"], f"{detail['state']} {att}")
    _, logs, _ = api("GET", f"/runs/{run_id}/logs")
    expect(any("cancellation requested" in l["line"] for l in logs), "no cancellation log line")
    return f"CANCELLING -> CANCELLED {took:.1f}s after the request (next lease renewal + SIGTERM); child terminated"


def c_concurrency_key() -> str:
    a, b = runs["sleep_a"], runs["sleep_b"]
    for run_id in (a, b):
        expect(wait_run(run_id, 240)["state"] == "SUCCEEDED", f"sleep run {run_id} did not succeed")
    overlap = max_overlap([a, b])
    expect(overlap == 1, f"max simultaneous attempts on demo:exclusive = {overlap}")
    first, second = sorted((attempts(a)[-1], attempts(b)[-1]), key=lambda x: x["started"])
    expect(second["started"] >= first["finished"] - 0.5, "second sleep started before the first finished")
    return f"both SUCCEEDED; never more than 1 attempt RUNNING; 2nd started {second['started'] - first['finished']:.1f}s after 1st ended"


def main() -> int:
    print(f"GE Data Control Center -- SAFE M1 checks (tag {TAG}, actor {ACTOR})\n")
    preflight()
    runner = Runner()
    try:
        runner.run("S01", "stack services up / healthy / one-shots exited 0", c_stack)
        runner.run("S02", "Control Center image contains no Dagster", c_no_dagster)
        runner.run("S03", "API health and bearer-token auth", c_api_health_auth)
        runner.run("S04", "both workers ACTIVE with fresh heartbeats", c_workers)
        runner.run("S05", "control plane and warehouse are separate DBs", c_db_separation)
        runner.run("S06", "migrations idempotent (checksums verified)", c_migrations)
        runner.run("S07", "step registry in sync", c_registry)
        runner.run("S08", "invalid specs and code injection rejected", c_invalid_specs)
        runner.run("S09", "pipeline versioning (hash, layout, params)", c_versioning)
        runner.run("S10", "published versions immutable", c_immutability)
        runner.run("S11", "audit tables append-only", c_audit_append_only)
        runner.run("S12", "least-privilege database logins", c_least_privilege)
        runner.run("S13", "launch execution checks (runs overlap)", launch_runs)
        runner.run("S14", "idempotent trigger + demo DAG, retry, logs, audit", c_idempotent_trigger_and_demo_run)
        runner.run("S15", "retries with exponential backoff", c_retry_backoff)
        runner.run("S16", "attempt timeouts", c_timeout)
        runner.run("S17", "cancellation of a running step", c_cancel_running)
        runner.run("S18", "concurrency key serialises across workers", c_concurrency_key)
    finally:
        shutil.rmtree(SCRATCH, ignore_errors=True)
    print("\n  runs created (explore with: gecc run show <id>):")
    for name, value in runs.items():
        if name != "versioning_key":
            print(f"    {name:<9} {value}")
    return runner.summary()


if __name__ == "__main__":
    raise SystemExit(main())
