"""Shared helpers for the M1 check scripts (standard library only).

Everything talks to the LOCAL Compose stack: `docker compose` for containers,
`psql` inside the control-db / warehouse-db containers, the API on
127.0.0.1:58000, and the `gecc` CLI inside the Control Center image.
"""

from __future__ import annotations

import getpass
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO / "deploy" / "compose" / "docker-compose.yml"
API = os.environ.get("GECC_CHECK_API", "http://127.0.0.1:58000")
TOKEN = os.environ.get("GECC_API_TOKEN", "gecc-local-dev-token")
ACTOR = f"checks:{getpass.getuser()}"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


class CheckFailed(AssertionError):
    pass


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailed(message)


def sh(args: list[str], *, input_text: str | None = None, timeout: int = 300) -> tuple[int, str]:
    proc = subprocess.run(args, input=input_text, capture_output=True, text=True, timeout=timeout, cwd=REPO)
    return proc.returncode, (proc.stdout + proc.stderr)


def compose(*args: str, timeout: int = 300) -> tuple[int, str]:
    return sh(["docker", "compose", "-f", str(COMPOSE_FILE), *args], timeout=timeout)


def gecc(*args: str, timeout: int = 300) -> tuple[int, str]:
    """Run the gecc CLI inside the Control Center image (admin login), attributed to ACTOR."""
    rc, out = compose("run", "--rm", "-T", "-e", f"GECC_ACTOR={ACTOR}",
                      "-v", f"{REPO / 'pipelines'}:/app/pipelines:ro", "bootstrap", "gecc", *args, timeout=timeout)
    lines = [l for l in out.splitlines()
             if " INFO " not in l and not l.strip().startswith("Container ") and l.strip()]
    return rc, "\n".join(lines)


def psql(sql: str, *, db: str = "ge_control", service: str = "control-db", user: str = "gecc_admin",
         password: str | None = None) -> tuple[int, str]:
    """Run SQL via psql inside a database container. Returns (rc, output); -At, '|' separated."""
    env = ["-e", f"PGPASSWORD={password}"] if password else []
    host = ["-h", "localhost"] if password else []
    return compose("exec", "-T", *env, service, "psql", "-X", "-At", "-F", "|", "-v", "ON_ERROR_STOP=1",
                   "-U", user, *host, "-d", db, "-c", sql)


def q(sql: str) -> list[list[str]]:
    rc, out = psql(sql)
    if rc != 0:
        raise CheckFailed(f"query failed: {out.strip()}")
    return [line.split("|") for line in out.splitlines() if line.strip()]


def q1(sql: str) -> str:
    rows = q(sql)
    return rows[0][0] if rows else ""


def api(method: str, path: str, body: dict | None = None, *, token: str | None = TOKEN,
        headers: dict | None = None) -> tuple[int, object, dict]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(API + path, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None), dict(response.headers)
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = raw.decode(errors="replace")
        return error.code, payload, dict(error.headers)


def trigger(pipeline_key: str, idempotency_key: str) -> str:
    status, payload, _ = api("POST", "/runs", {"pipeline_key": pipeline_key, "idempotency_key": idempotency_key})
    expect(status in (200, 201), f"trigger {pipeline_key} -> HTTP {status}: {payload}")
    return payload["run_id"]


def run_detail(run_id: str) -> dict:
    status, payload, _ = api("GET", f"/runs/{run_id}")
    expect(status == 200, f"GET /runs/{run_id} -> HTTP {status}")
    return payload


def wait_run(run_id: str, timeout: float, states: set[str] = TERMINAL) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        detail = run_detail(run_id)
        if detail["state"] in states:
            return detail
        if time.monotonic() > deadline:
            raise CheckFailed(f"run {run_id} still {detail['state']} after {timeout:.0f}s")
        time.sleep(1)


def wait_until(predicate, timeout: float, what: str, interval: float = 1.0):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise CheckFailed(f"timed out after {timeout:.0f}s waiting for: {what}")
        time.sleep(interval)


def attempts(run_id: str) -> list[dict]:
    rows = q(f"""
        select t.node_key, a.attempt_no, a.state, coalesce(a.error_class,''), w.worker_name, w.hostname,
               extract(epoch from a.started_at), coalesce(extract(epoch from a.finished_at)::text,'')
        from orchestration.task_attempt a
        join orchestration.task t using (task_id)
        join orchestration.worker w using (worker_id)
        where a.run_id = '{run_id}' order by a.started_at""")
    return [{"node": r[0], "no": int(r[1]), "state": r[2], "error": r[3], "worker": r[4], "host": r[5],
             "started": float(r[6]), "finished": float(r[7]) if r[7] else None} for r in rows]


def max_overlap(run_ids: list[str]) -> int:
    """Maximum number of simultaneously RUNNING attempts across runs, from the audit trail."""
    ids = ",".join(f"'{r}'" for r in run_ids)
    return int(q1(f"""
        with ev as (
            select transition_id, case when to_state = 'RUNNING' then 1 else -1 end as d
            from audit.state_transition
            where entity_type = 'ATTEMPT' and run_id in ({ids})
              and (to_state = 'RUNNING' or from_state = 'RUNNING'))
        select coalesce(max(s), 0) from (select sum(d) over (order by transition_id) as s from ev) x""") or 0)


def worker_containers() -> list[dict]:
    rc, out = sh(["docker", "ps", "--filter", "label=com.docker.compose.service=worker",
                  "--filter", "label=com.docker.compose.project=gecc", "--format", "{{.ID}}|{{.Names}}"])
    containers = []
    for line in out.splitlines():
        if "|" in line:
            cid, name = line.split("|", 1)
            _, hostname = sh(["docker", "inspect", "-f", "{{.Config.Hostname}}", cid])
            containers.append({"id": cid, "name": name, "hostname": hostname.strip()})
    return containers


@dataclass
class Result:
    check_id: str
    title: str
    status: str
    seconds: float
    detail: str = ""


@dataclass
class Runner:
    results: list[Result] = field(default_factory=list)

    def run(self, check_id: str, title: str, fn) -> None:
        interactive = sys.stdout.isatty()
        if interactive:
            print(f"  {check_id}  {title} ...", end="", flush=True)
        started = time.monotonic()
        try:
            detail = fn() or ""
            status = "PASS"
        except CheckFailed as error:
            detail, status = str(error), "FAIL"
        except Exception as error:  # noqa: BLE001
            detail, status = f"{type(error).__name__}: {error}", "ERROR"
        elapsed = time.monotonic() - started
        self.results.append(Result(check_id, title, status, elapsed, detail))
        print(f"{chr(13) if interactive else ''}  {check_id}  {title:<62} {status:<5} {elapsed:5.1f}s")
        if detail:
            for line in str(detail).splitlines():
                print(f"        {line}")

    def summary(self) -> int:
        failed = [r for r in self.results if r.status != "PASS"]
        print()
        print(f"  {len(self.results) - len(failed)}/{len(self.results)} checks passed")
        for r in failed:
            print(f"  {r.status}: {r.check_id} {r.title}")
        return 1 if failed else 0


def preflight() -> None:
    rc, out = compose("ps", "--status", "running", "--services")
    services = set(out.split())
    missing = {"control-db", "warehouse-db", "api", "worker"} - services
    if rc != 0 or missing:
        sys.exit(f"The local stack is not running (missing: {', '.join(sorted(missing)) or 'docker'}). Start it with:\n"
                 f"  docker compose -f {COMPOSE_FILE.relative_to(REPO)} up -d --build --scale worker=2")
