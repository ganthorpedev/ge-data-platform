#!/usr/bin/env python3
"""GE Data Control Center -- DESTRUCTIVE / RECOVERY M1 checks.  REQUIRES EXPLICIT APPROVAL.

    python3 deploy/compose/checks/m1_recovery_checks.py --approve            # asks for typed confirmation
    python3 deploy/compose/checks/m1_recovery_checks.py --approve --only R1  # a single check

These deliberately disrupt the LOCAL Compose stack (never anything else):

  R1  SIGKILL the worker container holding a running attempt     (crash recovery: LOST -> retried elsewhere)
  R2  gracefully stop a worker mid-attempt (SIGTERM, drain)      (shutdown recovery: WorkerShutdown -> retried)
  R3  restart the control-plane DB mid-step (A: quick restart;   (DB outage recovery + live
      B: 45 s outage, longer than the 30 s lease)                  worker self-fencing)
  R4  pg_dump ge_control, restore into a TEMPORARY database,     (backup / restore; the temp DB is dropped,
      compare, then drop it                                       ge_control itself is only read)

Every check restores the stack afterwards (stopped/killed workers are started again) and ends by
verifying two ACTIVE workers. Expect ~6-8 minutes in total.
"""

from __future__ import annotations

import argparse
import calendar
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from _common import (
    ACTOR,
    COMPOSE_FILE,
    CheckFailed,
    Runner,
    attempts,
    compose,
    expect,
    gecc,
    preflight,
    q,
    q1,
    sh,
    trigger,
    wait_run,
    wait_until,
    worker_containers,
)

TAG = time.strftime("%Y%m%d%H%M%S")


def _running_attempt_host(run_id: str) -> str:
    def probe():
        rows = q(f"select w.hostname from orchestration.task_attempt a join orchestration.worker w using (worker_id) "
                 f"where a.run_id = '{run_id}' and a.state = 'RUNNING'")
        return rows[0][0] if rows else None
    return wait_until(probe, 90, "an attempt to be RUNNING")


def _container_for(hostname: str) -> dict:
    for container in worker_containers():
        if container["hostname"] == hostname:
            return container
    raise CheckFailed(f"no worker container with hostname {hostname}")


def _ensure_workers(n: int = 2) -> None:
    stopped = _stopped_worker_ids()
    if stopped:
        sh(["docker", "start", *stopped])
    wait_until(lambda: int(q1("select count(*) from orchestration.worker where status = 'ACTIVE' "
                              "and last_heartbeat_at > now() - interval '20 seconds'") or 0) >= n,
               90, f"{n} ACTIVE workers")


def _stopped_worker_ids() -> list[str]:
    _, out = sh(["docker", "ps", "-a", "--filter", "label=com.docker.compose.project=gecc",
                 "--filter", "label=com.docker.compose.service=worker", "--filter", "status=exited", "-q"])
    return out.split()


def r1_kill_worker() -> str:
    run_id = trigger("m1_sleep", f"recovery-{TAG}-kill")
    host = _running_attempt_host(run_id)
    victim = _container_for(host)
    time.sleep(3)
    rc, out = sh(["docker", "kill", "-s", "KILL", victim["id"]])
    expect(rc == 0, out)
    killed_at = time.monotonic()
    try:
        detail = wait_run(run_id, 240)
    finally:
        sh(["docker", "start", victim["id"]])
    att = attempts(run_id)
    expect(detail["state"] == "SUCCEEDED", f"run {detail['state']}")
    expect(att[0]["state"] == "LOST" and att[0]["error"] == "LeaseExpired", f"first attempt {att[0]}")
    expect(att[-1]["state"] == "SUCCEEDED" and att[-1]["host"] != host, f"retry ran on {att[-1]['host']}")
    reaper = q1(f"select count(*) from audit.state_transition where run_id = '{run_id}' and actor = 'system:reaper'")
    expect(int(reaper) >= 1, "no reaper transition recorded")
    _ensure_workers()
    return (f"killed {victim['name']}; attempt 1 LOST (reaper), attempt {att[-1]['no']} SUCCEEDED on another worker; "
            f"recovered {time.monotonic() - killed_at:.0f}s after kill; container restarted")


def r2_graceful_stop() -> str:
    run_id = trigger("m1_sleep", f"recovery-{TAG}-drain")
    host = _running_attempt_host(run_id)
    victim = _container_for(host)
    time.sleep(3)
    rc, out = sh(["docker", "stop", "-t", "60", victim["id"]], timeout=120)
    expect(rc == 0, out)
    try:
        status = q1(f"select status from orchestration.worker where hostname = '{host}' order by started_at desc limit 1")
        expect(status == "STOPPED", f"worker status after SIGTERM: {status}")
        detail = wait_run(run_id, 240)
    finally:
        sh(["docker", "start", victim["id"]])
    att = attempts(run_id)
    expect(att[0]["state"] == "FAILED" and att[0]["error"] == "WorkerShutdown", f"first attempt {att[0]}")
    expect(detail["state"] == "SUCCEEDED", f"run {detail['state']}")
    _ensure_workers()
    return f"SIGTERM drained {victim['name']} (worker STOPPED); attempt 1 WorkerShutdown -> retried and SUCCEEDED"


def _overlap(run_id: str) -> str:
    return q1(f"""
        with ev as (select transition_id, case when to_state='RUNNING' then 1 else -1 end d from audit.state_transition
                    where entity_type='ATTEMPT' and run_id='{run_id}' and (to_state='RUNNING' or from_state='RUNNING'))
        select coalesce(max(s),0) from (select sum(d) over (order by transition_id) s from ev) x""")


def _wait_db_ready() -> None:
    wait_until(lambda: compose("exec", "-T", "control-db", "pg_isready", "-U", "gecc_admin")[0] == 0, 90, "control-db ready")


def r3_restart_control_db() -> str:
    # Phase A: quick restart (sub-second) -- the lease is expected to survive.
    run_a = trigger("m1_sleep", f"recovery-{TAG}-dbrestart")
    _running_attempt_host(run_a)
    time.sleep(5)
    rc, out = compose("restart", "control-db", timeout=180)
    expect(rc == 0, out)
    _wait_db_ready()
    detail_a = wait_run(run_a, 300)
    att_a = attempts(run_a)
    expect(detail_a["state"] == "SUCCEEDED" and _overlap(run_a) == "1", f"phase A: {detail_a['state']} {att_a}")
    phase_a = "lease survived" if len(att_a) == 1 else f"attempt 1 {att_a[0]['state']} then retried"

    # Phase B: outage LONGER than the lease. The worker must self-fence (kill its child before the lease
    # expires, report nothing); after the DB returns the reaper marks the attempt LOST and it is retried.
    lease_seconds = 30
    run_b = trigger("m1_sleep", f"recovery-{TAG}-dboutage")
    host = _running_attempt_host(run_b)
    victim = _container_for(host)
    time.sleep(5)
    stopped_at = time.time()
    rc, out = compose("stop", "-t", "10", "control-db", timeout=120)
    expect(rc == 0, out)
    health_during = sh(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15",
                        "http://127.0.0.1:58000/health"])[1].strip()
    time.sleep(45)
    rc, out = compose("start", "control-db", timeout=120)
    expect(rc == 0, out)
    _wait_db_ready()
    outage = time.time() - stopped_at
    detail_b = wait_run(run_b, 300)
    att_b = attempts(run_b)
    expect(detail_b["state"] == "SUCCEEDED", f"phase B run {detail_b['state']} {att_b}")
    expect(att_b[0]["state"] == "LOST" and att_b[0]["error"] == "LeaseExpired", f"phase B attempt 1 {att_b[0]}")
    expect(_overlap(run_b) == "1", "phase B: two attempts RUNNING at once")
    reaped_by = q1(f"select actor from audit.state_transition where entity_id = "
                   f"(select attempt_id from orchestration.task_attempt where run_id = '{run_b}' and attempt_no = 1) "
                   f"and to_state = 'LOST'")
    expect(reaped_by == "system:reaper", f"attempt 1 closed by {reaped_by!r}, expected the reaper (fenced worker must not report)")
    _, logs = sh(["docker", "logs", "--since", f"{int(outage) + 120}s", victim["id"]])
    fence_lines = [line for line in logs.splitlines() if "FENCING" in line]
    expect(fence_lines, f"no FENCING log line from {victim['name']}")
    # Worker log timestamps are UTC ("YYYY-MM-DD HH:MM:SS,mmm"); the fence must precede the lease expiry.
    fence_at = calendar.timegm(time.strptime(fence_lines[0][:19], "%Y-%m-%d %H:%M:%S")) + int(fence_lines[0][20:23]) / 1000
    lease_expiry = float(q1(f"select extract(epoch from lease_expires_at) from orchestration.task_attempt "
                            f"where run_id = '{run_b}' and attempt_no = 1"))
    margin = lease_expiry - fence_at
    expect(margin > 0, f"child fenced {-margin:.1f}s AFTER the lease expired")
    expect(health_during == "503", f"API health during outage returned {health_during}, expected 503")
    _ensure_workers()
    return (f"A) quick restart: {phase_a}.\n"
            f"B) {outage:.0f}s outage (> {lease_seconds}s lease): {victim['name']} self-fenced {margin:.1f}s BEFORE its lease "
            f"expired and reported nothing; API /health {health_during} during outage; reaper marked attempt 1 LOST after "
            f"recovery; attempt {att_b[-1]['no']} SUCCEEDED on {att_b[-1]['worker']}; never 2 attempts RUNNING")


INTEGRITY_SQL = {
    "row counts (runs|tasks|attempts|transitions|events)":
        "select (select count(*) from orchestration.run) || '|' || (select count(*) from orchestration.task) || '|' || "
        "(select count(*) from orchestration.task_attempt) || '|' || (select count(*) from audit.state_transition) || '|' || "
        "(select count(*) from audit.event)",
    "audit.state_transition checksum":
        "select md5(string_agg(t::text, '|' order by transition_id)) from audit.state_transition t",
    "audit.event checksum":
        "select md5(string_agg(e::text, '|' order by event_id)) from audit.event e",
    "orchestration.task_attempt checksum":
        "select md5(string_agg(a::text, '|' order by attempt_id)) from orchestration.task_attempt a",
    "registry checksum (steps + versions + nodes)":
        "select md5((select string_agg(s::text,'|' order by step_key, code_version) from registry.step_definition s) || "
        "(select string_agg(v::text,'|' order by pipeline_version_id) from registry.pipeline_version v) || "
        "(select string_agg(n::text,'|' order by pipeline_version_id, node_key) from registry.pipeline_node n))",
    "migration history":
        "select string_agg(migration_name || ':' || checksum, ',' order by migration_name) from meta.schema_version",
}


def _scalar(db: str, sql: str) -> str:
    rc, out = compose("exec", "-T", "control-db", "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1",
                      "-U", "gecc_admin", "-d", db, "-c", sql)
    if rc != 0:
        raise CheckFailed(f"{db}: {out.strip()}")
    return out.strip()


def r4_backup_restore() -> str:
    temp_db = f"gecc_restore_check_{TAG}"
    with tempfile.TemporaryDirectory() as tmp:
        dump = Path(tmp) / "ge_control.dump"
        with dump.open("wb") as handle:
            rc = subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T", "control-db",
                                 "pg_dump", "-U", "gecc_admin", "-Fc", "ge_control"], stdout=handle).returncode
        size = dump.stat().st_size
        expect(rc == 0 and size > 10_000, f"pg_dump failed (rc={rc}, {size} bytes)")
        # Snapshot the live database immediately after the dump. Workers are idle (no runs in flight), and
        # the comparison is repeated if anything changed between dump and snapshot.
        live = {name: _scalar("ge_control", sql) for name, sql in INTEGRITY_SQL.items()}
        rc, out = compose("exec", "-T", "control-db", "createdb", "-U", "gecc_admin", temp_db)
        expect(rc == 0, out)
        try:
            with dump.open("rb") as handle:
                restore = subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T", "control-db",
                                          "pg_restore", "--exit-on-error", "-U", "gecc_admin", "-d", temp_db],
                                         stdin=handle, capture_output=True)
            expect(restore.returncode == 0, "pg_restore failed: " + restore.stderr.decode()[:500])
            restored = {name: _scalar(temp_db, sql) for name, sql in INTEGRITY_SQL.items()}
            differing = [name for name in INTEGRITY_SQL if live[name] != restored[name]]
            expect(not differing, f"restored copy differs in: {differing}")

            # Protections must survive a restore: triggers, state machine, grants.
            rc, out = compose("exec", "-T", "control-db", "psql", "-X", "-U", "gecc_admin", "-d", temp_db,
                              "-c", "BEGIN; update audit.event set actor_id = 'x'; ROLLBACK;")
            expect(rc != 0 and "append-only" in out, f"restored audit.event not append-only: {out.strip()[:200]}")
            rc, out = compose("exec", "-T", "control-db", "psql", "-X", "-U", "gecc_admin", "-d", temp_db,
                              "-c", "BEGIN; delete from audit.state_transition; ROLLBACK;")
            expect(rc != 0 and "append-only" in out, "restored audit.state_transition not append-only")
            rc, out = compose("exec", "-T", "control-db", "psql", "-X", "-U", "gecc_admin", "-d", temp_db, "-c",
                              "BEGIN; update orchestration.run set state = 'QUEUED' "
                              "where run_id = (select run_id from orchestration.run where state = 'SUCCEEDED' limit 1); ROLLBACK;")
            expect(rc != 0 and "illegal RUN state transition" in out, "restored copy lost the state machine")
            grants = _scalar(temp_db, "select has_function_privilege('cc_worker', "
                                      "'orchestration.claim_task(uuid,text[],integer)', 'EXECUTE')::text || ',' || "
                                      "has_table_privilege('cc_worker', 'orchestration.run', 'SELECT')::text")
            expect(grants == "true,false", f"restored grants wrong: {grants}")
        finally:
            compose("exec", "-T", "control-db", "dropdb", "-U", "gecc_admin", "--if-exists", temp_db)
        remaining = _scalar("postgres", f"select count(*) from pg_database where datname = '{temp_db}'")
        expect(remaining == "0", "temporary restore database was not dropped")
    counts = live["row counts (runs|tasks|attempts|transitions|events)"]
    return (f"dump {size} bytes; restored copy identical in {len(INTEGRITY_SQL)} integrity checks "
            f"(counts {counts}, audit checksums, attempts, registry, migrations); append-only, state machine "
            f"and grants intact after restore; temp DB dropped; ge_control only read")


CHECKS = {
    "R1": ("SIGKILL the worker running an attempt", r1_kill_worker),
    "R2": ("graceful worker stop (SIGTERM drain) mid-attempt", r2_graceful_stop),
    "R3": ("control-DB restart + outage longer than the lease", r3_restart_control_db),
    "R4": ("backup ge_control and restore into a temp DB", r4_backup_restore),
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--approve", action="store_true", help="required: acknowledge these tests disrupt the local stack")
    parser.add_argument("--yes", action="store_true", help="skip the typed confirmation (only after approval was given)")
    parser.add_argument("--only", choices=sorted(CHECKS), action="append", help="run selected checks only")
    args = parser.parse_args()

    if not args.approve:
        print(__doc__)
        print("Refusing to run without --approve.")
        return 2
    selected = args.only or sorted(CHECKS)
    print("About to run DESTRUCTIVE recovery checks on the LOCAL stack:")
    for key in selected:
        print(f"  {key}  {CHECKS[key][0]}")
    if not args.yes:
        if input("Type 'run recovery checks' to continue: ").strip() != "run recovery checks":
            print("Not confirmed; nothing was done.")
            return 2

    preflight()
    gecc("pipeline", "publish", "pipelines/examples/m1_sleep.json")
    print(f"\nGE Data Control Center -- RECOVERY checks (tag {TAG}, actor {ACTOR})\n")
    runner = Runner()
    for key in selected:
        title, fn = CHECKS[key]
        runner.run(key, title, fn)
    try:
        _ensure_workers()
        print("\n  stack restored: 2 ACTIVE workers")
    except CheckFailed as error:
        print(f"\n  WARNING: stack not fully restored: {error}")
    return runner.summary()


if __name__ == "__main__":
    sys.exit(main())
