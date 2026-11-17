# M1 manual test checklist

Every check runs against the **local** Docker Compose stack only. Nothing here
touches production, the legacy `telemetry_warehouse`, or a PostgreSQL on port
5432. Tick each box and note anything unexpected.

## Setup (run once per shell)

```sh
cd ge-data-platform
export C=deploy/compose/docker-compose.yml
alias gecc='docker compose -f $C run --rm -T -e GECC_ACTOR="cli:$USER" -v "$PWD/pipelines:/app/pipelines:ro" bootstrap gecc'
alias cpsql='docker compose -f $C exec -T control-db psql -U gecc_admin -d ge_control'
alias wpsql='docker compose -f $C exec -T warehouse-db psql -U ge_wh_admin -d ge_warehouse'
export API=http://127.0.0.1:58000 AUTH="Authorization: Bearer gecc-local-dev-token"
```

`gecc` runs inside the Control Center image as the admin login (the
`bootstrap` service), so no local Python install is needed. `GECC_ACTOR`
makes your actions appear in the audit trail as `cli:<you>`.

## Automated checks

Most of this checklist is automated (standard-library Python + Docker only):

| Script | What it does | Approval |
|---|---|---|
| `python3 deploy/compose/checks/m1_safe_checks.py` | S01–S18: MT-0…MT-8, MT-10–MT-12 plus least-privilege and append-only probes. Creates only new, check-named pipelines/runs; forbidden-write probes run inside `BEGIN … ROLLBACK`; never stops, restarts or deletes anything. ~2.5 min | not needed |
| `python3 deploy/compose/checks/m1_recovery_checks.py --approve` | R1 worker SIGKILL (MT-9), R2 graceful worker drain, R3 control-db quick restart + 45 s outage longer than the lease, proving worker self-fencing (MT-13), R4 backup → restore into a temp DB (MT-14). Disrupts the local stack, then restores it. ~6–8 min | **`--approve` + typed confirmation** |
| `python3 deploy/compose/checks/m1_integrity.py control` / `snapshot FILE` / `compare FILE` | Read-only: control-plane invariants over all runs (no duplicate claims, legal & contiguous transitions, no leaked slots, consistent states) and a warehouse/volume fingerprint to prove nothing was written | not needed |

The manual steps below remain the reference for what each check means. Latest results: [`m1-recovery-test-report.md`](m1-recovery-test-report.md).

---

### MT-0 — Stack starts; image has no Dagster
```sh
docker compose -f $C down -v          # start clean: removes only this stack's two local volumes
docker compose -f $C up -d --build --scale worker=2
docker compose -f $C ps -a --format '{{.Service}}\t{{.Status}}'
docker run --rm ge-data-control-center:dev sh -c 'pip list 2>/dev/null | grep -ci dagster'
curl -s $API/health
```
- [ ] `bootstrap` and `warehouse-setup` show **Exited (0)**; `api` and two `worker` rows are **Up**
- [ ] the `grep -ci dagster` count is **0**
- [ ] health is `{"status":"ok", … "latest_migration":"006_grant_control_roles.sql"}`

### MT-1 — Control plane and warehouse are separate databases
```sh
cpsql -c '\dn'
wpsql -c '\dn'
```
- [ ] `ge_control` has only `audit`, `meta`, `orchestration`, `public`, `registry`
- [ ] `ge_warehouse` has `raw_*`, `stg_*`, `core`, `mart_*`, `ops` — and **no** `registry`/`orchestration`/`audit`

### MT-2 — Migrations are idempotent
```sh
gecc migrate
cpsql -c 'select migration_name, left(checksum,12), applied_at from meta.schema_version order by 1'
```
- [ ] every line says `present` (nothing re-applied); 6 rows with checksums

### MT-3 — Step registry sync
```sh
gecc registry sync
gecc registry list
cpsql -c "select action, details->'inserted' from audit.event where action = 'registry.synced'"
```
- [ ] sync prints `unchanged` for all 4 steps (the first sync happened in bootstrap)
- [ ] list shows `core.fail_n_times`, `core.noop`, `core.sleep`, `warehouse.validate_ge_warehouse_baseline`
- [ ] exactly **one** `registry.synced` event (unchanged syncs are not audited as changes)

### MT-4 — Publishing, versioning, immutability
```sh
gecc pipeline publish pipelines/examples/m1_demo.json        # 1st
gecc pipeline publish pipelines/examples/m1_demo.json        # 2nd, identical
mkdir -p pipelines/scratch
sed 's/"x": 660/"x": 999/' pipelines/examples/m1_demo.json > pipelines/scratch/m1_demo_layout.json
gecc pipeline publish pipelines/scratch/m1_demo_layout.json  # node moved on the canvas only
sed 's/all done/all done (v2)/' pipelines/examples/m1_demo.json > pipelines/scratch/m1_demo_v2.json
gecc pipeline publish pipelines/scratch/m1_demo_v2.json      # a step parameter changed
cpsql -c "update registry.pipeline_version set spec = '{}' where version_no = 1"
cpsql -c "delete from registry.pipeline_node"
```
- [ ] 1st: `published: m1_demo v1`; 2nd: `unchanged: m1_demo v1`
- [ ] layout-only change: `unchanged: m1_demo v1` (layout is cosmetic and is not versioned)
- [ ] parameter change: `published: m1_demo v2` with a **different** spec_hash
- [ ] both SQL statements fail with `… is immutable …`

### MT-5 — Invalid pipelines are rejected; no code can be attached
```sh
gecc pipeline validate pipelines/examples/invalid/cycle.json
gecc pipeline validate pipelines/examples/invalid/unknown_step_and_bad_params.json
gecc pipeline publish  pipelines/examples/invalid/code_injection.json
cpsql -c "select count(*) from registry.pipeline where pipeline_key like 'bad_%'"
```
- [ ] cycle: `the graph has a cycle involving: a, b, c`
- [ ] second file lists **all four** problems (dangling edge, unknown step, negative seconds, extra `shell` param)
- [ ] code injection: `nodes.0.code: Extra inputs are not permitted`
- [ ] count is **0** — nothing was written

### MT-6 — Run creation is idempotent (API)
```sh
curl -s -w ' HTTP %{http_code}\n' -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"pipeline_key":"m1_demo","idempotency_key":"mt6-demo"}' $API/runs
# run the same command again
curl -s -o /dev/null -w '%{http_code}\n' $API/runs/00000000-0000-0000-0000-000000000000   # no token
```
- [ ] first call **HTTP 201**, second **HTTP 200** with the **same** `run_id`
- [ ] request without a token → **401**

### MT-7 — DAG execution order, retry, read-only warehouse step, audit trail
```sh
RUN=<run_id from MT-6>
gecc run wait $RUN
gecc run logs $RUN --node validate_warehouse
cpsql -c "select entity_type, from_state, to_state, actor, reason from audit.state_transition where run_id = '$RUN' order by transition_id"
```
- [ ] run **SUCCEEDED**; tasks ran extract → flaky → validate_warehouse → report
- [ ] `flaky` shows `#1 FAILED RuntimeError` then `#2 SUCCEEDED`
- [ ] validate_warehouse logs 15 `PASS` lines
- [ ] every transition has an actor (`api:dev-token`, `worker:<uuid>`) and a reason

### MT-8 — Retries with exponential backoff
```sh
gecc run trigger m1_retry --idempotency-key mt8-retry      # note run_id
gecc run wait <run_id>
cpsql -c "select attempt_no, state, started_at, finished_at from orchestration.task_attempt where run_id = '<run_id>' order by 1"
```
- [ ] attempts 1 and 2 **FAILED**, attempt 3 **SUCCEEDED**
- [ ] the gap before attempt 2 is ~3 s and before attempt 3 is ~6 s (±20 % jitter, plus up to 2 s polling)

### MT-9 — Worker crash (kill -9) is recovered by another worker
```sh
gecc run trigger m1_sleep --idempotency-key mt9-kill        # note run_id
sleep 8
cpsql -c "select w.worker_name, w.hostname from orchestration.task_attempt a join orchestration.worker w using (worker_id) where a.state = 'RUNNING'"
docker ps --filter name=gecc-worker --format '{{.Names}}'   # pick the container whose ID starts with that hostname
docker kill -s KILL <that container>
gecc run wait <run_id> --timeout 240
docker start <that container>
```
- [ ] ~30–45 s after the kill, attempt #1 becomes **LOST** (`LeaseExpired`, actor `system:reaper`)
- [ ] attempt #2 runs on the **other** worker and the run **SUCCEEDED**
- [ ] `select count(*) from orchestration.task_attempt where state = 'RUNNING'` never exceeded 1 for this task

### MT-10 — Concurrency key serialises runs across workers
```sh
gecc run trigger m1_sleep --idempotency-key mt10-a
gecc run trigger m1_sleep --idempotency-key mt10-b
# repeat a few times during the next ~2 minutes:
cpsql -c "select count(*) from orchestration.task_attempt where state = 'RUNNING'"
```
- [ ] both runs are **RUNNING** (max_active_runs = 5) but the count of RUNNING attempts is never above **1** — `demo:exclusive` has 1 slot
- [ ] the second sleep starts only after the first finishes; both runs **SUCCEEDED**

### MT-11 — Cancellation stops a running step
```sh
gecc run trigger m1_sleep --idempotency-key mt11-cancel     # note run_id
sleep 8
curl -s -H "$AUTH" -X POST $API/runs/<run_id>/cancel
sleep 10; gecc run show <run_id>
gecc run logs <run_id> | tail -3
```
- [ ] the cancel call returns `CANCELLING`; within ~10 s the run is **CANCELLED** and the attempt **CANCELLED**
- [ ] logs end with `run cancellation requested; terminating`

### MT-12 — Timeouts
```sh
gecc run trigger m1_timeout --idempotency-key mt12-timeout
gecc run wait <run_id> --timeout 120
```
- [ ] both attempts **TIMED_OUT** (`Timeout`), task **FAILED**, run **FAILED** after ~25 s

### MT-13 — Control-plane database restart is survived
```sh
gecc run trigger m1_sleep --idempotency-key mt13-dbrestart  # note run_id
sleep 8
docker compose -f $C restart control-db
docker compose -f $C logs --since 2m worker | grep -E "failed|FENCING|claimed" | tail
gecc run wait <run_id> --timeout 300
```
- [ ] workers log connection errors while the DB is down, then recover without being restarted
- [ ] the run ends **SUCCEEDED**: either the lease survived the restart, or the attempt was fenced/LOST and retried — never two attempts RUNNING at once

### MT-14 — Backup and restore of the control plane
```sh
docker compose -f $C exec -T control-db pg_dump -U gecc_admin -Fc ge_control > /tmp/ge_control.dump
docker compose -f $C exec -T control-db createdb -U gecc_admin ge_control_restore_test
docker compose -f $C exec -T control-db pg_restore -U gecc_admin -d ge_control_restore_test < /tmp/ge_control.dump
for db in ge_control ge_control_restore_test; do
  docker compose -f $C exec -T control-db psql -U gecc_admin -d $db -Atc \
    "select (select count(*) from orchestration.run), (select count(*) from orchestration.task_attempt), (select count(*) from audit.state_transition)"
done
docker compose -f $C exec -T control-db psql -U gecc_admin -d ge_control_restore_test -c "update audit.event set actor_id = 'x'"
docker compose -f $C exec -T control-db dropdb -U gecc_admin ge_control_restore_test
```
- [ ] restore completes without errors; the three counts are identical in both databases
- [ ] the restored copy still rejects the audit UPDATE (`append-only`)

### MT-15 — Automated tests
```sh
python -m pip install -e ".[test,control-center]"
GECC_TEST_ADMIN_DB_URL=postgresql+psycopg2://gecc_admin:gecc_admin_dev@127.0.0.1:55432/postgres python -m pytest
```
- [ ] all tests pass (existing platform suite + `tests/control_center`, including the no-Dagster guard)

---

## Reset the local stack
```sh
docker compose -f $C down -v     # deletes ONLY the two local Compose volumes
```
