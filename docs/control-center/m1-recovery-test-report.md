# M1 recovery test report

**Date:** 2026-11-17 · **Environment:** local Docker Compose stack only (`deploy/compose`), 2 workers ·
**Build:** `ge_control_center 0.1.0`, branch `feature/control-center-m1` (uncommitted) ·
**Approval:** recovery checks R1–R4 approved by the platform owner before running.

No production system, legacy database, or the developer's own PostgreSQL (port 5432) was touched.

## Summary

| Area | Result |
|---|---|
| R1 worker SIGKILL | **PASS** (run twice: before and after the fix below) |
| R2 graceful worker stop | **PASS** (run twice) |
| R3 control-DB restart + outage longer than the lease | **PASS** after strengthening the check and fixing 2 defects it exposed |
| R4 backup → restore | **PASS** |
| Control-plane integrity audit (I1–I8) after every check | **8/8 every time** |
| Warehouse data / Docker volumes | **Unchanged** (42 tables content-hashed, write counter 7685 → 7685, 4 → 4 volumes) |
| 18 safe checks after recovery testing | **18/18** |
| Automated tests | **335 passed** (incl. 4 new worker-fencing tests) |

## How results were verified

* `deploy/compose/checks/m1_recovery_checks.py --approve --only Rn` — each check run on its own.
* `deploy/compose/checks/m1_integrity.py control` after every check, over **all** runs ever executed:
  I1 no overlapping attempts per task (no duplicate claims) · I2 every recorded transition is legal ·
  I3 transition chains contiguous · I4 current state = last audited transition · I5 no leaked
  concurrency slots · I6 run/task/attempt states consistent · I7 attempt numbering consistent ·
  I8 no stale RUNNING leases.
* `m1_integrity.py snapshot/compare` — every `ge_warehouse` table fingerprinted (row count + MD5 of
  contents), plus the warehouse's cumulative insert/update/delete counter and the Docker volume list,
  before R1 and after all testing.

## Results

### R1 — SIGKILL the worker running an attempt
| UTC | Event |
|---|---|
| 11:50:46 | attempt 1 claimed by worker A; lease renewed 11:50:47 → expires 11:51:17 |
| ~11:50:49 | worker A's container SIGKILLed (its step dies with it) |
| 11:51:24 | reaper on worker B: attempt 1 `LOST` (`LeaseExpired`, actor `system:reaper`), task `UP_FOR_RETRY` |
| 11:51:28 | attempt 2 claimed by worker B → `SUCCEEDED` 11:52:28 |
| +120 s | worker A's stale registration marked `LOST` by the reaper |

### R2 — graceful worker stop (SIGTERM) mid-attempt
SIGTERM ~4 s into the step → worker stopped claiming, drained 20 s, terminated the step (exit 143),
reported `FAILED / WorkerShutdown` (retryable), marked itself `STOPPED`; the other worker retried 2 s
later → `SUCCEEDED`.

### R3 — control-plane database restart and outage
* **A — quick restart:** the database was unavailable ~0.2 s; the lease survived; run `SUCCEEDED`.
* **B — 46 s outage (> 30 s lease):**

| UTC | Event |
|---|---|
| 12:06:38.3 | attempt 1 claimed |
| 12:06:41.1 | last successful renewal → lease expires **12:07:11.1** |
| 12:06:44.5 | control DB shut down: renewals, heartbeats, claims, log shipping fail; API `/health` → 503 |
| 12:07:04 | **worker self-fences**: step killed **6.8 s before lease expiry**; no outcome reported |
| 12:07:29.9 | control DB accepting connections again |
| 12:07:35.1 | reaper marks attempt 1 `LOST`; the fenced attempt's 8 buffered log lines delivered |
| 12:07:46.4 | attempt 2 claimed → `SUCCEEDED` 12:08:46.8; never two attempts `RUNNING` |

### R4 — backup and restore
`pg_dump -Fc ge_control` (145 KB) restored into a temporary database. Identical to the live database
in: row counts (runs 26 · tasks 38 · attempts 56 · transitions 381 · events 57), MD5 of
`audit.state_transition`, `audit.event`, `orchestration.task_attempt`, the full registry, and the
migration history. In the restored copy, audit UPDATE/DELETE are still rejected (append-only), an
illegal run transition is still rejected (state machine), and role grants are intact. The temporary
database was dropped; `ge_control` was only read.

## Failures found and fixes

| # | Finding | Root cause | Fix | Verified by |
|---|---|---|---|---|
| 1 | **R3 as first written passed without testing anything:** the DB was down 0.2 s, between heartbeats; no worker saw an error. | A `restart` is too short to exercise outage handling. | R3 now also runs phase B: a 46 s stop (> lease) and asserts self-fencing *before* lease expiry (from worker log vs. `lease_expires_at`), no report from the fenced worker, reaper `LOST`, retry, no overlap, API 503 during the outage. | R3 runs at 12:06 and after fixes |
| 2 | **Logs of a fenced attempt were lost.** Only 2 of its lines were stored; output produced during the outage and the fence reason were dropped. | The final log flush made one attempt (DB still down) and then the attempt ended. | `AttemptExecution._final_flush()` retries with backoff (bounded: max(60 s, 4 × lease)). Log appends are fenced by lease *token*, not attempt state, so a fenced/LOST attempt's logs are accepted once the DB is back. | `test_fenced_attempt_logs_survive_an_outage_and_shipping_backs_off` (fails on the old code, passes on the new); live R3: all 8 lines of the fenced attempt stored, including the fence reason |
| 3 | **Log shipping hammered an unavailable DB**: a retry and a warning every second per running attempt (21 warnings for one attempt in one outage). | No backoff on log-shipping failures (every other control-plane call already backed off). | Exponential backoff 1 s → 30 s; one warning per failure streak plus a "log shipping recovered" message. | Same test (≤ 7 append calls in a 9 s outage, exactly 1 warning); live R3: 1 warning |
| 4 | Worker self-fencing (alive worker that cannot prove its lease) was not covered by any test. | R1 kills the worker outright, which is a different path. | Added `tests/control_center/test_cc_supervisor_fencing.py` (real worker + real step subprocess, fake control plane): unreachable → fence before expiry, rejected lease → immediate fence, cancel → `CANCELLED`, outage log delivery. | 4 tests, 3 consecutive green runs; plus live R3-B |

Not a defect, recorded for transparency: Docker Desktop quit unexpectedly during an earlier safe-check
run (before this testing). The worker shutdown path handled it correctly (drain → `WorkerShutdown` →
retry → `SUCCEEDED`).

## Residual limitations (accepted for M1)
* A shutdown that outlasts the 20 s drain consumes one attempt of the task.
* Self-fencing relies on the worker's monotonic clock vs. the database clock; the lease is always
  measured from *before* the renewal request, so the worker's view is conservative.
* If the control DB stays down longer than max(60 s, 4 × lease) after an attempt ends, its remaining
  buffered log lines are dropped (logged as an error). Execution safety is unaffected.
* Single shared API token; no scheduler; no source pipelines — all planned for later milestones.
