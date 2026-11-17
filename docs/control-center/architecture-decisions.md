# GE Data Control Center — architecture decisions

Continues the ADR series in `docs/architecture/architecture-decisions.md`
(ADR-001 … ADR-008). Status reflects milestone **M1**.

---

### ADR-009: Replace Dagster completely with the GE Data Control Center

**Status:** Accepted (final).

**Decision:** The GE Data Platform is orchestrated by its own system, the
GE Data Control Center (`ge_control_center`). Dagster is not used anywhere in
the new system — not for orchestration, scheduling, execution, monitoring or
pipeline definitions. No new functionality is built on Dagster.

**Transition:** The existing Dagster production services (Windows host,
legacy `telemetry_warehouse`) remain operational only until each pipeline is
cut over, and are then removed completely, together with
`ge_data_platform/orchestration/`, `workspace.yaml`, `dagster.yaml.example`
and the Dagster dependencies.

**Enforcement:** `ge_control_center` never imports `dagster` or
`ge_data_platform.orchestration` (`tests/control_center/test_cc_no_dagster.py`);
the Control Center image installs `requirements/control-center.txt` with
`--no-deps` and its build fails if `dagster` is importable.

---

### ADR-010: A separate control-plane database (`ge_control`)

**Decision:** Orchestration state lives in its own database, `ge_control`
(schemas `registry`, `orchestration`, `audit`, plus `meta` for migrations),
with its own migration sequence (`sql/control/migrations/`), roles, backups
and — later — replication. There are no cross-database references.

The warehouse-side `ge_warehouse.ops.*` tables stay where they are: load
records and watermarks must commit **in the same transaction as the data
they describe**, which is only possible inside the warehouse. A later
additive warehouse migration links `ops.pipeline_run` to the control plane
via `control_run_id` / `control_attempt_id` (backlog E9).

---

### ADR-011: PostgreSQL is the job queue

**Decision:** Tasks are claimed with `SELECT … FOR NO KEY UPDATE SKIP LOCKED`
inside `orchestration.claim_task`, locking **one candidate row at a time**.
No Redis, RabbitMQ or Celery. Polling is the source of truth (LISTEN/NOTIFY
may be added as a wake-up hint later).

Row locks use `FOR NO KEY UPDATE` so they never conflict with the
`KEY SHARE` locks foreign-key checks take (e.g. inserting an attempt
references its run). Lock order: attempt → run → task → slots → pipeline.

---

### ADR-012: At-least-once delivery with idempotent steps

**Decision:** The system does **not** claim exactly-once execution.

* Every claimed attempt holds a lease `(attempt_id, lease_token)` that its
  worker renews. Expired leases cannot be revived.
* The reaper marks expired attempts `LOST`; the task is retried if attempts
  remain. Any later write carrying the old token is rejected (fencing).
* A worker that cannot renew before expiry kills its own child process
  ("self-fencing"); a child whose worker dies exits on stdin EOF.
* A partial unique index guarantees at most one `RUNNING` attempt per task.
* Every step declares an idempotency mode (`read_only`, `upsert`,
  `replace_partition`, `full_replace`, `append_dedup`). Runs carry a fixed
  logical window (`window_start` / `window_end`) so retries reprocess the
  same data rather than "now".
* Run creation is idempotent on `idempotency_key`.

The remaining overlap window (a partitioned worker's child still writing to
the warehouse in the seconds before self-fencing) is covered by idempotent
writes; exclusive warehouse steps will additionally take a warehouse-side
advisory lock (M2).

---

### ADR-013: Steps are registered Python code; pipelines never contain code

**Decision:** A step is a Python function decorated with
`@step(key, version, params=<Pydantic model>, idempotency, concurrency_keys, …)`
in a module listed in `ge_control_center/steps/__init__.py`. Nothing is
imported by name at run time; there is no `eval` and no code field anywhere.

* Parameter models forbid unknown fields; pipeline specs forbid unknown fields.
* `gecc registry sync` stores each step's parameter JSON Schema and a
  `definition_hash` of its contract; changing a contract without bumping the
  version is refused.
* Workers refuse to start if their code does not match the active registry,
  and the child refuses any step key or version its code does not provide.

---

### ADR-014: Immutable, content-hashed pipeline versions

**Decision:** `registry.pipeline` is a mutable header (name, owner,
`max_active_runs`); `registry.pipeline_version` rows are immutable
(enforced by trigger) and identified by a SHA-256 of the canonical, resolved
DAG. Publishing an unchanged DAG is a no-op; the visual layout (`ui_layout`)
is cosmetic and never creates a version. Every run is pinned to one version.

The spec format (`nodes` + `edges` + `ui_layout`) maps one-to-one onto
React Flow, so the future visual designer edits the same artefact the CLI
publishes.

---

### ADR-015: Execution model and state machine

**Decision:** run → task (one per node) → attempt. Each attempt runs in its
own child process (`python -m ge_control_center.worker.step_runner`) supervised
by the worker: timeouts, cancellation and kills never affect the worker or
other attempts. The child receives warehouse settings but **never**
control-plane credentials.

Allowed transitions live in `orchestration.allowed_transition` and are
enforced by triggers; every transition is written to
`audit.state_transition` with actor and reason.

```
RUN:     QUEUED → RUNNING → SUCCEEDED | FAILED
         QUEUED → CANCELLED;  RUNNING → CANCELLING → CANCELLED
TASK:    PENDING → READY → RUNNING → SUCCEEDED | FAILED | UP_FOR_RETRY | CANCELLED
         UP_FOR_RETRY → RUNNING;  PENDING → UPSTREAM_FAILED | CANCELLED
ATTEMPT: RUNNING → SUCCEEDED | FAILED | TIMED_OUT | CANCELLED | LOST
```

---

### ADR-016: Scheduler with single active leader (M2)

**Decision:** Scheduler replicas elect a leader with a PostgreSQL advisory
lock; cron is evaluated in `Africa/Harare`; each tick creates its run with
the idempotency key `schedule:<id>:<scheduled_for>`, so a tick fires at most
once. Misfire policy: skip / latest / bounded catch-up. *Not built in M1.*

---

### ADR-017: Concurrency control in the database

**Decision:** Concurrency keys (e.g. `provider:trackunit`) have slot limits
in `registry.concurrency_limit`; slots are rows in
`orchestration.concurrency_slot`, acquired in the same transaction as the
claim and released when the attempt ends. Keys without an explicit limit
default to **1**. Pipelines also have `max_active_runs`; extra runs wait in
`QUEUED` and are admitted as earlier runs finish.

---

### ADR-018: Checkpointing

**Decision:** Step progress checkpoints go to `orchestration.task_checkpoint`
(token-fenced); data watermarks go to `ge_warehouse.ops.source_watermark` in
the same transaction as the data. *Table exists in M1; API in M3.*

---

### ADR-019: Append-only audit

**Decision:** `audit.event` (who/what/when, before/after, request id) and
`audit.state_transition` reject UPDATE, DELETE and TRUNCATE by trigger.

---

### ADR-020: Deployment, backup and replication

**Decision:** Docker Compose for local development and initial deployment
(`deploy/compose/`). Backups: `pg_dump` of `ge_control` with regular restore
drills, then WAL archiving; later streaming replication with controlled
promotion. Recovery after crash, restart or failover relies on lease expiry
and needs no manual repair.

---

### ADR-021: Least-privilege worker protocol

**Decision:** Workers connect as a login in `cc_worker`, which may only
EXECUTE the protocol functions (`register_worker`, `worker_heartbeat`,
`claim_task`, `renew_lease`, `finish_attempt`, `append_task_logs`,
`reap_expired_leases`, `stop_worker`) and read the step registry. The API
connects as `cc_api` (read + `create_run` / `request_cancel` + audit
inserts). All worker access goes through the `ControlPlaneClient` interface
so remote workers can later use an HTTP implementation unchanged.
