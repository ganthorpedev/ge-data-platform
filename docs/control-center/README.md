# GE Data Control Center

The GE Data Platform's own orchestration system: pipeline registry and
versioning, DAG execution, PostgreSQL-backed job queue, distributed workers,
retries and crash recovery, logging and audit. **It has no Dagster
dependency** and replaces the legacy Dagster deployment completely
([ADR-009](architecture-decisions.md#adr-009-replace-dagster-completely-with-the-ge-data-control-center)).

**Status: M1 (local only).** Not connected to any production system.

## Architecture at a glance

```
            gecc CLI ─┐          ┌─ API (FastAPI, cc_api role)
                      ▼          ▼
   ┌──────────── ge_control (PostgreSQL) ─────────────┐
   │ registry: steps, pipelines, immutable versions   │
   │ orchestration: runs, tasks, attempts, leases,    │
   │   concurrency slots, workers, logs               │
   │ audit: events, state transitions (append-only)   │
   └───────────────▲──────────────────────────────────┘
                   │ claim / renew / finish (SQL functions, cc_worker role)
        ┌──────────┴──────────┐
     worker 1   …   worker N        each attempt = isolated child process
        │                           (no control-plane credentials)
        ▼
   ge_warehouse (PostgreSQL): raw_* → stg_* → core → mart_*
```

| Concept | Where |
|---|---|
| Steps (the only executable units) | `src/ge_control_center/steps/` — registered with `@step` |
| Pipeline specs (data, no code) | JSON, e.g. `pipelines/examples/*.json` |
| Control-plane schema & logic | `sql/control/migrations/` (state machine, queue, leases in SQL) |
| Worker | `src/ge_control_center/worker/` |
| API | `src/ge_control_center/api/app.py` |
| Decisions | [`architecture-decisions.md`](architecture-decisions.md) |
| Manual testing | [`m1-manual-test-checklist.md`](m1-manual-test-checklist.md) |

## Quick start (local)

Requires Docker. Nothing touches port 5432 or any existing database.

```sh
docker compose -f deploy/compose/docker-compose.yml up -d --build --scale worker=2
alias gecc='docker compose -f deploy/compose/docker-compose.yml run --rm -T -e GECC_ACTOR="cli:$USER" -v "$PWD/pipelines:/app/pipelines:ro" bootstrap gecc'

gecc pipeline publish pipelines/examples/m1_demo.json
gecc run trigger m1_demo --idempotency-key demo-1     # prints run_id
gecc run wait <run_id>
gecc run logs <run_id>
```

API: `http://127.0.0.1:58000` (`/health` is public; everything else needs
`Authorization: Bearer gecc-local-dev-token`).

## Writing a step

```python
from pydantic import Field
from ge_control_center.registry import StepParams, StepContext, step

class MyParams(StepParams):          # unknown fields are rejected
    lookback_days: int = Field(2, ge=1, le=30)

@step(key="sendem.sync", version="1", display_name="Sendem sync",
      params=MyParams, idempotency="upsert", concurrency_keys=["provider:sendem"])
def sendem_sync(ctx: StepContext, params: MyParams) -> dict:
    ...                              # use ctx.window_start/window_end, ctx.warehouse_engine()
    return {"rows_loaded": 123}      # JSON-serialisable summary
```

Then add the module to `steps/__init__.py`, run `gecc registry sync`, and
reference `"step": "sendem.sync"` from a pipeline. Changing a step's
parameters, idempotency, concurrency keys or defaults requires a new
`version`.

## Running tests

```sh
python -m pytest tests/control_center                          # unit tests, no database
GECC_TEST_ADMIN_DB_URL=postgresql+psycopg2://gecc_admin:gecc_admin_dev@127.0.0.1:55432/postgres \
  python -m pytest tests/control_center                        # + integration (throwaway DB)
```

## Configuration

| Variable | Meaning |
|---|---|
| `GECC_ENVIRONMENT` | Only `local` is accepted in M1 |
| `GECC_CONTROL_DB_URL` | Control-plane DB (local hosts / Compose services only in M1) |
| `GECC_WAREHOUSE_DB_URL` | Warehouse DB, passed to step processes |
| `GECC_API_TOKEN` | Bearer token for the API (required to start it) |
| `GECC_LEASE_SECONDS` / `GECC_HEARTBEAT_SECONDS` / `GECC_LEASE_SAFETY_SECONDS` | Lease timing (default 30 / 10 / lease÷4) |
| `GECC_POLL_SECONDS`, `GECC_REAP_SECONDS`, `GECC_DRAIN_SECONDS`, `GECC_WORKER_LOST_AFTER_SECONDS` | Worker loop timing |

Host-side CLI settings can go in `.env.control-center` (see
`.env.control-center.example`). The legacy `.env` is never read by `gecc`.

## Known limitations in M1

* No scheduler (cron) yet — runs are triggered by CLI/API (M2).
* No source pipelines yet — Sendem/EzyTrack/Trackunit/Evolution adapters are M2–M3.
* Checkpoint table exists but has no API yet (M3).
* Single shared bearer token; no user identity (M4).
* A worker shutdown that outlasts the drain period consumes one attempt.
* Task logs are stored in PostgreSQL (bounded per attempt); retention/pruning is not automated yet.
