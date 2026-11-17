#!/usr/bin/env python3
"""GE Data Control Center -- READ-ONLY integrity audit of the local stack.

    python3 deploy/compose/checks/m1_integrity.py control                 # control-plane invariants
    python3 deploy/compose/checks/m1_integrity.py snapshot FILE           # fingerprint ge_warehouse + volumes
    python3 deploy/compose/checks/m1_integrity.py compare FILE            # compare against a snapshot

Control-plane invariants (over ALL runs ever executed):
  I1  no task ever had two attempts RUNNING at the same time (no duplicate claims)
  I2  every recorded state transition is in orchestration.allowed_transition
  I3  per entity, transitions are contiguous (each from_state == previous to_state)
  I4  current states match the last recorded transition
  I5  no concurrency slot is held by an attempt that is not RUNNING
  I6  terminal runs have only terminal tasks; RUNNING tasks have exactly one RUNNING attempt
  I7  attempt numbers per task are 1..n with no gaps; attempt_count matches
  I8  no RUNNING attempt has a lease expired for longer than the reap interval allows

Nothing is written anywhere.
"""

from __future__ import annotations

import json
import sys

from _common import CheckFailed, Runner, expect, psql, q, q1, sh


def i1() -> str:
    overlaps = q("""
        select a.task_id, a.attempt_no, b.attempt_no
        from orchestration.task_attempt a
        join orchestration.task_attempt b on a.task_id = b.task_id and a.attempt_no < b.attempt_no
        where b.started_at < coalesce(a.finished_at, 'infinity')""")
    expect(not overlaps, f"overlapping attempts: {overlaps[:5]}")
    return f"{q1('select count(*) from orchestration.task_attempt')} attempts, 0 overlapping per task"


def i2() -> str:
    illegal = q("""
        select st.entity_type, coalesce(st.from_state,''), st.to_state, count(*)
        from audit.state_transition st
        left join orchestration.allowed_transition t
          on t.entity_type = st.entity_type and t.from_state = coalesce(st.from_state,'') and t.to_state = st.to_state
        where t.entity_type is null group by 1,2,3""")
    expect(not illegal, f"illegal transitions recorded: {illegal}")
    return f"{q1('select count(*) from audit.state_transition')} transitions, all allowed"


def i3() -> str:
    broken = q("""
        select entity_type, entity_id, prev_to, from_state from (
            select entity_type, entity_id, from_state,
                   lag(to_state) over (partition by entity_id order by transition_id) as prev_to
            from audit.state_transition) x
        where coalesce(prev_to,'') is distinct from coalesce(from_state,'')""")
    expect(not broken, f"non-contiguous transition chains: {broken[:5]}")
    return "every entity's transition chain is contiguous from creation"


def i4() -> str:
    mismatched = q("""
        with last as (
            select distinct on (entity_id) entity_type, entity_id, to_state
            from audit.state_transition order by entity_id, transition_id desc)
        select l.entity_type, l.entity_id, l.to_state, coalesce(r.state, t.state, a.state)
        from last l
        left join orchestration.run r on l.entity_type = 'RUN' and r.run_id = l.entity_id
        left join orchestration.task t on l.entity_type = 'TASK' and t.task_id = l.entity_id
        left join orchestration.task_attempt a on l.entity_type = 'ATTEMPT' and a.attempt_id = l.entity_id
        where l.to_state is distinct from coalesce(r.state, t.state, a.state)""")
    expect(not mismatched, f"state differs from audit trail: {mismatched[:5]}")
    return "current run/task/attempt states equal their last audited transition"


def i5() -> str:
    leaked = q("""
        select s.concurrency_key, s.slot_no, a.state from orchestration.concurrency_slot s
        left join orchestration.task_attempt a on a.attempt_id = s.holder_attempt_id
        where s.holder_attempt_id is not null and coalesce(a.state,'?') <> 'RUNNING'""")
    expect(not leaked, f"leaked slots: {leaked}")
    return f"{q1('select count(*) from orchestration.concurrency_slot where holder_attempt_id is not null')} slots held, none leaked"


def i6() -> str:
    bad_runs = q("""
        select r.run_id, r.state, t.node_key, t.state from orchestration.run r join orchestration.task t using (run_id)
        where r.state in ('SUCCEEDED','FAILED','CANCELLED')
          and t.state not in ('SUCCEEDED','FAILED','UPSTREAM_FAILED','CANCELLED')""")
    expect(not bad_runs, f"terminal runs with live tasks: {bad_runs[:5]}")
    bad_tasks = q("""
        select t.task_id, count(a.*) filter (where a.state = 'RUNNING') from orchestration.task t
        left join orchestration.task_attempt a using (task_id)
        group by t.task_id, t.state
        having (t.state = 'RUNNING') <> (count(a.*) filter (where a.state = 'RUNNING') = 1)
            or count(a.*) filter (where a.state = 'RUNNING') > 1""")
    expect(not bad_tasks, f"task/attempt RUNNING mismatch: {bad_tasks[:5]}")
    return "terminal runs fully settled; each RUNNING task has exactly one RUNNING attempt"


def i7() -> str:
    gaps = q("""
        select t.task_id, t.attempt_count, count(a.*), coalesce(max(a.attempt_no),0)
        from orchestration.task t left join orchestration.task_attempt a using (task_id)
        group by t.task_id, t.attempt_count
        having t.attempt_count <> count(a.*) or coalesce(max(a.attempt_no),0) <> count(a.*)""")
    expect(not gaps, f"attempt numbering/count mismatch: {gaps[:5]}")
    return "attempt numbers contiguous; attempt_count consistent"


def i8() -> str:
    stale = q("""select attempt_id, now() - lease_expires_at from orchestration.task_attempt
                 where state = 'RUNNING' and lease_expires_at < now() - interval '60 seconds'""")
    expect(not stale, f"RUNNING attempts with long-expired leases (reaper not running?): {stale}")
    running = q1("select count(*) from orchestration.task_attempt where state = 'RUNNING'")
    return f"{running} attempts RUNNING, all with live leases"


WAREHOUSE_TABLES_SQL = """
select n.nspname || '.' || c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
where c.relkind = 'r' and n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'
order by 1
"""


def warehouse_fingerprint() -> dict:
    def wq(sql: str) -> str:
        rc, out = psql(sql, db="ge_warehouse", service="warehouse-db", user="ge_wh_admin")
        if rc != 0:
            raise CheckFailed(out)
        return out.strip()

    tables = wq(WAREHOUSE_TABLES_SQL).split()
    fingerprint = {}
    for table in tables:
        fingerprint[table] = wq(f"select count(*) || ':' || coalesce(md5(string_agg(t::text, '|' order by t::text)), '-') "
                                f"from {table} t")
    stats = wq("select coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del),0) from pg_stat_user_tables")
    _, volumes = sh(["docker", "volume", "ls", "--format", "{{.Name}}"])
    return {"tables": fingerprint, "write_counter": int(stats), "volumes": sorted(volumes.split())}


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in {"control", "snapshot", "compare"}:
        print(__doc__)
        return 2
    mode = sys.argv[1]
    if mode == "control":
        print("GE Data Control Center -- control-plane integrity audit (read-only)\n")
        runner = Runner()
        for check_id, title, fn in (
            ("I1", "no duplicate claims (overlapping attempts)", i1),
            ("I2", "all recorded transitions are legal", i2),
            ("I3", "transition chains contiguous", i3),
            ("I4", "current states match audit trail", i4),
            ("I5", "no leaked concurrency slots", i5),
            ("I6", "run/task/attempt states consistent", i6),
            ("I7", "attempt numbering consistent", i7),
            ("I8", "no stale RUNNING leases", i8),
        ):
            runner.run(check_id, title, fn)
        return runner.summary()

    path = sys.argv[2]
    current = warehouse_fingerprint()
    if mode == "snapshot":
        with open(path, "w") as handle:
            json.dump(current, handle, indent=1, sort_keys=True)
        print(f"snapshot: {len(current['tables'])} warehouse tables, "
              f"{sum(int(v.split(':')[0]) for v in current['tables'].values())} rows, "
              f"write counter {current['write_counter']}, {len(current['volumes'])} docker volumes -> {path}")
        return 0

    with open(path) as handle:
        before = json.load(handle)
    changed = sorted(t for t in set(before["tables"]) | set(current["tables"])
                     if before["tables"].get(t) != current["tables"].get(t))
    print(f"warehouse tables compared: {len(current['tables'])}; changed: {changed or 'none'}")
    print(f"warehouse write counter: {before['write_counter']} -> {current['write_counter']}")
    missing_volumes = sorted(set(before["volumes"]) - set(current["volumes"]))
    print(f"docker volumes: {len(before['volumes'])} -> {len(current['volumes'])}; missing: {missing_volumes or 'none'}")
    ok = not changed and before["write_counter"] == current["write_counter"] and not missing_volumes
    print("RESULT:", "UNCHANGED" if ok else "CHANGED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
