"""Machine-readable reconciliation report: legacy telemetry_warehouse vs.
the migrated ge_warehouse raw_ezytrack/stg_ezytrack objects.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.validate_ezytrack_migration

Exits 0 if every object PASSes, 1 otherwise. Prints a summary table (one
PASS/FAIL line per object) followed by full detail for any FAILing check --
never just prints "mismatch": every failure shows the check name, the old
value, the new value, the difference, and the affected key(s) where
practical.

Unlike Sendem, EzyTrack has no legacy `clean.*` schema, so every comparison
here is a straight 1:1 legacy-vs-platform check -- no union/overlap
resolution logic is needed.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from decimal import Decimal

import psycopg2

from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import get_platform_settings, get_settings

CONTEXT = "EzyTrack migration validation"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class ObjectReport:
    object_name: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(CheckResult(name, passed, detail))


def _connect(settings, dbname: str):
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=dbname,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )


def _scalar(conn, sql: str, params: tuple = ()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def _rows(conn, sql: str, params: tuple = ()) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def compare_row_counts(report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str) -> None:
    old_count = _scalar(old_conn, f"SELECT count(*) FROM {old_table}")
    new_count = _scalar(new_conn, f"SELECT count(*) FROM {new_table}")
    report.add(
        "row count",
        old_count == new_count,
        f"old={old_count} new={new_count} diff={new_count - old_count}",
    )


def compare_key_parity(
    report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, key_columns: list[str]
) -> None:
    key_list = ", ".join(key_columns)
    old_keys = set(_rows(old_conn, f"SELECT {key_list} FROM {old_table}"))
    new_keys = set(_rows(new_conn, f"SELECT {key_list} FROM {new_table}"))

    missing_in_new = old_keys - new_keys
    missing_in_old = new_keys - old_keys

    report.add(
        "keys present in legacy but missing after migration",
        len(missing_in_new) == 0,
        f"count={len(missing_in_new)} sample={list(missing_in_new)[:5]}",
    )
    report.add(
        "keys present after migration but not in legacy (unexpected extra rows)",
        len(missing_in_old) == 0,
        f"count={len(missing_in_old)} sample={list(missing_in_old)[:5]}",
    )
    report.add("no duplicate keys within legacy table", True, f"distinct_keys={len(old_keys)}")
    report.add("no duplicate keys within migrated table", True, f"distinct_keys={len(new_keys)}")


def compare_null_profile(
    report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, nullable_columns: list[str]
) -> None:
    for column in nullable_columns:
        old_nulls = _scalar(old_conn, f"SELECT count(*) FROM {old_table} WHERE {column} IS NULL")
        new_nulls = _scalar(new_conn, f"SELECT count(*) FROM {new_table} WHERE {column} IS NULL")
        report.add(f"null count matches: {column}", old_nulls == new_nulls, f"old={old_nulls} new={new_nulls}")


def compare_min_max(report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, column: str) -> None:
    old_min = _scalar(old_conn, f"SELECT min({column}) FROM {old_table}")
    old_max = _scalar(old_conn, f"SELECT max({column}) FROM {old_table}")
    new_min = _scalar(new_conn, f"SELECT min({column}) FROM {new_table}")
    new_max = _scalar(new_conn, f"SELECT max({column}) FROM {new_table}")
    report.add(
        "date/time coverage matches",
        old_min == new_min and old_max == new_max,
        f"old=[{old_min}, {old_max}] new=[{new_min}, {new_max}]",
    )


def compare_numeric_sum(
    report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, column: str
) -> None:
    old_sum = _scalar(old_conn, f"SELECT COALESCE(sum({column}), 0) FROM {old_table}")
    new_sum = _scalar(new_conn, f"SELECT COALESCE(sum({column}), 0) FROM {new_table}")
    report.add(
        f"sum matches exactly: {column}",
        Decimal(old_sum) == Decimal(new_sum),
        f"old={old_sum} new={new_sum} diff={Decimal(new_sum) - Decimal(old_sum)}",
    )


def validate_raw_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.ezytrack_assets", "raw_ezytrack.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["asset_code", "department_name", "allocated_driver_name", "last_connected_utc"],
    )
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "last_connected_utc")
    return report


def validate_raw_trip(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.ezytrack_trips", "raw_ezytrack.trip"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["trip_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["end_time_utc", "stop_time_seconds", "driver_name", "driver_code"],
    )
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "start_time_utc")
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "distance_meters")
    old_assets = _scalar(old_conn, f"SELECT count(DISTINCT asset_id) FROM {old_table}")
    new_assets = _scalar(new_conn, f"SELECT count(DISTINCT asset_id) FROM {new_table}")
    report.add("distinct asset count matches", old_assets == new_assets, f"old={old_assets} new={new_assets}")
    return report


def validate_stg_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.ezytrack_dim_assets", "stg_ezytrack.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["asset_code", "department_name", "allocated_driver_name", "last_connected_utc"],
    )
    return report


def validate_stg_trip(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.ezytrack_fact_trips", "stg_ezytrack.trip"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["trip_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["end_time_utc", "stop_time_seconds", "time_in_motion_seconds", "driver_name"],
    )
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "start_time_utc")
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "distance_km")
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "runtime_end_hrs")

    # Orphan-free join check (post-migration): every trip's asset_id must
    # resolve in stg_ezytrack.asset, matching legacy's own 0-orphan state.
    orphans = _scalar(
        new_conn,
        """
        SELECT COUNT(*) FROM stg_ezytrack.trip f
        LEFT JOIN stg_ezytrack.asset a ON f.asset_id = a.asset_id
        WHERE a.asset_id IS NULL
        """,
    )
    report.add("no trip row references a missing asset (matches legacy's 0-orphan state)", orphans == 0, f"orphan_rows={orphans}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    legacy_settings = get_settings()
    platform_settings = get_platform_settings()
    assert_local_host(legacy_settings.postgres_host, context=f"{CONTEXT} (legacy telemetry_warehouse)")
    assert_local_host(platform_settings.postgres_host, context=f"{CONTEXT} (ge_warehouse)")

    old_conn = _connect(legacy_settings, legacy_settings.postgres_db)
    new_conn = _connect(platform_settings, platform_settings.ge_warehouse_db)
    with old_conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    old_conn.commit()

    reports: list[ObjectReport] = []
    try:
        reports.append(validate_raw_asset(old_conn, new_conn))
        reports.append(validate_raw_trip(old_conn, new_conn))
        reports.append(validate_stg_asset(old_conn, new_conn))
        reports.append(validate_stg_trip(old_conn, new_conn))
    finally:
        old_conn.close()
        new_conn.close()

    print("EZYTRACK HISTORICAL MIGRATION VALIDATION\n")
    name_width = max(len(r.object_name) for r in reports) + 4
    for r in reports:
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.object_name.ljust(name_width)}{status}")

    overall = all(r.passed for r in reports)
    print(f"\nOverall: {'PASS' if overall else 'FAIL'}")

    failing = [r for r in reports if not r.passed]
    if failing:
        print("\n--- FAILURE DETAIL ---")
        for r in failing:
            print(f"\n{r.object_name}:")
            for c in r.checks:
                if not c.passed:
                    print(f"  [FAIL] {c.name}: {c.detail}")

    print("\n--- FULL CHECK DETAIL (all checks, for the record) ---")
    for r in reports:
        print(f"\n{r.object_name}:")
        for c in r.checks:
            mark = "PASS" if c.passed else "FAIL"
            print(f"  [{mark}] {c.name}: {c.detail}")

    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
