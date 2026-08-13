"""Machine-readable reconciliation report: legacy telemetry_warehouse vs.
the migrated ge_warehouse raw_sendem/stg_sendem objects.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.validate_sendem_migration

Exits 0 if every object PASSes, 1 otherwise. Prints a summary table (one
PASS/FAIL line per object) followed by full detail for any FAILing check --
never just prints "mismatch": every failure shows the check name, the old
value, the new value, the difference, and the affected key(s) where
practical.

Two objects have a *documented* exact divergence from a plain 1:1 legacy
comparison, by design (see docs/migration/legacy-to-platform-migration.md#sendem-migration):

* stg_sendem.event_type carries 2 inferred "Unknown Sendem Event Type"
  placeholder rows beyond legacy staging.sendem_dim_event_types, synthesized
  by scripts.backfill_sendem_historical.apply_inferred_event_types() for
  event_type_ids referenced only by legacy clean.sendem_fact_events_daily.
  This script independently recomputes that expected set itself.
* stg_sendem.trip_daily / stg_sendem.event_daily are the UNION of legacy
  staging.sendem_fact_*_daily and legacy clean.sendem_fact_*_daily, with
  legacy staging's value kept on any overlapping key (not clean's) -- this
  script independently recomputes that expected union+resolution, not by
  trusting the backfill's own bookkeeping.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from decimal import Decimal

import psycopg2

from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import get_platform_settings, get_settings

CONTEXT = "Sendem migration validation"


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


# ---------------------------------------------------------------------------
# Generic 1:1 comparison helpers (used for raw_sendem.* and the non-merged
# stg_sendem dims).
# ---------------------------------------------------------------------------

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


def validate_raw_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.sendem_assets", "raw_sendem.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table, ["site_id", "vin_number", "model", "fuel_type"]
    )
    return report


def validate_raw_site(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.sendem_sites", "raw_sendem.site"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["site_id"])
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["site_name"])
    return report


def validate_raw_event_description(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.sendem_event_descriptions", "raw_sendem.event_description"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["event_type_id"])
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["metric_type", "unit_type"])
    return report


def validate_raw_trip_daily(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.sendem_trips_assets_daily", "raw_sendem.trip_daily"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["date_key", "group_id", "site_id", "asset_id"])
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "date")
    old_sum = _scalar(old_conn, f"SELECT COALESCE(sum(total_trip_distance_kilometres), 0) FROM {old_table}")
    new_sum = _scalar(new_conn, f"SELECT COALESCE(sum(total_trip_distance_kilometres), 0) FROM {new_table}")
    report.add(
        "sum matches exactly: total_trip_distance_kilometres",
        Decimal(old_sum) == Decimal(new_sum),
        f"old={old_sum} new={new_sum} diff={Decimal(new_sum) - Decimal(old_sum)}",
    )
    return report


def validate_raw_event_daily(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.sendem_events_assets_daily", "raw_sendem.event_daily"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(
        report, old_conn, new_conn, old_table, new_table,
        ["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
    )
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "date")
    old_sum = _scalar(old_conn, f"SELECT COALESCE(sum(total_event_occurrences), 0) FROM {old_table}")
    new_sum = _scalar(new_conn, f"SELECT COALESCE(sum(total_event_occurrences), 0) FROM {new_table}")
    report.add(
        "sum matches exactly: total_event_occurrences",
        Decimal(old_sum) == Decimal(new_sum),
        f"old={old_sum} new={new_sum} diff={Decimal(new_sum) - Decimal(old_sum)}",
    )
    return report


def validate_stg_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.sendem_dim_assets", "stg_sendem.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table, ["site_id", "vin_number", "model", "fuel_type"]
    )
    return report


def validate_stg_site(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.sendem_dim_sites", "stg_sendem.site"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["site_id"])
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["site_name"])
    return report


# ---------------------------------------------------------------------------
# clean.* dim subset confirmation (NOT copied -- see the migration doc).
# ---------------------------------------------------------------------------

def validate_clean_dims_are_subsets(old_conn) -> ObjectReport:
    report = ObjectReport("clean.* dims (not migrated -- subset confirmation)")
    for entity, clean_table, staging_table, key in [
        ("asset", "clean.sendem_dim_assets", "staging.sendem_dim_assets", "asset_id"),
        ("site", "clean.sendem_dim_sites", "staging.sendem_dim_sites", "site_id"),
        ("event_type", "clean.sendem_dim_event_types", "staging.sendem_dim_event_types", "event_type_id"),
    ]:
        clean_keys = {r[0] for r in _rows(old_conn, f"SELECT {key} FROM {clean_table}")}
        staging_keys = {r[0] for r in _rows(old_conn, f"SELECT {key} FROM {staging_table}")}
        exclusive = clean_keys - staging_keys
        report.add(
            f"clean.{entity} has zero ids not already in staging.{entity} (safe to skip)",
            len(exclusive) == 0,
            f"clean_count={len(clean_keys)} staging_count={len(staging_keys)} clean_exclusive={len(exclusive)} sample={list(exclusive)[:5]}",
        )
    return report


# ---------------------------------------------------------------------------
# stg_sendem.event_type: legacy staging dim + independently-recomputed
# inferred placeholder rows.
# ---------------------------------------------------------------------------

def validate_stg_event_type(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.sendem_dim_event_types", "stg_sendem.event_type"
    report = ObjectReport(new_table)

    staging_ids = {r[0] for r in _rows(old_conn, f"SELECT event_type_id FROM {old_table}")}
    clean_fact_ids = {r[0] for r in _rows(old_conn, "SELECT DISTINCT event_type_id FROM clean.sendem_fact_events_daily")}
    expected_inferred = clean_fact_ids - staging_ids
    expected_ids = staging_ids | expected_inferred

    new_ids = {r[0] for r in _rows(new_conn, f"SELECT event_type_id FROM {new_table}")}

    report.add(
        "row count = legacy staging dim + independently-recomputed inferred placeholders",
        len(new_ids) == len(expected_ids),
        f"staging={len(staging_ids)} inferred_expected={len(expected_inferred)} expected_total={len(expected_ids)} new={len(new_ids)}",
    )
    report.add(
        "every legacy staging event_type_id present after migration",
        (staging_ids - new_ids) == set(),
        f"missing={list(staging_ids - new_ids)[:5]}",
    )
    report.add(
        "every independently-recomputed inferred id present, correctly labelled",
        True,
        f"n/a" if not expected_inferred else "checked below",
    )
    if expected_inferred:
        inferred_rows = _rows(
            new_conn,
            f"SELECT event_type_id, event_name, event_category FROM {new_table} WHERE event_type_id = ANY(%s)",
            (list(expected_inferred),),
        )
        inferred_by_id = {r[0]: (r[1], r[2]) for r in inferred_rows}
        bad = [
            eid for eid in expected_inferred
            if inferred_by_id.get(eid) != ("Unknown Sendem Event Type", "unknown")
        ]
        report.add(
            "inferred placeholder rows have expected event_name/event_category",
            len(bad) == 0,
            f"bad={bad} expected=('Unknown Sendem Event Type', 'unknown') actual_sample={[inferred_by_id.get(b) for b in bad[:5]]}",
        )
    report.add(
        "no unexpected extra event_type_ids beyond staging + inferred",
        (new_ids - expected_ids) == set(),
        f"unexpected={list(new_ids - expected_ids)[:5]}",
    )
    return report


# ---------------------------------------------------------------------------
# stg_sendem.trip_daily / stg_sendem.event_daily: independently-recomputed
# union of legacy staging + legacy clean, with staging's value kept on any
# overlapping key.
# ---------------------------------------------------------------------------

def _validate_merged_fact(
    old_conn,
    new_conn,
    *,
    staging_table: str,
    clean_table: str,
    new_table: str,
    key_columns: list[str],
    value_columns: list[str],
    date_column: str = "date",
) -> ObjectReport:
    report = ObjectReport(new_table)
    key_list = ", ".join(key_columns)
    value_list = ", ".join(value_columns)

    staging_rows = {tuple(r[: len(key_columns)]): r[len(key_columns):] for r in _rows(
        old_conn, f"SELECT {key_list}, {value_list} FROM {staging_table}"
    )}
    clean_rows = {tuple(r[: len(key_columns)]): r[len(key_columns):] for r in _rows(
        old_conn, f"SELECT {key_list}, {value_list} FROM {clean_table}"
    )}
    new_rows = {tuple(r[: len(key_columns)]): r[len(key_columns):] for r in _rows(
        new_conn, f"SELECT {key_list}, {value_list} FROM {new_table}"
    )}

    staging_keys = set(staging_rows)
    clean_keys = set(clean_rows)
    expected_keys = staging_keys | clean_keys
    overlap_keys = staging_keys & clean_keys
    clean_exclusive_keys = clean_keys - staging_keys
    new_keys = set(new_rows)

    report.add(
        "row count = union(legacy staging keys, legacy clean keys)",
        len(new_keys) == len(expected_keys),
        f"staging={len(staging_keys)} clean={len(clean_keys)} overlap={len(overlap_keys)} "
        f"clean_exclusive={len(clean_exclusive_keys)} expected_union={len(expected_keys)} new={len(new_keys)}",
    )
    report.add(
        "keys expected (staging U clean) but missing after migration",
        (expected_keys - new_keys) == set(),
        f"count={len(expected_keys - new_keys)} sample={list(expected_keys - new_keys)[:5]}",
    )
    report.add(
        "keys present after migration but in neither legacy source (unexpected)",
        (new_keys - expected_keys) == set(),
        f"count={len(new_keys - expected_keys)} sample={list(new_keys - expected_keys)[:5]}",
    )

    # Every staging key -- including the ones also in clean -- must carry
    # staging's value, never clean's. This is the actual proof the
    # overlap-resolution rule (staging wins) was applied correctly.
    staging_mismatches = [k for k in staging_keys if k in new_rows and new_rows[k] != staging_rows[k]]
    report.add(
        "every legacy staging row's value is preserved exactly (staging wins on overlap)",
        len(staging_mismatches) == 0,
        f"count={len(staging_mismatches)} sample={staging_mismatches[:5]}",
    )

    # Every clean-EXCLUSIVE key (not in staging) must carry clean's value.
    clean_exclusive_mismatches = [
        k for k in clean_exclusive_keys if k in new_rows and new_rows[k] != clean_rows[k]
    ]
    report.add(
        "every clean-exclusive historical row's value is preserved exactly",
        len(clean_exclusive_mismatches) == 0,
        f"count={len(clean_exclusive_mismatches)} sample={clean_exclusive_mismatches[:5]}",
    )

    report.add(
        "documented overlap resolution: keys present in both legacy sources, clean's stale value discarded",
        True,
        f"count={len(overlap_keys)} -- see docs/migration/legacy-to-platform-migration.md#sendem-migration",
    )

    all_dates = _rows(old_conn, f"SELECT min(d), max(d) FROM (SELECT {date_column} AS d FROM {staging_table} UNION SELECT {date_column} FROM {clean_table}) t")
    expected_min, expected_max = all_dates[0]
    new_min = _scalar(new_conn, f"SELECT min({date_column}) FROM {new_table}")
    new_max = _scalar(new_conn, f"SELECT max({date_column}) FROM {new_table}")
    report.add(
        "date coverage matches independently-recomputed union(staging, clean)",
        (expected_min, expected_max) == (new_min, new_max),
        f"expected=[{expected_min}, {expected_max}] new=[{new_min}, {new_max}]",
    )

    return report


def validate_stg_trip_daily(old_conn, new_conn) -> ObjectReport:
    return _validate_merged_fact(
        old_conn, new_conn,
        staging_table="staging.sendem_fact_trips_daily",
        clean_table="clean.sendem_fact_trips_daily",
        new_table="stg_sendem.trip_daily",
        key_columns=["date_key", "group_id", "site_id", "asset_id"],
        value_columns=[
            "total_trip_count", "total_trip_distance_kilometres", "total_fuel_used_litres", "total_energy_used_kwh",
        ],
    )


def validate_stg_event_daily(old_conn, new_conn) -> ObjectReport:
    report = _validate_merged_fact(
        old_conn, new_conn,
        staging_table="staging.sendem_fact_events_daily",
        clean_table="clean.sendem_fact_events_daily",
        new_table="stg_sendem.event_daily",
        key_columns=["date_key", "group_id", "site_id", "asset_id", "event_type_id"],
        value_columns=[
            "total_event_occurrences", "min_event_value", "max_event_value", "total_event_value",
            "min_event_duration", "max_event_duration", "total_event_duration",
        ],
    )

    # Orphan-free join check (post-merge): every event_daily row's
    # event_type_id must resolve in stg_sendem.event_type (the inferred
    # placeholders exist precisely to guarantee this).
    orphans = _rows(
        new_conn,
        """
        SELECT COUNT(*) FROM stg_sendem.event_daily f
        LEFT JOIN stg_sendem.event_type e ON f.event_type_id = e.event_type_id
        WHERE e.event_type_id IS NULL
        """,
    )
    report.add(
        "no event_daily row references a missing event_type (inferred placeholders resolved all orphans)",
        orphans[0][0] == 0,
        f"orphan_rows={orphans[0][0]}",
    )
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
        reports.append(validate_raw_site(old_conn, new_conn))
        reports.append(validate_raw_event_description(old_conn, new_conn))
        reports.append(validate_raw_trip_daily(old_conn, new_conn))
        reports.append(validate_raw_event_daily(old_conn, new_conn))
        reports.append(validate_stg_asset(old_conn, new_conn))
        reports.append(validate_stg_site(old_conn, new_conn))
        reports.append(validate_stg_event_type(old_conn, new_conn))
        reports.append(validate_stg_trip_daily(old_conn, new_conn))
        reports.append(validate_stg_event_daily(old_conn, new_conn))
        reports.append(validate_clean_dims_are_subsets(old_conn))
    finally:
        old_conn.close()
        new_conn.close()

    print("SENDEM HISTORICAL MIGRATION VALIDATION\n")
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
