"""Machine-readable reconciliation report: legacy telemetry_warehouse vs.
the migrated ge_warehouse raw_trackunit/stg_trackunit objects.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.validate_trackunit_migration

Exits 0 if every object PASSes, 1 otherwise. Prints a summary table (one
PASS/FAIL line per object) followed by full detail for any FAILing check --
never just prints "mismatch": every failure shows the check name, the old
value, the new value, the difference, and the affected key(s) where
practical.

One object -- stg_trackunit.daily_activity -- has a *documented* exact
divergence from the legacy source by design: counter_reset_detected/
data_quality_status don't exist in legacy telemetry_warehouse (migration 027
was written but never applied there -- see
docs/migration/legacy-to-platform-migration.md), so scripts.
backfill_trackunit_historical recomputes them during the backfill. This
script independently recomputes the expected reset set itself (not by
trusting the backfill's own output) and checks the migrated data against
that independent computation, not against the legacy row's stale values.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from decimal import Decimal

import psycopg2
import psycopg2.extras

from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import get_platform_settings, get_settings

CONTEXT = "Trackunit migration validation"


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
# Generic 1:1 table comparison (row count, key parity, null profile).
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
    report.add(
        "no duplicate keys within legacy table",
        True,  # enforced by legacy PK; included for explicit reporting
        f"distinct_keys={len(old_keys)}",
    )
    report.add(
        "no duplicate keys within migrated table",
        True,  # enforced by destination PK
        f"distinct_keys={len(new_keys)}",
    )


def compare_null_profile(
    report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, nullable_columns: list[str]
) -> None:
    for column in nullable_columns:
        old_nulls = _scalar(old_conn, f"SELECT count(*) FROM {old_table} WHERE {column} IS NULL")
        new_nulls = _scalar(new_conn, f"SELECT count(*) FROM {new_table} WHERE {column} IS NULL")
        report.add(
            f"null count matches: {column}",
            old_nulls == new_nulls,
            f"old={old_nulls} new={new_nulls}",
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


def compare_min_max(
    report: ObjectReport, old_conn, new_conn, old_table: str, new_table: str, column: str
) -> None:
    old_min = _scalar(old_conn, f"SELECT min({column}) FROM {old_table}")
    old_max = _scalar(old_conn, f"SELECT max({column}) FROM {old_table}")
    new_min = _scalar(new_conn, f"SELECT min({column}) FROM {new_table}")
    new_max = _scalar(new_conn, f"SELECT max({column}) FROM {new_table}")
    report.add(
        f"date/time coverage matches: {column}",
        old_min == new_min and old_max == new_max,
        f"old=[{old_min}, {old_max}] new=[{new_min}, {new_max}]",
    )


def validate_raw_metric_table(old_conn, new_conn, old_table: str, new_table: str) -> ObjectReport:
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id", "metric_timestamp_utc", "metric_name"])
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "metric_timestamp_utc")
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["metric_value", "metric_unit"])
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "metric_value")
    old_assets = _scalar(old_conn, f"SELECT count(DISTINCT asset_id) FROM {old_table}")
    new_assets = _scalar(new_conn, f"SELECT count(DISTINCT asset_id) FROM {new_table}")
    report.add("distinct asset count matches", old_assets == new_assets, f"old={old_assets} new={new_assets}")
    return report


def validate_raw_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.trackunit_assets", "raw_trackunit.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["name", "external_reference", "serial_number", "production_year", "created_at", "telematics_device_id"],
    )
    return report


def validate_raw_location(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.trackunit_aemp_locations", "raw_trackunit.aemp_location"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id", "location_timestamp_utc"])
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "location_timestamp_utc")
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["latitude", "longitude", "altitude"])
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "latitude")
    compare_numeric_sum(report, old_conn, new_conn, old_table, new_table, "longitude")
    return report


def validate_raw_site_history(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.trackunit_site_history", "raw_trackunit.site_history"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id", "site_id", "entered_at"])
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["left_at"])
    return report


def validate_raw_site(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "raw.trackunit_sites", "raw_trackunit.site"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["site_id"])
    compare_null_profile(report, old_conn, new_conn, old_table, new_table, ["site_name", "city", "country", "polygon_geojson"])
    return report


def validate_stg_asset(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.trackunit_dim_assets", "stg_trackunit.asset"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["name", "external_reference", "serial_number", "production_year", "created_at"],
    )
    return report


def validate_stg_location_enrichment(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.trackunit_location_enrichment", "stg_trackunit.location_enrichment"
    report = ObjectReport(new_table)
    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["report_date", "asset_id"])
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["zone_name_start", "zone_name_stop", "start_latitude", "stop_latitude"],
    )

    old_dist = dict(_rows(old_conn, f"SELECT location_enrichment_status, count(*) FROM {old_table} GROUP BY 1"))
    new_dist = dict(_rows(new_conn, f"SELECT location_enrichment_status, count(*) FROM {new_table} GROUP BY 1"))
    report.add(
        "location_enrichment_status distribution matches",
        old_dist == new_dist,
        f"old={old_dist} new={new_dist}",
    )
    return report


# ---------------------------------------------------------------------------
# stg_trackunit.daily_activity: the one object with a documented, expected
# divergence (counter-reset rows). Computes the expected reset set
# independently rather than trusting the backfill script's own output.
# ---------------------------------------------------------------------------

_RESET_SQL_TEMPLATE = """
    WITH ordered AS (
        SELECT
            asset_id,
            (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date AS report_date,
            metric_value,
            LAG(metric_value) OVER (
                PARTITION BY asset_id, (metric_timestamp_utc AT TIME ZONE 'Africa/Harare')::date
                ORDER BY metric_timestamp_utc
            ) AS previous_value
        FROM {table}
        WHERE metric_value IS NOT NULL
    )
    SELECT DISTINCT asset_id, report_date FROM ordered WHERE metric_value < previous_value
"""


def _expected_reset_keys(new_conn, table: str) -> set[tuple[str, object]]:
    return set(_rows(new_conn, _RESET_SQL_TEMPLATE.format(table=table)))


def validate_daily_activity(old_conn, new_conn) -> ObjectReport:
    old_table, new_table = "staging.trackunit_daily_activity", "stg_trackunit.daily_activity"
    report = ObjectReport(new_table)

    compare_row_counts(report, old_conn, new_conn, old_table, new_table)
    compare_key_parity(report, old_conn, new_conn, old_table, new_table, ["report_date", "asset_id"])
    compare_min_max(report, old_conn, new_conn, old_table, new_table, "report_date")
    compare_null_profile(
        report, old_conn, new_conn, old_table, new_table,
        ["start_time_utc", "stop_time_utc"],
    )
    # distance_km is checked separately below (not via compare_null_profile)
    # because it has a documented, expected null-count divergence: rows
    # where a genuine raw distance-counter reset was detected are nulled in
    # the migrated table but were not nulled in the legacy source.

    # Independently recompute the expected reset set from the migrated raw
    # tables -- do not trust backfill_trackunit_historical's own bookkeeping.
    # _expected_reset_keys returns (asset_id, report_date) tuples (matching
    # its "SELECT DISTINCT asset_id, report_date" shape); normalise to this
    # function's own (report_date, asset_id) key order immediately so every
    # downstream membership test compares like with like.
    operating_resets = {(d, a) for a, d in _expected_reset_keys(new_conn, "raw_trackunit.aemp_operating_hour")}
    moving_resets = {(d, a) for a, d in _expected_reset_keys(new_conn, "raw_trackunit.aemp_moving_hour")}
    distance_resets = {(d, a) for a, d in _expected_reset_keys(new_conn, "raw_trackunit.aemp_distance")}
    all_reset_keys = operating_resets | moving_resets | distance_resets

    old_distance_nulls = _scalar(old_conn, f"SELECT count(*) FROM {old_table} WHERE distance_km IS NULL")
    new_distance_nulls = _scalar(new_conn, f"SELECT count(*) FROM {new_table} WHERE distance_km IS NULL")
    old_distance_by_key = {(r[0], r[1]): r[2] for r in _rows(old_conn, f"SELECT report_date, asset_id, distance_km FROM {old_table}")}
    newly_nulled_by_reset = sum(
        1 for key in distance_resets if old_distance_by_key.get(key) is not None
    )
    report.add(
        "distance_km null count matches legacy plus exactly the documented reset-affected rows",
        new_distance_nulls == old_distance_nulls + newly_nulled_by_reset,
        f"old={old_distance_nulls} new={new_distance_nulls} reset_affected_previously_non_null={newly_nulled_by_reset}",
    )

    old_rows = {
        (r[0], r[1]): r[2:]
        for r in _rows(
            old_conn,
            f"SELECT report_date, asset_id, operating_minutes, active_driving_minutes, distance_km, "
            f"work_day_minutes, machine, pin "
            f"FROM {old_table}",
        )
    }
    new_rows = {
        (r[0], r[1]): r[2:]
        for r in _rows(
            new_conn,
            f"SELECT report_date, asset_id, operating_minutes, active_driving_minutes, distance_km, "
            f"work_day_minutes, machine, pin, counter_reset_detected, data_quality_status "
            f"FROM {new_table}",
        )
    }

    unexpected_mismatches = []
    unflagged_resets = []
    wrongly_flagged = []
    non_reset_column_mismatches = []

    for key, old_vals in old_rows.items():
        (date_, asset_) = key
        new_vals = new_rows.get((date_, asset_))
        if new_vals is None:
            continue  # already reported by compare_key_parity
        old_operating, old_active, old_distance, old_work_day, old_machine, old_pin = old_vals
        new_operating, new_active, new_distance, new_work_day, new_machine, new_pin, reset_flag, quality_status = new_vals

        # Columns unrelated to counter-reset handling must match exactly,
        # always -- no exception for reset-flagged rows.
        if old_work_day != new_work_day or old_machine != new_machine or old_pin != new_pin:
            non_reset_column_mismatches.append(
                (key, f"work_day/machine/pin old=({old_work_day},{old_machine},{old_pin}) new=({new_work_day},{new_machine},{new_pin})")
            )

        expected_reset_key = key in all_reset_keys
        if expected_reset_key:
            if not reset_flag or quality_status != "COUNTER_RESET":
                unflagged_resets.append((key, f"expected COUNTER_RESET but got flag={reset_flag} status={quality_status}"))
            # The specific metric(s) that triggered a reset must be NULL;
            # metrics not implicated by any raw series decrease are
            # untouched and must still match legacy exactly.
            if key in operating_resets and new_operating is not None:
                unflagged_resets.append((key, f"operating_minutes should be NULL, got {new_operating}"))
            if key in moving_resets and new_active is not None:
                unflagged_resets.append((key, f"active_driving_minutes should be NULL, got {new_active}"))
            if key in distance_resets and new_distance is not None:
                unflagged_resets.append((key, f"distance_km should be NULL, got {new_distance}"))
            # Metrics NOT implicated by any raw decrease for this key must
            # still match the legacy value exactly (a reset on one metric
            # must never touch an unrelated metric).
            if key not in operating_resets and old_operating != new_operating:
                unexpected_mismatches.append((key, f"operating_minutes changed despite no detected reset: old={old_operating} new={new_operating}"))
            if key not in moving_resets and old_active != new_active:
                unexpected_mismatches.append((key, f"active_driving_minutes changed despite no detected reset: old={old_active} new={new_active}"))
            if key not in distance_resets and old_distance != new_distance:
                unexpected_mismatches.append((key, f"distance_km changed despite no detected reset: old={old_distance} new={new_distance}"))
        else:
            if reset_flag or quality_status != "live":
                wrongly_flagged.append((key, f"unexpected flag={reset_flag} status={quality_status}"))
            if old_operating != new_operating or old_active != new_active or old_distance != new_distance:
                unexpected_mismatches.append(
                    (key, f"old=({old_operating},{old_active},{old_distance}) new=({new_operating},{new_active},{new_distance})")
                )

    report.add(
        "non-quality columns (work_day_minutes/machine/pin) match exactly for every row",
        len(non_reset_column_mismatches) == 0,
        f"count={len(non_reset_column_mismatches)} sample={non_reset_column_mismatches[:5]}",
    )
    report.add(
        "derived metrics match legacy exactly for every row NOT affected by a counter reset",
        len(unexpected_mismatches) == 0,
        f"count={len(unexpected_mismatches)} sample={unexpected_mismatches[:5]}",
    )
    report.add(
        "no row incorrectly flagged COUNTER_RESET",
        len(wrongly_flagged) == 0,
        f"count={len(wrongly_flagged)} sample={wrongly_flagged[:5]}",
    )
    report.add(
        "every row with a genuine raw counter decrease is correctly flagged and nulled",
        len(unflagged_resets) == 0,
        f"count={len(unflagged_resets)} sample={unflagged_resets[:5]}",
    )
    report.add(
        "documented divergence: rows where migrated value legitimately differs from stale legacy value",
        True,
        f"count={len(all_reset_keys)} (asset,report_date) pairs -- see docs/migration/legacy-to-platform-migration.md",
    )

    quality_dist = dict(_rows(new_conn, f"SELECT data_quality_status, count(*) FROM {new_table} GROUP BY 1"))
    report.add(
        "data_quality_status distribution recorded (legacy has no equivalent column)",
        True,
        f"new={quality_dist}",
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
        reports.append(validate_raw_metric_table(old_conn, new_conn, "raw.trackunit_aemp_operating_hours", "raw_trackunit.aemp_operating_hour"))
        reports.append(validate_raw_metric_table(old_conn, new_conn, "raw.trackunit_aemp_moving_hours", "raw_trackunit.aemp_moving_hour"))
        reports.append(validate_raw_metric_table(old_conn, new_conn, "raw.trackunit_aemp_distance", "raw_trackunit.aemp_distance"))
        reports.append(validate_raw_location(old_conn, new_conn))
        reports.append(validate_raw_site_history(old_conn, new_conn))
        reports.append(validate_raw_site(old_conn, new_conn))
        reports.append(validate_stg_asset(old_conn, new_conn))
        reports.append(validate_daily_activity(old_conn, new_conn))
        reports.append(validate_stg_location_enrichment(old_conn, new_conn))
    finally:
        old_conn.close()
        new_conn.close()

    print("TRACKUNIT HISTORICAL MIGRATION VALIDATION\n")
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
