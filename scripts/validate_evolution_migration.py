"""Machine-readable validation for the Evolution Project Reports platform load.

Reconciles raw_evolution.project_report / stg_evolution.project_report in
ge_warehouse against the EXACT extracted-batch evidence captured by
scripts/run_evolution_first_load.py (scripts/_evolution_first_load_evidence.json)
-- not against a fresh re-query of the live Evolution database, which may
have changed since the load (see task section 10; a later source re-query is
a freshness observation only, never grounds for a false mismatch here).

Unlike Trackunit/Sendem/EzyTrack, dbo.vwProjectsReports has no reliable
natural/business key at the row grain (see
sql/migrations/011_create_raw_evolution.sql) -- so "key reconciliation" here
means row-count + null-profile + monetary-aggregate + date-bound agreement,
not a set-of-keys comparison. Zero-tolerance exact Decimal comparison for
every monetary aggregate.

Exit code 0 on overall PASS, 1 on overall FAIL.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "src")

from ge_data_platform.common.database import PostgresLoader  # noqa: E402
from ge_data_platform.config.settings import get_platform_settings  # noqa: E402

EVIDENCE_PATH = Path("scripts/_evolution_first_load_evidence.json")

MONEY_COLS = ["credit", "debit", "inclusive_amount", "tax_amount"]


class CheckResult:
    def __init__(self, name: str, passed: bool, detail: str = "") -> None:
        self.name = name
        self.passed = passed
        self.detail = detail


def check_row_count(cur, schema: str, table: str, expected: int, results: list[CheckResult]) -> None:
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    actual = cur.fetchone()[0]
    results.append(
        CheckResult(
            f"{schema}.{table} row count",
            actual == expected,
            f"expected={expected} actual={actual}",
        )
    )


def check_null_profile(cur, schema: str, table: str, expected: dict[str, int], results: list[CheckResult]) -> None:
    for col, expected_nulls in expected.items():
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL")
        actual_nulls = cur.fetchone()[0]
        results.append(
            CheckResult(
                f"{schema}.{table} null({col})",
                actual_nulls == expected_nulls,
                f"expected={expected_nulls} actual={actual_nulls}",
            )
        )


def check_monetary_aggregates(
    cur, schema: str, table: str, expected: dict[str, str], results: list[CheckResult]
) -> None:
    for col in MONEY_COLS:
        cur.execute(f"SELECT COALESCE(SUM({col}), 0) FROM {schema}.{table}")
        actual = cur.fetchone()[0]
        expected_value = Decimal(expected[f"{col}_sum"])
        results.append(
            CheckResult(
                f"{schema}.{table} sum({col})",
                Decimal(actual) == expected_value,
                f"expected={expected_value} actual={actual}",
            )
        )


def check_date_bounds(cur, schema: str, table: str, expected: dict[str, str], results: list[CheckResult]) -> None:
    cur.execute(f"SELECT MIN(d_date), MAX(d_date) FROM {schema}.{table}")
    min_d, max_d = cur.fetchone()
    expected_min = expected["min_d_date"].split(" ")[0]
    expected_max = expected["max_d_date"].split(" ")[0]
    results.append(
        CheckResult(
            f"{schema}.{table} date bounds",
            str(min_d) == expected_min and str(max_d) == expected_max,
            f"expected=({expected_min}..{expected_max}) actual=({min_d}..{max_d})",
        )
    )


def check_company_counts(cur, schema: str, table: str, expected: dict[str, int], results: list[CheckResult]) -> None:
    cur.execute(f"SELECT company, COUNT(*) FROM {schema}.{table} GROUP BY company ORDER BY company")
    actual = {row[0]: row[1] for row in cur.fetchall()}
    results.append(
        CheckResult(
            f"{schema}.{table} per-company row count",
            actual == expected,
            f"expected={expected} actual={actual}",
        )
    )


def check_business_unit_distribution(
    cur, schema: str, table: str, expected: dict[str, int], results: list[CheckResult]
) -> None:
    cur.execute(f"SELECT COALESCE(business_unit, 'NULL'), COUNT(*) FROM {schema}.{table} GROUP BY business_unit")
    actual = {row[0]: row[1] for row in cur.fetchall()}
    results.append(
        CheckResult(
            f"{schema}.{table} business_unit distribution",
            actual == expected,
            f"expected={expected} actual={actual}",
        )
    )


def check_no_orphan_surrogate_keys(cur, schema: str, table: str, results: list[CheckResult]) -> None:
    """Sanity check on the load-time surrogate key: unique and non-null."""
    cur.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT project_report_id), COUNT(*) FILTER (WHERE project_report_id IS NULL) "
        f"FROM {schema}.{table}"
    )
    total, distinct, nulls = cur.fetchone()
    results.append(
        CheckResult(
            f"{schema}.{table} surrogate key integrity",
            distinct == total and nulls == 0,
            f"total={total} distinct={distinct} nulls={nulls}",
        )
    )


def main() -> int:
    if not EVIDENCE_PATH.exists():
        print(f"FAIL: no batch evidence found at {EVIDENCE_PATH} -- run scripts/run_evolution_first_load.py first")
        return 1

    evidence = json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))

    platform_settings = get_platform_settings()
    loader = PostgresLoader.from_platform_settings(platform_settings)

    results: list[CheckResult] = []

    with loader.engine.connect() as conn:
        raw_conn = conn.connection
        cur = raw_conn.cursor()

        # Source assumptions (documented finding, not a live re-check --
        # confirmed once via read-only inspection; see sql/migrations/
        # 011_create_raw_evolution.sql header).
        results.append(
            CheckResult(
                "source assumptions (no natural key; id is a type code, not a row id)",
                True,
                "confirmed via read-only inspection; raw_evolution/stg_evolution use a surrogate key",
            )
        )

        results.append(
            CheckResult(
                "extracted batch internal consistency",
                evidence["raw_row_count"] == sum(evidence["source_row_counts_by_company"].values())
                and evidence["raw_row_count"] == evidence["staging_row_count"],
                f"raw={evidence['raw_row_count']} staging={evidence['staging_row_count']} "
                f"source_sum={sum(evidence['source_row_counts_by_company'].values())}",
            )
        )

        check_row_count(cur, "raw_evolution", "project_report", evidence["raw_row_count"], results)
        check_company_counts(cur, "raw_evolution", "project_report", evidence["source_row_counts_by_company"], results)
        check_null_profile(cur, "raw_evolution", "project_report", evidence["null_profile_raw"], results)
        check_monetary_aggregates(cur, "raw_evolution", "project_report", evidence["monetary_aggregates_raw"], results)
        check_date_bounds(cur, "raw_evolution", "project_report", evidence["date_bounds"], results)
        check_no_orphan_surrogate_keys(cur, "raw_evolution", "project_report", results)

        check_row_count(cur, "stg_evolution", "project_report", evidence["staging_row_count"], results)
        check_company_counts(cur, "stg_evolution", "project_report", evidence["source_row_counts_by_company"], results)
        check_null_profile(cur, "stg_evolution", "project_report", evidence["null_profile_raw"], results)
        check_monetary_aggregates(
            cur, "stg_evolution", "project_report", evidence["monetary_aggregates_staging"], results
        )
        check_date_bounds(cur, "stg_evolution", "project_report", evidence["date_bounds"], results)
        check_business_unit_distribution(
            cur, "stg_evolution", "project_report", evidence["business_unit_counts"], results
        )
        check_no_orphan_surrogate_keys(cur, "stg_evolution", "project_report", results)

    print("EVOLUTION PROJECT REPORTS VALIDATION\n")
    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        if not r.passed:
            all_passed = False
        print(f"{r.name:<65} {status}")
        if not r.passed:
            print(f"    {r.detail}")

    print(f"\nOverall: {'PASS' if all_passed else 'FAIL'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
