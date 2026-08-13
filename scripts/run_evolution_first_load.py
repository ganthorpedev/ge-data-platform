"""Perform the first controlled Evolution Project Reports load into ge_warehouse.

This is the "controlled Evolution load" referenced in
docs/migration/legacy-to-platform-migration.md#evolution-migration-completed:

    Evolution SQL Server -> dbo.vwProjectsReports -> Python extraction
        -> raw_evolution.project_report -> stg_evolution.project_report

Because the live Evolution database may change while this runs, batch
evidence (row counts, null profile, date bounds, monetary aggregates, a
content fingerprint) is captured from the EXACT extracted DataFrames before
any load happens, and written to a JSON file. `scripts/validate_evolution_
migration.py` reconciles ge_warehouse against that captured evidence, not
against a fresh re-query of Evolution -- a later source re-query would only
be a freshness observation, never grounds for a false mismatch (see task
section 10).

Uses the exact same production extraction/transform/load functions
(ge_data_platform.sources.evolution.project_reports.extract_all/build_raw/
add_business_unit_classification, ge_data_platform.common.database.
PostgresLoader.replace_evolution_project_reports_platform) that
`project_reports.run(target="platform")` uses -- this script exists only to
capture pre-load batch evidence around that same call, not a second
implementation.

Target is always ge_warehouse (raw_evolution/stg_evolution). Never writes to
telemetry_warehouse or to Evolution.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "src")

from ge_data_platform.common.database import PostgresLoader  # noqa: E402
from ge_data_platform.common.safety import assert_local_host  # noqa: E402
from ge_data_platform.config.settings import get_evolution_settings, get_platform_settings  # noqa: E402
from ge_data_platform.sources.evolution.project_reports import (  # noqa: E402
    add_business_unit_classification,
    build_raw,
    extract_all,
)

EVIDENCE_PATH = Path("scripts/_evolution_first_load_evidence.json")


def _decimal_sum(series) -> str:
    total = Decimal("0")
    for value in series:
        if value is None:
            continue
        total += Decimal(value)
    return str(total)


def _null_counts(df, columns: list[str]) -> dict[str, int]:
    return {c: int(df[c].isna().sum()) for c in columns if c in df.columns}


def _fingerprint(df) -> str:
    """Deterministic content fingerprint: sha256 of every row's sorted repr.

    Order-independent (sorted) so it is comparable against a re-read of the
    same rows from Postgres, which has no guaranteed row order.
    """
    row_reprs = sorted(repr(tuple(row)) for row in df.itertuples(index=False, name=None))
    hasher = hashlib.sha256()
    for r in row_reprs:
        hasher.update(r.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def main() -> None:
    platform_settings = get_platform_settings()
    assert_local_host(platform_settings.postgres_host, context="Evolution first platform load")
    print(f"Target: ge_warehouse database={platform_settings.ge_warehouse_db} host={platform_settings.postgres_host}")

    evolution_settings = get_evolution_settings()
    print(f"Source: {evolution_settings.project_reports_view}")
    for source in evolution_settings.sources:
        print(f"  company={source.company} database={source.database}")

    print("\nExtracting (read-only) ...")
    datasets = extract_all(evolution_settings)
    source_row_counts = {company: len(df) for company, df in datasets.items()}
    print(f"Source row counts at extraction: {source_row_counts}")

    raw_df = build_raw(datasets)
    staging_df = add_business_unit_classification(raw_df)
    print(f"Combined batch row count: {len(raw_df):,}")

    money_cols = ["credit", "debit", "inclusive_amount", "tax_amount"]
    evidence = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_row_counts_by_company": source_row_counts,
        "raw_row_count": len(raw_df),
        "staging_row_count": len(staging_df),
        "null_profile_raw": _null_counts(
            raw_df, ["company", "id", "project_code", "customer", "d_date", *money_cols]
        ),
        "date_bounds": {
            "min_d_date": str(raw_df["d_date"].min()),
            "max_d_date": str(raw_df["d_date"].max()),
        },
        "monetary_aggregates_raw": {f"{c}_sum": _decimal_sum(raw_df[c]) for c in money_cols},
        "monetary_aggregates_staging": {f"{c}_sum": _decimal_sum(staging_df[c]) for c in money_cols},
        "business_unit_counts": staging_df["business_unit"].value_counts(dropna=False).to_dict(),
        "raw_fingerprint_sha256": _fingerprint(raw_df),
        "staging_fingerprint_sha256": _fingerprint(staging_df),
    }
    # value_counts keys may not be JSON-serializable-clean (NaN); normalize.
    evidence["business_unit_counts"] = {str(k): int(v) for k, v in evidence["business_unit_counts"].items()}

    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
    print(f"\nBatch evidence captured -> {EVIDENCE_PATH}")
    print(json.dumps(evidence, indent=2, default=str))

    print("\nLoading into ge_warehouse (full replace, raw_evolution + stg_evolution) ...")
    loader = PostgresLoader.from_platform_settings(platform_settings)
    load_counts = loader.replace_evolution_project_reports_platform(raw_df, staging_df)
    print(f"Load counts: {load_counts}")


if __name__ == "__main__":
    main()
