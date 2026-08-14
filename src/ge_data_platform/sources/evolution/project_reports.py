"""Accounts/Evolution Project Reports: extract, transform, and sync.

Evolution is a single source system extracting one dataset (project reports)
from GE and TLS via a shared code path, so -- unlike the API-based providers,
which split client/sync/transform into three files -- extraction,
transformation, and sync orchestration for this dataset live together here as
one cohesive module. Only SQL Server connection handling (shared plumbing,
not dataset-specific) stays separate, in
ge_data_platform.sources.evolution.connection.

Run the sync with:
    python -m ge_data_platform.sources.evolution.project_reports

EXTRACTION
This module is the only place that should run the project-reports SQL
query. It reads sql/sources/evolution/project_reports/extract_project_reports.sql
once, then executes it against every configured Evolution source database
(ge_data_platform.config.settings.EvolutionSettings.sources) through the same
function -- there is no per-company extraction pipeline.

TRANSFORM
Owns combination (GE + TLS), business-unit classification, and snake_case
column conversion. Performs no I/O (no ODBC/SQL Server calls, no database
access, no environment reads). Every rule below (cost-code parsing, the
TLS/GE business-unit tables, the 2026-03-01 cutover date, and the snake_case
regex) is ported verbatim from the working evolution_extraction_pipeline
notebook. None of it is reinterpreted here.

SYNC
Orchestrates extract -> transform -> load
(ge_data_platform.common.database), and records the run in etl.sync_runs
(legacy target) or ops.pipeline_run (platform target) -- the same
bookkeeping mechanism the telemetry providers use.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd

from ge_data_platform.common.database import PostgresLoader, finish_sync_run_failed_safe
from ge_data_platform.common.logging import configure_logging
from ge_data_platform.config.settings import (
    EvolutionSettings,
    get_etl_ops_settings,
    get_evolution_settings,
    get_platform_settings,
    get_settings,
    validate_evolution_view_name,
)
from ge_data_platform.sources.evolution.connection import sql_server_connection

logger = logging.getLogger(__name__)

SOURCE_SYSTEM = "evolution_project_reports"
JOB_NAME = "evolution_project_reports_sync"

SOURCE_COLUMNS = [
    "AccountDescription",
    "AccountTypeDescription",
    "CostType",
    "Credit",
    "Customer",
    "CustomerUniqueID",
    "DDate",
    "Debit",
    "Description",
    "FleetNumber",
    "Id",
    "InclusiveAmount",
    "Master_Sub_Account",
    "Module",
    "Project",
    "ProjectCode",
    "ProjectName",
    "QuantityInvoiced",
    "Reference",
    "TaxAmount",
    "TransactionDescription",
]

_SQL_PATH = Path(__file__).resolve().parents[4] / "sql" / "sources" / "evolution" / "project_reports" / "extract_project_reports.sql"

BUSINESS_UNIT_CUTOVER_DATE = pd.Timestamp("2026-03-01")

TLS_LEGACY_COST_TYPE_PREFIXES = {
    "1050001": "Haulage",
    "1055001": "Shunting",
    "1080001": "Intrachem",
}

TLS_COST_CODE_BUSINESS_UNITS = {
    "101": "Haulage",
    "955": "Shunting",
    "103": "Intrachem",
    "104": "Lowbed",
}

GE_HIRE_COST_CODES = {"101", "102"}
GE_COMMERCIAL_WHOLE_GOODS_COST_CODE = "104"
GE_COMMERCIAL_PARTS_AND_SERVICES_COST_CODES = {"105", "109", "110"}
GE_COMMERCIAL_TRAINING_COST_CODE = "106"

UNCLASSIFIED_BUSINESS_UNIT = "Unclassified"


# --- Extraction --------------------------------------------------------


def _load_query(view_name: str) -> str:
    """Read the extraction .sql file and substitute a validated view name.

    `view_name` is re-validated here (in addition to ge_data_platform.config.
    settings.get_evolution_settings()'s fail-fast check) immediately before
    it is substituted into dynamically constructed SQL, and the
    bracket-quoted form returned by the validator -- never the raw input --
    is what actually reaches the query text.
    """
    safe_view_name = validate_evolution_view_name(view_name)
    template = _SQL_PATH.read_text(encoding="utf-8")
    return template.format(view_name=safe_view_name)


def validate_columns(dataframe: pd.DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Stop when an expected source column is missing."""
    missing = [column for column in required_columns if column not in dataframe.columns]
    if missing:
        logger.error("%s is missing columns: %s", dataset_name, missing)
        raise ValueError(f"{dataset_name} is missing columns: {missing}")


def extract_company(company: str, database: str, settings: EvolutionSettings) -> pd.DataFrame:
    """Extract project-reports rows for one Evolution company database.

    Logs the company being extracted and the row count once complete.
    `coerce_float=False` is required so DECIMAL columns arrive as Python
    `decimal.Decimal` rather than being coerced to float, preserving exact
    accounting precision.
    """
    query = _load_query(settings.project_reports_view)

    logger.info("Extracting Evolution project reports: company=%s database=%s", company, database)

    with sql_server_connection(database, settings) as connection:
        dataframe = pd.read_sql_query(query, connection, coerce_float=False)

    validate_columns(dataframe, SOURCE_COLUMNS, company)
    dataframe.insert(0, "company", company)

    logger.info("%s: %s rows extracted", company, f"{len(dataframe):,}")
    return dataframe


def extract_all(settings: EvolutionSettings) -> dict[str, pd.DataFrame]:
    """Extract project-reports rows for every configured Evolution source.

    Returns a dict keyed by company code (e.g. {"GE": ..., "TLS": ...}).
    Every source is extracted through `extract_company`, so adding a third
    Evolution company only requires another `EvolutionSourceDatabase` entry
    in configuration, not a new extraction function.
    """
    return {source.company: extract_company(source.company, source.database, settings) for source in settings.sources}


# --- Transform -----------------------------------------------------------


def combine_data(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine per-company datasets that all carry `company` + SOURCE_COLUMNS."""
    expected_columns = ["company", *SOURCE_COLUMNS]

    for name, dataset in datasets.items():
        validate_columns(dataset, expected_columns, f"Extracted dataset ({name})")

    return pd.concat(
        [dataset[expected_columns] for dataset in datasets.values()],
        ignore_index=True,
    )


def extract_cost_code(cost_type: object) -> str:
    """Extract the cost code after the first '/' in a CostType value."""
    parts = str(cost_type).strip().split("/")
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def determine_tls_business_unit(d_date: object, cost_type: object) -> str:
    """Classify a TLS row's business unit from its date and CostType."""
    date_value = pd.to_datetime(d_date)
    cost_value = str(cost_type).strip()

    if date_value < BUSINESS_UNIT_CUTOVER_DATE:
        for prefix, business_unit in TLS_LEGACY_COST_TYPE_PREFIXES.items():
            if cost_value.startswith(prefix):
                return business_unit
        return UNCLASSIFIED_BUSINESS_UNIT

    code = extract_cost_code(cost_value)
    return TLS_COST_CODE_BUSINESS_UNITS.get(code, UNCLASSIFIED_BUSINESS_UNIT)


def determine_ge_business_unit(d_date: object, cost_type: object) -> str:
    """Classify a GE row's business unit from its date and CostType."""
    date_value = pd.to_datetime(d_date)

    if date_value < BUSINESS_UNIT_CUTOVER_DATE:
        return UNCLASSIFIED_BUSINESS_UNIT

    code = extract_cost_code(cost_type)

    if code in GE_HIRE_COST_CODES:
        return "Hire"
    if code == GE_COMMERCIAL_WHOLE_GOODS_COST_CODE:
        return "Commercial Whole Goods"
    if code in GE_COMMERCIAL_PARTS_AND_SERVICES_COST_CODES:
        return "Commercial Parts and Services"
    if code == GE_COMMERCIAL_TRAINING_COST_CODE:
        return "Commercial Training"
    return UNCLASSIFIED_BUSINESS_UNIT


def determine_business_unit(company: object, d_date: object, cost_type: object) -> str:
    """Determine the business unit for one row, dispatching on company."""
    if pd.isna(company) or pd.isna(d_date) or pd.isna(cost_type):
        return UNCLASSIFIED_BUSINESS_UNIT

    normalized_company = str(company).strip().upper()

    if normalized_company == "TLS":
        return determine_tls_business_unit(d_date, cost_type)
    if normalized_company == "GE":
        return determine_ge_business_unit(d_date, cost_type)
    return UNCLASSIFIED_BUSINESS_UNIT


def to_snake_case(column_name: str) -> str:
    """Convert a PascalCase/camelCase column name to snake_case."""
    column_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", column_name)
    column_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column_name)
    column_name = column_name.replace(" ", "_")
    return column_name.lower()


def build_raw(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine and snake_case per-company extracts -- the raw_evolution.project_report shape.

    Source-faithful: combines GE + TLS and converts every column name to
    snake_case, but applies no business_unit classification (see
    `add_business_unit_classification`). This is the platform raw layer's
    exact input; `build_combined` composes this with classification for
    the legacy (and platform staging) shape. See
    docs/migration/legacy-to-platform-migration.md#evolution-migration-completed
    for why raw and staging are split this way for Evolution specifically.
    """
    combined = combine_data(datasets)
    combined.columns = [to_snake_case(column) for column in combined.columns]
    return combined


def add_business_unit_classification(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Add `business_unit` to an already-combined, already-snake_case DataFrame.

    Expects `raw_df`'s columns already snake_case (i.e. `build_raw` output)
    -- reads `company`/`d_date`/`cost_type`, not `DDate`/`CostType`. Appends
    `business_unit` as a new trailing column, matching `build_combined`'s
    historical column order (business_unit last).
    """
    staged = raw_df.copy()
    staged["business_unit"] = staged.apply(
        lambda row: determine_business_unit(row["company"], row["d_date"], row["cost_type"]),
        axis=1,
    )
    return staged


def build_combined(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine, classify, and snake_case per-company extracts into one DataFrame.

    Mirrors the notebook's run_pipeline(): combine GE + TLS, add
    `business_unit`, then convert every column name to snake_case. The
    output shape is unchanged from before this function was split into two
    steps -- composed from `build_raw` (combine + snake_case) and
    `add_business_unit_classification` (add business_unit) so the platform
    target can reuse the same two steps for its raw_evolution/stg_evolution
    split.
    """
    return add_business_unit_classification(build_raw(datasets))


# --- Sync ------------------------------------------------------------------


def run(*, target: str = "legacy") -> None:
    """Execute one Evolution Project Reports sync: extract, transform, load, record.

    `target="legacy"` (default, unchanged behavior): writes the classified,
    combined extract straight into `raw.evolution_project_reports` in
    `telemetry_warehouse`, via `PostgresLoader(settings)` and
    `replace_accounts_evolution_project_reports`. This is the only target
    Dagster ever passes.

    `target="platform"`: writes into `ge_warehouse` instead, split into two
    layers -- `raw_evolution.project_report` (source-faithful, no
    `business_unit`) and `stg_evolution.project_report` (adds
    `business_unit`) -- via `PostgresLoader.from_platform_settings` and
    `replace_evolution_project_reports_platform`. Records the run and each
    table load in `ops.pipeline_run`/`ops.table_load` (via
    `tracking_backend="platform"`, same as the other three sources' platform
    target -- see `ge_data_platform.common.audit`) instead of
    `etl.sync_runs`/`etl.sync_table_loads`, and still skips post-load
    validation (hardcoded to legacy schema names). Opt-in, manual-only, not
    wired to any Dagster schedule. See
    docs/migration/legacy-to-platform-migration.md#evolution-migration-completed.
    """
    configure_logging()
    if target not in ("legacy", "platform"):
        raise ValueError(f"target must be 'legacy' or 'platform', got {target!r}")

    evolution_settings = get_evolution_settings()

    if target == "platform":
        loader = PostgresLoader.from_platform_settings(get_platform_settings())
        ops_settings = None
    else:
        loader = PostgresLoader(get_settings())
        ops_settings = get_etl_ops_settings()

    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=None,
        end_date=None,
    )
    logger.info("sync_run_id: %s (target=%s)", sync_run_id, target)

    try:
        logger.info(
            "Extracting Evolution project reports for %s source(s)",
            len(evolution_settings.sources),
        )
        datasets = extract_all(evolution_settings)
        rows_fetched = sum(len(dataset) for dataset in datasets.values())

        if target == "platform":
            raw_df = build_raw(datasets)
            staging_df = add_business_unit_classification(raw_df)
            logger.info("Extracted batch row count: %s", f"{len(raw_df):,}")
            load_counts = loader.replace_evolution_project_reports_platform(
                raw_df, staging_df, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM
            )
        else:
            combined = build_combined(datasets)
            logger.info("Combined row count: %s", f"{len(combined):,}")
            load_counts = loader.replace_accounts_evolution_project_reports(
                combined, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM
            )

        rows_loaded = sum(load_counts.values())
        for table_name, row_count in load_counts.items():
            logger.info("  %s: %s rows loaded (full replace)", table_name, f"{row_count:,}")

        if target == "legacy":
            loader.run_post_load_validation(
                SOURCE_SYSTEM,
                mode=ops_settings.validation_mode,
                lookback_hours=ops_settings.validation_lookback_hours,
            )

        loader.finish_sync_run(
            sync_run_id=sync_run_id,
            status="SUCCESS",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
        )
        logger.info(
            "Sync run %s completed: SUCCESS (fetched=%s, loaded=%s)",
            sync_run_id,
            rows_fetched,
            rows_loaded,
        )

    except Exception as error:
        logger.exception("Sync run %s failed", sync_run_id)
        finish_sync_run_failed_safe(loader, sync_run_id, error)
        raise


def main() -> None:
    """Parse arguments and run the sync."""
    parser = argparse.ArgumentParser(description="Accounts/Evolution Project Reports sync")
    parser.add_argument(
        "--target",
        choices=["legacy", "platform"],
        default="legacy",
        help="legacy (default): telemetry_warehouse raw.evolution_project_reports. "
        "platform: ge_warehouse raw_evolution/stg_evolution.project_report (opt-in, manual-only).",
    )
    args = parser.parse_args()
    run(target=args.target)


if __name__ == "__main__":
    main()
