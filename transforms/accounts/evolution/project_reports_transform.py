"""Transforms raw Evolution Project Reports extracts into the combined table.

This module owns combination (GE + TLS), business-unit classification, and
snake_case column conversion. It must not perform any I/O (no ODBC/SQL Server
calls, no database access, no environment reads) -- extraction lives in
connectors.accounts.evolution.project_reports and loading in
loaders.postgres_loader.

Every rule below (cost-code parsing, the TLS/GE business-unit tables, the
2026-03-01 cutover date, and the snake_case regex) is ported verbatim from
the working evolution_extraction_pipeline notebook. None of it is
reinterpreted here.
"""

from __future__ import annotations

import re

import pandas as pd

from connectors.accounts.evolution.project_reports import SOURCE_COLUMNS, validate_columns

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


def build_combined(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine, classify, and snake_case per-company extracts into one DataFrame.

    Mirrors the notebook's run_pipeline(): combine GE + TLS, add
    `business_unit` (from `BusinessUnit` pre-rename), then convert every
    column name to snake_case.
    """
    combined = combine_data(datasets)

    combined["BusinessUnit"] = combined.apply(
        lambda row: determine_business_unit(row["company"], row["DDate"], row["CostType"]),
        axis=1,
    )

    combined.columns = [to_snake_case(column) for column in combined.columns]

    return combined
