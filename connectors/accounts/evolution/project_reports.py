"""Extracts Evolution Project Reports from GE and TLS via a shared code path.

This module is the only place that should run the project-reports SQL
query. It reads sql/accounts/evolution/project_reports/extract_project_reports.sql
once, then executes it against every configured Evolution source database
(config.settings.EvolutionSettings.sources) through the same function --
there is no per-company extraction pipeline. Transform modules must not
perform any I/O; loader modules must not know how to talk to SQL Server.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.settings import EvolutionSettings, validate_evolution_view_name
from connectors.accounts.evolution.connection import sql_server_connection

logger = logging.getLogger(__name__)

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

_SQL_PATH = Path(__file__).resolve().parents[3] / "sql" / "accounts" / "evolution" / "project_reports" / "extract_project_reports.sql"


def _load_query(view_name: str) -> str:
    """Read the extraction .sql file and substitute a validated view name.

    `view_name` is re-validated here (in addition to config.settings.
    get_evolution_settings()'s fail-fast check) immediately before it is
    substituted into dynamically constructed SQL, and the bracket-quoted
    form returned by the validator -- never the raw input -- is what
    actually reaches the query text.
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
