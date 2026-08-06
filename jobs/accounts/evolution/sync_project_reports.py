"""Entry point for the Accounts/Evolution Project Reports sync job.

Run with:
    python -m jobs.accounts.evolution.sync_project_reports

This orchestrates: extract (connectors.accounts.evolution.project_reports)
-> transform (transforms.accounts.evolution.project_reports_transform) ->
load (loaders.postgres_loader), and records the run in etl.sync_runs, the
same bookkeeping tables the telemetry providers use.
"""

from __future__ import annotations

import argparse
import logging

from config.settings import get_etl_ops_settings, get_evolution_settings, get_settings
from connectors.accounts.evolution.project_reports import extract_all
from loaders.postgres_loader import PostgresLoader, finish_sync_run_failed_safe
from transforms.accounts.evolution.project_reports_transform import build_combined
from utils.logging_config import configure_logging

SOURCE_SYSTEM = "evolution_project_reports"
JOB_NAME = "evolution_project_reports_sync"
logger = logging.getLogger(__name__)


def run() -> None:
    """Execute one Evolution Project Reports sync: extract, transform, load, record."""
    configure_logging()
    settings = get_settings()
    evolution_settings = get_evolution_settings()
    ops_settings = get_etl_ops_settings()

    loader = PostgresLoader(settings)

    sync_run_id = loader.start_sync_run(
        source_system=SOURCE_SYSTEM,
        job_name=JOB_NAME,
        start_date=None,
        end_date=None,
    )
    logger.info("sync_run_id: %s", sync_run_id)

    try:
        logger.info(
            "Extracting Evolution project reports for %s source(s)",
            len(evolution_settings.sources),
        )
        datasets = extract_all(evolution_settings)
        rows_fetched = sum(len(dataset) for dataset in datasets.values())

        combined = build_combined(datasets)
        logger.info("Combined row count: %s", f"{len(combined):,}")

        load_counts = loader.replace_accounts_evolution_project_reports(
            combined, sync_run_id=sync_run_id, provider=SOURCE_SYSTEM
        )
        rows_loaded = sum(load_counts.values())
        for table_name, row_count in load_counts.items():
            logger.info("  %s: %s rows loaded (full replace)", table_name, f"{row_count:,}")

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
    """Parse arguments (none yet -- full extract every run) and run the sync."""
    parser = argparse.ArgumentParser(description="Accounts/Evolution Project Reports sync")
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
