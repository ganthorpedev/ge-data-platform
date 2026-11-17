"""GE Data Control Center: the GE Data Platform's own orchestration system.

Pipeline registry and versioning, DAG execution, job queue, workers, retries,
recovery and audit, built on PostgreSQL. This package must never import
Dagster or ``ge_data_platform.orchestration`` (the legacy Dagster code);
``tests/control_center/test_no_dagster.py`` enforces that.
"""

__version__ = "0.1.0"
