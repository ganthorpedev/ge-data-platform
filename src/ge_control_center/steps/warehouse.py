"""Warehouse steps.

``warehouse.validate_ge_warehouse_baseline`` runs the existing, read-only
``sql/validation/validate_ge_warehouse_baseline.sql`` against the warehouse
inside a ``READ ONLY`` transaction (PostgreSQL rejects any write), then
rolls back. It is the first real warehouse step and is safe to run
repeatedly.
"""

from __future__ import annotations

from sqlalchemy import text

from ge_control_center.config import PROJECT_ROOT
from ge_control_center.registry import PermanentStepError, StepContext, StepParams, step

BASELINE_VALIDATION_SQL = PROJECT_ROOT / "sql" / "validation" / "validate_ge_warehouse_baseline.sql"


class ValidateBaselineParams(StepParams):
    fail_on_check_failure: bool = True


def split_sql_statements(sql_text: str) -> list[str]:
    """Split on ';' after dropping full-line comments.

    Sufficient for the repository's validation packs (no semicolons inside
    literals or dollar-quoted blocks); not a general SQL parser.
    """
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


@step(
    key="warehouse.validate_ge_warehouse_baseline",
    version="1",
    display_name="Validate ge_warehouse baseline",
    description=(
        "Runs sql/validation/validate_ge_warehouse_baseline.sql in a READ ONLY transaction and "
        "fails if any check reports FAIL."
    ),
    params=ValidateBaselineParams,
    idempotency="read_only",
    max_attempts=3,
    retry_backoff_seconds=30,
    retry_backoff_max_seconds=300,
    timeout_seconds=600,
)
def validate_ge_warehouse_baseline(ctx: StepContext, params: ValidateBaselineParams) -> dict:
    statements = split_sql_statements(BASELINE_VALIDATION_SQL.read_text(encoding="utf-8"))
    checks: list[dict] = []

    with ctx.warehouse_engine().connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        for statement in statements:
            result = conn.execute(text(statement))
            if not result.returns_rows:
                continue
            for row in result.mappings():
                check = {key: (str(value) if value is not None else None) for key, value in row.items()}
                checks.append(check)
                ctx.log.info("%s  %s", check.get("status"), check.get("check_name"))
        conn.rollback()

    failed = [c.get("check_name") for c in checks if c.get("status") != "PASS"]
    summary = {"checks": len(checks), "passed": len(checks) - len(failed), "failed": len(failed), "failed_checks": failed}
    ctx.log.info("baseline validation: %d checks, %d failed", len(checks), len(failed))

    if failed and params.fail_on_check_failure:
        # Retrying cannot fix a failed structural check.
        raise PermanentStepError(f"{len(failed)} baseline check(s) failed: {', '.join(map(str, failed))}")
    return summary
