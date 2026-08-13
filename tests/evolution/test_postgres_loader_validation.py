from __future__ import annotations

import pandas as pd
import pytest

from ge_data_platform.common.database import (
    validate_combined_for_full_replace,
    validate_project_report_batch_for_platform_load,
)


def _valid_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "company": ["GE", "GE", "TLS"],
            "id": [1, 2, 1],
            "credit": [0, 0, 0],
        }
    )


def test_valid_dataframe_passes_without_raising() -> None:
    validate_combined_for_full_replace(_valid_df())  # must not raise


def test_empty_dataframe_is_refused() -> None:
    with pytest.raises(ValueError, match="unexpectedly empty"):
        validate_combined_for_full_replace(pd.DataFrame(columns=["company", "id"]))


def test_missing_required_columns_are_refused() -> None:
    df = pd.DataFrame({"company": ["GE"], "credit": [0]})  # no "id" column
    with pytest.raises(ValueError, match="missing required column"):
        validate_combined_for_full_replace(df)


def test_null_company_is_refused() -> None:
    df = _valid_df()
    df.loc[0, "company"] = None
    with pytest.raises(ValueError, match="missing company"):
        validate_combined_for_full_replace(df)


def test_blank_company_is_refused() -> None:
    df = _valid_df()
    df.loc[0, "company"] = "   "
    with pytest.raises(ValueError, match="missing company"):
        validate_combined_for_full_replace(df)


def test_null_id_is_refused() -> None:
    df = _valid_df()
    df.loc[0, "id"] = None
    with pytest.raises(ValueError, match="missing id"):
        validate_combined_for_full_replace(df)


def test_duplicate_company_id_pair_is_refused() -> None:
    df = pd.DataFrame({"company": ["GE", "GE"], "id": [1, 1], "credit": [0, 0]})
    with pytest.raises(ValueError, match="duplicate \\(company, id\\) key"):
        validate_combined_for_full_replace(df)


def test_same_id_different_company_is_not_a_duplicate() -> None:
    df = pd.DataFrame({"company": ["GE", "TLS"], "id": [1, 1], "credit": [0, 0]})
    validate_combined_for_full_replace(df)  # must not raise


# ---------------------------------------------------------------------------
# validate_project_report_batch_for_platform_load -- the platform target's
# corrected counterpart. Unlike validate_combined_for_full_replace above,
# duplicate (company, id) pairs -- and even fully duplicate rows -- are
# expected and must NOT be refused: live read-only inspection of
# dbo.vwProjectsReports showed `id` is a transaction-type code (11 distinct
# values total), not a per-row identifier, so there is no natural key to
# enforce. See sql/migrations/011_create_raw_evolution.sql.
# ---------------------------------------------------------------------------


def test_platform_validation_accepts_duplicate_company_id_pairs() -> None:
    df = pd.DataFrame({"company": ["GE", "GE", "GE"], "id": ["Inv", "Inv", "Inv"], "credit": [0, 0, 0]})
    validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")  # must not raise


def test_platform_validation_accepts_fully_identical_duplicate_rows() -> None:
    df = pd.DataFrame({"company": ["GE", "GE"], "id": ["Inv", "Inv"], "credit": [10, 10]})
    validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")  # must not raise


def test_platform_validation_refuses_empty_batch() -> None:
    with pytest.raises(ValueError, match="unexpectedly empty"):
        validate_project_report_batch_for_platform_load(
            pd.DataFrame(columns=["company", "id"]), dataset_name="raw_evolution.project_report"
        )


def test_platform_validation_refuses_missing_company_column() -> None:
    df = pd.DataFrame({"id": ["Inv"], "credit": [0]})
    with pytest.raises(ValueError, match="missing required column 'company'"):
        validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")


def test_platform_validation_refuses_null_company() -> None:
    df = pd.DataFrame({"company": ["GE", None], "id": ["Inv", "JL"]})
    with pytest.raises(ValueError, match="missing company"):
        validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")


def test_platform_validation_refuses_blank_company() -> None:
    df = pd.DataFrame({"company": ["GE", "   "], "id": ["Inv", "JL"]})
    with pytest.raises(ValueError, match="missing company"):
        validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")


def test_platform_validation_does_not_require_an_id_column() -> None:
    # Unlike the legacy validator, id is not part of any enforced key here.
    df = pd.DataFrame({"company": ["GE"], "credit": [0]})
    validate_project_report_batch_for_platform_load(df, dataset_name="raw_evolution.project_report")  # must not raise
