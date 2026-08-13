from __future__ import annotations

import pandas as pd
import pytest

from ge_data_platform.common.database import validate_combined_for_full_replace


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
