from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from ge_data_platform.sources.evolution.project_reports import (
    SOURCE_COLUMNS,
    add_business_unit_classification,
    build_combined,
    build_raw,
    combine_data,
    determine_business_unit,
    determine_ge_business_unit,
    determine_tls_business_unit,
    extract_cost_code,
    to_snake_case,
)


def _row(**overrides: object) -> dict[str, object]:
    """One extracted row with every SOURCE_COLUMNS field defaulted, plus company."""
    base: dict[str, object] = {column: None for column in SOURCE_COLUMNS}
    base.update(
        {
            "AccountDescription": "COS/HIRE/Fuel/US$",
            "AccountTypeDescription": "Cost of Sales",
            "CostType": "2000/107/00/HIRE/US$ :COS/HIRE/Fuel/US$",
            "Credit": Decimal("0.0000"),
            "Customer": "",
            "CustomerUniqueID": "",
            "DDate": "2026-03-03",
            "Debit": Decimal("54.7080"),
            "Description": "",
            "FleetNumber": None,
            "Id": 1,
            "InclusiveAmount": Decimal("54.7080"),
            "Master_Sub_Account": "2000/107/00/HIRE/US$",
            "Module": "",
            "Project": "Project: D4950 - 4950-MANITOU DIESEL MI2.5T",
            "ProjectCode": "D4950",
            "ProjectName": "4950-MANITOU DIESEL MI2.5T",
            "QuantityInvoiced": 0.0,
            "Reference": "09394950",
            "TaxAmount": Decimal("0.0000"),
            "TransactionDescription": "",
        }
    )
    base.update(overrides)
    return base


def _dataset(company: str, rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a per-company DataFrame the way extract_company() does:
    company inserted as the first column, all SOURCE_COLUMNS present.
    """
    frame = pd.DataFrame(rows)
    frame.insert(0, "company", company)
    return frame


# ---------------------------------------------------------------------------
# Company assignment
# ---------------------------------------------------------------------------


def test_combine_data_preserves_each_row_original_company() -> None:
    ge = _dataset("GE", [_row(Id=1), _row(Id=2)])
    tls = _dataset("TLS", [_row(Id=1)])

    combined = combine_data({"GE": ge, "TLS": tls})

    assert list(combined["company"]) == ["GE", "GE", "TLS"]


def test_combine_data_rejects_a_dataset_missing_a_required_column() -> None:
    ge = _dataset("GE", [_row(Id=1)]).drop(columns=["CostType"])

    with pytest.raises(ValueError, match="missing columns"):
        combine_data({"GE": ge})


# ---------------------------------------------------------------------------
# GE + TLS combination
# ---------------------------------------------------------------------------


def test_combine_data_concatenates_ge_and_tls_row_counts() -> None:
    ge = _dataset("GE", [_row(Id=1), _row(Id=2), _row(Id=3)])
    tls = _dataset("TLS", [_row(Id=1), _row(Id=2)])

    combined = combine_data({"GE": ge, "TLS": tls})

    assert len(combined) == 5
    assert list(combined.columns) == ["company", *SOURCE_COLUMNS]


def test_combine_data_handles_a_single_source_without_the_other() -> None:
    ge = _dataset("GE", [_row(Id=1)])

    combined = combine_data({"GE": ge})

    assert len(combined) == 1
    assert combined.loc[0, "company"] == "GE"


# ---------------------------------------------------------------------------
# Business-unit classification
# ---------------------------------------------------------------------------


def test_extract_cost_code_returns_segment_after_first_slash() -> None:
    assert extract_cost_code("2000/101/00/-/US$") == "101"
    assert extract_cost_code("no-slash-here") == ""
    assert extract_cost_code(None) == ""


@pytest.mark.parametrize(
    ("d_date", "cost_type", "expected"),
    [
        ("2026-01-15", "1050001/anything", "Haulage"),
        ("2026-01-15", "1055001/anything", "Shunting"),
        ("2026-01-15", "1080001/anything", "Intrachem"),
        ("2026-01-15", "9999999/anything", "Unclassified"),
        ("2026-03-01", "2000/101/00/-/US$", "Haulage"),
        ("2026-06-01", "2000/955/00/-/US$", "Shunting"),
        ("2026-06-01", "2000/103/00/-/US$", "Intrachem"),
        ("2026-06-01", "2000/104/00/-/US$", "Lowbed"),
        ("2026-06-01", "2000/999/00/-/US$", "Unclassified"),
    ],
)
def test_determine_tls_business_unit_matches_notebook_rules(d_date: str, cost_type: str, expected: str) -> None:
    assert determine_tls_business_unit(d_date, cost_type) == expected


@pytest.mark.parametrize(
    ("d_date", "cost_type", "expected"),
    [
        ("2026-01-15", "2000/101/00/-/US$", "Unclassified"),
        ("2026-03-01", "2000/101/00/-/US$", "Hire"),
        ("2026-06-01", "2000/102/00/-/US$", "Hire"),
        ("2026-06-01", "2000/104/00/-/US$", "Commercial Whole Goods"),
        ("2026-06-01", "2000/105/00/-/US$", "Commercial Parts and Services"),
        ("2026-06-01", "2000/109/00/-/US$", "Commercial Parts and Services"),
        ("2026-06-01", "2000/110/00/-/US$", "Commercial Parts and Services"),
        ("2026-06-01", "2000/106/00/-/US$", "Commercial Training"),
        ("2026-06-01", "2000/999/00/-/US$", "Unclassified"),
    ],
)
def test_determine_ge_business_unit_matches_notebook_rules(d_date: str, cost_type: str, expected: str) -> None:
    assert determine_ge_business_unit(d_date, cost_type) == expected


def test_determine_business_unit_dispatches_on_company_case_insensitively() -> None:
    assert determine_business_unit("tls", "2026-06-01", "2000/101/00/-/US$") == "Haulage"
    assert determine_business_unit("ge", "2026-06-01", "2000/106/00/-/US$") == "Commercial Training"


def test_determine_business_unit_is_unclassified_for_unknown_company_or_missing_fields() -> None:
    assert determine_business_unit("OTHER", "2026-06-01", "2000/101/00/-/US$") == "Unclassified"
    assert determine_business_unit(pd.NA, "2026-06-01", "2000/101/00/-/US$") == "Unclassified"
    assert determine_business_unit("GE", pd.NA, "2000/101/00/-/US$") == "Unclassified"
    assert determine_business_unit("GE", "2026-06-01", pd.NA) == "Unclassified"


# ---------------------------------------------------------------------------
# snake_case conversion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        ("company", "company"),
        ("AccountDescription", "account_description"),
        ("DDate", "d_date"),
        ("CustomerUniqueID", "customer_unique_id"),
        ("Master_Sub_Account", "master_sub_account"),
        ("QuantityInvoiced", "quantity_invoiced"),
        ("BusinessUnit", "business_unit"),
    ],
)
def test_to_snake_case_matches_notebook_output(column: str, expected: str) -> None:
    assert to_snake_case(column) == expected


def test_build_combined_produces_the_exact_final_column_set() -> None:
    ge = _dataset("GE", [_row(Id=1)])
    tls = _dataset("TLS", [_row(Id=1)])

    combined = build_combined({"GE": ge, "TLS": tls})

    assert list(combined.columns) == [
        "company",
        "account_description",
        "account_type_description",
        "cost_type",
        "credit",
        "customer",
        "customer_unique_id",
        "d_date",
        "debit",
        "description",
        "fleet_number",
        "id",
        "inclusive_amount",
        "master_sub_account",
        "module",
        "project",
        "project_code",
        "project_name",
        "quantity_invoiced",
        "reference",
        "tax_amount",
        "transaction_description",
        "business_unit",
    ]


# ---------------------------------------------------------------------------
# Accounting decimal handling
# ---------------------------------------------------------------------------


def test_money_columns_stay_python_decimal_through_combine_and_classify() -> None:
    ge = _dataset(
        "GE",
        [
            _row(
                Id=1,
                Credit=Decimal("100.1241"),
                Debit=Decimal("0.0000"),
                InclusiveAmount=Decimal("100.1241"),
                TaxAmount=Decimal("12.3456"),
            )
        ],
    )

    combined = build_combined({"GE": ge})

    for column in ("credit", "debit", "inclusive_amount", "tax_amount"):
        value = combined.loc[0, column]
        assert isinstance(value, Decimal), f"{column} was coerced to {type(value)}"

    assert combined.loc[0, "inclusive_amount"] == Decimal("100.1241")


def test_money_columns_are_not_rounded_or_altered_by_the_pipeline() -> None:
    exact_value = Decimal("47.1504")
    ge = _dataset("GE", [_row(Id=1, Debit=exact_value)])

    combined = build_combined({"GE": ge})

    assert combined.loc[0, "debit"] == exact_value
    assert str(combined.loc[0, "debit"]) == "47.1504"


# ---------------------------------------------------------------------------
# raw/staging split (build_raw + add_business_unit_classification) --
# platform target's raw_evolution/stg_evolution layering. See
# docs/migration/legacy-to-platform-migration.md#evolution-migration-completed.
# ---------------------------------------------------------------------------


def test_build_raw_has_no_business_unit_column() -> None:
    ge = _dataset("GE", [_row(Id=1)])

    raw_df = build_raw({"GE": ge})

    assert "business_unit" not in raw_df.columns
    assert list(raw_df.columns) == ["company", *[to_snake_case(c) for c in SOURCE_COLUMNS]]


def test_build_raw_is_snake_case_and_preserves_row_count() -> None:
    ge = _dataset("GE", [_row(Id=1), _row(Id=2)])
    tls = _dataset("TLS", [_row(Id=1)])

    raw_df = build_raw({"GE": ge, "TLS": tls})

    assert len(raw_df) == 3
    assert "d_date" in raw_df.columns and "DDate" not in raw_df.columns


def test_add_business_unit_classification_matches_build_combined_exactly() -> None:
    # build_combined is now build_raw + add_business_unit_classification
    # composed -- prove the two-step platform path produces byte-identical
    # output to the still-used legacy one-step path.
    ge = _dataset("GE", [_row(Id=1, DDate="2026-06-01", CostType="2000/101/00/-/US$")])
    tls = _dataset("TLS", [_row(Id=1, DDate="2026-01-15", CostType="1050001/anything")])
    datasets = {"GE": ge, "TLS": tls}

    raw_df = build_raw(datasets)
    staged_df = add_business_unit_classification(raw_df)
    combined_df = build_combined(datasets)

    pd.testing.assert_frame_equal(staged_df, combined_df)


def test_add_business_unit_classification_appends_business_unit_last() -> None:
    ge = _dataset("GE", [_row(Id=1, DDate="2026-06-01", CostType="2000/106/00/-/US$")])
    raw_df = build_raw({"GE": ge})

    staged_df = add_business_unit_classification(raw_df)

    assert list(staged_df.columns)[-1] == "business_unit"
    assert staged_df.loc[0, "business_unit"] == "Commercial Training"


def test_add_business_unit_classification_does_not_mutate_its_input() -> None:
    ge = _dataset("GE", [_row(Id=1)])
    raw_df = build_raw({"GE": ge})

    add_business_unit_classification(raw_df)

    assert "business_unit" not in raw_df.columns
