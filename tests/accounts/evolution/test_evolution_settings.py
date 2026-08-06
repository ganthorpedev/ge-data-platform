from __future__ import annotations

import pytest

from config.settings import validate_evolution_view_name


@pytest.mark.parametrize(
    ("view_name", "expected"),
    [
        ("dbo.vwProjectsReports", "[dbo].[vwProjectsReports]"),
        ("dbo.vwProjectsReports  ", "[dbo].[vwProjectsReports]"),
        ("  dbo.vwProjectsReports", "[dbo].[vwProjectsReports]"),
        ("_dbo._vw_reports", "[_dbo].[_vw_reports]"),
        ("dbo.Report123", "[dbo].[Report123]"),
    ],
)
def test_validate_evolution_view_name_accepts_plain_schema_qualified_names(
    view_name: str, expected: str
) -> None:
    assert validate_evolution_view_name(view_name) == expected


@pytest.mark.parametrize(
    "view_name",
    [
        "dbo.vwProjectsReports; DROP TABLE dbo.vwProjectsReports--",
        "dbo.vwProjectsReports UNION SELECT * FROM sys.tables",
        "dbo.vwProjectsReports -- comment",
        "dbo.vwProjectsReports/*comment*/",
        "vwProjectsReports",  # missing schema part
        "dbo.schema.vwProjectsReports",  # too many parts
        "dbo..vwProjectsReports",
        "[dbo].[vwProjectsReports]",  # brackets not accepted as input
        "dbo.vw Projects Reports",  # embedded whitespace
        "dbo.vwProjectsReports;",
        "dbo.'vwProjectsReports'",
        "1dbo.vwProjectsReports",  # part cannot start with a digit
        "",
        "   ",
    ],
)
def test_validate_evolution_view_name_rejects_anything_else(view_name: str) -> None:
    with pytest.raises(ValueError, match="schema-qualified identifier"):
        validate_evolution_view_name(view_name)


def test_validate_evolution_view_name_rejects_non_string_input() -> None:
    with pytest.raises(ValueError, match="must be a string"):
        validate_evolution_view_name(None)  # type: ignore[arg-type]
