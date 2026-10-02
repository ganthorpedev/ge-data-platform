"""Tests for scripts/bootstrap_core_assets.py.

Runs the real validate -> load -> validate -> commit flow against an
in-memory SQLite `core` schema shaped like
sql/migrations/013_create_core_asset_model.sql and
sql/migrations/014_add_asset_type_and_asset_attributes.sql (see
_core_asset_sqlite_helpers.build_sqlite_core_engine) -- never against a real
PostgreSQL server. Covers pre-load validation, reference-dimension
deduplication, the nullable-make_key dim_model uniqueness rule, the
asset_type dimension and its FK integrity, stage/size passthrough, make/model
consistency enforcement, idempotent rerun, and rollback-on-failure.
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import text

from scripts.bootstrap_core_assets import (
    BootstrapValidationError,
    bootstrap_core_assets,
    build_asset_rows,
    load_source_csv,
    validate_source,
)

from tests.platform._core_asset_sqlite_helpers import build_sqlite_core_engine

BASE_COLUMNS = [
    "asset_number",
    "partner_code",
    "serial_number",
    "brand",
    "model",
    "owner",
    "state",
    "asset_category",
    "asset_type",
    "stage",
    "size",
    "sale_year",
    "dispersal_date",
]


def _row(asset_number, **overrides) -> dict:
    row = {
        "asset_number": asset_number,
        "partner_code": None,
        "serial_number": None,
        "brand": "Toyota",
        "model": "Hilux",
        "owner": "Ganthorpe Enterprises",
        "state": "Hire",
        "asset_category": "Transport",
        "asset_type": "Pickup Truck",
        "stage": None,
        "size": None,
        "sale_year": 2022,
        "dispersal_date": None,
    }
    row.update(overrides)
    return row


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=BASE_COLUMNS)


def _write_csv(tmp_path, rows: list[dict]) -> str:
    path = tmp_path / "asset_master_bootstrap.csv"
    _df(rows).to_csv(path, index=False)
    return str(path)


# ---------------------------------------------------------------------------
# load_source_csv: file existence
# ---------------------------------------------------------------------------


def test_load_source_csv_rejects_missing_file(tmp_path) -> None:
    with pytest.raises(BootstrapValidationError, match="does not exist"):
        load_source_csv(tmp_path / "does_not_exist.csv")


def test_load_source_csv_rejects_directory(tmp_path) -> None:
    with pytest.raises(BootstrapValidationError, match="not a file"):
        load_source_csv(tmp_path)


# ---------------------------------------------------------------------------
# validate_source: pre-load validation
# ---------------------------------------------------------------------------


def test_validate_source_rejects_missing_required_column() -> None:
    df = _df([_row(1)]).drop(columns=["brand"])
    with pytest.raises(BootstrapValidationError, match="missing required column"):
        validate_source(df)


def test_validate_source_rejects_empty_input() -> None:
    df = _df([])
    with pytest.raises(BootstrapValidationError, match="zero rows"):
        validate_source(df)


def test_validate_source_rejects_missing_asset_number() -> None:
    df = _df([_row(None), _row(2)])
    with pytest.raises(BootstrapValidationError, match="missing asset_number"):
        validate_source(df)


def test_validate_source_rejects_duplicate_asset_number() -> None:
    df = _df([_row(1), _row(1)])
    with pytest.raises(BootstrapValidationError, match="duplicated"):
        validate_source(df)


def test_validate_source_accepts_unique_populated_asset_numbers() -> None:
    df = _df([_row(1), _row(2), _row(3)])
    validate_source(df)  # must not raise


@pytest.mark.parametrize("token", ["unknown", "Unknown", "N/A", "n/a", "NULL", ""])
def test_validate_source_rejects_leftover_fake_null_tokens(token) -> None:
    df = _df([_row(1, brand=token if token else None)])
    if token == "":
        # An empty string is what a real leftover fake-null looks like on
        # disk (as opposed to a true CSV blank, which pandas reads as NaN).
        df.loc[0, "brand"] = ""
    with pytest.raises(BootstrapValidationError, match="fake-null"):
        validate_source(df)


def test_validate_source_rejects_invalid_sale_year() -> None:
    df = _df([_row(1, sale_year=1500), _row(2, sale_year=2022)])
    with pytest.raises(BootstrapValidationError, match="sale_year"):
        validate_source(df)


def test_validate_source_rejects_non_integer_sale_year() -> None:
    df = _df([_row(1, sale_year=2020.5)])
    with pytest.raises(BootstrapValidationError, match="sale_year"):
        validate_source(df)


def test_validate_source_accepts_null_sale_year() -> None:
    df = _df([_row(1, sale_year=None)])
    validate_source(df)  # must not raise


def test_validate_source_rejects_unparseable_dispersal_date() -> None:
    df = _df([_row(1, dispersal_date="not-a-date")])
    with pytest.raises(BootstrapValidationError, match="dispersal_date"):
        validate_source(df)


def test_validate_source_accepts_null_dispersal_date() -> None:
    df = _df([_row(1, dispersal_date=None)])
    validate_source(df)  # must not raise


# ---------------------------------------------------------------------------
# full bootstrap flow (SQLite core schema)
# ---------------------------------------------------------------------------


def test_bootstrap_loads_assets_and_reports_accurate_counts(tmp_path) -> None:
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, brand="Toyota", model="Hilux", owner="Ganthorpe Enterprises", state="Hire", asset_category="Transport"),
            _row(2, brand="Manitou", model="MI 25 D", owner="Intrachem", state="Sale", asset_category="Material Handling"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["source_rows"] == 2
    assert report["assets_processed"] == 2
    assert report["makes"] == 2
    assert report["models"] == 2
    assert report["owners"] == 2
    assert report["usage_types"] == 2
    assert report["asset_categories"] == 2
    assert report["duplicate_asset_numbers"] == 0
    assert report["unresolved_references"] == 0
    assert report["broken_foreign_keys"] == 0
    assert report["make_model_mismatches"] == 0
    assert report["status"] == "SUCCESS"

    with engine.connect() as conn:
        loaded = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset")).scalar_one()
    assert loaded == 2


def test_bootstrap_deduplicates_reference_dimensions(tmp_path) -> None:
    # Three assets share the same brand/owner/state/category -- the
    # dimension tables must end up with exactly one row each, not three.
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, brand="Toyota", model="Hilux"),
            _row(2, brand="Toyota", model="Hilux"),
            _row(3, brand="Toyota", model="Corolla"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["makes"] == 1
    assert report["models"] == 2  # Hilux, Corolla
    assert report["owners"] == 1
    with engine.connect() as conn:
        make_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_make")).scalar_one()
    assert make_count == 1


def test_bootstrap_leaves_reference_keys_null_when_source_value_missing(tmp_path) -> None:
    csv_path = _write_csv(
        tmp_path,
        [
            _row(
                1,
                brand=None,
                model=None,
                owner="Ganthorpe Enterprises",
                state="Hire",
                asset_category=None,
                asset_type=None,
            )
        ],
    )
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT make_key, model_key, asset_category_key, asset_type_key "
                "FROM core.dim_asset WHERE asset_number = '1'"
            )
        ).mappings().one()
    assert row["make_key"] is None
    assert row["model_key"] is None
    assert row["asset_category_key"] is None
    assert row["asset_type_key"] is None


# ---------------------------------------------------------------------------
# core.dim_asset_type dimension
# ---------------------------------------------------------------------------


def test_bootstrap_loads_asset_type_dimension_and_resolves_key(tmp_path) -> None:
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, asset_type="Forklift"),
            _row(2, asset_type="Telehandler"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["asset_types"] == 2
    with engine.connect() as conn:
        asset_type_name = conn.execute(
            text("""
                SELECT t.asset_type_name FROM core.dim_asset a
                JOIN core.dim_asset_type t ON t.asset_type_key = a.asset_type_key
                WHERE a.asset_number = '1'
            """)
        ).scalar_one()
    assert asset_type_name == "Forklift"


def test_bootstrap_deduplicates_asset_type_dimension(tmp_path) -> None:
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, asset_type="Forklift"),
            _row(2, asset_type="Forklift"),
            _row(3, asset_type="TLB"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["asset_types"] == 2
    with engine.connect() as conn:
        asset_type_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset_type")).scalar_one()
    assert asset_type_count == 2


def test_bootstrap_asset_type_stays_distinct_from_asset_category(tmp_path) -> None:
    # asset_category ("broad business category") and asset_type ("physical
    # asset type/class") must never be collapsed into one dimension.
    csv_path = _write_csv(
        tmp_path,
        [_row(1, asset_category="Material Handling", asset_type="Forklift")],
    )
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        category_names = conn.execute(text("SELECT asset_category_name FROM core.dim_asset_category")).scalars().all()
        type_names = conn.execute(text("SELECT asset_type_name FROM core.dim_asset_type")).scalars().all()
    assert category_names == ["Material Handling"]
    assert type_names == ["Forklift"]


def test_validate_post_load_rejects_broken_asset_type_foreign_key(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1, asset_type="Forklift")])
    engine = build_sqlite_core_engine()
    bootstrap_core_assets(csv_path, engine)

    # Corrupt the loaded row's asset_type_key to point at a non-existent
    # dim_asset_type row, then re-run post-load validation directly.
    from scripts.bootstrap_core_assets import validate_post_load

    with engine.begin() as conn:
        conn.execute(text("UPDATE core.dim_asset SET asset_type_key = 999999 WHERE asset_number = '1'"))
        with pytest.raises(BootstrapValidationError, match="broken foreign key"):
            validate_post_load(conn, [{"asset_number": "1"}])


# ---------------------------------------------------------------------------
# stage / size passthrough (fixed dim_asset attributes, not dimensions)
# ---------------------------------------------------------------------------


def test_bootstrap_preserves_stage_and_size_exactly(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1, stage="2.5t / 3s", size=2.5)])
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        row = conn.execute(text("SELECT stage, size FROM core.dim_asset WHERE asset_number = '1'")).mappings().one()
    assert row["stage"] == "2.5t / 3s"
    assert row["size"] == 2.5


def test_bootstrap_leaves_stage_and_size_null_when_missing(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1, stage=None, size=None)])
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        row = conn.execute(text("SELECT stage, size FROM core.dim_asset WHERE asset_number = '1'")).mappings().one()
    assert row["stage"] is None
    assert row["size"] is None


def test_validate_source_rejects_negative_size() -> None:
    df = _df([_row(1, size=-5.0)])
    with pytest.raises(BootstrapValidationError, match="size"):
        validate_source(df)


def test_validate_source_rejects_non_numeric_size() -> None:
    df = _df([_row(1, size="lots")])
    with pytest.raises(BootstrapValidationError, match="size"):
        validate_source(df)


# ---------------------------------------------------------------------------
# asset_type column currently missing from the real approved snapshot
# ---------------------------------------------------------------------------


def test_validate_source_rejects_input_missing_asset_type_column() -> None:
    # Mirrors the real data/local/assets/asset_master_bootstrap.csv gap as
    # of this version: the approved cleaned snapshot has no asset_type
    # classification yet. The loader must refuse to run rather than
    # silently leaving core.dim_asset_type empty or guessing from `stage`.
    df = _df([_row(1)]).drop(columns=["asset_type"])
    with pytest.raises(BootstrapValidationError, match="asset_type"):
        validate_source(df)


def test_bootstrap_allows_same_model_name_under_different_makes(tmp_path) -> None:
    # Mirrors the real register's CPD30-GB2LI conflict: same model string,
    # different brands -- both must be modeled as distinct dim_model rows.
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, brand="Heli", model="CPD30-GB2LI"),
            _row(2, brand="Toyota", model="CPD30-GB2LI"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["models"] == 2
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT make_key FROM core.dim_model WHERE model_name = 'CPD30-GB2LI'")
        ).scalars().all()
    assert len(rows) == 2
    assert rows[0] != rows[1]


def test_bootstrap_prevents_duplicate_make_less_models(tmp_path) -> None:
    # Two assets share a model name with NO brand on either row -- must
    # resolve to the SAME single make-less dim_model row, not two.
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, brand=None, model="Bucket"),
            _row(2, brand=None, model="Bucket"),
        ],
    )
    engine = build_sqlite_core_engine()

    report = bootstrap_core_assets(csv_path, engine)

    assert report["models"] == 1
    with engine.connect() as conn:
        model_keys = conn.execute(
            text("SELECT DISTINCT model_key FROM core.dim_asset WHERE asset_number IN ('1', '2')")
        ).scalars().all()
        model_row_count = conn.execute(
            text("SELECT COUNT(*) FROM core.dim_model WHERE model_name = 'Bucket'")
        ).scalar_one()
    assert len(model_keys) == 1
    assert model_row_count == 1


def test_bootstrap_does_not_invent_unknown_make_for_make_less_model(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1, brand=None, model="Bucket")])
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        make_names = conn.execute(text("SELECT make_name FROM core.dim_make")).scalars().all()
    assert make_names == []  # no synthetic "Unknown" make row created


# ---------------------------------------------------------------------------
# make/model mismatch rejection
# ---------------------------------------------------------------------------


def test_build_asset_rows_rejects_make_model_mismatch() -> None:
    # A deliberately inconsistent dims map: the model was resolved under
    # make_key=1, but the asset's own brand resolves to make_key=2. This
    # simulates the resolution bug the defensive check exists to catch.
    df = _df([_row(1, brand="Toyota", model="Hilux")])
    dims = {
        "makes": {"Toyota": 2},
        "owners": {"Ganthorpe Enterprises": 1},
        "usage_types": {"Hire": 1},
        "asset_categories": {"Transport": 1},
        "asset_types": {"Pickup Truck": 1},
        # Registered under make_key=1 (not Toyota's real key, 2) so the
        # asset's resolved make_key (2) won't match the model's (1).
        "models": {(2, "Hilux"): 99},
    }
    # Force the mismatch: model lookup key must match (asset_make_key,
    # model_name), so make the map resolvable but internally inconsistent
    # via a manually patched model_key_to_make_key equivalent -- simplest is
    # to register the SAME model_key under two different make_key entries,
    # which build_asset_rows's reverse map cannot happen to agree with.
    dims["models"] = {(2, "Hilux"): 99, (1, "Hilux"): 99}

    with pytest.raises(BootstrapValidationError, match="make_key does not match"):
        build_asset_rows(df, dims)


def test_bootstrap_rolls_back_entirely_on_make_model_mismatch(tmp_path, monkeypatch) -> None:
    csv_path = _write_csv(tmp_path, [_row(1), _row(2, brand="Manitou", model="MI 25 D")])
    engine = build_sqlite_core_engine()

    import scripts.bootstrap_core_assets as bootstrap_module

    original_build_asset_rows = bootstrap_module.build_asset_rows

    def _poison_second_call(df, dims):
        # Corrupt the models map so build_asset_rows detects a mismatch,
        # forcing the transaction to roll back after dimensions were staged.
        poisoned = dict(dims)
        poisoned["models"] = {key: 999999 for key in dims["models"]}
        return original_build_asset_rows(df, poisoned)

    monkeypatch.setattr(bootstrap_module, "build_asset_rows", _poison_second_call)

    with pytest.raises(BootstrapValidationError):
        bootstrap_module.bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        asset_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset")).scalar_one()
        # Reference dimensions were inserted before the mismatch was
        # detected inside the same transaction -- they must be rolled back
        # too, since the whole run is one transaction.
        make_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_make")).scalar_one()
    assert asset_count == 0
    assert make_count == 0


# ---------------------------------------------------------------------------
# idempotent rerun
# ---------------------------------------------------------------------------


def test_bootstrap_rerun_is_idempotent(tmp_path) -> None:
    csv_path = _write_csv(
        tmp_path,
        [
            _row(1, brand="Toyota", model="Hilux", owner="Ganthorpe Enterprises"),
            _row(2, brand="Manitou", model="MI 25 D", owner="Intrachem"),
        ],
    )
    engine = build_sqlite_core_engine()

    first_report = bootstrap_core_assets(csv_path, engine)
    second_report = bootstrap_core_assets(csv_path, engine)

    assert first_report["assets_processed"] == second_report["assets_processed"] == 2
    assert first_report["makes"] == second_report["makes"] == 2
    assert first_report["models"] == second_report["models"] == 2
    assert first_report["owners"] == second_report["owners"] == 2

    with engine.connect() as conn:
        asset_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset")).scalar_one()
        make_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_make")).scalar_one()
        model_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_model")).scalar_one()
        owner_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_owner")).scalar_one()
    assert asset_count == 2  # no duplicate logical assets
    assert make_count == 2
    assert model_count == 2
    assert owner_count == 2


def test_bootstrap_rerun_updates_changed_values_on_same_asset_number(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1, owner="Ganthorpe Enterprises")])
    engine = build_sqlite_core_engine()

    bootstrap_core_assets(csv_path, engine)

    revised_csv_path = _write_csv(tmp_path, [_row(1, owner="Intrachem")])
    bootstrap_core_assets(revised_csv_path, engine)

    with engine.connect() as conn:
        asset_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset")).scalar_one()
        owner_name = conn.execute(
            text("""
                SELECT o.owner_name FROM core.dim_asset a
                JOIN core.dim_owner o ON o.owner_key = a.owner_key
                WHERE a.asset_number = '1'
            """)
        ).scalar_one()
    assert asset_count == 1  # updated in place, not duplicated
    assert owner_name == "Intrachem"


# ---------------------------------------------------------------------------
# rollback / no partial writes on failure
# ---------------------------------------------------------------------------


def test_bootstrap_makes_zero_database_changes_when_pre_load_validation_fails(tmp_path) -> None:
    csv_path = _write_csv(tmp_path, [_row(1), _row(1)])  # duplicate asset_number
    engine = build_sqlite_core_engine()

    with pytest.raises(BootstrapValidationError, match="duplicated"):
        bootstrap_core_assets(csv_path, engine)

    with engine.connect() as conn:
        asset_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_asset")).scalar_one()
        make_count = conn.execute(text("SELECT COUNT(*) FROM core.dim_make")).scalar_one()
    assert asset_count == 0
    assert make_count == 0
