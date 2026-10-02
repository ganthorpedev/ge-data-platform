"""One-time bootstrap loader: approved cleaned asset snapshot -> core asset model.

Loads the approved, cleaned GE asset register CSV (see
notebooks/core/asset/01_asset_master_profile.ipynb for the profiling that
approved it) into the core asset tables created by
sql/migrations/013_create_core_asset_model.sql and
sql/migrations/014_add_asset_type_and_asset_attributes.sql:

    approved cleaned asset snapshot
                ↓
    core.dim_make / core.dim_model / core.dim_owner / core.dim_usage_type /
    core.dim_asset_category / core.dim_asset_type
                ↓
    core.dim_asset

This is a one-time bootstrap tool, not a scheduled pipeline: no Dagster job,
no raw/staging tables, no source API calls. It is safe to rerun against the
same (possibly revised) snapshot -- see `upsert_assets` -- but is intended to
be run by a human, deliberately, from the command line.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python scripts/bootstrap_core_assets.py "data\\local\\assets\\asset_master_bootstrap.csv"

Source -> core column mapping:

    brand            -> core.dim_make
    model            -> core.dim_model (scoped to the row's resolved make_key)
    owner            -> core.dim_owner
    state            -> core.dim_usage_type
    asset_category   -> core.dim_asset_category
    asset_type       -> core.dim_asset_type
    stage            -> core.dim_asset.stage    (fixed attribute, not a dimension)
    size             -> core.dim_asset.size     (fixed attribute, not a dimension)

    asset_number     -> core.dim_asset.asset_number  (canonical business key)
    partner_code     -> core.dim_asset.partner_code
    serial_number    -> core.dim_asset.serial_number
    sale_year        -> core.dim_asset.sale_year
    dispersal_date   -> core.dim_asset.dispersal_date

`asset_category` (broad business category, e.g. Material Handling) and
`asset_type` (physical asset type/class, e.g. Forklift, Telehandler, TLB)
are deliberately kept as two separate dimensions -- never collapsed into
one. `stage` and `size` are preserved exactly as supplied by the approved
cleaned snapshot: no renaming, no reinterpretation, no invented unit for
`size` (see 014_add_asset_type_and_asset_attributes.sql for why they are
plain dim_asset columns rather than dimensions).

`tracking` and `warranty_valid` are deliberately not loaded -- still
deferred from the core asset model v1 (see
sql/migrations/013_create_core_asset_model.sql).

`asset_type` is a REQUIRED column (see REQUIRED_COLUMNS): as of this
version, the approved cleaned snapshot does not yet contain an asset-type
classification, so running this script against it fails pre-load
validation with a clear "missing required column" error rather than
silently leaving core.dim_asset_type empty or guessing a type from `stage`.
An upstream classification step must add a real `asset_type` column before
this loader can populate core.dim_asset_type/core.dim_asset.asset_type_key.

Refuses to run against a non-local POSTGRES_HOST (see
ge_data_platform.common.safety.assert_local_host) -- this tool is for local
bootstrap only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from ge_data_platform.common.database import PostgresLoader
from ge_data_platform.common.safety import assert_local_host
from ge_data_platform.config.settings import PlatformSettings, get_platform_settings

# The exact source columns this loader reads. Deliberately excludes
# tracking/warranty_valid -- see module docstring. `asset_type` is required
# even though the current approved snapshot does not yet have it: that is
# what makes validate_source fail loudly ("missing required column") instead
# of silently loading with an empty core.dim_asset_type -- see the module
# docstring's "asset_type is a REQUIRED column" note.
REQUIRED_COLUMNS = [
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

# Controlled-dimension source columns (feed a core dimension table).
CONTROLLED_DIM_COLUMNS = ["brand", "model", "owner", "state", "asset_category", "asset_type"]

# Free-text source columns that are not controlled dimensions but must still
# be checked for leftover placeholder tokens. `stage` is a fixed dim_asset
# attribute (see 014_add_asset_type_and_asset_attributes.sql), not a
# dimension -- but it is still free text sourced the same way, so the same
# fake-null-token check applies to it.
OTHER_TEXT_COLUMNS = ["partner_code", "serial_number", "stage"]

# Same placeholder tokens the profiling notebook normalizes to real null --
# see notebooks/core/asset/01_asset_master_profile.ipynb "Null/unknown
# cleanup". If any of these survive into the approved snapshot, cleaning was
# incomplete or was undone by a re-export step; refuse to load rather than
# silently pushing fake values into core dimensions.
FAKE_NULL_TOKENS = {"unknown", "n/a", "na", "none", "null", "nan", ""}

SALE_YEAR_MIN = 1900
SALE_YEAR_MAX = 2100


class BootstrapValidationError(ValueError):
    """Raised when the source snapshot fails pre-load or post-load validation.

    Distinct from a plain ValueError only so callers/tests can distinguish
    "the data was bad" from other programming errors.
    """


# =============================================================================
# Loading and pre-load validation
# =============================================================================


def load_source_csv(csv_path: str | Path) -> pd.DataFrame:
    """Read the approved cleaned asset snapshot. Raises if the file is missing."""
    path = Path(csv_path)
    if not path.exists():
        raise BootstrapValidationError(f"Input file does not exist: {path}")
    if not path.is_file():
        raise BootstrapValidationError(f"Input path is not a file: {path}")

    df = pd.read_csv(path)
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def validate_source(df: pd.DataFrame) -> None:
    """Validate the source snapshot before any database connection is opened.

    Raises `BootstrapValidationError` (never touches the database) on the
    first category of problem found; the message lists every offending row
    for that category. Checks, in order:

    * required columns present
    * row count > 0
    * asset_number populated on every row
    * asset_number unique
    * no leftover fake-null tokens in any controlled-dimension or free-text
      column (see FAKE_NULL_TOKENS)
    * sale_year is null or a whole number in [1900, 2100]
    * dispersal_date is null or parseable
    * size is null or a non-negative number (no unit is asserted; this only
      rejects garbage/non-numeric values, matching "size does not represent
      one consistent unit" from the profiling notebook)
    """
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_columns:
        raise BootstrapValidationError(f"Input is missing required column(s): {missing_columns}")

    if df.shape[0] == 0:
        raise BootstrapValidationError("Input has zero rows -- refusing to load an empty snapshot")

    missing_asset_number = df["asset_number"].isna()
    if missing_asset_number.any():
        raise BootstrapValidationError(
            f"{int(missing_asset_number.sum())} row(s) are missing asset_number "
            f"(row indexes: {df.index[missing_asset_number].tolist()[:20]})"
        )

    duplicate_mask = df["asset_number"].duplicated(keep=False)
    if duplicate_mask.any():
        duplicates = sorted(df.loc[duplicate_mask, "asset_number"].unique().tolist())
        raise BootstrapValidationError(
            f"{len(duplicates)} asset_number value(s) are duplicated in the input: {duplicates[:20]}"
        )

    for column in CONTROLLED_DIM_COLUMNS + OTHER_TEXT_COLUMNS:
        series = df[column].dropna().astype(str).str.strip()
        fake_null_mask = series.str.lower().isin(FAKE_NULL_TOKENS)
        if fake_null_mask.any():
            offenders = df.loc[series[fake_null_mask].index, "asset_number"].tolist()
            raise BootstrapValidationError(
                f"Column {column!r} still contains {int(fake_null_mask.sum())} fake-null placeholder "
                f"value(s) (e.g. 'unknown', 'N/A') that should already have been cleaned to real nulls "
                f"upstream (asset_number(s): {sorted(offenders)[:20]})"
            )

    sale_year_numeric = pd.to_numeric(df["sale_year"], errors="coerce")
    invalid_sale_year = df["sale_year"].notna() & (
        sale_year_numeric.isna()
        | (sale_year_numeric != sale_year_numeric.round())
        | (sale_year_numeric < SALE_YEAR_MIN)
        | (sale_year_numeric > SALE_YEAR_MAX)
    )
    if invalid_sale_year.any():
        offenders = df.loc[invalid_sale_year, "asset_number"].tolist()
        raise BootstrapValidationError(
            f"{int(invalid_sale_year.sum())} row(s) have an invalid sale_year (must be null or a whole "
            f"number between {SALE_YEAR_MIN} and {SALE_YEAR_MAX}; asset_number(s): {sorted(offenders)[:20]})"
        )

    dispersal_parsed = pd.to_datetime(df["dispersal_date"], errors="coerce")
    unparseable_dispersal = df["dispersal_date"].notna() & dispersal_parsed.isna()
    if unparseable_dispersal.any():
        offenders = df.loc[unparseable_dispersal, "asset_number"].tolist()
        raise BootstrapValidationError(
            f"{int(unparseable_dispersal.sum())} row(s) have an unparseable dispersal_date "
            f"(asset_number(s): {sorted(offenders)[:20]})"
        )

    size_numeric = pd.to_numeric(df["size"], errors="coerce")
    invalid_size = df["size"].notna() & (size_numeric.isna() | (size_numeric < 0))
    if invalid_size.any():
        offenders = df.loc[invalid_size, "asset_number"].tolist()
        raise BootstrapValidationError(
            f"{int(invalid_size.sum())} row(s) have an invalid size (must be null or a non-negative "
            f"number; asset_number(s): {sorted(offenders)[:20]})"
        )


def _clean_text(value: object) -> str | None:
    """Normalise a source cell to a stripped string, or None if blank/missing."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text_value = str(value).strip()
    return text_value or None


def _clean_sale_year(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return int(round(float(value)))


def _clean_dispersal_date(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value)
    return parsed.date().isoformat()


def _clean_size(value: object) -> float | None:
    """Preserve `size` exactly as supplied -- no unit conversion, no rounding beyond source precision."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return float(value)


# =============================================================================
# Reference dimension loading
# =============================================================================


def _upsert_simple_dimension(conn: Connection, table: str, name_column: str, key_column: str, names: set[str]) -> dict[str, int]:
    """Upsert a set of unique, non-null names into a simple `core.<table>` dimension.

    Returns {name: surrogate_key}. `updated_at` is bumped on conflict so a
    rerun with an unchanged name is a harmless no-op write, never a
    duplicate row (the table's own UNIQUE(name_column) constraint is what
    makes ON CONFLICT possible here).
    """
    resolved: dict[str, int] = {}
    for name in sorted(names):
        row = conn.execute(
            text(f"""
                INSERT INTO core.{table} ({name_column})
                VALUES (:name)
                ON CONFLICT ({name_column}) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING {key_column}
            """),
            {"name": name},
        ).mappings().one()
        resolved[name] = row[key_column]
    return resolved


def _upsert_model_dimension(conn: Connection, pairs: set[tuple[int | None, str]]) -> dict[tuple[int | None, str], int]:
    """Upsert (make_key, model_name) pairs into core.dim_model.

    `make_key` may be None (model with no known make). core.dim_model has no
    single UNIQUE(make_key, model_name) constraint -- Postgres treats NULLs
    as distinct, which would let make-less duplicates proliferate -- so
    013_create_core_asset_model.sql instead defines two partial unique
    indexes: one on (make_key, model_name) WHERE make_key IS NOT NULL, one
    on (model_name) WHERE make_key IS NULL. The ON CONFLICT target below
    must match one of those exactly, so the two cases are upserted
    separately. Returns {(make_key, model_name): model_key}.
    """
    resolved: dict[tuple[int | None, str], int] = {}
    for make_key, model_name in sorted(pairs, key=lambda p: (p[0] is None, p[0] or 0, p[1])):
        if make_key is not None:
            row = conn.execute(
                text("""
                    INSERT INTO core.dim_model (make_key, model_name)
                    VALUES (:make_key, :model_name)
                    ON CONFLICT (make_key, model_name) WHERE make_key IS NOT NULL
                    DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                    RETURNING model_key
                """),
                {"make_key": make_key, "model_name": model_name},
            ).mappings().one()
        else:
            row = conn.execute(
                text("""
                    INSERT INTO core.dim_model (make_key, model_name)
                    VALUES (NULL, :model_name)
                    ON CONFLICT (model_name) WHERE make_key IS NULL
                    DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                    RETURNING model_key
                """),
                {"model_name": model_name},
            ).mappings().one()
        resolved[(make_key, model_name)] = row["model_key"]
    return resolved


def load_reference_dimensions(conn: Connection, df: pd.DataFrame) -> dict[str, dict]:
    """Load/upsert all six reference dimensions from the source snapshot.

    Returns a dict of resolution maps:
        makes:            {make_name: make_key}
        owners:           {owner_name: owner_key}
        usage_types:      {usage_type_name: usage_type_key}
        asset_categories: {asset_category_name: asset_category_key}
        asset_types:      {asset_type_name: asset_type_key}
        models:           {(make_key or None, model_name): model_key}

    `models` is resolved after `makes` because it depends on make_key.
    `asset_types` feeds core.dim_asset_type (sql/migrations/
    014_add_asset_type_and_asset_attributes.sql) -- kept entirely separate
    from `asset_categories` (core.dim_asset_category): asset_category is the
    broad business category (e.g. Material Handling), asset_type is the
    physical asset type/class (e.g. Forklift). Never collapsed together.
    """
    makes = _upsert_simple_dimension(
        conn, "dim_make", "make_name", "make_key", {_clean_text(v) for v in df["brand"].dropna()} - {None}
    )
    owners = _upsert_simple_dimension(
        conn, "dim_owner", "owner_name", "owner_key", {_clean_text(v) for v in df["owner"].dropna()} - {None}
    )
    usage_types = _upsert_simple_dimension(
        conn, "dim_usage_type", "usage_type_name", "usage_type_key", {_clean_text(v) for v in df["state"].dropna()} - {None}
    )
    asset_categories = _upsert_simple_dimension(
        conn,
        "dim_asset_category",
        "asset_category_name",
        "asset_category_key",
        {_clean_text(v) for v in df["asset_category"].dropna()} - {None},
    )
    asset_types = _upsert_simple_dimension(
        conn,
        "dim_asset_type",
        "asset_type_name",
        "asset_type_key",
        {_clean_text(v) for v in df["asset_type"].dropna()} - {None},
    )

    # Do NOT invent an "Unknown" make: a model with no brand on its source
    # row resolves to make_key=None, not to a synthetic make row.
    model_make_pairs: set[tuple[int | None, str]] = set()
    for _, source_row in df.iterrows():
        model_name = _clean_text(source_row["model"])
        if model_name is None:
            continue
        brand_name = _clean_text(source_row["brand"])
        make_key = makes[brand_name] if brand_name is not None else None
        model_make_pairs.add((make_key, model_name))

    models = _upsert_model_dimension(conn, model_make_pairs)

    return {
        "makes": makes,
        "owners": owners,
        "usage_types": usage_types,
        "asset_categories": asset_categories,
        "asset_types": asset_types,
        "models": models,
    }


# =============================================================================
# Asset resolution and loading
# =============================================================================


def build_asset_rows(df: pd.DataFrame, dims: dict[str, dict]) -> list[dict]:
    """Resolve every source row's foreign keys and return dim_asset-ready rows.

    Enforces make/model consistency: if a resolved asset has both a
    make_key and a model_key, the model's own make_key must equal the
    asset's make_key (both are derived from the same source row's brand, so
    this should always hold by construction -- this is a defensive check
    against a resolution bug, not an expected data problem). Raises
    `BootstrapValidationError` naming the offending asset_number(s) if it
    ever does not hold; never silently corrects the data.

    Also defends against any populated reference value (brand, owner,
    state, asset_category, asset_type) that fails to resolve against its
    dimension map -- should never happen given `dims` was built from this
    same `df` by `load_reference_dimensions`, but is checked explicitly
    (rather than left to raise an opaque KeyError) and reported by name, per
    asset_number, rather than silently treated as null.
    """
    makes, owners, usage_types, asset_categories, asset_types, models = (
        dims["makes"],
        dims["owners"],
        dims["usage_types"],
        dims["asset_categories"],
        dims["asset_types"],
        dims["models"],
    )

    # Precomputed once so the defensive make/model check below is O(1) per
    # row instead of re-scanning `models` for every asset.
    model_key_to_make_key = {model_key: make_key for (make_key, _model_name), model_key in models.items()}

    def _resolve(dimension_map: dict[str, int], name: str | None, *, field: str, asset_number: str, unresolved: list) -> int | None:
        if name is None:
            return None
        if name not in dimension_map:
            unresolved.append((asset_number, field, name))
            return None
        return dimension_map[name]

    rows: list[dict] = []
    mismatches: list[tuple[object, int | None, int | None]] = []
    unresolved: list[tuple[str, str, str]] = []

    for _, source_row in df.iterrows():
        asset_number = str(int(source_row["asset_number"]))
        brand_name = _clean_text(source_row["brand"])
        model_name = _clean_text(source_row["model"])
        owner_name = _clean_text(source_row["owner"])
        usage_type_name = _clean_text(source_row["state"])
        asset_category_name = _clean_text(source_row["asset_category"])
        asset_type_name = _clean_text(source_row["asset_type"])

        asset_make_key = _resolve(makes, brand_name, field="brand", asset_number=asset_number, unresolved=unresolved)
        model_key = models.get((asset_make_key, model_name)) if model_name is not None else None
        if model_name is not None and model_key is None:
            unresolved.append((asset_number, "model", model_name))

        if model_key is not None:
            model_make_key = model_key_to_make_key[model_key]
            if model_make_key != asset_make_key:
                mismatches.append((asset_number, asset_make_key, model_make_key))

        rows.append(
            {
                "asset_number": asset_number,
                "partner_code": _clean_text(source_row["partner_code"]),
                "serial_number": _clean_text(source_row["serial_number"]),
                "make_key": asset_make_key,
                "model_key": model_key,
                "owner_key": _resolve(owners, owner_name, field="owner", asset_number=asset_number, unresolved=unresolved),
                "usage_type_key": _resolve(
                    usage_types, usage_type_name, field="state", asset_number=asset_number, unresolved=unresolved
                ),
                "asset_category_key": _resolve(
                    asset_categories, asset_category_name, field="asset_category", asset_number=asset_number, unresolved=unresolved
                ),
                "asset_type_key": _resolve(
                    asset_types, asset_type_name, field="asset_type", asset_number=asset_number, unresolved=unresolved
                ),
                "sale_year": _clean_sale_year(source_row["sale_year"]),
                "dispersal_date": _clean_dispersal_date(source_row["dispersal_date"]),
                "stage": _clean_text(source_row["stage"]),
                "size": _clean_size(source_row["size"]),
            }
        )

    if unresolved:
        raise BootstrapValidationError(
            f"{len(unresolved)} reference value(s) did not resolve against their dimension map -- "
            f"refusing to load (asset_number, field, value): {unresolved[:20]}"
        )

    if mismatches:
        raise BootstrapValidationError(
            f"{len(mismatches)} asset(s) resolved to a model whose make_key does not match the "
            f"asset's own make_key -- refusing to load (asset_number, asset_make_key, model_make_key): "
            f"{mismatches[:20]}"
        )

    return rows


def upsert_assets(conn: Connection, asset_rows: list[dict]) -> None:
    """Upsert `asset_rows` into core.dim_asset, keyed on asset_number.

    Safe to rerun: ON CONFLICT (asset_number) DO UPDATE means a second run
    over the same (or a revised) snapshot updates the same physical assets
    in place rather than inserting duplicates. asset_key is never taken
    from the source -- it is always the destination's own surrogate
    identity column.
    """
    if not asset_rows:
        return

    statement = text("""
        INSERT INTO core.dim_asset (
            asset_number, partner_code, serial_number,
            make_key, model_key, owner_key, usage_type_key, asset_category_key, asset_type_key,
            sale_year, dispersal_date, stage, size
        ) VALUES (
            :asset_number, :partner_code, :serial_number,
            :make_key, :model_key, :owner_key, :usage_type_key, :asset_category_key, :asset_type_key,
            :sale_year, :dispersal_date, :stage, :size
        )
        ON CONFLICT (asset_number) DO UPDATE SET
            partner_code = EXCLUDED.partner_code,
            serial_number = EXCLUDED.serial_number,
            make_key = EXCLUDED.make_key,
            model_key = EXCLUDED.model_key,
            owner_key = EXCLUDED.owner_key,
            usage_type_key = EXCLUDED.usage_type_key,
            asset_category_key = EXCLUDED.asset_category_key,
            asset_type_key = EXCLUDED.asset_type_key,
            sale_year = EXCLUDED.sale_year,
            dispersal_date = EXCLUDED.dispersal_date,
            stage = EXCLUDED.stage,
            size = EXCLUDED.size,
            updated_at = CURRENT_TIMESTAMP
    """)

    for row in asset_rows:
        conn.execute(statement, row)


# =============================================================================
# Post-load validation (still inside the open transaction)
# =============================================================================


def validate_post_load(conn: Connection, asset_rows: list[dict]) -> dict[str, int]:
    """Verify the load before the caller commits. Raises on any failure.

    Checks, against core.dim_asset restricted to this run's asset_numbers:
    * every source asset_number exists exactly once
    * no duplicate asset_number
    * every populated FK value resolves to a real dimension row
    * zero make/model mismatches (model's make_key == asset's make_key)

    Returns a dict of the zero-valued counters for the final report (all
    zero, or this function has already raised).
    """
    asset_numbers = [row["asset_number"] for row in asset_rows]
    # bindparam(expanding=True) turns a single ":asset_numbers" placeholder
    # into the right number of positional parameters for an IN (...) clause
    # on whichever dialect is connected (PostgreSQL for real runs, SQLite in
    # tests) -- no ANY()/IN dialect branching needed.
    in_clause = bindparam("asset_numbers", expanding=True)

    count_row = conn.execute(
        text("""
            SELECT asset_number, COUNT(*) AS n
            FROM core.dim_asset
            WHERE asset_number IN :asset_numbers
            GROUP BY asset_number
            HAVING COUNT(*) <> 1
        """).bindparams(in_clause),
        {"asset_numbers": asset_numbers},
    ).mappings().all()
    if count_row:
        raise BootstrapValidationError(
            f"Post-load check failed: {len(count_row)} asset_number(s) do not resolve to exactly one "
            f"core.dim_asset row after load: {[r['asset_number'] for r in count_row][:20]}"
        )

    loaded_count = conn.execute(
        text("SELECT COUNT(*) FROM core.dim_asset WHERE asset_number IN :asset_numbers").bindparams(in_clause),
        {"asset_numbers": asset_numbers},
    ).scalar_one()
    if loaded_count != len(set(asset_numbers)):
        raise BootstrapValidationError(
            f"Post-load check failed: expected {len(set(asset_numbers))} distinct asset(s) in "
            f"core.dim_asset for this run, found {loaded_count}"
        )

    broken_fk_row = conn.execute(
        text("""
            SELECT COUNT(*) FROM core.dim_asset a
            WHERE (a.make_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_make m WHERE m.make_key = a.make_key))
               OR (a.model_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_model mo WHERE mo.model_key = a.model_key))
               OR (a.owner_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_owner o WHERE o.owner_key = a.owner_key))
               OR (a.usage_type_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_usage_type u WHERE u.usage_type_key = a.usage_type_key))
               OR (a.asset_category_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_asset_category c WHERE c.asset_category_key = a.asset_category_key))
               OR (a.asset_type_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM core.dim_asset_type t WHERE t.asset_type_key = a.asset_type_key))
        """)
    ).scalar_one()
    if broken_fk_row:
        raise BootstrapValidationError(f"Post-load check failed: {broken_fk_row} core.dim_asset row(s) have a broken foreign key")

    # COALESCE(..., -1) rather than IS DISTINCT FROM: NULL-safe equality
    # comparison that works identically on PostgreSQL (production) and
    # SQLite (tests) -- IS DISTINCT FROM is not portable to SQLite, and -1
    # never collides with a real GENERATED ALWAYS AS IDENTITY key.
    mismatch_row = conn.execute(
        text("""
            SELECT COUNT(*) FROM core.dim_asset a
            JOIN core.dim_model m ON m.model_key = a.model_key
            WHERE COALESCE(m.make_key, -1) <> COALESCE(a.make_key, -1)
        """)
    ).scalar_one()
    if mismatch_row:
        raise BootstrapValidationError(f"Post-load check failed: {mismatch_row} core.dim_asset row(s) have a make/model mismatch")

    return {
        "duplicate_asset_numbers": 0,
        "unresolved_references": 0,
        "broken_foreign_keys": 0,
        "make_model_mismatches": 0,
    }


# =============================================================================
# Orchestration
# =============================================================================


def bootstrap_core_assets(csv_path: str | Path, engine: Engine) -> dict[str, object]:
    """Run the full validate -> load -> validate -> commit flow. Returns the report dict.

    All database writes happen inside one transaction (`engine.begin()`):
    reference dimensions, asset resolution, asset load, and post-load
    validation. Any exception anywhere in that block rolls the whole
    transaction back before propagating -- there is no partially loaded
    canonical asset master. Pre-load validation (`validate_source`) runs
    before the transaction opens, so a bad file never touches the database
    at all.
    """
    df = load_source_csv(csv_path)
    validate_source(df)

    with engine.begin() as conn:
        dims = load_reference_dimensions(conn, df)
        asset_rows = build_asset_rows(df, dims)
        upsert_assets(conn, asset_rows)
        post_load_counts = validate_post_load(conn, asset_rows)

    report = {
        "source_rows": len(df),
        "assets_processed": len(asset_rows),
        "makes": len(dims["makes"]),
        "models": len(dims["models"]),
        "owners": len(dims["owners"]),
        "usage_types": len(dims["usage_types"]),
        "asset_categories": len(dims["asset_categories"]),
        "asset_types": len(dims["asset_types"]),
        **post_load_counts,
        "status": "SUCCESS",
    }
    return report


def print_report(report: dict[str, object]) -> None:
    print()
    print("CORE ASSET BOOTSTRAP")
    print()
    print(f"Source rows:                 {report['source_rows']}")
    print(f"Assets processed:            {report['assets_processed']}")
    print()
    print(f"Makes:                       {report['makes']}")
    print(f"Models:                      {report['models']}")
    print(f"Owners:                      {report['owners']}")
    print(f"Usage types:                 {report['usage_types']}")
    print(f"Asset categories:            {report['asset_categories']}")
    print(f"Asset types:                 {report['asset_types']}")
    print()
    print(f"Duplicate asset numbers:     {report['duplicate_asset_numbers']}")
    print(f"Unresolved references:       {report['unresolved_references']}")
    print(f"Broken foreign keys:         {report['broken_foreign_keys']}")
    print(f"Make/model mismatches:       {report['make_model_mismatches']}")
    print()
    print(f"Status: {report['status']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_path", help="Path to the approved cleaned asset master CSV")
    args = parser.parse_args(argv)

    try:
        settings: PlatformSettings = get_platform_settings()
        assert_local_host(settings.postgres_host, context="Core asset bootstrap")
        loader = PostgresLoader.from_platform_settings(settings)
        print(
            f"Target: host={settings.postgres_host} port={settings.postgres_port} "
            f"db={settings.ge_warehouse_db} user={settings.postgres_user} (password hidden)"
        )
        report = bootstrap_core_assets(args.csv_path, loader.engine)
    except BootstrapValidationError as error:
        print(f"VALIDATION ERROR: {error}", file=sys.stderr)
        print("No database changes were made.", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 -- top-level CLI boundary, must never leak a traceback silently
        print(f"ERROR: {error}", file=sys.stderr)
        print("The transaction was rolled back; no database changes were made.", file=sys.stderr)
        return 1

    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
