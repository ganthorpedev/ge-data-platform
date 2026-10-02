-- core asset model v1: simple current-state canonical asset dimensions.
--
-- Built from the cleaned, profiled GE asset master (see
-- notebooks/core/asset/01_asset_master_profile.ipynb). This migration is
-- structure only -- no asset data is loaded here, and no bootstrap loader
-- exists yet. See docs/ge_warehouse_architecture.md for the broader core
-- layer design.
--
-- Scope is deliberately narrow (v1, current-state only):
--   - core.dim_make, core.dim_model, core.dim_owner, core.dim_usage_type,
--     core.dim_asset_category, core.dim_asset.
--   - No tracking, warranty, stage, or size columns -- none of these have a
--     sufficiently proven, stable definition yet (tracking belongs to a
--     future telemetry assignment concept; size has no confirmed consistent
--     unit; stage has no clear canonical business meaning; warranty
--     modeling is deferred).
--   - No asset_source_map, no telemetry assignment, no owner/usage history,
--     no facts, no SCD Type 2. Those are later, separate migrations once
--     the corresponding source mappings are proven.
--
-- All reference keys on core.dim_asset are nullable: missing metadata is a
-- valid, expected state for this source data, not an error. No "Unknown"
-- placeholder rows are seeded into any dimension to paper over NULLs.
--
-- core.dim_usage_type and core.dim_asset_category are structure-only here
-- (no seed rows) -- the value lists live in the profiling notebook and
-- source data, not in this migration.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS throughout. Transactional.

BEGIN;

-- =============================================================================
-- core.dim_make
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_make (
    make_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    make_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_make_make_name UNIQUE (make_name)
);

COMMENT ON TABLE core.dim_make IS
    'One row per distinct asset make (manufacturer). Canonical, current-state only -- see 013_create_core_asset_model.sql header.';

-- =============================================================================
-- core.dim_model
--
-- The same model string may exist under different makes, and make_key may
-- be NULL when the make is not known. A plain UNIQUE (make_key, model_name)
-- constraint would not stop duplicate make-less model rows, since Postgres
-- treats NULLs as distinct for uniqueness purposes. Two partial unique
-- indexes close that gap without inventing an "Unknown" make row:
--   - one make_key + model_name combination, for models with a known make
--   - one model_name overall, among models with no known make
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_model (
    model_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    make_key BIGINT REFERENCES core.dim_make (make_key),
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_model_make_key_model_name
    ON core.dim_model (make_key, model_name)
    WHERE make_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_model_model_name_no_make
    ON core.dim_model (model_name)
    WHERE make_key IS NULL;

COMMENT ON TABLE core.dim_model IS
    'One row per distinct (make, model) pair. make_key is nullable when the make is not known for that model; see the two partial unique indexes above for how duplicates are prevented in both the known-make and make-less cases.';

-- =============================================================================
-- core.dim_owner
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_owner (
    owner_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_owner_owner_name UNIQUE (owner_name)
);

COMMENT ON TABLE core.dim_owner IS
    'One row per distinct asset owner. Current-state only -- no owner history in v1.';

-- =============================================================================
-- core.dim_usage_type
--
-- Cleaned form of the source "state" field. Known values include Attachment,
-- Company Equipment, Hire, Sale, Scrap, Service Contract, Sub Contract --
-- not seeded here; this migration is structure only.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_usage_type (
    usage_type_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usage_type_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_usage_type_usage_type_name UNIQUE (usage_type_name)
);

COMMENT ON TABLE core.dim_usage_type IS
    'Cleaned form of the source asset master "state" field (e.g. Hire, Sale, Scrap, Company Equipment). Structure only -- no values seeded by this migration.';

-- =============================================================================
-- core.dim_asset_category
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_asset_category (
    asset_category_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_category_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_asset_category_asset_category_name UNIQUE (asset_category_name)
);

COMMENT ON TABLE core.dim_asset_category IS
    'One row per distinct asset category. Structure only -- no values seeded by this migration.';

-- =============================================================================
-- core.dim_asset
--
-- One row per physical GE-managed asset, current state only. asset_number
-- is the canonical GE business/fleet identifier -- kept as TEXT even though
-- current values are numeric, since it is an identifier, not a quantity.
-- serial_number is deliberately NOT unique at this stage (not yet proven
-- clean enough to enforce). sale_year has a light sanity range check only;
-- nothing tighter has been proven from the source data yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_asset (
    asset_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_number TEXT NOT NULL,
    partner_code TEXT,
    serial_number TEXT,
    make_key BIGINT REFERENCES core.dim_make (make_key),
    model_key BIGINT REFERENCES core.dim_model (model_key),
    owner_key BIGINT REFERENCES core.dim_owner (owner_key),
    usage_type_key BIGINT REFERENCES core.dim_usage_type (usage_type_key),
    asset_category_key BIGINT REFERENCES core.dim_asset_category (asset_category_key),
    sale_year SMALLINT,
    dispersal_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_asset_asset_number UNIQUE (asset_number),
    CONSTRAINT ck_dim_asset_sale_year_range
        CHECK (sale_year IS NULL OR sale_year BETWEEN 1900 AND 2100)
);

CREATE INDEX IF NOT EXISTS ix_dim_asset_make_key ON core.dim_asset (make_key);
CREATE INDEX IF NOT EXISTS ix_dim_asset_model_key ON core.dim_asset (model_key);
CREATE INDEX IF NOT EXISTS ix_dim_asset_owner_key ON core.dim_asset (owner_key);
CREATE INDEX IF NOT EXISTS ix_dim_asset_usage_type_key ON core.dim_asset (usage_type_key);
CREATE INDEX IF NOT EXISTS ix_dim_asset_asset_category_key ON core.dim_asset (asset_category_key);

COMMENT ON TABLE core.dim_asset IS
    'One row per physical GE-managed asset, current state only (no SCD history in v1). All *_key reference columns are nullable -- missing metadata is a valid, expected state for this source data. serial_number is intentionally not unique at this stage.';
COMMENT ON COLUMN core.dim_asset.asset_number IS
    'Canonical GE business/fleet identifier. Kept as TEXT even though current values are numeric.';
COMMENT ON COLUMN core.dim_asset.sale_year IS
    'Light sanity range only (1900-2100); no tighter constraint has been proven from source data yet.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('013_create_core_asset_model.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
