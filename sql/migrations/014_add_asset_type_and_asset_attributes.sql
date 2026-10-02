-- core asset model v1.1: core.dim_asset_type, plus stage/size as fixed
-- core.dim_asset attributes.
--
-- Additive follow-up to sql/migrations/013_create_core_asset_model.sql
-- (already applied locally -- not modified here, per the "never edit an
-- applied migration" convention this repo already follows for every prior
-- migration). Adds:
--
--   - core.dim_asset_type: the physical asset type/class (e.g. Forklift,
--     Compressor, Telehandler, TLB, Water Pump) -- deliberately distinct
--     from core.dim_asset_category (the broad business category, e.g.
--     Material Handling, Agriculture, Transport). Structure only here --
--     no values seeded, same as dim_usage_type/dim_asset_category in 013.
--   - core.dim_asset.asset_type_key: nullable FK to core.dim_asset_type.
--     No "Unknown" asset type is invented -- missing classification is a
--     valid, expected state, same reasoning as every other *_key column on
--     core.dim_asset.
--   - core.dim_asset.stage and core.dim_asset.size: existing GE
--     asset-register attributes, added directly to core.dim_asset (not
--     modeled as dimensions -- they are relatively static per-asset
--     metadata, not controlled/repeated categorical values). Preserved
--     exactly as supplied by the approved cleaned asset master: `stage` is
--     a free-text descriptor (e.g. "2.5t / 3s", "TLB", "Underground
--     truck" -- 79 distinct values observed, not a controlled vocabulary),
--     so TEXT. `size` is a numeric magnitude observed in the cleaned
--     source ranging 0-9220 with up to 2 decimal places (e.g. 0.75, 3.8,
--     9220.00); its unit is not consistent across asset_category (see
--     notebooks/core/asset/01_asset_master_profile.ipynb "size does not
--     appear to represent one consistent unit"), so NUMERIC(10, 2) stores
--     the value exactly as supplied without asserting or converting a
--     unit.
--
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS and ADD COLUMN IF NOT
-- EXISTS throughout. The asset_type_key FK is declared inline on the ADD
-- COLUMN IF NOT EXISTS clause itself (Postgres has no ADD CONSTRAINT IF
-- NOT EXISTS) -- so on a rerun where the column already exists, the whole
-- clause is skipped and the constraint is left exactly as the first
-- successful run created it. Transactional.

BEGIN;

-- =============================================================================
-- core.dim_asset_type
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.dim_asset_type (
    asset_type_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_type_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_asset_type_asset_type_name UNIQUE (asset_type_name)
);

COMMENT ON TABLE core.dim_asset_type IS
    'Physical asset type/class (e.g. Forklift, Compressor, Telehandler, TLB, Water Pump) -- distinct from core.dim_asset_category, which is the broader business category. Structure only -- no values seeded by this migration.';

-- =============================================================================
-- core.dim_asset: asset_type_key (nullable FK), stage, size
-- =============================================================================

ALTER TABLE core.dim_asset
    ADD COLUMN IF NOT EXISTS asset_type_key BIGINT REFERENCES core.dim_asset_type (asset_type_key);

ALTER TABLE core.dim_asset
    ADD COLUMN IF NOT EXISTS stage TEXT;

ALTER TABLE core.dim_asset
    ADD COLUMN IF NOT EXISTS size NUMERIC(10, 2);

CREATE INDEX IF NOT EXISTS ix_dim_asset_asset_type_key ON core.dim_asset (asset_type_key);

COMMENT ON COLUMN core.dim_asset.asset_type_key IS
    'Nullable FK to core.dim_asset_type. Missing classification is valid -- no "Unknown" asset type is invented.';
COMMENT ON COLUMN core.dim_asset.stage IS
    'Existing GE asset-register attribute, preserved exactly as supplied (free-text descriptor/configuration string, not a controlled vocabulary). Not renamed, not reinterpreted.';
COMMENT ON COLUMN core.dim_asset.size IS
    'Existing GE asset-register attribute, preserved exactly as supplied. Not a consistent unit across asset_category (see notebooks/core/asset/01_asset_master_profile.ipynb) -- no unit is asserted or converted here.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('014_add_asset_type_and_asset_attributes.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
