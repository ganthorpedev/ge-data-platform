-- raw_evolution: source-faithful Evolution (Accounts) raw tables.
--
-- FIRST PLATFORM LOAD, not a historical migration: telemetry_warehouse has
-- no Evolution data at all. sql/legacy/telemetry_migrations/029_create_
-- accounts_evolution_project_reports_schema.sql exists but was never
-- actually applied to the local telemetry_warehouse (confirmed by live
-- catalog inspection: no raw/staging/accounts object matching
-- evolution/project_report exists anywhere) -- its own etl.sync_runs history
-- shows only 3 FAILED attempts (2026-08-12), two of them refused by
-- validate_combined_for_full_replace() itself. See
-- docs/migration/legacy-to-platform-migration.md#evolution-migration-completed.
--
-- IMPORTANT SCHEMA CORRECTION vs. the frozen (never-applied) legacy 029:
-- 029 assumed `id` was a BIGINT row identifier and that (company, id) was
-- the natural primary key. Live read-only inspection of dbo.vwProjectsReports
-- on both GE and TLS databases disproves this: `Id` is VARCHAR and takes
-- only 11 distinct values total (e.g. 'Inv', 'JL', 'CB', 'APTx', ...), each
-- mapping 1:1 to exactly one `Module` value -- it is a transaction-type/
-- module code, not a per-row identifier. Even the widest practical
-- composite (id, cost_type, module, reference, d_date) has thousands of
-- duplicate rows in both databases, and a handful of rows (~200 GE, ~213
-- TLS, by CHECKSUM) are fully identical across every SOURCE_COLUMNS value.
-- dbo.vwProjectsReports has NO reliable natural/business key at the row
-- grain. This table therefore uses a generated surrogate primary key and
-- enforces no uniqueness on the source columns -- duplicate source rows are
-- loaded as-is (not deduplicated), consistent with "do not silently clean
-- questionable source data."
--
-- Purpose: source-faithful persisted representation of dbo.vwProjectsReports
-- (GE + TLS combined, snake_case column names, money preserved as
-- decimal.Decimal / NUMERIC(20,4) end to end). Does NOT include
-- `business_unit` -- that is a derived classification and belongs in
-- stg_evolution.project_report only.
--
-- Load strategy: full replace (atomic DELETE + INSERT in one transaction),
-- matching legacy raw.evolution_project_reports's existing design --
-- dbo.vwProjectsReports has no evidenced incremental key and is re-extracted
-- in full every run, so a row absent from a later extract must also
-- disappear here. See ge_data_platform.common.database.
-- replace_evolution_project_reports_platform.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows on its own (only the full-replace load path
-- does, deliberately, at load time). Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw_evolution;

-- =============================================================================
-- raw_evolution.project_report
-- Source: dbo.vwProjectsReports (GE, TLS). One row per source row, verbatim.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_evolution.project_report (
    project_report_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company TEXT NOT NULL,
    id TEXT,
    account_description TEXT,
    account_type_description TEXT,
    cost_type TEXT,
    credit NUMERIC(20, 4),
    customer TEXT,
    customer_unique_id TEXT,
    d_date DATE,
    debit NUMERIC(20, 4),
    description TEXT,
    fleet_number TEXT,
    inclusive_amount NUMERIC(20, 4),
    master_sub_account TEXT,
    module TEXT,
    project TEXT,
    project_code TEXT,
    project_name TEXT,
    quantity_invoiced NUMERIC,
    reference TEXT,
    tax_amount NUMERIC(20, 4),
    transaction_description TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_evolution_project_report_company_ddate
    ON raw_evolution.project_report (company, d_date);

COMMENT ON TABLE raw_evolution.project_report IS
    'One row per dbo.vwProjectsReports row (GE + TLS combined), source-faithful. project_report_id is a load-time surrogate key -- the source has no reliable natural key at the row grain (see migration header); it is not stable across full-replace reloads and nothing outside this table should reference it. id is the source "Id" column: a transaction-type/module code (11 distinct values), not a row identifier.';

COMMENT ON COLUMN raw_evolution.project_report.id IS
    'Source column "Id" -- a transaction-type/module code (e.g. Inv, JL, CB), not a per-row identifier. Do not assume uniqueness.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('011_create_raw_evolution.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
