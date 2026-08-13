-- stg_evolution: typed, cleaned, normalized Evolution (Accounts) staging table.
--
-- FIRST PLATFORM LOAD -- see 011_create_raw_evolution.sql for the full
-- discovery context (no legacy Evolution data exists anywhere in
-- telemetry_warehouse; the `id`-as-natural-key assumption in the frozen,
-- never-applied legacy 029 is disproven by live source inspection).
--
-- Purpose: the same rows as raw_evolution.project_report, plus the one
-- derived column the existing pipeline already computes --
-- `business_unit`, from ge_data_platform.sources.evolution.project_reports.
-- determine_business_unit() (date- and cost-code-based classification,
-- ported verbatim from the working notebook; not reinterpreted here). No
-- other cleaning/normalization is applied: the source has no reliable
-- natural key (see 011), so no deduplication is performed here either --
-- doing so would silently discard genuine source rows on unverified
-- assumptions.
--
-- Load strategy: full replace, same reasoning and same load-time surrogate
-- key design as raw_evolution.project_report (see 011). Loaded from
-- raw_evolution.project_report's own extracted batch (not re-extracted from
-- Evolution), so staging is always a deterministic function of what raw
-- just received.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS stg_evolution;

-- =============================================================================
-- stg_evolution.project_report
-- Same grain and columns as raw_evolution.project_report, plus business_unit.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_evolution.project_report (
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
    business_unit TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_evolution_project_report_company_ddate
    ON stg_evolution.project_report (company, d_date);
CREATE INDEX IF NOT EXISTS idx_stg_evolution_project_report_business_unit
    ON stg_evolution.project_report (business_unit);

COMMENT ON TABLE stg_evolution.project_report IS
    'raw_evolution.project_report rows plus business_unit classification. project_report_id is a load-time surrogate key, independent of raw_evolution.project_report.project_report_id (not stable across reloads, not a foreign key to raw).';

INSERT INTO ops.schema_version (migration_name)
VALUES ('012_create_stg_evolution.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
