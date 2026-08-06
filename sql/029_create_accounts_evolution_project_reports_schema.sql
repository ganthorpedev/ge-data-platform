-- Accounts/Evolution Project Reports raw table, derived directly from:
--   transforms/accounts/evolution/project_reports_transform.py  (exact
--     combined DataFrame columns produced, including business_unit)
--   loaders/postgres_loader.py replace_accounts_evolution_project_reports
--     (schema.table name and the (company, id) primary key it stages and
--     atomically replaces into on every run -- this is a full-refresh
--     load, not an upsert, so a row missing from a later extract does not
--     remain behind)
--
-- Rerunnable: safe to execute against an empty or partially-created database.
--
-- NOTES ON TYPE CHOICES:
--   1. Money columns (credit, debit, inclusive_amount, tax_amount) are
--      NUMERIC(20, 4), matching the CAST(... AS DECIMAL(20, 4)) applied in
--      sql/accounts/evolution/project_reports/extract_project_reports.sql.
--      Combined with coerce_float=False on extraction, values travel from
--      SQL Server to PostgreSQL as decimal.Decimal end to end -- never
--      rounded through a binary float.
--   2. quantity_invoiced is plain NUMERIC (no fixed scale). The source query
--      does not CAST it (it is not in the notebook's MONEY_COLUMNS set), so
--      its native precision from dbo.vwProjectsReports is preserved as-is
--      rather than assumed.
--   3. id is BIGINT: Evolution's project-reports view exposes a numeric
--      transaction identifier. It is unique per company database, not
--      globally, so the natural key -- and this table's primary key -- is
--      the (company, id) pair, not id alone. This is an assumption (the
--      notebook does not document id's exact type/range); if id later
--      proves non-numeric, this column's type is the only thing that needs
--      to change.
--   4. d_date is DATE, matching the notebook's business/calendar-date usage
--      (only dates are ever printed/compared, never times).

CREATE SCHEMA IF NOT EXISTS raw;
-- Also needed here (not just by sql/001_create_sendem_schema.sql) because
-- replace_accounts_evolution_project_reports() stages every load in a
-- staging.evolution_project_reports_stage_* table before the atomic swap.
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS raw.evolution_project_reports (
    company TEXT NOT NULL,
    id BIGINT NOT NULL,
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
    loaded_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (company, id)
);
