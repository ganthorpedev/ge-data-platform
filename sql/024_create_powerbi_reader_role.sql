-- Power BI read-only role.
--
-- DO NOT RUN THIS FILE YET. Created for review only, per explicit
-- instruction. When approved, run it manually against telemetry_warehouse.
--
-- Grants SELECT on the `reporting` schema only -- current objects and
-- anything created in `reporting` in the future. Does not touch raw,
-- staging, etl, or warehouse in any way.
--
-- Set a real password before running (replace 'CHANGE_ME_BEFORE_RUNNING'):
-- this file must never be committed with a real credential in it.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
        CREATE ROLE powerbi_reader LOGIN PASSWORD 'CHANGE_ME_BEFORE_RUNNING';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE telemetry_warehouse TO powerbi_reader;

GRANT USAGE ON SCHEMA reporting TO powerbi_reader;

-- Current objects in `reporting` (views created by sql/022_...).
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO powerbi_reader;

-- Future objects created in `reporting` by the same role that ran
-- sql/022_... (ALTER DEFAULT PRIVILEGES only applies to objects created by
-- the role executing this statement, so run this as the same user/role that
-- owns the reporting views -- typically the same admin/service account used
-- for sql/022_create_reporting_powerbi_views.sql).
ALTER DEFAULT PRIVILEGES IN SCHEMA reporting
    GRANT SELECT ON TABLES TO powerbi_reader;

-- Explicitly no grants on raw, staging, etl, or warehouse. powerbi_reader
-- has no USAGE on those schemas, so even a fully-qualified query against
-- e.g. raw.trackunit_assets will fail with a permission error for this role.
