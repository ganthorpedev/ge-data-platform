-- ge_warehouse role model.
--
-- Three NOLOGIN group roles. No password is ever created or stored here --
-- actual login accounts (the ETL service account, a Power BI account, an
-- individual admin) are created out-of-band by an operator and granted
-- membership manually, e.g.:
--     GRANT ge_etl TO some_login_role;
-- This mirrors the existing telemetry_warehouse pattern (a dedicated
-- read-only role, `excel_reader`, granted SELECT on the reporting-facing
-- schema only) and generalizes it across the new schema layout. See
-- docs/ge_warehouse_architecture.md "Roles" for the full design rationale.
--
-- Idempotent: role creation is guarded by an existence check; every GRANT
-- and ALTER DEFAULT PRIVILEGES statement is safe to re-run. Transactional.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ge_platform_admin') THEN
        CREATE ROLE ge_platform_admin NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ge_etl') THEN
        CREATE ROLE ge_etl NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ge_bi_readonly') THEN
        CREATE ROLE ge_bi_readonly NOLOGIN;
    END IF;
END
$$;

COMMENT ON ROLE ge_platform_admin IS 'Owns/administers all ge_warehouse platform objects.';
COMMENT ON ROLE ge_etl IS 'Writes ingestion, staging, core, marts, and ops (all schemas).';
COMMENT ON ROLE ge_bi_readonly IS 'Reads approved reporting/mart objects only -- no access to raw_*, stg_*, core, or ops.';

-- ---------------------------------------------------------------------------
-- ge_platform_admin: ALL on every platform schema, current and future
-- objects, via ownership-style default privileges scoped to itself.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    schema_name TEXT;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY[
        'raw_trackunit', 'raw_sendem', 'raw_ezytrack', 'raw_evolution', 'raw_fieldops',
        'stg_trackunit', 'stg_sendem', 'stg_ezytrack', 'stg_evolution', 'stg_fieldops',
        'core',
        'mart_fleet', 'mart_finance', 'mart_operations', 'mart_maintenance', 'mart_procurement', 'mart_commercial',
        'ops'
    ]
    LOOP
        EXECUTE format('GRANT ALL ON SCHEMA %I TO ge_platform_admin', schema_name);
        EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA %I TO ge_platform_admin', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO ge_platform_admin', schema_name);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- ge_etl: USAGE + CREATE on every schema; SELECT/INSERT/UPDATE/DELETE on
-- current and future tables in every schema (it writes raw/staging/core/
-- marts/ops end to end).
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    schema_name TEXT;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY[
        'raw_trackunit', 'raw_sendem', 'raw_ezytrack', 'raw_evolution', 'raw_fieldops',
        'stg_trackunit', 'stg_sendem', 'stg_ezytrack', 'stg_evolution', 'stg_fieldops',
        'core',
        'mart_fleet', 'mart_finance', 'mart_operations', 'mart_maintenance', 'mart_procurement', 'mart_commercial',
        'ops'
    ]
    LOOP
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO ge_etl', schema_name);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO ge_etl', schema_name);
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ge_etl',
            schema_name
        );
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- ge_bi_readonly: USAGE + SELECT on mart_* schemas ONLY -- current and
-- future objects. No grant of any kind on raw_*, stg_*, core, or ops:
-- source data and pipeline internals are never exposed to BI by default.
-- Default privileges are scoped "FOR ROLE ge_etl" because ge_etl is the
-- role expected to create the marts' objects in a later phase; if a
-- different role creates them, re-run the ALTER DEFAULT PRIVILEGES line for
-- that role.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    schema_name TEXT;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY[
        'mart_fleet', 'mart_finance', 'mart_operations', 'mart_maintenance', 'mart_procurement', 'mart_commercial'
    ]
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO ge_bi_readonly', schema_name);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO ge_bi_readonly', schema_name);
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE ge_etl IN SCHEMA %I GRANT SELECT ON TABLES TO ge_bi_readonly',
            schema_name
        );
    END LOOP;
END
$$;

INSERT INTO ops.schema_version (migration_name)
VALUES ('003_create_platform_roles.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
