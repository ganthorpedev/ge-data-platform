-- ge_warehouse baseline validation.
--
-- Not a migration: run manually (or via scripts/setup_ge_warehouse.py
-- --validate) against ge_warehouse after applying sql/migrations/. Read-only
-- -- makes no changes. Every check below returns a human-reviewable
-- PASS/FAIL row, matching the existing style of sql/validation/validate_*.sql
-- for telemetry_warehouse.
--
-- Run against ge_warehouse, not telemetry_warehouse:
--   psql -X -v ON_ERROR_STOP=1 -d ge_warehouse -f .\sql\validation\validate_ge_warehouse_baseline.sql

-- 1) Currently connected to the right database.
SELECT
    CASE WHEN current_database() = 'ge_warehouse' THEN 'PASS' ELSE 'FAIL' END AS status,
    'connected to ge_warehouse' AS check_name,
    current_database() AS actual_database;

-- 2) Every expected platform schema exists.
WITH expected AS (
    SELECT unnest(ARRAY[
        'raw_trackunit', 'raw_sendem', 'raw_ezytrack', 'raw_evolution', 'raw_fieldops',
        'stg_trackunit', 'stg_sendem', 'stg_ezytrack', 'stg_evolution', 'stg_fieldops',
        'core',
        'mart_fleet', 'mart_finance', 'mart_operations', 'mart_maintenance', 'mart_procurement', 'mart_commercial',
        'ops'
    ]) AS schema_name
)
SELECT
    CASE WHEN count(*) FILTER (WHERE n.nspname IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'all expected platform schemas exist' AS check_name,
    string_agg(e.schema_name, ', ') FILTER (WHERE n.nspname IS NULL) AS missing_schemas
FROM expected e
LEFT JOIN pg_namespace n ON n.nspname = e.schema_name;

-- 3) No stray generic raw/staging/reporting/etl/warehouse schema was
--    accidentally introduced by the baseline (those names are reserved for
--    telemetry_warehouse only).
SELECT
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'no legacy-named generic schemas present' AS check_name,
    string_agg(nspname, ', ') AS unexpected_schemas
FROM pg_namespace
WHERE nspname IN ('raw', 'staging', 'reporting', 'etl', 'warehouse', 'clean');

-- 4) Ops metadata tables exist.
WITH expected AS (
    SELECT unnest(ARRAY[
        'schema_version', 'pipeline_run', 'table_load',
        'source_watermark', 'data_quality_result', 'alert_event'
    ]) AS table_name
)
SELECT
    CASE WHEN count(*) FILTER (WHERE t.table_name IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'all ops metadata tables exist' AS check_name,
    string_agg(e.table_name, ', ') FILTER (WHERE t.table_name IS NULL) AS missing_tables
FROM expected e
LEFT JOIN information_schema.tables t
    ON t.table_schema = 'ops' AND t.table_name = e.table_name;

-- 5) Platform roles exist.
WITH expected AS (
    SELECT unnest(ARRAY['ge_platform_admin', 'ge_etl', 'ge_bi_readonly']) AS rolname
)
SELECT
    CASE WHEN count(*) FILTER (WHERE r.rolname IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'all platform roles exist' AS check_name,
    string_agg(e.rolname, ', ') FILTER (WHERE r.rolname IS NULL) AS missing_roles
FROM expected e
LEFT JOIN pg_roles r ON r.rolname = e.rolname;

-- 6) Roles are NOLOGIN (no password should ever be set via migration).
SELECT
    CASE WHEN count(*) FILTER (WHERE rolcanlogin) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'platform roles are NOLOGIN' AS check_name,
    string_agg(rolname, ', ') FILTER (WHERE rolcanlogin) AS roles_with_login
FROM pg_roles
WHERE rolname IN ('ge_platform_admin', 'ge_etl', 'ge_bi_readonly');

-- 7) ge_bi_readonly has USAGE on every mart_* schema and nothing on
--    raw_*/stg_*/core/ops.
SELECT
    CASE WHEN count(*) FILTER (
        WHERE schema_name LIKE 'mart_%' AND NOT has_usage
    ) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'ge_bi_readonly has USAGE on every mart_* schema' AS check_name,
    string_agg(schema_name, ', ') FILTER (WHERE schema_name LIKE 'mart_%' AND NOT has_usage) AS missing_usage
FROM (
    SELECT nspname AS schema_name, has_schema_privilege('ge_bi_readonly', nspname, 'USAGE') AS has_usage
    FROM pg_namespace
    WHERE nspname LIKE 'mart_%' OR nspname IN ('raw_trackunit', 'raw_sendem', 'raw_ezytrack', 'raw_evolution', 'raw_fieldops',
        'stg_trackunit', 'stg_sendem', 'stg_ezytrack', 'stg_evolution', 'stg_fieldops', 'core', 'ops')
) usage_check;

SELECT
    CASE WHEN count(*) FILTER (
        WHERE schema_name NOT LIKE 'mart_%' AND has_usage
    ) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'ge_bi_readonly has NO access outside mart_* schemas' AS check_name,
    string_agg(schema_name, ', ') FILTER (WHERE schema_name NOT LIKE 'mart_%' AND has_usage) AS unexpected_access
FROM (
    SELECT nspname AS schema_name, has_schema_privilege('ge_bi_readonly', nspname, 'USAGE') AS has_usage
    FROM pg_namespace
    WHERE nspname IN ('raw_trackunit', 'raw_sendem', 'raw_ezytrack', 'raw_evolution', 'raw_fieldops',
        'stg_trackunit', 'stg_sendem', 'stg_ezytrack', 'stg_evolution', 'stg_fieldops', 'core', 'ops',
        'mart_fleet', 'mart_finance', 'mart_operations', 'mart_maintenance', 'mart_procurement', 'mart_commercial')
) usage_check;

-- 8) schema_version has one row per shipped migration file (kept in sync by
--    hand with sql/migrations/ -- update this list when a new migration ships).
WITH expected AS (
    SELECT unnest(ARRAY[
        '001_create_platform_schemas.sql',
        '002_create_ops_metadata.sql',
        '003_create_platform_roles.sql',
        '004_create_core_dim_date.sql'
    ]) AS migration_name
)
SELECT
    CASE WHEN count(*) FILTER (WHERE sv.migration_name IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'schema_version has a row for every shipped migration' AS check_name,
    string_agg(e.migration_name, ', ') FILTER (WHERE sv.migration_name IS NULL) AS missing_migrations
FROM expected e
LEFT JOIN ops.schema_version sv ON sv.migration_name = e.migration_name;

-- =============================================================================
-- core.dim_date checks
-- =============================================================================

-- 9) date_key is unique (guaranteed by PK, but confirmed explicitly).
SELECT
    CASE WHEN count(*) = count(DISTINCT date_key) THEN 'PASS' ELSE 'FAIL' END AS status,
    'dim_date.date_key is unique' AS check_name,
    count(*) - count(DISTINCT date_key) AS duplicate_count
FROM core.dim_date;

-- 10) date is unique.
SELECT
    CASE WHEN count(*) = count(DISTINCT date) THEN 'PASS' ELSE 'FAIL' END AS status,
    'dim_date.date is unique' AS check_name,
    count(*) - count(DISTINCT date) AS duplicate_count
FROM core.dim_date;

-- 11) year/month/day derivation is internally consistent for every row.
SELECT
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'dim_date year/month/day derivation matches the date column' AS check_name,
    count(*) AS mismatched_rows
FROM core.dim_date
WHERE year <> EXTRACT(YEAR FROM date)::INTEGER
   OR month <> EXTRACT(MONTH FROM date)::INTEGER
   OR day_of_month <> EXTRACT(DAY FROM date)::INTEGER
   OR quarter <> EXTRACT(QUARTER FROM date)::INTEGER
   OR day_of_week_iso <> EXTRACT(ISODOW FROM date)::INTEGER
   OR date_key <> (to_char(date, 'YYYYMMDD'))::INTEGER;

-- 12) Leap days (Feb 29) are present in every leap year in range, and every
--     row's is_leap_year flag matches Postgres's own leap-year determination.
SELECT
    CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'dim_date.is_leap_year matches actual leap-year status' AS check_name,
    count(*) AS mismatched_rows
FROM core.dim_date
WHERE is_leap_year <> (
    (EXTRACT(YEAR FROM date)::INTEGER % 4 = 0
        AND (EXTRACT(YEAR FROM date)::INTEGER % 100 != 0 OR EXTRACT(YEAR FROM date)::INTEGER % 400 = 0))
);

SELECT
    CASE WHEN count(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Feb 29 present for every leap year in range (2016, 2020, 2024, 2028, 2032 -- 5 leap years between 2015 and 2035)' AS check_name,
    count(*) AS leap_day_count
FROM core.dim_date
WHERE month = 2 AND day_of_month = 29;

-- 13) Reasonable, exact supported date range: 2015-01-01 to 2035-12-31 with
--     no gaps (row count must equal the exact number of days in range).
SELECT
    CASE WHEN min(date) = '2015-01-01' AND max(date) = '2035-12-31'
              AND count(*) = ('2035-12-31'::date - '2015-01-01'::date + 1)
         THEN 'PASS' ELSE 'FAIL' END AS status,
    'dim_date covers exactly 2015-01-01..2035-12-31 with no gaps' AS check_name,
    min(date) AS actual_min_date,
    max(date) AS actual_max_date,
    count(*) AS actual_row_count,
    ('2035-12-31'::date - '2015-01-01'::date + 1) AS expected_row_count
FROM core.dim_date;
