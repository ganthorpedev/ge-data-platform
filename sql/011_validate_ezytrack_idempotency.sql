-- EzyTrack reliability/idempotency validation pack.
-- Read-only. Creates nothing, alters nothing.

-- =============================================================================
-- 1. Constraint inspection: does every UPSERT target have the expected
--    PRIMARY KEY / UNIQUE constraint on its idempotency key?
--    Same type-safe array comparison fix used in
--    sql/003_validate_sendem_idempotency.sql (kcu.column_name cast to text,
--    so actual_columns is text[] and comparable to the text[] literals in
--    expected_columns).
-- =============================================================================

-- 1a. Raw listing of every PK/UNIQUE constraint on the 4 EzyTrack tables.
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS constrained_columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema IN ('raw', 'staging')
  AND tc.table_name IN (
      'ezytrack_assets',
      'ezytrack_trips',
      'ezytrack_dim_assets',
      'ezytrack_fact_trips'
  )
  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type
ORDER BY tc.table_schema, tc.table_name;

-- 1b. Explicit pass/fail: does the actual constraint match the exact
--     idempotency key every table is upserted on?
--     Healthy result: matches_expected = true on every row, no NULL
--     constraint_name (a NULL constraint_name means the table has no
--     PK/UNIQUE constraint at all).
WITH expected_keys(table_schema, table_name, expected_columns) AS (
    VALUES
        ('raw', 'ezytrack_assets', ARRAY['asset_id']),
        ('raw', 'ezytrack_trips', ARRAY['trip_id']),
        ('staging', 'ezytrack_dim_assets', ARRAY['asset_id']),
        ('staging', 'ezytrack_fact_trips', ARRAY['trip_id'])
),
actual_keys AS (
    SELECT
        tc.table_schema,
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS actual_columns
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
    GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type
)
SELECT
    e.table_schema,
    e.table_name,
    e.expected_columns,
    a.constraint_name,
    a.constraint_type,
    a.actual_columns,
    (a.actual_columns = e.expected_columns) AS matches_expected
FROM expected_keys e
LEFT JOIN actual_keys a
    ON a.table_schema = e.table_schema AND a.table_name = e.table_name
ORDER BY e.table_name;

-- =============================================================================
-- 2. Duplicate-key checks: asset tables
--    Key: asset_id
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.ezytrack_assets' AS table_name,
    asset_id,
    COUNT(*) AS duplicate_count
FROM raw.ezytrack_assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

SELECT
    'staging.ezytrack_dim_assets' AS table_name,
    asset_id,
    COUNT(*) AS duplicate_count
FROM staging.ezytrack_dim_assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 3. Duplicate-key checks: trip tables
--    Key: trip_id
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.ezytrack_trips' AS table_name,
    trip_id,
    COUNT(*) AS duplicate_count
FROM raw.ezytrack_trips
GROUP BY trip_id
HAVING COUNT(*) > 1;

SELECT
    'staging.ezytrack_fact_trips' AS table_name,
    trip_id,
    COUNT(*) AS duplicate_count
FROM staging.ezytrack_fact_trips
GROUP BY trip_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 4. Row counts: all EzyTrack raw + staging tables
-- =============================================================================

SELECT 'raw.ezytrack_assets' AS table_name, COUNT(*) AS row_count FROM raw.ezytrack_assets
UNION ALL
SELECT 'raw.ezytrack_trips', COUNT(*) FROM raw.ezytrack_trips
UNION ALL
SELECT 'staging.ezytrack_dim_assets', COUNT(*) FROM staging.ezytrack_dim_assets
UNION ALL
SELECT 'staging.ezytrack_fact_trips', COUNT(*) FROM staging.ezytrack_fact_trips
ORDER BY table_name;

-- =============================================================================
-- 5. Latest 5 EzyTrack sync runs
-- =============================================================================

SELECT
    sync_run_id,
    source_system,
    job_name,
    start_date,
    end_date,
    started_at,
    finished_at,
    status,
    rows_fetched,
    rows_loaded,
    error_message
FROM etl.sync_runs
WHERE source_system = 'ezytrack'
ORDER BY started_at DESC
LIMIT 5;

-- =============================================================================
-- 6. Table-load rows for the latest EzyTrack sync_run_id
--    Healthy result: exactly 4 rows, all status = SUCCESS.
-- =============================================================================

WITH latest AS (
    SELECT sync_run_id
    FROM etl.sync_runs
    WHERE source_system = 'ezytrack'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    l.provider,
    l.schema_name,
    l.table_name,
    l.rows_input,
    l.rows_loaded,
    l.status,
    l.error_message
FROM etl.sync_table_loads l
JOIN latest r ON r.sync_run_id = l.sync_run_id
ORDER BY l.started_at;

-- =============================================================================
-- 7. Orphan staging fact trips missing asset dimension
--    Healthy result: zero rows.
-- =============================================================================

SELECT f.trip_id, f.asset_id
FROM staging.ezytrack_fact_trips f
LEFT JOIN staging.ezytrack_dim_assets a ON a.asset_id = f.asset_id
WHERE f.asset_id IS NOT NULL
  AND a.asset_id IS NULL;

-- =============================================================================
-- 8. Null-key safety checks
--    Healthy result: zero rows from every query in this section.
-- =============================================================================

SELECT 'raw.ezytrack_assets' AS table_name, *
FROM raw.ezytrack_assets
WHERE asset_id IS NULL;

SELECT 'raw.ezytrack_trips' AS table_name, *
FROM raw.ezytrack_trips
WHERE trip_id IS NULL;

SELECT 'staging.ezytrack_dim_assets' AS table_name, *
FROM staging.ezytrack_dim_assets
WHERE asset_id IS NULL;

SELECT 'staging.ezytrack_fact_trips' AS table_name, *
FROM staging.ezytrack_fact_trips
WHERE trip_id IS NULL;
