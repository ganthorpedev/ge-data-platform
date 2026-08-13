-- Sendem reliability/idempotency validation pack.
-- Read-only. Creates nothing, alters nothing.
--
-- Companion to sql/validation/validate_sendem_pipeline.sql. Both are
-- read-only validation scripts and can be run independently in any order.

-- =============================================================================
-- 1. Constraint inspection: does every UPSERT target have the expected
--    PRIMARY KEY / UNIQUE constraint on its idempotency key?
-- =============================================================================

-- 1a. Raw listing of every PK/UNIQUE constraint on the 10 pipeline tables.
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
      'sendem_assets',
      'sendem_sites',
      'sendem_event_descriptions',
      'sendem_trips_assets_daily',
      'sendem_events_assets_daily',
      'sendem_dim_assets',
      'sendem_dim_sites',
      'sendem_dim_event_types',
      'sendem_fact_trips_daily',
      'sendem_fact_events_daily'
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
        ('raw', 'sendem_assets', ARRAY['asset_id']),
        ('raw', 'sendem_sites', ARRAY['site_id']),
        ('raw', 'sendem_event_descriptions', ARRAY['event_type_id']),
        ('raw', 'sendem_trips_assets_daily', ARRAY['date_key', 'group_id', 'site_id', 'asset_id']),
        ('raw', 'sendem_events_assets_daily', ARRAY['date_key', 'group_id', 'site_id', 'asset_id', 'event_type_id']),
        ('staging', 'sendem_dim_assets', ARRAY['asset_id']),
        ('staging', 'sendem_dim_sites', ARRAY['site_id']),
        ('staging', 'sendem_dim_event_types', ARRAY['event_type_id']),
        ('staging', 'sendem_fact_trips_daily', ARRAY['date_key', 'group_id', 'site_id', 'asset_id']),
        ('staging', 'sendem_fact_events_daily', ARRAY['date_key', 'group_id', 'site_id', 'asset_id', 'event_type_id'])
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
-- 2. Duplicate-key checks: trip tables
--    Key: (date_key, group_id, site_id, asset_id)
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.sendem_trips_assets_daily' AS table_name,
    date_key, group_id, site_id, asset_id,
    COUNT(*) AS duplicate_count
FROM raw.sendem_trips_assets_daily
GROUP BY date_key, group_id, site_id, asset_id
HAVING COUNT(*) > 1;

SELECT
    'staging.sendem_fact_trips_daily' AS table_name,
    date_key, group_id, site_id, asset_id,
    COUNT(*) AS duplicate_count
FROM staging.sendem_fact_trips_daily
GROUP BY date_key, group_id, site_id, asset_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 3. Duplicate-key checks: event tables
--    Key: (date_key, group_id, site_id, asset_id, event_type_id)
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.sendem_events_assets_daily' AS table_name,
    date_key, group_id, site_id, asset_id, event_type_id,
    COUNT(*) AS duplicate_count
FROM raw.sendem_events_assets_daily
GROUP BY date_key, group_id, site_id, asset_id, event_type_id
HAVING COUNT(*) > 1;

SELECT
    'staging.sendem_fact_events_daily' AS table_name,
    date_key, group_id, site_id, asset_id, event_type_id,
    COUNT(*) AS duplicate_count
FROM staging.sendem_fact_events_daily
GROUP BY date_key, group_id, site_id, asset_id, event_type_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 4. Row counts: all raw + staging tables
-- =============================================================================

SELECT 'raw.sendem_assets' AS table_name, COUNT(*) AS row_count FROM raw.sendem_assets
UNION ALL
SELECT 'raw.sendem_sites', COUNT(*) FROM raw.sendem_sites
UNION ALL
SELECT 'raw.sendem_event_descriptions', COUNT(*) FROM raw.sendem_event_descriptions
UNION ALL
SELECT 'raw.sendem_trips_assets_daily', COUNT(*) FROM raw.sendem_trips_assets_daily
UNION ALL
SELECT 'raw.sendem_events_assets_daily', COUNT(*) FROM raw.sendem_events_assets_daily
UNION ALL
SELECT 'staging.sendem_dim_assets', COUNT(*) FROM staging.sendem_dim_assets
UNION ALL
SELECT 'staging.sendem_dim_sites', COUNT(*) FROM staging.sendem_dim_sites
UNION ALL
SELECT 'staging.sendem_dim_event_types', COUNT(*) FROM staging.sendem_dim_event_types
UNION ALL
SELECT 'staging.sendem_fact_trips_daily', COUNT(*) FROM staging.sendem_fact_trips_daily
UNION ALL
SELECT 'staging.sendem_fact_events_daily', COUNT(*) FROM staging.sendem_fact_events_daily
ORDER BY table_name;

-- =============================================================================
-- 5. Latest 10 Sendem sync runs
-- =============================================================================

SELECT
    sync_run_id,
    source_system AS provider,
    job_name,
    start_date AS window_start,
    end_date AS window_end,
    started_at,
    finished_at,
    status,
    rows_fetched,
    rows_loaded,
    error_message
FROM etl.sync_runs
WHERE source_system = 'sendem'
ORDER BY started_at DESC
LIMIT 10;

-- =============================================================================
-- 6. Orphan fact checks
--    Healthy result: zero rows from every query in this section.
-- =============================================================================

-- 6a. Trip rows with missing asset_id in staging.sendem_dim_assets
SELECT f.date_key, f.group_id, f.site_id, f.asset_id
FROM staging.sendem_fact_trips_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
WHERE a.asset_id IS NULL;

-- 6b. Trip rows with missing site_id in staging.sendem_dim_sites
SELECT f.date_key, f.group_id, f.site_id, f.asset_id
FROM staging.sendem_fact_trips_daily f
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id
WHERE s.site_id IS NULL;

-- 6c. Event rows with missing asset_id in staging.sendem_dim_assets
SELECT f.date_key, f.group_id, f.site_id, f.asset_id, f.event_type_id
FROM staging.sendem_fact_events_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
WHERE a.asset_id IS NULL;

-- 6d. Event rows with missing site_id in staging.sendem_dim_sites
SELECT f.date_key, f.group_id, f.site_id, f.asset_id, f.event_type_id
FROM staging.sendem_fact_events_daily f
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id
WHERE s.site_id IS NULL;

-- 6e. Event rows with missing event_type_id in staging.sendem_dim_event_types
SELECT f.date_key, f.group_id, f.site_id, f.asset_id, f.event_type_id
FROM staging.sendem_fact_events_daily f
LEFT JOIN staging.sendem_dim_event_types e ON f.event_type_id = e.event_type_id
WHERE e.event_type_id IS NULL;
