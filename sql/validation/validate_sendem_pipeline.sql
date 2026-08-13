-- Sendem ETL + warehouse validation pack.
-- Read-only. Creates nothing, alters nothing.
--
-- Sections 7-9 assume sql/migrations/002_create_sendem_warehouse_views.sql has already
-- been run. If it has not, those queries will fail clearly with
-- "relation ... does not exist" -- that failure is intentional, not hidden.

-- =============================================================================
-- 1. Latest Sendem sync run
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
LIMIT 1;

-- =============================================================================
-- 2. Row counts: raw + staging tables
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
-- 3. Duplicate trip keys: (date_key, group_id, site_id, asset_id)
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.sendem_trips_assets_daily' AS table_name,
    date_key,
    group_id,
    site_id,
    asset_id,
    COUNT(*) AS duplicate_count
FROM raw.sendem_trips_assets_daily
GROUP BY date_key, group_id, site_id, asset_id
HAVING COUNT(*) > 1;

SELECT
    'staging.sendem_fact_trips_daily' AS table_name,
    date_key,
    group_id,
    site_id,
    asset_id,
    COUNT(*) AS duplicate_count
FROM staging.sendem_fact_trips_daily
GROUP BY date_key, group_id, site_id, asset_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 4. Duplicate event keys: (date_key, group_id, site_id, asset_id, event_type_id)
--    Healthy result: zero rows from both queries.
-- =============================================================================

SELECT
    'raw.sendem_events_assets_daily' AS table_name,
    date_key,
    group_id,
    site_id,
    asset_id,
    event_type_id,
    COUNT(*) AS duplicate_count
FROM raw.sendem_events_assets_daily
GROUP BY date_key, group_id, site_id, asset_id, event_type_id
HAVING COUNT(*) > 1;

SELECT
    'staging.sendem_fact_events_daily' AS table_name,
    date_key,
    group_id,
    site_id,
    asset_id,
    event_type_id,
    COUNT(*) AS duplicate_count
FROM staging.sendem_fact_events_daily
GROUP BY date_key, group_id, site_id, asset_id, event_type_id
HAVING COUNT(*) > 1;

-- =============================================================================
-- 5. Orphan trip facts: asset_id or site_id missing from staging dimensions
--    Healthy result: zero rows.
-- =============================================================================

SELECT
    f.date_key,
    f.group_id,
    f.site_id,
    f.asset_id,
    (a.asset_id IS NULL) AS missing_asset,
    (s.site_id IS NULL) AS missing_site
FROM staging.sendem_fact_trips_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id
WHERE a.asset_id IS NULL
   OR s.site_id IS NULL;

-- =============================================================================
-- 6. Orphan event facts: asset_id, site_id, or event_type_id missing from
--    staging dimensions. Healthy result: zero rows.
-- =============================================================================

SELECT
    f.date_key,
    f.group_id,
    f.site_id,
    f.asset_id,
    f.event_type_id,
    (a.asset_id IS NULL) AS missing_asset,
    (s.site_id IS NULL) AS missing_site,
    (e.event_type_id IS NULL) AS missing_event_type
FROM staging.sendem_fact_events_daily f
LEFT JOIN staging.sendem_dim_assets a ON f.asset_id = a.asset_id
LEFT JOIN staging.sendem_dim_sites s ON f.site_id = s.site_id
LEFT JOIN staging.sendem_dim_event_types e ON f.event_type_id = e.event_type_id
WHERE a.asset_id IS NULL
   OR s.site_id IS NULL
   OR e.event_type_id IS NULL;

-- =============================================================================
-- 7. Warehouse views that currently exist
--    (Reads information_schema directly, so this section works even if
--    002_create_sendem_warehouse_views.sql has not been run yet -- it will
--    just return zero rows.)
-- =============================================================================

SELECT
    table_schema,
    table_name AS view_name
FROM information_schema.views
WHERE table_schema = 'warehouse'
ORDER BY table_name;

-- =============================================================================
-- 8. Warehouse view row counts
--    Requires 002_create_sendem_warehouse_views.sql to have been run.
--    A view name here that does not exist yet will fail clearly rather than
--    being silently skipped.
-- =============================================================================

SELECT 'warehouse.v_sendem_assets' AS view_name, COUNT(*) AS row_count FROM warehouse.v_sendem_assets
UNION ALL
SELECT 'warehouse.v_sendem_sites', COUNT(*) FROM warehouse.v_sendem_sites
UNION ALL
SELECT 'warehouse.v_sendem_event_types', COUNT(*) FROM warehouse.v_sendem_event_types
UNION ALL
SELECT 'warehouse.v_sendem_daily_trips', COUNT(*) FROM warehouse.v_sendem_daily_trips
UNION ALL
SELECT 'warehouse.v_sendem_daily_events', COUNT(*) FROM warehouse.v_sendem_daily_events
UNION ALL
SELECT 'warehouse.v_sendem_asset_daily_summary', COUNT(*) FROM warehouse.v_sendem_asset_daily_summary
ORDER BY view_name;

-- =============================================================================
-- 9. Sample rows (LIMIT 10) from each warehouse view
--    Requires 002_create_sendem_warehouse_views.sql to have been run.
--    SELECT * is used here only for these quick samples, per the agreed rule.
-- =============================================================================

SELECT * FROM warehouse.v_sendem_assets LIMIT 10;
SELECT * FROM warehouse.v_sendem_sites LIMIT 10;
SELECT * FROM warehouse.v_sendem_event_types LIMIT 10;
SELECT * FROM warehouse.v_sendem_daily_trips LIMIT 10;
SELECT * FROM warehouse.v_sendem_daily_events LIMIT 10;
SELECT * FROM warehouse.v_sendem_asset_daily_summary LIMIT 10;
