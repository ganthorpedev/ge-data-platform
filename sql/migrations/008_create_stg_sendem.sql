-- stg_sendem: cleaned, enriched Sendem/MiX staging tables.
--
-- Mirrors legacy telemetry_warehouse staging.sendem_* in shape, column for
-- column. One deliberate difference from a plain 1:1 port: stg_sendem.trip_daily
-- and stg_sendem.event_daily are the ONE place this migration folds in
-- legacy clean.sendem_fact_trips_daily / clean.sendem_fact_events_daily --
-- six months (2026-01-01 to 2026-06-30) of trip/event history that exists
-- nowhere else (not re-fetchable from the Sendem API, and staging/raw only
-- ever carry a rolling window). See
-- docs/migration/legacy-to-platform-migration.md#sendem-migration for the
-- full clean.* investigation (key-set analysis, orphan-risk check, and the
-- overlap-resolution rule: on any (date_key, group_id, site_id, asset_id[,
-- event_type_id]) present in both clean and staging, the live staging value
-- wins -- scripts/backfill_sendem_historical.py loads staging first, then
-- INSERT ... ON CONFLICT DO NOTHING for clean, so only clean's exclusive
-- history is added, never overwriting a fresher staging value).
--
-- clean.sendem_dim_assets/_dim_sites/_dim_event_types are NOT separately
-- migrated: confirmed by key-set diff (see the migration doc) to be proper
-- subsets of the current staging dims, carrying no unique asset/site/event
-- type. Dims are current-state master data, not a time series, so there is
-- no history to lose by not appending them.
--
-- Table names drop the "sendem_" prefix (schema already identifies the
-- source):
--   staging.sendem_dim_assets       -> stg_sendem.asset
--   staging.sendem_dim_sites        -> stg_sendem.site
--   staging.sendem_dim_event_types  -> stg_sendem.event_type
--   staging.sendem_fact_trips_daily  -> stg_sendem.trip_daily
--   staging.sendem_fact_events_daily -> stg_sendem.event_daily
--
-- Grants: stg_sendem already has default privileges for
-- ge_platform_admin/ge_etl from 003_create_platform_roles.sql -- no
-- additional GRANT statements needed here.
--
-- Idempotent: CREATE SCHEMA/TABLE IF NOT EXISTS throughout. Never truncates
-- or deletes existing rows. Transactional.

BEGIN;

CREATE SCHEMA IF NOT EXISTS stg_sendem;

-- =============================================================================
-- stg_sendem.asset
-- Clean asset master. Column-for-column mirror of raw_sendem.asset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.asset (
    asset_id BIGINT PRIMARY KEY,
    site_id BIGINT,
    asset_type TEXT,
    description TEXT,
    vin_number TEXT,
    country TEXT,
    group_id BIGINT,
    registration_number TEXT,
    is_available BOOLEAN,
    fleet_number TEXT,
    make TEXT,
    model TEXT,
    fuel_type TEXT,
    year INTEGER,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE stg_sendem.asset IS
    'Clean Sendem asset master, one row per asset_id.';

-- =============================================================================
-- stg_sendem.site
-- Clean site master. No group_id -- see raw_sendem.site.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.site (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE stg_sendem.site IS
    'Clean Sendem site master, one row per site_id.';

-- =============================================================================
-- stg_sendem.event_type
-- Clean event-type dictionary. Includes inferred placeholder rows for any
-- event_type_id seen in fact data but missing from the real event
-- description dimension -- mirrors
-- ge_data_platform.sources.sendem.transform.build_dim_event_types(), and is
-- also how scripts/backfill_sendem_historical.py resolves the 2
-- event_type_ids referenced only by legacy clean.sendem_fact_events_daily
-- (event_name='Unknown Sendem Event Type', event_category='unknown').
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.event_type (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE stg_sendem.event_type IS
    'Clean Sendem event-type dictionary, including inferred "unknown" placeholder rows for event_type_ids seen in fact data but missing from the real dimension -- see ge_data_platform.sources.sendem.transform.build_dim_event_types().';

-- =============================================================================
-- stg_sendem.trip_daily
-- Grain: one row per (date_key, group_id, site_id, asset_id). Enriched with
-- descriptive attributes at load time (site_name, fleet_number, etc.),
-- matching legacy staging.sendem_fact_trips_daily exactly in shape.
--
-- Historical depth: 2026-01-01 onward -- see the header comment above for
-- how legacy clean.sendem_fact_trips_daily's exclusive history is folded in.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.trip_daily (
    date DATE,
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    site_name TEXT,
    asset_id BIGINT NOT NULL,
    fleet_number TEXT,
    registration_number TEXT,
    description TEXT,
    make TEXT,
    model TEXT,
    asset_type TEXT,
    total_trip_count NUMERIC,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_sendem_trip_daily_asset
    ON stg_sendem.trip_daily (asset_id);
CREATE INDEX IF NOT EXISTS idx_stg_sendem_trip_daily_date
    ON stg_sendem.trip_daily (date);

COMMENT ON TABLE stg_sendem.trip_daily IS
    'One row per Sendem asset per day, enriched with asset/site attributes. Historical depth 2026-01-01 onward: legacy staging.sendem_fact_trips_daily (rolling window) plus legacy clean.sendem_fact_trips_daily (2026-01-01 to 2026-06-30, exclusive keys only) -- see docs/migration/legacy-to-platform-migration.md.';

-- =============================================================================
-- stg_sendem.event_daily
-- Grain: one row per (date_key, group_id, site_id, asset_id, event_type_id).
-- Same enrichment/history pattern as stg_sendem.trip_daily.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_sendem.event_daily (
    date DATE,
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    site_name TEXT,
    asset_id BIGINT NOT NULL,
    fleet_number TEXT,
    registration_number TEXT,
    description TEXT,
    make TEXT,
    model TEXT,
    asset_type TEXT,
    event_type_id BIGINT NOT NULL,
    event_name TEXT,
    event_category TEXT,
    metric_type TEXT,
    unit_type TEXT,
    total_event_occurrences NUMERIC,
    min_event_value NUMERIC,
    max_event_value NUMERIC,
    total_event_value NUMERIC,
    min_event_duration NUMERIC,
    max_event_duration NUMERIC,
    total_event_duration NUMERIC,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_sendem_event_daily_asset
    ON stg_sendem.event_daily (asset_id);
CREATE INDEX IF NOT EXISTS idx_stg_sendem_event_daily_date
    ON stg_sendem.event_daily (date);

COMMENT ON TABLE stg_sendem.event_daily IS
    'One row per Sendem asset/event-type per day, enriched with asset/site/event-type attributes. Historical depth 2026-01-01 onward -- see stg_sendem.trip_daily comment.';

INSERT INTO ops.schema_version (migration_name)
VALUES ('008_create_stg_sendem.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
