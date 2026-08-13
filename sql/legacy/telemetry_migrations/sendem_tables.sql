-- Sendem/MiX raw + clean table definitions.
-- Assumes the telemetry_warehouse database and the raw/clean/warehouse/etl
-- schemas already exist. Safe to run repeatedly (CREATE ... IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS raw.sendem_assets (
    asset_id BIGINT PRIMARY KEY,
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
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sendem_sites (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    group_id BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sendem_event_descriptions (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sendem_trips_assets_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    total_trip_count INTEGER,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

CREATE TABLE IF NOT EXISTS raw.sendem_events_assets_daily (
    date_key INTEGER NOT NULL,
    group_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    event_type_id BIGINT NOT NULL,
    total_event_occurrences INTEGER,
    min_event_value NUMERIC,
    max_event_value NUMERIC,
    total_event_value NUMERIC,
    min_event_duration NUMERIC,
    max_event_duration NUMERIC,
    total_event_duration NUMERIC,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);

CREATE TABLE IF NOT EXISTS clean.sendem_dim_assets (
    asset_id BIGINT PRIMARY KEY,
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
    source_system TEXT DEFAULT 'sendem',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean.sendem_dim_sites (
    site_id BIGINT PRIMARY KEY,
    site_name TEXT,
    group_id BIGINT,
    source_system TEXT DEFAULT 'sendem',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean.sendem_dim_event_types (
    event_type_id BIGINT PRIMARY KEY,
    event_name TEXT,
    group_id BIGINT,
    metric_type TEXT,
    unit_type TEXT,
    event_category TEXT,
    source_system TEXT DEFAULT 'sendem',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean.sendem_fact_trips_daily (
    date DATE NOT NULL,
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
    total_trip_count INTEGER,
    total_trip_distance_kilometres NUMERIC,
    total_fuel_used_litres NUMERIC,
    total_energy_used_kwh NUMERIC,
    source_system TEXT DEFAULT 'sendem',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, group_id, site_id, asset_id)
);

CREATE TABLE IF NOT EXISTS clean.sendem_fact_events_daily (
    date DATE NOT NULL,
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
    total_event_occurrences INTEGER,
    min_event_value NUMERIC,
    max_event_value NUMERIC,
    total_event_value NUMERIC,
    min_event_duration NUMERIC,
    max_event_duration NUMERIC,
    total_event_duration NUMERIC,
    source_system TEXT DEFAULT 'sendem',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, group_id, site_id, asset_id, event_type_id)
);
