-- ge_warehouse platform baseline: schemas + migration tracking.
--
-- This is the first migration in a NEW, independent numbering sequence for
-- the ge_warehouse platform database. It has no relationship to the
-- pre-existing telemetry_warehouse migrations now under
-- sql/legacy/telemetry_migrations/ -- do not read "001" here as continuing
-- that sequence. See docs/ge_warehouse_architecture.md for the full design.
--
-- Idempotent: every statement uses IF NOT EXISTS. Safe to run against an
-- empty database or to re-run against an already-migrated one.
-- Transactional: this entire file succeeds or has no effect.

BEGIN;

-- ---------------------------------------------------------------------------
-- Source-scoped raw layer: source-faithful, persisted close to the
-- provider's own shape. One schema per source, never blended.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw_trackunit;
CREATE SCHEMA IF NOT EXISTS raw_sendem;
CREATE SCHEMA IF NOT EXISTS raw_ezytrack;
CREATE SCHEMA IF NOT EXISTS raw_evolution;
CREATE SCHEMA IF NOT EXISTS raw_fieldops;

-- ---------------------------------------------------------------------------
-- Source-scoped staging layer: cleaned/typed/deduplicated, still
-- single-source. Not GE corporate truth.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS stg_trackunit;
CREATE SCHEMA IF NOT EXISTS stg_sendem;
CREATE SCHEMA IF NOT EXISTS stg_ezytrack;
CREATE SCHEMA IF NOT EXISTS stg_evolution;
CREATE SCHEMA IF NOT EXISTS stg_fieldops;

-- ---------------------------------------------------------------------------
-- Enterprise warehouse: canonical, conformed GE business entities and facts.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- Business marts: denormalized, domain-facing, built only on core.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS mart_fleet;
CREATE SCHEMA IF NOT EXISTS mart_finance;
CREATE SCHEMA IF NOT EXISTS mart_operations;
CREATE SCHEMA IF NOT EXISTS mart_maintenance;
CREATE SCHEMA IF NOT EXISTS mart_procurement;
CREATE SCHEMA IF NOT EXISTS mart_commercial;

-- ---------------------------------------------------------------------------
-- Operational metadata.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- Migration tracking. Created here (rather than in a later file) because
-- every migration, including this one, registers itself into it as its
-- last statement -- it must exist before anything can self-register.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.schema_version (
    migration_name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    checksum TEXT
);

COMMENT ON TABLE ops.schema_version IS
    'One row per applied sql/migrations/*.sql file for ge_warehouse. Independent of any telemetry_warehouse migration tracking (there was none).';

INSERT INTO ops.schema_version (migration_name)
VALUES ('001_create_platform_schemas.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
