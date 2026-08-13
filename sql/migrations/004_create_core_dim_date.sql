-- core.dim_date: the first genuinely source-independent core dimension.
--
-- Safe to build now (unlike core.dim_asset and the rest of core) because no
-- source's identifiers are needed to define what a calendar date is -- see
-- docs/ge_warehouse_architecture.md "core.dim_date".
--
-- Range: 2015-01-01 through 2035-12-31. The earliest currently-loaded
-- historical data is the 2026-01-01 Sendem backfill; this range covers that
-- with 10+ years of margin on both ends. Extending the range later is a
-- cheap, additive migration (INSERT more rows) -- never a source-of-truth
-- concern.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS, and the seed INSERT is
-- ON CONFLICT DO NOTHING keyed on the primary key, so re-running this file
-- against an already-seeded table is a no-op. Transactional.

BEGIN;

CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week_iso INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year_iso INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_leap_year BOOLEAN NOT NULL
);

COMMENT ON TABLE core.dim_date IS
    'One row per calendar date, 2015-01-01 through 2035-12-31. date_key is YYYYMMDD as INTEGER, matching the date_key convention already used by Sendem raw/staging tables elsewhere in this platform.';
COMMENT ON COLUMN core.dim_date.day_of_week_iso IS 'ISO 8601: 1 = Monday .. 7 = Sunday.';
COMMENT ON COLUMN core.dim_date.week_of_year_iso IS 'ISO 8601 week number (1-53).';

INSERT INTO core.dim_date (
    date_key, date, year, quarter, month, month_name, day_of_month,
    day_of_week_iso, day_name, day_of_year, week_of_year_iso, is_weekend, is_leap_year
)
SELECT
    (to_char(d, 'YYYYMMDD'))::INTEGER AS date_key,
    d AS date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    to_char(d, 'FMMonth') AS month_name,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week_iso,
    to_char(d, 'FMDay') AS day_name,
    EXTRACT(DOY FROM d)::INTEGER AS day_of_year,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year_iso,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend,
    (EXTRACT(YEAR FROM d)::INTEGER % 4 = 0
        AND (EXTRACT(YEAR FROM d)::INTEGER % 100 != 0 OR EXTRACT(YEAR FROM d)::INTEGER % 400 = 0)
    ) AS is_leap_year
FROM generate_series('2015-01-01'::date, '2035-12-31'::date, interval '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO ops.schema_version (migration_name)
VALUES ('004_create_core_dim_date.sql')
ON CONFLICT (migration_name) DO NOTHING;

COMMIT;
