# `core.dim_date`

**Status: IMPLEMENTED.** The only populated object in `core` today.

## Why this one, first

Every other `core` dimension needs a cross-source conformance decision
before it can be built (see `docs/warehouse/source-mapping.md`).
`core.dim_date` doesn't: a calendar date means the same thing regardless of
source system, so it was safe to build immediately as part of the platform
baseline (`sql/migrations/004_create_core_dim_date.sql`) rather than wait
for the rest of `core`'s design.

## Grain and range

One row per calendar date, **2015-01-01 through 2035-12-31** (7,670 rows).
The earliest currently-loaded historical data anywhere in the platform is
the 2026-01-01 Sendem backfill in the legacy `clean` schema; this range
covers that with 10+ years of margin on both ends. Extending the range later
is a cheap, additive migration (insert more rows) -- never a source-of-truth
concern.

## Columns

| Column | Type | Notes |
|---|---|---|
| `date_key` | `INTEGER`, primary key | `YYYYMMDD`, matching the `date_key` convention already used by Sendem raw/staging tables elsewhere in the platform |
| `date` | `DATE`, unique | |
| `year` | `INTEGER` | |
| `quarter` | `INTEGER` | 1-4 |
| `month` | `INTEGER` | 1-12 |
| `month_name` | `TEXT` | e.g. `"January"` |
| `day_of_month` | `INTEGER` | |
| `day_of_week_iso` | `INTEGER` | ISO 8601: 1 = Monday .. 7 = Sunday |
| `day_name` | `TEXT` | e.g. `"Monday"` |
| `day_of_year` | `INTEGER` | |
| `week_of_year_iso` | `INTEGER` | ISO 8601 week number (1-53) |
| `is_weekend` | `BOOLEAN` | true for ISO day 6 or 7 |
| `is_leap_year` | `BOOLEAN` | |

## Validation

`sql/validation/validate_ge_warehouse_baseline.sql` checks (all passing
against the current local database):

- `date_key` is unique
- `date` is unique
- every row's `year`/`quarter`/`month`/`day_of_month`/`day_of_week_iso`/`date_key`
  derivation matches its `date` column exactly
- `is_leap_year` matches Postgres's own leap-year determination for every
  row
- exactly 5 leap days present (2016, 2020, 2024, 2028, 2032 -- the leap
  years between 2015 and 2035)
- the table covers exactly 2015-01-01 through 2035-12-31 with no gaps
  (`COUNT(*)` matches the exact day count for that range)

## Using it

Join any future `core.fact_*` table's date key to `core.dim_date.date_key`
for calendar attributes (weekday names, ISO week, leap-year flags) instead
of recomputing them per fact or per mart. No fact table exists yet to
demonstrate this join in practice -- see `docs/warehouse/core-model.md`.
