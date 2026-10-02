"""Read-only reconciliation of the Sendem engine-hours reporting source.

Proves, against a real warehouse, that reporting.vw_sendem_asset_daily_activity
(the driver-grain feed rolled up to one row per asset per day) is what it
claims to be:

  1. raw and staging hold the same driver rows (same count, same key set);
  2. the roll-up is one row per asset-day, and its driver_row_count equals
     the number of staging driver rows behind it (no row lost, none double
     counted);
  3. engine_hours == ROUND(engine_seconds / 3600, 2) on every row;
  4. trip_count and distance_km reconcile to reporting.vw_sendem_trips_daily
     summed to the same asset-day, over the whole loaded window;
  5. optionally, a named fleet number's engine hours match an expected
     series day by day (the 9308 proof: --fleet 9308 --expect 2026-09-21=3.88 ...).

It also prints a multi-driver and a single-driver example so a reader can
see both shapes aggregate once.

Usage (from the repository root, after `pip install -e . --no-deps`):

    python -m scripts.validate_sendem_engine_hours
    python -m scripts.validate_sendem_engine_hours --database telemetry_warehouse_dev \
        --fleet 9308 --expect 2026-09-21=3.88 2026-09-22=6.58 ...

Exits 0 if every check PASSes, 1 otherwise. Opens one read-only session
(SET default_transaction_read_only = on) and never writes.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg2

from ge_data_platform.config.settings import get_settings


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


def _connect(database: str | None):
    settings = get_settings()
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=database or settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    conn.commit()
    return conn


def _one(cur, sql: str, params=None):
    cur.execute(sql, params or {})
    return cur.fetchone()


def run(*, database: str | None, fleet: str | None, expected: dict[date, Decimal]) -> list[Check]:
    checks: list[Check] = []
    conn = _connect(database)
    try:
        cur = conn.cursor()

        raw_count, = _one(cur, "SELECT COUNT(*) FROM raw.sendem_trips_assetspeople_daily")
        stg_count, = _one(cur, "SELECT COUNT(*) FROM staging.sendem_fact_trips_assetspeople_daily")
        key_diff, = _one(cur, """
            SELECT COUNT(*) FROM (
                SELECT date_key, group_id, site_id, asset_id, driver_id FROM raw.sendem_trips_assetspeople_daily
                EXCEPT
                SELECT date_key, group_id, site_id, asset_id, driver_id FROM staging.sendem_fact_trips_assetspeople_daily
            ) d
        """)
        checks.append(Check(
            "raw and staging hold the same driver rows",
            raw_count == stg_count and key_diff == 0 and raw_count > 0,
            f"raw={raw_count} staging={stg_count} keys only in raw={key_diff}",
        ))

        lo, hi, days = _one(cur, "SELECT MIN(date_key), MAX(date_key), COUNT(DISTINCT date_key) FROM staging.sendem_fact_trips_assetspeople_daily")
        print(f"Driver-grain window: {lo}..{hi} ({days} days), {stg_count} driver rows")

        view_rows, dup_keys, rows_behind = _one(cur, """
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT (date_key, provider_asset_id)),
                   SUM(driver_row_count)
            FROM reporting.vw_sendem_asset_daily_activity
        """)
        checks.append(Check(
            "one row per asset-day, every driver row counted exactly once",
            dup_keys == 0 and rows_behind == stg_count,
            f"asset-days={view_rows} duplicate keys={dup_keys} driver rows behind them={rows_behind} (staging={stg_count})",
        ))

        bad_hours, = _one(cur, """
            SELECT COUNT(*) FROM reporting.vw_sendem_asset_daily_activity
            WHERE engine_hours IS DISTINCT FROM ROUND(engine_seconds / 3600.0, 2)
               OR (engine_seconds IS NULL) <> (engine_hours IS NULL)
        """)
        checks.append(Check("engine_hours = ROUND(engine_seconds / 3600, 2) everywhere", bad_hours == 0, f"violations={bad_hours}"))

        unknown, nought, positive = _one(cur, """
            SELECT COUNT(*) FILTER (WHERE engine_seconds IS NULL),
                   COUNT(*) FILTER (WHERE engine_seconds = 0),
                   COUNT(*) FILTER (WHERE engine_seconds > 0)
            FROM reporting.vw_sendem_asset_daily_activity
        """)
        print(f"Asset-days by engine time: unknown(NULL)={unknown} nought(0)={nought} positive={positive}")
        flag_mismatch, = _one(cur, """
            SELECT COUNT(*) FROM reporting.vw_sendem_asset_daily_activity
            WHERE (data_quality_status = 'engine_time_unknown') <> (engine_seconds IS NULL)
        """)
        checks.append(Check("data_quality_status says engine_time_unknown exactly when engine is NULL", flag_mismatch == 0, f"violations={flag_mismatch}"))

        cur.execute("""
            WITH trips AS (
                SELECT date_key, provider_asset_id, SUM(trip_count) AS trip_count, SUM(distance_km) AS distance_km
                FROM reporting.vw_sendem_trips_daily
                WHERE date_key BETWEEN %(lo)s AND %(hi)s
                GROUP BY date_key, provider_asset_id
            ),
            activity AS (
                SELECT date_key, provider_asset_id, trip_count, distance_km
                FROM reporting.vw_sendem_asset_daily_activity
            )
            SELECT
                COUNT(*) FILTER (WHERE a.provider_asset_id IS NOT NULL AND t.provider_asset_id IS NOT NULL) AS both_sides,
                COUNT(*) FILTER (WHERE a.provider_asset_id IS NULL) AS only_trips,
                COUNT(*) FILTER (WHERE t.provider_asset_id IS NULL) AS only_activity,
                COUNT(*) FILTER (WHERE a.provider_asset_id IS NOT NULL AND t.provider_asset_id IS NOT NULL
                                   AND (a.trip_count <> t.trip_count OR ABS(a.distance_km - t.distance_km) > 0.05)) AS mismatched
            FROM activity a
            FULL JOIN trips t ON t.date_key = a.date_key AND t.provider_asset_id = a.provider_asset_id
        """, {"lo": lo, "hi": hi})
        both, only_trips, only_activity, mismatched = cur.fetchone()
        checks.append(Check(
            "trip_count and distance_km reconcile to vw_sendem_trips_daily per asset-day",
            mismatched == 0 and only_trips == 0 and only_activity == 0,
            f"asset-days in both={both} only in trips feed={only_trips} only in driver feed={only_activity} mismatched={mismatched}",
        ))

        print("\nMulti-driver example (most drivers on one asset-day):")
        cur.execute("""
            SELECT asset_code, activity_date, driver_count, engine_seconds, engine_hours, trip_count, distance_km
            FROM reporting.vw_sendem_asset_daily_activity
            ORDER BY driver_count DESC, engine_seconds DESC NULLS LAST LIMIT 1
        """)
        multi = cur.fetchone()
        print(f"  {multi}")
        if multi:
            cur.execute("""
                SELECT driver_id, total_engine_second, total_trip_count, total_trip_distance_kilometres
                FROM staging.sendem_fact_trips_assetspeople_daily
                WHERE fleet_number = %s AND date = %s ORDER BY driver_id
            """, (multi[0], multi[1]))
            parts = cur.fetchall()
            for part in parts:
                print(f"    driver {part[0]}: engine={part[1]} trips={part[2]} km={part[3]}")
            summed = sum(Decimal(p[1]) for p in parts if p[1] is not None)
            checks.append(Check(
                f"multi-driver asset-day {multi[0]} {multi[1]} sums its {len(parts)} driver rows once",
                summed == Decimal(multi[3]) and len(parts) == multi[2],
                f"sum of driver engine seconds={summed} view engine_seconds={multi[3]}",
            ))

        print("\nSingle-driver example:")
        cur.execute("""
            SELECT asset_code, activity_date, driver_count, engine_seconds, engine_hours, trip_count, distance_km
            FROM reporting.vw_sendem_asset_daily_activity
            WHERE driver_count = 1 AND engine_seconds > 0 ORDER BY engine_seconds DESC LIMIT 1
        """)
        single = cur.fetchone()
        print(f"  {single}")
        if single:
            cur.execute("""
                SELECT total_engine_second FROM staging.sendem_fact_trips_assetspeople_daily
                WHERE fleet_number = %s AND date = %s
            """, (single[0], single[1]))
            rows = cur.fetchall()
            checks.append(Check(
                f"single-driver asset-day {single[0]} {single[1]} is its one row",
                len(rows) == 1 and Decimal(rows[0][0]) == Decimal(single[3]),
                f"rows={len(rows)} engine_seconds={single[3]}",
            ))

        if fleet:
            print(f"\nFleet {fleet} day by day:")
            cur.execute("""
                SELECT activity_date, driver_count, engine_seconds, engine_hours, asset_driving_hours, trip_count, distance_km
                FROM reporting.vw_sendem_asset_daily_activity
                WHERE asset_code = %(fleet)s
                  AND (%(lo)s IS NULL OR activity_date >= %(lo)s)
                  AND (%(hi)s IS NULL OR activity_date <= %(hi)s)
                ORDER BY activity_date
            """, {
                "fleet": fleet,
                "lo": min(expected) if expected else None,
                "hi": max(expected) if expected else None,
            })
            found = {row[0]: row for row in cur.fetchall()}
            for day, row in sorted(found.items()):
                print(f"  {day}: drivers={row[1]} engine={row[2]} s = {row[3]} h, driving={row[4]} h, trips={row[5]}, km={row[6]}")
            for day, hours in sorted(expected.items()):
                row = found.get(day)
                actual = None if row is None else Decimal(row[3])
                checks.append(Check(
                    f"fleet {fleet} {day} engine_hours = {hours}",
                    actual == hours,
                    f"actual={actual}",
                ))
    finally:
        conn.close()
    return checks


def _parse_expected(values: list[str]) -> dict[date, Decimal]:
    expected: dict[date, Decimal] = {}
    for item in values:
        day, _, hours = item.partition("=")
        expected[date.fromisoformat(day)] = Decimal(hours)
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", default=None, help="Database name instead of POSTGRES_DB (e.g. a dev restore)")
    parser.add_argument("--fleet", default=None, help="A fleet number to print day by day")
    parser.add_argument("--expect", nargs="*", default=[], help="YYYY-MM-DD=hours pairs the fleet's engine_hours must equal")
    args = parser.parse_args()

    checks = run(database=args.database, fleet=args.fleet, expected=_parse_expected(args.expect))
    print("\nChecks:")
    failed = 0
    for check in checks:
        print(f"  {'PASS' if check.passed else 'FAIL'}  {check.name}  [{check.detail}]")
        failed += not check.passed
    print(f"\n{len(checks) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
