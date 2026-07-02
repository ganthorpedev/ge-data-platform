# Telemetry ETL

Production pipeline that syncs telemetry provider data into the `telemetry_warehouse`
PostgreSQL database. This replaces the exploratory discovery notebooks with scheduled,
idempotent, source-specific ETL jobs.

## Providers

Sendem/MiX (Customer Insights API) is the first telemetry connector. The project is
structured so additional providers can be added later as their own connector/transform
pair without touching Sendem's code.

## Data flow

```
Provider API -> connector -> transform -> raw / clean (staging) -> warehouse (later)
```

- `raw` holds source-specific data close to the original API shape.
- `clean` holds source-specific cleaned/enriched tables.
- `warehouse` will hold unified, cross-source reporting tables once a second
  provider is connected. Power BI should eventually read from `warehouse`, not `raw`.

## Configuration

All secrets and environment-specific values are read from a `.env` file (never
hardcoded). Copy `.env.example` to `.env` and fill in real values before running
anything.

## Running

Once implemented, the Sendem sync job will be run with:

```
python -m jobs.sync_sendem
```
