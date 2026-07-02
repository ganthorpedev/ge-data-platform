"""Transforms raw Sendem/MiX API payloads into clean, warehouse-ready tables.

This module owns dataframe construction, enrichment (joining dimensions onto
facts), and final column selection for each Sendem table. It must not perform
any I/O (no HTTP calls, no database access, no environment reads).
"""

from __future__ import annotations

import pandas as pd

FACT_TRIPS_COLUMNS = [
    "date",
    "DateKey",
    "GroupId",
    "SiteId",
    "SiteName",
    "AssetId",
    "FleetNumber",
    "RegistrationNumber",
    "Description",
    "Make",
    "Model",
    "AssetType",
    "TotalTripCount",
    "TotalTripDistanceKilometres",
    "TotalFuelUsedLitres",
    "TotalEnergyUsedKwh",
]

FACT_EVENTS_COLUMNS = [
    "date",
    "DateKey",
    "GroupId",
    "SiteId",
    "SiteName",
    "AssetId",
    "FleetNumber",
    "RegistrationNumber",
    "Description",
    "Make",
    "Model",
    "AssetType",
    "EventTypeId",
    "EventName",
    "EventCategory",
    "MetricType",
    "UnitType",
    "TotalEventOccurrences",
    "MinEventValue",
    "MaxEventValue",
    "TotalEventValue",
    "MinEventDuration",
    "MaxEventDuration",
    "TotalEventDuration",
]


def to_dataframe(data: list | dict) -> pd.DataFrame:
    """Normalize a raw JSON payload (list or dict) into a flat DataFrame.

    Returns an empty DataFrame if `data` is empty.
    """
    if not data:
        return pd.DataFrame()

    return pd.json_normalize(data)


def add_date_column(df: pd.DataFrame, date_key_col: str = "DateKey") -> pd.DataFrame:
    """Add a `date` column parsed from an integer YYYYMMDD column.

    Returns `df` unchanged if `date_key_col` is not present.
    """
    if date_key_col not in df.columns:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df[date_key_col].astype(str), format="%Y%m%d")
    return df


def enrich_trips(
    trips_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    sites_df: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join trips onto assets and sites, and add a `date` column."""
    enriched = trips_df.merge(assets_df, on="AssetId", how="left", suffixes=("", "_asset"))
    enriched = enriched.merge(sites_df, on="SiteId", how="left", suffixes=("", "_site"))
    return add_date_column(enriched)


def build_fact_trips(trips_enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Select the fact_trips columns that exist on the enriched trips DataFrame."""
    existing_columns = [col for col in FACT_TRIPS_COLUMNS if col in trips_enriched_df.columns]
    return trips_enriched_df[existing_columns].copy()


def enrich_events(
    events_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    sites_df: pd.DataFrame,
    event_desc_df: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join events onto assets, sites, and event descriptions, and add a `date` column."""
    enriched = events_df.merge(assets_df, on="AssetId", how="left", suffixes=("", "_asset"))
    enriched = enriched.merge(sites_df, on="SiteId", how="left", suffixes=("", "_site"))
    enriched = enriched.merge(event_desc_df, on="EventTypeId", how="left", suffixes=("", "_event"))
    return add_date_column(enriched)


def build_fact_events(events_enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Select the fact_events columns that exist on the enriched events DataFrame."""
    existing_columns = [col for col in FACT_EVENTS_COLUMNS if col in events_enriched_df.columns]
    return events_enriched_df[existing_columns].copy()


def build_dim_event_types(event_desc_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """Build the staging event-type dimension.

    This is the actual Sendem event descriptions (`event_desc_df`, unchanged)
    plus one inferred placeholder row for every `EventTypeId` seen in the
    event facts but missing from the real event description data. This lets
    fact rows join cleanly in staging/warehouse without ever mutating
    `event_desc_df`, which must stay exactly what the Sendem API returned.
    """
    if "EventTypeId" not in events_df.columns:
        return event_desc_df.copy()

    known_ids = set(event_desc_df["EventTypeId"]) if "EventTypeId" in event_desc_df.columns else set()
    fact_ids = set(events_df["EventTypeId"].dropna().unique())
    missing_ids = sorted(fact_ids - known_ids)

    if not missing_ids:
        return event_desc_df.copy()

    group_id_by_event_type = (
        events_df[events_df["EventTypeId"].isin(missing_ids)]
        .drop_duplicates(subset="EventTypeId")
        .set_index("EventTypeId")["GroupId"]
    )

    inferred_rows = pd.DataFrame(
        {
            "EventTypeId": missing_ids,
            "EventName": "Unknown Sendem Event Type",
            "GroupId": [group_id_by_event_type.get(event_type_id) for event_type_id in missing_ids],
            "MetricType": "",
            "UnitType": "",
            "EventCategory": "unknown",
        }
    )

    return pd.concat([event_desc_df, inferred_rows], ignore_index=True, sort=False)


def build_all(raw: dict[str, list]) -> dict[str, pd.DataFrame]:
    """Build every Sendem DataFrame from raw API payloads.

    `raw` is expected to have the keys: assets, sites, people, organisations,
    event_descriptions, trips, events.
    """
    assets_df = to_dataframe(raw.get("assets", []))
    sites_df = to_dataframe(raw.get("sites", []))
    people_df = to_dataframe(raw.get("people", []))
    organisations_df = to_dataframe(raw.get("organisations", []))
    event_desc_df = to_dataframe(raw.get("event_descriptions", []))
    trips_df = add_date_column(to_dataframe(raw.get("trips", [])))
    events_df = add_date_column(to_dataframe(raw.get("events", [])))

    trips_enriched_df = enrich_trips(trips_df, assets_df, sites_df)
    events_enriched_df = enrich_events(events_df, assets_df, sites_df, event_desc_df)

    fact_trips = build_fact_trips(trips_enriched_df)
    fact_events = build_fact_events(events_enriched_df)

    dim_event_types_df = build_dim_event_types(event_desc_df, events_df)

    return {
        "assets_df": assets_df,
        "sites_df": sites_df,
        "people_df": people_df,
        "organisations_df": organisations_df,
        "event_desc_df": event_desc_df,
        "dim_event_types_df": dim_event_types_df,
        "trips_df": trips_df,
        "events_df": events_df,
        "trips_enriched_df": trips_enriched_df,
        "events_enriched_df": events_enriched_df,
        "fact_trips": fact_trips,
        "fact_events": fact_events,
    }
