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

ASSET_COLUMNS = [
    "AssetId",
    "SiteId",
    "AssetType",
    "Description",
    "VinNumber",
    "Country",
    "GroupId",
    "RegistrationNumber",
    "IsAvailable",
    "FleetNumber",
    "Make",
    "Model",
    "FuelType",
    "Year",
]
SITE_COLUMNS = ["SiteId", "SiteName"]
EVENT_DESCRIPTION_COLUMNS = [
    "EventTypeId",
    "EventName",
    "GroupId",
    "MetricType",
    "UnitType",
    "EventCategory",
]
TRIP_COLUMNS = [
    "DateKey",
    "GroupId",
    "SiteId",
    "AssetId",
    "TotalTripCount",
    "TotalTripDistanceKilometres",
    "TotalFuelUsedLitres",
    "TotalEnergyUsedKwh",
]
EVENT_COLUMNS = [
    "DateKey",
    "GroupId",
    "SiteId",
    "AssetId",
    "EventTypeId",
    "TotalEventOccurrences",
    "MinEventValue",
    "MaxEventValue",
    "TotalEventValue",
    "MinEventDuration",
    "MaxEventDuration",
    "TotalEventDuration",
]


def to_dataframe(data: list | dict, expected_columns: list[str] | None = None) -> pd.DataFrame:
    """Normalize a raw JSON payload (list or dict) into a flat DataFrame.

    Empty payloads retain `expected_columns`, which prevents downstream
    merges and loaders from receiving a schema-less DataFrame. For non-empty
    payloads, source columns not in the expected schema are preserved while
    missing expected columns are added as nullable values.
    """
    if not data:
        return pd.DataFrame(columns=expected_columns or [])

    frame = pd.json_normalize(data)
    for column in expected_columns or []:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame


def _left_join_dimension(
    facts_df: pd.DataFrame,
    dimension_df: pd.DataFrame,
    key: str,
    suffix: str,
) -> pd.DataFrame:
    """Left-join a dimension without failing when that dimension is empty.

    Avoiding pandas' merge for an empty right-hand frame also avoids dtype
    mismatch errors between a populated numeric fact key and an empty
    object-typed dimension key.
    """
    enriched = facts_df.copy()
    if enriched.empty or dimension_df.empty or key not in dimension_df.columns:
        for column in dimension_df.columns:
            if column == key:
                continue
            output_column = column if column not in enriched.columns else f"{column}{suffix}"
            if output_column not in enriched.columns:
                enriched[output_column] = pd.NA
        return enriched

    return enriched.merge(dimension_df, on=key, how="left", suffixes=("", suffix))


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
    enriched = _left_join_dimension(trips_df, assets_df, "AssetId", "_asset")
    enriched = _left_join_dimension(enriched, sites_df, "SiteId", "_site")
    return add_date_column(enriched)


def build_fact_trips(trips_enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Return the fact-trips schema, filling absent enrichment fields with nulls."""
    return trips_enriched_df.reindex(columns=FACT_TRIPS_COLUMNS).copy()


def enrich_events(
    events_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    sites_df: pd.DataFrame,
    event_desc_df: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join events onto assets, sites, and event descriptions, and add a `date` column."""
    enriched = _left_join_dimension(events_df, assets_df, "AssetId", "_asset")
    enriched = _left_join_dimension(enriched, sites_df, "SiteId", "_site")
    enriched = _left_join_dimension(enriched, event_desc_df, "EventTypeId", "_event")
    return add_date_column(enriched)


def build_fact_events(events_enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Return the fact-events schema, filling absent enrichment fields with nulls."""
    return events_enriched_df.reindex(columns=FACT_EVENTS_COLUMNS).copy()


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

    `raw` is expected to have assets, sites, event_descriptions, trips, and
    events. Legacy people/organisations values are still accepted in the
    returned diagnostic frames, but the production job no longer fetches
    those unused endpoints because neither the transforms nor load plan uses
    them.
    """
    assets_df = to_dataframe(raw.get("assets", []), ASSET_COLUMNS)
    sites_df = to_dataframe(raw.get("sites", []), SITE_COLUMNS)
    people_df = to_dataframe(raw.get("people", []))
    organisations_df = to_dataframe(raw.get("organisations", []))
    event_desc_df = to_dataframe(raw.get("event_descriptions", []), EVENT_DESCRIPTION_COLUMNS)
    trips_df = add_date_column(to_dataframe(raw.get("trips", []), TRIP_COLUMNS))
    events_df = add_date_column(to_dataframe(raw.get("events", []), EVENT_COLUMNS))

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
