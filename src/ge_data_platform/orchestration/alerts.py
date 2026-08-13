"""Provider-neutral operational webhook alerts and freshness helpers."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from ge_data_platform.config.settings import EtlOpsSettings, get_etl_ops_settings


logger = logging.getLogger(__name__)
ALERT_WEBHOOK_TIMEOUT_SECONDS = 10.0
FRESHNESS_CURSOR_VERSION = 1


@dataclass(frozen=True)
class AlertDelivery:
    """Non-throwing result from an operational alert attempt."""

    delivered: bool
    reason: str
    dedupe_key: str


@dataclass(frozen=True)
class FreshnessTarget:
    """One provider and its configured maximum successful-sync age."""

    provider: str
    source_system: str
    max_success_age_hours: int


def as_utc_datetime(value: object) -> datetime | None:
    """Normalize a database/ISO timestamp to aware UTC, or return ``None``."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        if not normalized:
            return None
        try:
            value = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def provider_freshness_targets(settings: EtlOpsSettings) -> tuple[FreshnessTarget, ...]:
    """Build all configured provider freshness checks."""
    return (
        FreshnessTarget("sendem", "sendem", settings.sendem_max_success_age_hours),
        FreshnessTarget("ezytrack", "ezytrack", settings.ezytrack_max_success_age_hours),
        FreshnessTarget("trackunit", "trackunit", settings.trackunit_max_success_age_hours),
    )


def latest_success_at(sync_run: dict[str, object] | None) -> datetime | None:
    """Return the best completion timestamp from a latest-success row."""
    if not sync_run:
        return None
    return as_utc_datetime(sync_run.get("finished_at")) or as_utc_datetime(
        sync_run.get("started_at")
    )


def is_freshness_stale(
    last_success_at: datetime | None,
    *,
    now_utc: datetime,
    max_success_age_hours: int,
) -> bool:
    """Return whether a provider has exceeded its success-age threshold."""
    if max_success_age_hours < 1:
        raise ValueError("max_success_age_hours must be at least 1")
    normalized_now = as_utc_datetime(now_utc)
    if normalized_now is None:
        raise ValueError("now_utc must be a datetime")
    normalized_success = as_utc_datetime(last_success_at)
    if normalized_success is None:
        return True
    return normalized_success < normalized_now - timedelta(hours=max_success_age_hours)


def alert_due_after_cooldown(
    last_alerted_at: datetime | None,
    *,
    now_utc: datetime,
    cooldown_minutes: int,
) -> bool:
    """Return whether a dedupe key may alert again."""
    if cooldown_minutes < 1:
        raise ValueError("cooldown_minutes must be at least 1")
    normalized_last = as_utc_datetime(last_alerted_at)
    normalized_now = as_utc_datetime(now_utc)
    if normalized_now is None:
        raise ValueError("now_utc must be a datetime")
    if normalized_last is None:
        return True
    return normalized_last <= normalized_now - timedelta(minutes=cooldown_minutes)


def decode_freshness_cursor(cursor: str | None) -> dict[str, datetime]:
    """Decode the durable Dagster sensor cursor; malformed state resets safely."""
    if not cursor:
        return {}
    try:
        document = json.loads(cursor)
    except (TypeError, ValueError):
        return {}
    if not isinstance(document, dict):
        return {}
    if document.get("version") != FRESHNESS_CURSOR_VERSION:
        return {}
    values = document.get("last_alerted_at", {})
    if not isinstance(values, dict):
        return {}

    decoded: dict[str, datetime] = {}
    for key, value in values.items():
        timestamp = as_utc_datetime(value)
        if isinstance(key, str) and timestamp is not None:
            decoded[key] = timestamp
    return decoded


def encode_freshness_cursor(last_alerted_at: dict[str, datetime]) -> str:
    """Encode cooldown state for Dagster's persistent sensor cursor."""
    encoded_values: dict[str, str] = {}
    for key, value in sorted(last_alerted_at.items()):
        timestamp = as_utc_datetime(value)
        if timestamp is not None:
            encoded_values[key] = timestamp.isoformat()
    document = {
        "version": FRESHNESS_CURSOR_VERSION,
        "last_alerted_at": encoded_values,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def build_alert_payload(
    *,
    event_type: str,
    provider: str,
    job_name: str,
    run_id: str | None,
    failure_message: str,
    timestamp: datetime,
    last_success_at: datetime | None,
    dedupe_key: str,
) -> dict[str, Any]:
    """Build a generic JSON document suitable for any webhook relay."""
    timestamp_utc = as_utc_datetime(timestamp)
    if timestamp_utc is None:
        raise ValueError("timestamp must be a datetime")
    last_success_utc = as_utc_datetime(last_success_at)
    last_success_text = last_success_utc.isoformat() if last_success_utc else None
    text = (
        f"Telemetry alert [{event_type}] provider={provider} job={job_name} "
        f"run_id={run_id or 'n/a'} failure={failure_message} "
        f"timestamp={timestamp_utc.isoformat()} "
        f"last_success={last_success_text or 'none'}"
    )
    return {
        "event_type": event_type,
        "provider": provider,
        "job_name": job_name,
        "run_id": run_id,
        "failure_message": failure_message,
        "timestamp": timestamp_utc.isoformat(),
        "last_success_at": last_success_text,
        "dedupe_key": dedupe_key,
        "text": text,
    }


def send_operational_alert(
    *,
    event_type: str,
    provider: str,
    job_name: str,
    run_id: str | None,
    failure_message: str,
    timestamp: datetime,
    last_success_at: datetime | None,
    dedupe_key: str,
    settings: EtlOpsSettings | None = None,
    log: object = logger,
) -> AlertDelivery:
    """Send one generic webhook alert without ever failing ETL work.

    Disabled/missing configuration and delivery errors are deliberately
    returned as data after being logged.  Alerting must remain an observability
    aid, never a second root cause that replaces the provider failure.
    """
    resolved_settings = settings or get_etl_ops_settings()
    payload = build_alert_payload(
        event_type=event_type,
        provider=provider,
        job_name=job_name,
        run_id=run_id,
        failure_message=failure_message,
        timestamp=timestamp,
        last_success_at=last_success_at,
        dedupe_key=dedupe_key,
    )

    if not resolved_settings.alerts_enabled:
        log.warning(  # type: ignore[attr-defined]
            "Operational alert not delivered because TELEMETRY_ALERTS_ENABLED=false: %s",
            payload["text"],
        )
        return AlertDelivery(False, "alerts_disabled", dedupe_key)
    if not resolved_settings.alert_webhook_url:
        log.warning(  # type: ignore[attr-defined]
            "Operational alert not delivered because TELEMETRY_ALERT_WEBHOOK_URL is empty: %s",
            payload["text"],
        )
        return AlertDelivery(False, "webhook_not_configured", dedupe_key)

    try:
        response = requests.post(
            resolved_settings.alert_webhook_url,
            json=payload,
            timeout=ALERT_WEBHOOK_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception as error:
        log.error(  # type: ignore[attr-defined]
            "Operational alert delivery failed for %s (%s): %s. Payload: %s",
            dedupe_key,
            type(error).__name__,
            error,
            payload["text"],
        )
        return AlertDelivery(False, f"delivery_error:{type(error).__name__}", dedupe_key)

    log.info(  # type: ignore[attr-defined]
        "Operational alert delivered: event=%s dedupe_key=%s",
        event_type,
        dedupe_key,
    )
    return AlertDelivery(True, "delivered", dedupe_key)
