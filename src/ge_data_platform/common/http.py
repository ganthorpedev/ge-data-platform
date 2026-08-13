"""Shared `requests` session factory with bounded transient-failure retries.

Used by the Sendem and EzyTrack connectors (and the Trackunit token request
indirectly via the same policy values). Retries apply ONLY to transient
failures: connection errors, timeouts, and HTTP 500/502/503/504. Ordinary
4xx responses are never retried by the adapter. Trackunit's 401 refresh and
429 backoff are provider-specific and live in
ge_data_platform.sources.trackunit.client -- that client deliberately does
NOT use this adapter, so retries are never doubled up.
"""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ge_data_platform.config.settings import HttpSettings, get_http_settings

TRANSIENT_STATUS_CODES = (500, 502, 503, 504)


def build_retrying_session(
    http_settings: HttpSettings | None = None,
    *,
    allowed_methods: tuple[str, ...] = ("GET",),
) -> requests.Session:
    """Build a `requests.Session` that retries transient failures with backoff.

    `allowed_methods` must explicitly include "POST" for callers whose POSTs
    are safe to repeat (e.g. read-only GraphQL queries) -- POST is not
    retried by default. `raise_on_status=False` means that after retries are
    exhausted the final response is returned as-is, so existing
    `response.ok` / `raise_for_status()` error handling in the connectors
    keeps working unchanged.
    """
    settings = http_settings or get_http_settings()

    # HTTP_MAX_RETRIES is intentionally interpreted as the maximum number of
    # attempts, including the initial request. urllib3's Retry counters mean
    # retries *after* the initial request, hence the subtraction here.
    retry_count = max(settings.max_retries - 1, 0)
    retry = Retry(
        total=retry_count,
        connect=retry_count,
        read=retry_count,
        status=retry_count,
        status_forcelist=TRANSIENT_STATUS_CODES,
        backoff_factor=settings.backoff_seconds,
        allowed_methods=frozenset(allowed_methods),
        raise_on_status=False,
        # urllib3 otherwise treats 413/429/503 with Retry-After as retryable
        # even when they are absent from status_forcelist.  Provider-neutral
        # retries must stay limited to TRANSIENT_STATUS_CODES; Trackunit owns
        # its explicit 429 policy in its connector.
        respect_retry_after_header=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
