"""Safe standalone check that EzyTrack / Telematics Guru dynamic auth works.

Run with:
    python -m scripts.check_ezytrack_auth

Confirms only that the auth request succeeded and that token_type/expires_in
were returned. Never prints the access token, username, or password -- if
you need to verify credentials are even loaded, check them yourself outside
this tool; this script deliberately shows nothing secret.

Does not touch etl.sync_runs, does not fetch assets/trips, does not write to
PostgreSQL. Exit code is non-zero on any auth failure.
"""

from __future__ import annotations

import sys

from connectors.ezytrack_client import EzytrackClient


def run() -> None:
    """Authenticate once and confirm the response shape, without exposing the token."""
    client = EzytrackClient()
    client.authenticate()

    if not client.has_valid_token():
        raise RuntimeError("EzyTrack auth check failed: no token stored after authenticate().")
    if not client.token_type:
        raise RuntimeError("EzyTrack auth check failed: no token_type received.")
    if client.expires_in is None:
        raise RuntimeError("EzyTrack auth check failed: no expires_in received.")

    print(
        f"EzyTrack authentication check: OK "
        f"(token_type={client.token_type}, expires_in={client.expires_in}s)"
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as error:
        print(f"EzyTrack authentication check: FAILED ({error})")
        sys.exit(1)
