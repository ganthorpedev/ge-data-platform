# EzyTrack / Telematics Guru authentication

## What changed and why

The EzyTrack / Telematics Guru ETL (`jobs/sync_ezytrack.py`) used to send a
static bearer token (`TELEMATICS_TOKEN` from `.env`) with every GraphQL
request. Telematics Guru access tokens expire after roughly **24 hours**, so
that token inevitably went stale and every sync failed with an
`AUTH_NOT_AUTHENTICATED` GraphQL error until someone manually generated a new
token (e.g. via Postman) and pasted it into `.env`.

The ETL now **authenticates dynamically at the start of every run** instead
of relying on a manually-copied token. `.env` stores a username/password,
not an access token.

This only affects EzyTrack / Telematics Guru. Sendem uses a static
`X-API-KEY` header (`SENDEM_API_KEY`), a fundamentally different auth model
that doesn't expire on a fixed schedule — confirmed unaffected, not changed.
Trackunit already had its own dynamic OAuth password-grant flow
(`connectors/trackunit_client.py`) and was not touched either.

## How it works now

`connectors/ezytrack_client.py` — `EzytrackClient`:

- `authenticate()` — `GET https://api-emea04.telematics.guru/user/authenticate`
  with an `x-www-form-urlencoded` body of `username`/`password`/`grant_type`
  (per the confirmed Postman contract — a GET request carrying a form body).
  Reads `access_token`, `token_type`, `expires_in` from the JSON response and
  stores them **in memory only** — never written to disk, never written to
  the database, never printed. Only `token_type` and `expires_in` (not
  secrets) are ever logged.
- Every GraphQL request builds its `Authorization` header from the
  in-memory token at call time.
- If a request comes back unauthorized — either a raw HTTP 401, or (what
  this API actually returns) an HTTP 200 with a GraphQL `errors` array
  containing `AUTH_NOT_AUTHENTICATED`/`UNAUTHENTICATED`/`UNAUTHORIZED` — the
  client re-authenticates once and retries that one request once. If the
  retry also fails, the error propagates normally. There is no further
  retry loop.
- `jobs/sync_ezytrack.py` constructs one `EzytrackClient`, calls
  `.authenticate()` explicitly right after the `sync_runs` row is created,
  and reuses that same authenticated client for the asset fetch and every
  trip chunk in the run. An auth failure marks the run `FAILED` with a safe
  message (never the username, password, or token) and re-raises.

## Rate limits are a separate concern

`RateLimitError` (GraphQL `GRAPHQL_COST_RATE_LIMIT_EXCEEDED`) is completely
independent of authentication. It is never retried automatically, and
token refresh is never used as a workaround for it — re-authenticating does
nothing to help a cost-limit rejection. If EzyTrack is rate limited, don't
repeatedly rerun the job; wait and check `etl.sync_runs`/`reporting.vw_provider_sync_health`
before retrying.

## `.env` configuration

```
TELEMATICS_AUTH_URL=https://api-emea04.telematics.guru/user/authenticate
TELEMATICS_GRAPHQL_URL=https://api-emea04.telematics.guru/graphql/
TELEMATICS_USERNAME=<real username>
TELEMATICS_PASSWORD=<real password>
TELEMATICS_GRANT_TYPE=password
TELEMATICS_ORGANISATION_ID=<real organisation id>
```

`TELEMATICS_TOKEN` is **deprecated** and no longer read by any code path —
it's left in `.env.example` only for backward-compatible visibility. Real
credentials belong in `.env` only, never in `.env.example`, never committed,
never printed or logged.

## Verifying the fix without running a full sync

```
python -m scripts.check_ezytrack_auth
```

Confirms the auth request succeeds and that `token_type`/`expires_in` came
back — it never prints the access token itself. Non-zero exit code on
failure.
