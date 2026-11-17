#!/bin/sh
# LOCAL DEVELOPMENT ONLY -- runs once, when the control-db volume is first
# initialised by the official postgres image (docker-entrypoint-initdb.d).
#
# Creates the control-plane group roles (also created idempotently by
# sql/control/migrations/001) and the dev LOGIN accounts the api and worker
# services connect with, so local runs exercise the same least-privilege
# grants production will use. Production logins are created by an operator,
# never by this script.
set -eu

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v api_password="$GECC_API_DB_PASSWORD" \
     -v worker_password="$GECC_WORKER_DB_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE cc_api NOLOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_api') \gexec
SELECT 'CREATE ROLE cc_worker NOLOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_worker') \gexec
SELECT 'CREATE ROLE cc_readonly NOLOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cc_readonly') \gexec

CREATE ROLE gecc_api LOGIN PASSWORD :'api_password' IN ROLE cc_api;
CREATE ROLE gecc_worker LOGIN PASSWORD :'worker_password' IN ROLE cc_worker;
SQL
