#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

PG_MAJOR="${PG_MAJOR:-17}"
PGDATA="/var/lib/postgresql/${PG_MAJOR}/main"
PGCONF="/etc/postgresql/${PG_MAJOR}/main/postgresql.conf"

if [ -n "${POSTGRES_PASSWORD:-}" ]; then
    pg_ctl -D "${PGDATA}" -o "-c config_file=${PGCONF}" -w start >/dev/null
    psql -U postgres -c "ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}';" >/dev/null
    pg_ctl -D "${PGDATA}" -m fast -w stop >/dev/null
fi

exec postgres -D "${PGDATA}" -c "config_file=${PGCONF}" "$@"
