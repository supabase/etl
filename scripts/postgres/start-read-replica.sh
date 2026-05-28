#!/usr/bin/env bash
set -euo pipefail

replication_slot="source_postgres_read_replica"
primary_host="${POSTGRES_PRIMARY_HOST:-source-postgres}"
primary_port="${POSTGRES_PRIMARY_PORT:-5432}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_password="${POSTGRES_PASSWORD:-postgres}"
pgdata="${PGDATA:-/var/lib/postgresql/data}"

mkdir -p "$pgdata"
chown -R postgres:postgres "$(dirname "$pgdata")"

if [ ! -s "$pgdata/PG_VERSION" ]; then
  rm -rf "$pgdata"
  mkdir -p "$pgdata"
  chown -R postgres:postgres "$pgdata"

  export PGPASSWORD="$postgres_password"

  until gosu postgres psql \
    --host="$primary_host" \
    --port="$primary_port" \
    --username="$postgres_user" \
    --dbname=postgres \
    --set=ON_ERROR_STOP=1 \
    --command="select pg_create_physical_replication_slot('$replication_slot') where not exists (select 1 from pg_replication_slots where slot_name = '$replication_slot');"
  do
    sleep 1
  done

  gosu postgres pg_basebackup \
    --host="$primary_host" \
    --port="$primary_port" \
    --username="$postgres_user" \
    --pgdata="$pgdata" \
    --wal-method=stream \
    --slot="$replication_slot"

  escaped_password="${postgres_password//\'/\'\'}"
  cat >> "$pgdata/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=$primary_host port=$primary_port user=$postgres_user password=$escaped_password application_name=$replication_slot'
primary_slot_name = '$replication_slot'
hot_standby = on
hot_standby_feedback = on
wal_level = logical
max_wal_senders = 100
max_replication_slots = 100
wal_sender_timeout = '10s'
EOF
  touch "$pgdata/standby.signal"
  chown -R postgres:postgres "$pgdata"
  chmod 700 "$pgdata"
fi

chown -R postgres:postgres "$pgdata"
chmod 700 "$pgdata"

exec gosu postgres postgres \
  -D "$pgdata" \
  -N 1000 \
  -c listen_addresses='*' \
  -c hot_standby=on \
  -c hot_standby_feedback=on \
  -c wal_level=logical \
  -c max_wal_senders=100 \
  -c max_replication_slots=100 \
  -c wal_sender_timeout=10s \
  -c hba_file=/etc/postgresql/pg_hba.conf \
  ${POSTGRES_SSL_ARGS:-}
