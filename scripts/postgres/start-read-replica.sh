#!/usr/bin/env bash
set -euo pipefail

replication_slot="source_postgres_read_replica"
primary_host="${POSTGRES_PRIMARY_HOST:-source-postgres}"
primary_port="${POSTGRES_PRIMARY_PORT:-5432}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_password="${POSTGRES_PASSWORD:-postgres}"
pgdata="${PGDATA:-/var/lib/postgresql/data}"
max_wal_senders="${POSTGRES_MAX_WAL_SENDERS:-100}"
max_replication_slots="${POSTGRES_MAX_REPLICATION_SLOTS:-100}"
wal_sender_timeout="${POSTGRES_WAL_SENDER_TIMEOUT:-10s}"
wal_receiver_status_interval="${POSTGRES_REPLICA_WAL_RECEIVER_STATUS_INTERVAL:-1s}"
max_standby_streaming_delay="${POSTGRES_REPLICA_MAX_STANDBY_STREAMING_DELAY:--1}"

mkdir -p "$pgdata"
chown -R postgres:postgres "$(dirname "$pgdata")"

if [ ! -s "$pgdata/PG_VERSION" ]; then
  mkdir -p "$pgdata"
  find "$pgdata" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
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
primary_conninfo = 'host=$primary_host port=$primary_port dbname=postgres user=$postgres_user password=$escaped_password application_name=$replication_slot'
primary_slot_name = '$replication_slot'
hot_standby = on
hot_standby_feedback = on
wal_level = logical
max_wal_senders = $max_wal_senders
max_replication_slots = $max_replication_slots
wal_sender_timeout = '$wal_sender_timeout'
wal_receiver_status_interval = '$wal_receiver_status_interval'
max_standby_streaming_delay = '$max_standby_streaming_delay'
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
  -c max_wal_senders="$max_wal_senders" \
  -c max_replication_slots="$max_replication_slots" \
  -c wal_sender_timeout="$wal_sender_timeout" \
  -c wal_receiver_status_interval="$wal_receiver_status_interval" \
  -c max_standby_streaming_delay="$max_standby_streaming_delay" \
  -c hba_file=/etc/postgresql/pg_hba.conf \
  ${POSTGRES_SSL_ARGS:-}
