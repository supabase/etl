#!/usr/bin/env bash
set -euo pipefail

# These additional endpoints share one database and replication history.
ip -6 addr add fd42:6574:6c::11/64 dev eth0 nodad
ip -6 addr add fd42:6574:6c::12/64 dev eth0 nodad
install -d -o postgres -g postgres /run/postgres-tls
install -m 600 -o postgres -g postgres /certs/server.key /run/postgres-tls/server.key
install -m 644 -o postgres -g postgres /certs/server.crt /run/postgres-tls/server.crt
docker-entrypoint.sh postgres -c listen_addresses='*' -c wal_level=logical \
  -c max_replication_slots=100 -c max_wal_senders=100 -c wal_sender_timeout=10s \
  -c ssl=on -c ssl_cert_file=/run/postgres-tls/server.crt \
  -c ssl_key_file=/run/postgres-tls/server.key &
postgres_pid=$!
python3 /lab/controller.py &
controller_pid=$!
trap 'kill "$postgres_pid" "$controller_pid" 2>/dev/null || true; wait || true' EXIT
wait -n "$postgres_pid" "$controller_pid"
exit 1
