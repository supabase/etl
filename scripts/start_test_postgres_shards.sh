#!/usr/bin/env bash
set -euo pipefail

# Starts one full local test stack plus one additional source Postgres container
# per extra local database shard.
#
# Environment:
#   POSTGRES_VERSION: Postgres image tag for source-postgres.
#     Defaults to 18.
#   NUM_LOCAL_DATABASES: number of local Postgres clusters to start.
#     Defaults to 3.
#   TESTS_DATABASE_START_PORT: base port for the first local Postgres cluster.
#     Defaults to TESTS_DATABASE_PORT, POSTGRES_PORT, or 5430.
#   POSTGRES_USER: user for pg_isready.
#     Defaults to postgres.
#
# Usage:
#   POSTGRES_VERSION=18 NUM_LOCAL_DATABASES=4 TESTS_DATABASE_START_PORT=5430 \
#     bash ./scripts/start_test_postgres_shards.sh

postgres_version="${POSTGRES_VERSION:-18}"
db_count="${NUM_LOCAL_DATABASES:-3}"
base_port="${TESTS_DATABASE_START_PORT:-${TESTS_DATABASE_PORT:-${POSTGRES_PORT:-5430}}}"
postgres_user="${POSTGRES_USER:-postgres}"

if ! [[ "$db_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "expected NUM_LOCAL_DATABASES to be a positive integer, got: $db_count" >&2
  exit 1
fi

if ! [[ "$base_port" =~ ^[0-9]+$ ]]; then
  echo "expected TESTS_DATABASE_START_PORT to be a numeric port, got: $base_port" >&2
  exit 1
fi

echo "starting ${db_count} local Postgres clusters for Postgres ${postgres_version} on ports ${base_port}..$((base_port + db_count - 1))." >&2

POSTGRES_VERSION="$postgres_version" docker compose -f ./scripts/docker-compose.yaml up -d

for ((shard = 2; shard <= db_count; shard++)); do
  port=$((base_port + shard - 1))
  POSTGRES_VERSION="$postgres_version" POSTGRES_PORT="$port" docker compose \
    -p "etl-stack-pg-${postgres_version}-shard-${shard}" \
    -f ./scripts/docker-compose.yaml \
    up -d source-postgres
done

for ((offset = 0; offset < db_count; offset++)); do
  port=$((base_port + offset))
  until pg_isready -h localhost -p "$port" -U "$postgres_user"; do
    sleep 1
  done
done
