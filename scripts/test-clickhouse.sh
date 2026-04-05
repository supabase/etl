#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-./scripts/docker-compose.yaml}"
DOCKER_COMPOSE_BIN="${DOCKER_COMPOSE_BIN:-docker-compose}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-source-postgres}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
POSTGRES_PORT="${POSTGRES_PORT:-5430}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-etl}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-etl}"
CARGO_TOOLCHAIN="${CARGO_TOOLCHAIN:-}"
TEST_TARGET="${TEST_TARGET:-clickhouse_pipeline}"
TEST_NAME_FILTER="${TEST_NAME_FILTER:-}"
CARGO_PACKAGE="${CARGO_PACKAGE:-etl-destinations}"
FEATURES="${FEATURES:-clickhouse,test-utils}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo >&2 "❌ Error: required command '$1' is not installed."
    exit 1
  fi
}

require_cmd "$DOCKER_COMPOSE_BIN"
require_cmd cargo

if [[ -z "${SKIP_DOCKER:-}" ]]; then
  echo "🐳 Starting local Postgres and ClickHouse services..."
  "$DOCKER_COMPOSE_BIN" -f "$COMPOSE_FILE" up -d "$POSTGRES_SERVICE" "$CLICKHOUSE_SERVICE"

  echo "⏳ Waiting for Postgres to be ready..."
  until "$DOCKER_COMPOSE_BIN" -f "$COMPOSE_FILE" exec -T "$POSTGRES_SERVICE" pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; do
    echo "Waiting for Postgres..."
    sleep 1
  done

  echo "⏳ Waiting for ClickHouse to be ready..."
  until "$DOCKER_COMPOSE_BIN" -f "$COMPOSE_FILE" exec -T "$CLICKHOUSE_SERVICE" clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1" >/dev/null 2>&1; do
    echo "Waiting for ClickHouse..."
    sleep 1
  done
fi

export TESTS_DATABASE_HOST="${TESTS_DATABASE_HOST:-localhost}"
export TESTS_DATABASE_PORT="${TESTS_DATABASE_PORT:-$POSTGRES_PORT}"
export TESTS_DATABASE_USERNAME="${TESTS_DATABASE_USERNAME:-$POSTGRES_USER}"
export TESTS_DATABASE_PASSWORD="${TESTS_DATABASE_PASSWORD:-$POSTGRES_PASSWORD}"
export TESTS_CLICKHOUSE_URL="${TESTS_CLICKHOUSE_URL:-http://localhost:$CLICKHOUSE_HTTP_PORT}"
export TESTS_CLICKHOUSE_USER="${TESTS_CLICKHOUSE_USER:-$CLICKHOUSE_USER}"
export TESTS_CLICKHOUSE_PASSWORD="${TESTS_CLICKHOUSE_PASSWORD:-$CLICKHOUSE_PASSWORD}"

if [[ -n "$CARGO_TOOLCHAIN" ]]; then
  CARGO_CMD=(cargo "+$CARGO_TOOLCHAIN")
else
  CARGO_CMD=(cargo)
fi

echo "🧪 Running ClickHouse destination test with:"
echo "   TESTS_DATABASE_HOST=$TESTS_DATABASE_HOST"
echo "   TESTS_DATABASE_PORT=$TESTS_DATABASE_PORT"
echo "   TESTS_DATABASE_USERNAME=$TESTS_DATABASE_USERNAME"
echo "   TESTS_CLICKHOUSE_URL=$TESTS_CLICKHOUSE_URL"
echo "   TESTS_CLICKHOUSE_USER=$TESTS_CLICKHOUSE_USER"
echo "   TESTS_CLICKHOUSE_PASSWORD=${TESTS_CLICKHOUSE_PASSWORD:+[set]}"
echo "   cargo toolchain=${CARGO_TOOLCHAIN:-project default}"

TEST_ARGS=(test -p "$CARGO_PACKAGE" --features "$FEATURES" --test "$TEST_TARGET")
if [[ -n "$TEST_NAME_FILTER" ]]; then
  TEST_ARGS+=("$TEST_NAME_FILTER")
fi
TEST_ARGS+=(-- --nocapture)

echo "🚀 ${CARGO_CMD[*]} ${TEST_ARGS[*]}"
"${CARGO_CMD[@]}" "${TEST_ARGS[@]}"
