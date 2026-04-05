#!/usr/bin/env bash
set -eo pipefail

if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: Postgres client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

if ! [ -x "$(command -v docker-compose)" ]; then
  echo >&2 "❌ Error: Docker Compose is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# kubectl check
if ! [ -x "$(command -v kubectl)" ]; then
  echo >&2 "❌ Error: kubectl is not installed."
  echo >&2 "Please install kubectl: https://kubernetes.io/docs/tasks/tools/"
  exit 1
fi

# Database configuration
echo "🔧 Configuring database settings..."
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=postgres}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"
CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:=8123}"
CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:=9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:=etl}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:=etl}"

# Docker compose setup
USING_DOCKER_COMPOSE=0
if [[ -z "${SKIP_DOCKER}" ]]
then
  USING_DOCKER_COMPOSE=1
  echo "🐳 Starting all services with Docker Compose..."
  
  # Export environment variables for docker-compose
  export POSTGRES_USER="${DB_USER}"
  export POSTGRES_PASSWORD="${DB_PASSWORD}"
  export POSTGRES_DB="${DB_NAME}"
  export POSTGRES_PORT="${DB_PORT}"
  export CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT}"
  export CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT}"
  export CLICKHOUSE_USER="${CLICKHOUSE_USER}"
  export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}"

  # Handle persistent storage
  if [[ -n "${POSTGRES_DATA_VOLUME}" ]]; then
    echo "📁 Setting up Postgres persistent storage at ${POSTGRES_DATA_VOLUME}"
    mkdir -p "${POSTGRES_DATA_VOLUME}"
    export POSTGRES_DATA_VOLUME="${POSTGRES_DATA_VOLUME}"
  else
    echo "📁 No Postgres storage path specified, using default Docker volume"
  fi

  if [[ -n "${CLICKHOUSE_DATA_VOLUME}" ]]; then
    echo "📁 Setting up ClickHouse persistent storage at ${CLICKHOUSE_DATA_VOLUME}"
    mkdir -p "${CLICKHOUSE_DATA_VOLUME}"
    export CLICKHOUSE_DATA_VOLUME="${CLICKHOUSE_DATA_VOLUME}"
  else
    echo "📁 No ClickHouse storage path specified, using default Docker volume"
  fi

  # Pull latest images before starting services
  echo "📥 Pulling latest images..."
  docker-compose -f ./scripts/docker-compose.yaml pull

  # Start all services using docker-compose
  docker-compose -f ./scripts/docker-compose.yaml up -d
  echo "✅ All services started"
fi

# Export DATABASE_URL for potential use by other scripts
export DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}

# Wait for Postgres to be ready
if [[ "${USING_DOCKER_COMPOSE}" == "1" ]]; then
  echo "⏳ Waiting for Postgres to be ready..."
  until docker-compose -f ./scripts/docker-compose.yaml exec -T source-postgres pg_isready -U postgres > /dev/null 2>&1; do
    echo "Waiting for Postgres..."
    sleep 1
  done

  echo "✅ Postgres is up and running on port ${DB_PORT}"

  echo "⏳ Waiting for ClickHouse to be ready..."
  until docker-compose -f ./scripts/docker-compose.yaml exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1" > /dev/null 2>&1; do
    echo "Waiting for ClickHouse..."
    sleep 1
  done

  echo "✅ ClickHouse is up and running on port ${CLICKHOUSE_HTTP_PORT}"
  echo "🔗 ClickHouse HTTP URL: http://localhost:${CLICKHOUSE_HTTP_PORT}"
  echo "🧪 ClickHouse test env: TESTS_CLICKHOUSE_URL=http://localhost:${CLICKHOUSE_HTTP_PORT} TESTS_CLICKHOUSE_USER=${CLICKHOUSE_USER} TESTS_CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}"
else
  echo "⏳ Waiting for Postgres to be ready..."
  until psql "${DATABASE_URL}" -c "select 1" > /dev/null 2>&1; do
    echo "Waiting for Postgres..."
    sleep 1
  done

  echo "✅ Postgres is up and running on port ${DB_PORT}"
  echo "ℹ️  SKIP_DOCKER is set; skipping ClickHouse readiness checks."
fi

echo "🔗 Database URL: ${DATABASE_URL}"

# Run database migrations
echo "🔄 Running database migrations..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "${SCRIPT_DIR}/../etl-api/scripts/run_migrations.sh"

# Seed default replicator image (idempotent).
echo "🖼️ Seeding default replicator image..."
DEFAULT_REPLICATOR_IMAGE="${REPLICATOR_IMAGE:-public.ecr.aws/supabase/etl-replicator:latest}"
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "select app.update_default_image('${DEFAULT_REPLICATOR_IMAGE}');"

# Ensure OrbStack Kubernetes context is available
if ! kubectl config get-contexts -o name | grep -qx "orbstack"; then
  echo >&2 "❌ Error: Kubernetes context 'orbstack' not found."
  echo >&2 "Please install OrbStack (https://orbstack.dev) and enable Kubernetes in its settings."
  exit 1
fi

echo "☸️ Configuring Kubernetes environment..."
kubectl --context orbstack apply -f "${SCRIPT_DIR}/etl-data-plane.yaml"
kubectl --context orbstack apply -f "${SCRIPT_DIR}/trusted-root-certs-config.yaml"

echo "✨ Complete development environment setup finished! Ready to go!"
