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

# Docker compose setup
if [[ -z "${SKIP_DOCKER}" ]]
then
  echo "🐳 Starting all services with Docker Compose..."
  
  # Export environment variables for docker-compose
  export POSTGRES_USER="${DB_USER}"
  export POSTGRES_PASSWORD="${DB_PASSWORD}"
  export POSTGRES_DB="${DB_NAME}"
  export POSTGRES_PORT="${DB_PORT}"

  # Handle persistent storage
  if [[ -n "${POSTGRES_DATA_VOLUME}" ]]; then
    echo "📁 Setting up persistent storage at ${POSTGRES_DATA_VOLUME}"
    mkdir -p "${POSTGRES_DATA_VOLUME}"
    export POSTGRES_DATA_VOLUME="${POSTGRES_DATA_VOLUME}"
  else
    echo "📁 No storage path specified, using default Docker volume"
  fi

  # Start all services using docker-compose
  docker-compose -f ./scripts/docker-compose.yaml up -d
  echo "✅ All services started"
fi

# Wait for Postgres to be ready
echo "⏳ Waiting for Postgres to be ready..."
until docker-compose -f ./scripts/docker-compose.yaml exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do 
  echo "Waiting for Postgres..."
  sleep 1
done

echo "✅ Postgres is up and running on port ${DB_PORT}"

# Export DATABASE_URL for potential use by other scripts
export DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}
echo "🔗 Database URL: ${DATABASE_URL}"

# Run database migrations
echo "🔄 Running database migrations..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "${SCRIPT_DIR}/../etl-api/scripts/run_migrations.sh"

# Seed default replicator image (idempotent).
echo "🖼️ Seeding default replicator image..."
DEFAULT_REPLICATOR_IMAGE="${REPLICATOR_IMAGE:-ramsup/replicator:0.0.22}"
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "\
insert into app.images (name, is_default) \
select '${DEFAULT_REPLICATOR_IMAGE}', true \
where not exists (select 1 from app.images where name = '${DEFAULT_REPLICATOR_IMAGE}');"

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
