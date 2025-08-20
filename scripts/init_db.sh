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
  echo "🐳 Checking Docker container status..."
  RUNNING_POSTGRES_CONTAINER_ID=$(docker ps --filter 'name=postgres' --format '{{.ID}}')
  if [[ -n $RUNNING_POSTGRES_CONTAINER_ID ]]; then
    echo "✅ Postgres container is already running"
  else
    echo "🚀 Starting Postgres container with Docker Compose..."

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

    # Start the container using docker-compose
    docker-compose up -d postgres
    echo "✅ Postgres container started"
  fi
fi

# Wait for Postgres to be ready
echo "⏳ Waiting for Postgres to be ready..."
until PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "postgres" -c '\q'; do
  echo "⏳ Postgres is still starting up... waiting"
  sleep 1
done

echo "✅ Postgres is up and running on port ${DB_PORT}"

# Export DATABASE_URL for potential use by other scripts
export DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}
echo "🔗 Database URL: ${DATABASE_URL}"
echo "✨ Database setup complete! Ready to go!"
