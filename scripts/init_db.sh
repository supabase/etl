#!/usr/bin/env bash
set -eo pipefail

if [ ! -d "etl-api/migrations" ]; then
  echo >&2 "❌ Error: 'etl-api/migrations' folder not found."
  echo >&2 "Please run this script from the 'etl' directory."
  exit 1
fi

if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: Postgres client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Only check for SQLx if we're not skipping migrations
if [[ -z "${SKIP_MIGRATIONS}" ]]; then
  if ! [ -x "$(command -v sqlx)" ]; then
    echo >&2 "❌ Error: SQLx CLI is not installed."
    echo >&2 "To install it, run:"
    echo >&2 "    cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres"
    exit 1
  fi
fi

# Database configuration
echo "🔧 Configuring database settings..."
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=postgres}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# Docker container setup
if [[ -z "${SKIP_DOCKER}" ]]
then
  echo "🐳 Checking Docker container status..."
  RUNNING_POSTGRES_CONTAINER_ID=$(docker ps --filter 'name=postgres' --format '{{.ID}}')
  if [[ -n $RUNNING_POSTGRES_CONTAINER_ID ]]; then
    echo "✅ Postgres container is already running"
  else
    echo "🚀 Starting new Postgres container..."
    
    # Prepare docker run command
    DOCKER_RUN_CMD="docker run \
        -e POSTGRES_USER=${DB_USER} \
        -e POSTGRES_PASSWORD=${DB_PASSWORD} \
        -e POSTGRES_DB=${DB_NAME} \
        -p "${DB_PORT}":5432 \
        -d"

    # Handle persistent storage
    if [[ -n "${POSTGRES_DATA_VOLUME}" ]]; then
      echo "📁 Setting up persistent storage at ${POSTGRES_DATA_VOLUME}"
      mkdir -p "${POSTGRES_DATA_VOLUME}"
      DOCKER_RUN_CMD="${DOCKER_RUN_CMD} \
        -v "${POSTGRES_DATA_VOLUME}":/var/lib/Postgres/data"
    else
      echo "📁 No storage path specified, using default Docker volume"
    fi

    # Complete the docker run command
    # Increased Postgres settings for logical replication for tests to run smoothly
    DOCKER_RUN_CMD="${DOCKER_RUN_CMD} \
        --name "postgres_$(date '+%s')" \
        postgres:15 -N 1000 \
        -c wal_level=logical \
        -c max_wal_senders=100 \
        -c max_replication_slots=100"

    # Start the container
    eval "${DOCKER_RUN_CMD}"
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

# Set up the database
echo "🔄 Setting up the database..."
export DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}

if [[ -z "${SKIP_MIGRATIONS}" ]]; then
  echo "🔄 Running database migrations..."
  sqlx database create
  sqlx migrate run --source etl-api/migrations
  echo "✨ Database setup complete with migrations! Ready to go!"
else
  echo "⏭️ Skipping migrations as requested."
  echo "✨ Database setup complete! Ready to go!"
fi
