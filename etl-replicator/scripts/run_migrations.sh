#!/usr/bin/env bash
set -eo pipefail

if [ ! -d "etl/migrations" ]; then
  echo >&2 "❌ Error: 'etl/migrations' folder not found."
  echo >&2 "Please run this script from the 'etl' directory."
  exit 1
fi

if ! [ -x "$(command -v sqlx)" ]; then
  echo >&2 "❌ Error: SQLx CLI is not installed."
  echo >&2 "To install it, run:"
  echo >&2 "    cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres"
  exit 1
fi

if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: Postgres client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Database configuration
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=postgres}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# Set up the database URL
export DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}

echo "🔄 Running postgres state store migrations..."

# Create the etl schema if it doesn't exist
# This matches the behavior in etl/src/store/both/postgres.rs
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "create schema if not exists etl;" > /dev/null

# Create a temporary sqlx-cli compatible database URL that sets the search_path
# This ensures the _sqlx_migrations table is created in the etl schema
SQLX_MIGRATIONS_OPTS="options=-csearch_path%3Detl"
MIGRATION_URL="${DATABASE_URL}?${SQLX_MIGRATIONS_OPTS}"

# Run migrations with the modified URL
sqlx database create --database-url "${DATABASE_URL}"
sqlx migrate run --source etl/migrations --database-url "${MIGRATION_URL}"

echo "✨ Postgres state store migrations complete! Ready to go!"
