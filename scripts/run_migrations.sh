#!/usr/bin/env bash
set -eo pipefail

if [ ! -d "etl-api/migrations" ]; then
  echo >&2 "❌ Error: 'etl-api/migrations' folder not found."
  echo >&2 "Please run this script from the 'etl' directory."
  exit 1
fi

if ! [ -x "$(command -v sqlx)" ]; then
  echo >&2 "❌ Error: SQLx CLI is not installed."
  echo >&2 "To install it, run:"
  echo >&2 "    cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres"
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

echo "🔄 Running database migrations..."
sqlx database create
sqlx migrate run --source etl-api/migrations
echo "✨ Database migrations complete! Ready to go!"
