#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

usage() {
  echo "Usage: $0 [OPTIONS] [TARGETS...]"
  echo ""
  echo "Run database migrations for etl components."
  echo ""
  echo "Targets:"
  echo "  etl-api    Run etl-api migrations (public schema)"
  echo "  etl        Run etl migrations (etl schema)"
  echo "  all        Run all migrations (default if no target specified)"
  echo ""
  echo "Options:"
  echo "  -h, --help    Show this help message"
  echo ""
  echo "Examples:"
  echo "  $0              # Run all migrations"
  echo "  $0 etl-api      # Run only etl-api migrations"
  echo "  $0 etl          # Run only etl migrations"
  echo "  $0 etl-api etl  # Run both explicitly"
}

check_sqlx() {
  if ! [ -x "$(command -v sqlx)" ]; then
    echo >&2 "Error: SQLx CLI is not installed."
    echo >&2 "To install it, run:"
    echo >&2 "    cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres"
    exit 1
  fi
}

check_psql() {
  if ! [ -x "$(command -v psql)" ]; then
    echo >&2 "Error: Postgres client (psql) is not installed."
    echo >&2 "Please install it using your system's package manager."
    exit 1
  fi
}

setup_database_url() {
  DB_USER="${POSTGRES_USER:=postgres}"
  DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
  DB_NAME="${POSTGRES_DB:=postgres}"
  DB_PORT="${POSTGRES_PORT:=5430}"
  DB_HOST="${POSTGRES_HOST:=localhost}"

  export DATABASE_URL="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
}

run_etl_api_migrations() {
  local migrations_dir="${ROOT_DIR}/etl-api/migrations"

  if [ ! -d "$migrations_dir" ]; then
    echo >&2 "Error: 'etl-api/migrations' folder not found at $migrations_dir"
    exit 1
  fi

  echo "Running etl-api migrations..."
  sqlx database create
  sqlx migrate run --source "$migrations_dir"
  echo "etl-api migrations complete!"
}

run_etl_migrations() {
  local migrations_dir="${ROOT_DIR}/etl/migrations"

  if [ ! -d "$migrations_dir" ]; then
    echo >&2 "Error: 'etl/migrations' folder not found at $migrations_dir"
    exit 1
  fi

  echo "Running etl migrations..."

  # Create the etl schema if it doesn't exist.
  # This matches the behavior in etl/src/migrations.rs.
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "create schema if not exists etl;" > /dev/null

  # Create a temporary sqlx-cli compatible database URL that sets the search_path.
  # This ensures the _sqlx_migrations table is created in the etl schema.
  local sqlx_migrations_opts="options=-csearch_path%3Detl"
  local migration_url="${DATABASE_URL}?${sqlx_migrations_opts}"

  sqlx database create --database-url "${DATABASE_URL}"
  sqlx migrate run --source "$migrations_dir" --database-url "${migration_url}"
  echo "etl migrations complete!"
}

# Parse arguments
RUN_ETL_API=false
RUN_ETL=false

if [ $# -eq 0 ]; then
  RUN_ETL_API=true
  RUN_ETL=true
fi

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    etl-api)
      RUN_ETL_API=true
      ;;
    etl)
      RUN_ETL=true
      ;;
    all)
      RUN_ETL_API=true
      RUN_ETL=true
      ;;
    *)
      echo >&2 "Error: Unknown argument '$arg'"
      usage
      exit 1
      ;;
  esac
done

# Check dependencies
check_sqlx
if [ "$RUN_ETL" = true ]; then
  check_psql
fi

# Setup database URL
setup_database_url

# Run migrations
if [ "$RUN_ETL_API" = true ]; then
  run_etl_api_migrations
fi

if [ "$RUN_ETL" = true ]; then
  run_etl_migrations
fi

echo "All requested migrations complete!"
