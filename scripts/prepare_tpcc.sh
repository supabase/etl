#!/usr/bin/env bash
set -eo pipefail

# Check if go-tpc is installed
if ! [ -x "$(command -v go-tpc)" ]; then
  echo >&2 "❌ Error: go-tpc is not installed."
  echo >&2 "Please install it first. You can find installation instructions at:"
  echo >&2 "    https://github.com/pingcap/go-tpc"
  exit 1
fi

# Check if psql is installed
if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: PostgreSQL client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Database configuration with defaults
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=bench}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"
WAREHOUSES="${TPCC_WAREHOUSES:=100}"
THREADS="${TPCC_THREADS:=40}"

echo "🏭 Preparing TPC-C data with go-tpc..."
echo "📊 Configuration:"
echo "   Database: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
echo "   User: ${DB_USER}"
echo "   Warehouses: ${WAREHOUSES}"
echo "   Threads: ${THREADS}"

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
until PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "postgres" -c '\q'; do
  echo "⏳ PostgreSQL is still starting up... waiting"
  sleep 1
done

echo "✅ PostgreSQL is up and running on port ${DB_PORT}"

# Create the database if it doesn't exist
echo "🔄 Ensuring database '${DB_NAME}' exists..."
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "postgres" -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 || {
  echo "📦 Creating database '${DB_NAME}'..."
  PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "postgres" -c "CREATE DATABASE \"${DB_NAME}\""
  echo "✅ Database '${DB_NAME}' created successfully"
}

echo "✅ Database '${DB_NAME}' is ready"

# Check if TPC-C data already exists
echo "🔍 Checking if TPC-C data already exists..."
TPCC_EXISTS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
  select count(*) 
  from pg_catalog.pg_tables 
  where schemaname = 'public' 
    and tablename in ('customer', 'district', 'item', 'new_order', 'order_line', 'orders', 'stock', 'warehouse');
" 2>/dev/null || echo "0")

if [[ "${TPCC_EXISTS}" -eq 9 ]]; then
  echo "✅ TPC-C tables already exist, skipping data generation"
  echo "💡 To regenerate data, drop the tables first or use a different database"
else
  echo "📦 TPC-C tables not found, proceeding with data generation..."
  
  # Run the go-tpc command
  go-tpc tpcc \
    --warehouses "${WAREHOUSES}" \
    prepare \
    -d postgres \
    -U "${DB_USER}" \
    -p "${DB_PASSWORD}" \
    -D "${DB_NAME}" \
    -H "${DB_HOST}" \
    -P "${DB_PORT}" \
    --conn-params sslmode=disable \
    -T "${THREADS}" \
    --no-check

  echo "✅ TPC-C data preparation complete!"
fi

# Create publication for logical replication
echo "📡 Creating publication 'bench_pub' for TPC-C tables..."
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -c "
  -- Drop publication if it exists
  drop publication if exists bench_pub;
  
  -- Create publication with all TPC-C tables
  create publication bench_pub for table 
    customer,
    district,
    item,
    new_order,
    order_line,
    orders,
    stock,
    warehouse;
"

echo "✅ Publication 'bench_pub' created successfully!"
echo "✨ TPC-C preparation complete with publication setup!"
