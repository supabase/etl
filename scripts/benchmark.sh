#!/usr/bin/env bash
set -eo pipefail

# Check if hyperfine is installed
if ! [ -x "$(command -v hyperfine)" ]; then
  echo >&2 "❌ Error: hyperfine is not installed."
  echo >&2 "Please install it first. You can find installation instructions at:"
  echo >&2 "    https://github.com/sharkdp/hyperfine"
  exit 1
fi

# Check if psql is installed
if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: PostgreSQL client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Database configuration with defaults (matching prepare_tpcc.sh)
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=bench}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# Benchmark configuration
RUNS="${HYPERFINE_RUNS:=3}"
PUBLICATION_NAME="${PUBLICATION_NAME:=bench_pub}"
BATCH_MAX_SIZE="${BATCH_MAX_SIZE:=1000000}"
BATCH_MAX_FILL_MS="${BATCH_MAX_FILL_MS:=10000}"
MAX_TABLE_SYNC_WORKERS="${MAX_TABLE_SYNC_WORKERS:=8}"

echo "🏁 Running table copies benchmark with hyperfine..."
echo "📊 Configuration:"
echo "   Database: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
echo "   User: ${DB_USER}"
echo "   Runs: ${RUNS}"
echo "   Publication: ${PUBLICATION_NAME}"
echo "   Batch size: ${BATCH_MAX_SIZE}"
echo "   Batch fill time: ${BATCH_MAX_FILL_MS}ms"
echo "   Workers: ${MAX_TABLE_SYNC_WORKERS}"

# Get table IDs from the database for TPC-C tables
echo "🔍 Querying table IDs from database..."
TPCC_TABLE_IDS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
  select string_agg(oid::text, ',')
  from pg_class
  where relname in ('warehouse', 'district', 'customer', 'history', 'new_order', 'orders', 'order_line', 'stock', 'item')
    and relkind = 'r';
" 2>/dev/null || echo "")

if [[ -z "${TPCC_TABLE_IDS}" ]]; then
  echo "❌ Error: Could not retrieve table IDs from database. Make sure TPC-C tables exist."
  echo "💡 Run './scripts/prepare_tpcc.sh' first to create the tables."
  exit 1
fi

echo "✅ Found table IDs: ${TPCC_TABLE_IDS}"

# Build the prepare command
PREPARE_CMD="cargo bench --bench table_copies -- prepare --host ${DB_HOST} --port ${DB_PORT} --database ${DB_NAME} --username ${DB_USER}"
if [[ -n "${DB_PASSWORD}" && "${DB_PASSWORD}" != "" ]]; then
  PREPARE_CMD="${PREPARE_CMD} --password ${DB_PASSWORD}"
fi

# Build the run command
RUN_CMD="cargo bench --bench table_copies -- run --host ${DB_HOST} --port ${DB_PORT} --database ${DB_NAME} --username ${DB_USER}"
if [[ -n "${DB_PASSWORD}" && "${DB_PASSWORD}" != "" ]]; then
  RUN_CMD="${RUN_CMD} --password ${DB_PASSWORD}"
fi
RUN_CMD="${RUN_CMD} --publication-name ${PUBLICATION_NAME} --batch-max-size ${BATCH_MAX_SIZE} --batch-max-fill-ms ${BATCH_MAX_FILL_MS} --max-table-sync-workers ${MAX_TABLE_SYNC_WORKERS} --table-ids ${TPCC_TABLE_IDS}"

echo ""
echo "🚀 Starting benchmark..."

# Run hyperfine
hyperfine \
  --runs "${RUNS}" \
  --show-output \
  --prepare "${PREPARE_CMD}" \
  "${RUN_CMD}"

echo "✨ Benchmark complete!"
