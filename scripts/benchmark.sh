#!/usr/bin/env bash
set -eo pipefail

# Check if hyperfine is installed
if ! [ -x "$(command -v hyperfine)" ]; then
  echo >&2 "❌ Error: hyperfine is not installed."
  echo >&2 "Please install it first. You can find installation instructions at:"
  echo >&2 "    https://github.com/sharkdp/hyperfine"
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
BATCH_MAX_SIZE="${BATCH_MAX_SIZE:=100000}"
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
RUN_CMD="${RUN_CMD} --publication-name ${PUBLICATION_NAME} --batch-max-size ${BATCH_MAX_SIZE} --batch-max-fill-ms ${BATCH_MAX_FILL_MS} --max-table-sync-workers ${MAX_TABLE_SYNC_WORKERS}"

echo ""
echo "🚀 Starting benchmark..."

# Run hyperfine
hyperfine \
  --runs "${RUNS}" \
  --show-output \
  --prepare "${PREPARE_CMD}" \
  "${RUN_CMD}"

echo "✨ Benchmark complete!"
