#!/usr/bin/env bash
set -eo pipefail

# Table Copies Benchmark Script
#
# This script runs the table copies benchmark using hyperfine with configurable destinations.
#
# How it works:
# - Creates replication slots automatically when the benchmark starts
# - Runs the table copy operation and measures performance
# - Automatically cleans up replication slots after completion
#
# Supported destinations:
# - null: Discards all data (fastest, default)
# - big-query: Streams data to Google BigQuery
#
# Environment Variables:
#   Database Configuration:
#     POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT, POSTGRES_HOST
#
#   Benchmark Configuration:
#     HYPERFINE_RUNS, PUBLICATION_NAME, BATCH_MAX_SIZE, BATCH_MAX_FILL_MS, MAX_TABLE_SYNC_WORKERS
#     MAX_COPY_CONNECTIONS_PER_TABLE - Number of parallel connections per table for copying (default: 1)
#     TPCC_TABLES - Comma-separated list of TPC-C tables to replicate (default: all 8 tables)
#                   Example: TPCC_TABLES="order_line,stock,customer"
#                   Available: customer,district,item,new_order,order_line,orders,stock,warehouse
#                   Note: Publication will be recreated with only these tables
#     DESTINATION (null or big-query)
#     LOG_TARGET (terminal or file) - Where to send logs (default: terminal)
#     DRY_RUN (true/false) - Show commands without executing them
#     PREPARE_TPCC (true/false) - Automatically run prepare_tpcc.sh if tables don't exist (default: true)
#   
#   BigQuery Configuration (required when DESTINATION=big-query):
#     BQ_PROJECT_ID - Google Cloud project ID
#     BQ_DATASET_ID - BigQuery dataset ID
#     BQ_SA_KEY_FILE - Path to service account key JSON file
#     BQ_MAX_STALENESS_MINS - Optional staleness setting
#
# Examples:
#   # Run with null destination and terminal logs (default)
#   ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with file logging
#   LOG_TARGET=file ./etl-benchmarks/scripts/benchmark.sh
#
#   # Dry run to see commands that would be executed
#   DRY_RUN=true ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with only large tables
#   TPCC_TABLES="order_line,stock,customer" ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with only small tables
#   TPCC_TABLES="warehouse,district,item" ./etl-benchmarks/scripts/benchmark.sh
#
#   # Run with BigQuery destination
#   DESTINATION=big-query \
#   BQ_PROJECT_ID=my-project \
#   BQ_DATASET_ID=my_dataset \
#   BQ_SA_KEY_FILE=/path/to/sa-key.json \
#   ./etl-benchmarks/scripts/benchmark.sh

# Check if hyperfine is installed
if ! [ -x "$(command -v hyperfine)" ]; then
  echo >&2 "❌ Error: hyperfine is not installed."
  echo >&2 "Please install it first. You can find installation instructions at:"
  echo >&2 "    https://github.com/sharkdp/hyperfine"
  exit 1
fi

# Check if psql is installed
if ! [ -x "$(command -v psql)" ]; then
  echo >&2 "❌ Error: Postgres client (psql) is not installed."
  echo >&2 "Please install it using your system's package manager."
  exit 1
fi

# Database configuration with defaults (matching prepare_tpcc.sh)
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=bench}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# TPC-C table configuration
# Default: all TPC-C tables
# Override with comma-separated list: TPCC_TABLES="order_line,stock,customer"
DEFAULT_TPCC_TABLES="customer,district,item,new_order,order_line,orders,stock,warehouse"
TPCC_TABLES="${TPCC_TABLES:=$DEFAULT_TPCC_TABLES}"

# Benchmark configuration
RUNS="${HYPERFINE_RUNS:=1}"
PUBLICATION_NAME="${PUBLICATION_NAME:=bench_pub}"
BATCH_MAX_SIZE="${BATCH_MAX_SIZE:=1000000}"
BATCH_MAX_FILL_MS="${BATCH_MAX_FILL_MS:=10000}"
MAX_TABLE_SYNC_WORKERS="${MAX_TABLE_SYNC_WORKERS:=8}"
MAX_COPY_CONNECTIONS_PER_TABLE="${MAX_COPY_CONNECTIONS_PER_TABLE:=1}"

# Logging configuration
LOG_TARGET="${LOG_TARGET:=terminal}"  # terminal or file

# Destination configuration
DESTINATION="${DESTINATION:=null}"  # null or big-query
BQ_PROJECT_ID="${BQ_PROJECT_ID:=}"
BQ_DATASET_ID="${BQ_DATASET_ID:=}"
BQ_SA_KEY_FILE="${BQ_SA_KEY_FILE:=}"
BQ_MAX_STALENESS_MINS="${BQ_MAX_STALENESS_MINS:=}"
BQ_MAX_CONCURRENT_STREAMS="${BQ_MAX_CONCURRENT_STREAMS:=}"

# Optional dry-run mode
DRY_RUN="${DRY_RUN:=false}"

# Optional skip automatic TPC-C preparation
PREPARE_TPCC="${PREPARE_TPCC:=true}"

echo "🏁 Running table copies benchmark with hyperfine..."
echo "📊 Configuration:"
echo "   Database: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
echo "   User: ${DB_USER}"
echo "   Runs: ${RUNS}"
echo "   Publication: ${PUBLICATION_NAME}"
echo "   Batch size: ${BATCH_MAX_SIZE}"
echo "   Batch fill time: ${BATCH_MAX_FILL_MS}ms"
echo "   Workers: ${MAX_TABLE_SYNC_WORKERS}"
echo "   Max copy connections per table: ${MAX_COPY_CONNECTIONS_PER_TABLE}"
echo "   Log target: ${LOG_TARGET}"
echo "   Destination: ${DESTINATION}"
echo "   TPC-C tables: ${TPCC_TABLES}"
if [[ "${DESTINATION}" == "big-query" ]]; then
  echo "   BigQuery Project: ${BQ_PROJECT_ID}"
  echo "   BigQuery Dataset: ${BQ_DATASET_ID}"
  echo "   BigQuery SA Key: ${BQ_SA_KEY_FILE}"
  if [[ -n "${BQ_MAX_STALENESS_MINS}" ]]; then
    echo "   BigQuery Max Staleness: ${BQ_MAX_STALENESS_MINS} mins"
  fi
  if [[ -n "${BQ_MAX_CONCURRENT_STREAMS}" ]]; then
    echo "   BigQuery Max Concurrent Streams: ${BQ_MAX_CONCURRENT_STREAMS}"
  fi
fi

# Convert comma-separated table names to SQL IN clause format
# e.g., "order_line,stock" -> "'order_line','stock'"
TPCC_TABLES_SQL=$(echo "${TPCC_TABLES}" | sed "s/,/','/g" | sed "s/^/'/;s/$/'/")

# Get table IDs from the database for TPC-C tables
echo "🔍 Querying table IDs from database..."
echo "   Tables: ${TPCC_TABLES}"
TPCC_TABLE_IDS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
  select string_agg(oid::text, ',')
  from pg_class
  where relname in (${TPCC_TABLES_SQL})
    and relkind = 'r';
" 2>/dev/null || echo "")

if [[ -z "${TPCC_TABLE_IDS}" ]]; then
  if [[ "${PREPARE_TPCC}" == "true" ]]; then
    echo "⚠️  TPC-C tables not found. Running prepare_tpcc.sh automatically..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    "${SCRIPT_DIR}/prepare_tpcc.sh"

    # Retry querying table IDs after preparation
    echo "🔍 Querying table IDs from database (after preparation)..."
    TPCC_TABLE_IDS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
      select string_agg(oid::text, ',')
      from pg_class
      where relname in (${TPCC_TABLES_SQL})
        and relkind = 'r';
    " 2>/dev/null || echo "")

    if [[ -z "${TPCC_TABLE_IDS}" ]]; then
      echo "❌ Error: Could not retrieve table IDs even after running prepare_tpcc.sh"
      exit 1
    fi
  else
    echo "❌ Error: Could not retrieve table IDs from database. Make sure TPC-C tables exist."
    echo "💡 Run './etl-benchmarks/scripts/prepare_tpcc.sh' first to create the tables."
    echo "💡 Or set PREPARE_TPCC=true to run it automatically."
    exit 1
  fi
fi

echo "✅ Found table IDs: ${TPCC_TABLE_IDS}"

# Get expected total row count for validation by querying each table exactly
echo "🔍 Querying exact row count for each table..."

# Build the row count query dynamically based on selected tables
ROW_COUNT_QUERY="select "
FIRST=true
IFS=',' read -ra TABLES <<< "${TPCC_TABLES}"
for table in "${TABLES[@]}"; do
  if [ "$FIRST" = true ]; then
    ROW_COUNT_QUERY="${ROW_COUNT_QUERY}(select count(*) from ${table})"
    FIRST=false
  else
    ROW_COUNT_QUERY="${ROW_COUNT_QUERY} + (select count(*) from ${table})"
  fi
done
ROW_COUNT_QUERY="${ROW_COUNT_QUERY};"

EXPECTED_ROW_COUNT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "${ROW_COUNT_QUERY}" 2>/dev/null || echo "0")

if [[ -z "${EXPECTED_ROW_COUNT}" || "${EXPECTED_ROW_COUNT}" == "0" ]]; then
  echo "❌ Error: Could not determine expected row count"
  exit 1
fi

echo "✅ Expected total row count: ${EXPECTED_ROW_COUNT}"

# Create/recreate publication for selected tables
echo "📡 Setting up publication '${PUBLICATION_NAME}' for selected tables..."

# Build comma-separated table list for publication
PUBLICATION_TABLES=$(echo "${TPCC_TABLES}" | sed 's/,/, /g')

# Drop and recreate publication
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -c "
  -- Drop publication if it exists
  drop publication if exists ${PUBLICATION_NAME};

  -- Create publication with selected tables
  create publication ${PUBLICATION_NAME} for table ${PUBLICATION_TABLES};
" >/dev/null 2>&1

if [[ $? -eq 0 ]]; then
  echo "✅ Publication '${PUBLICATION_NAME}' created for tables: ${PUBLICATION_TABLES}"
else
  echo "❌ Error: Failed to create publication"
  exit 1
fi

# Validate BigQuery configuration if using BigQuery destination
if [[ "${DESTINATION}" == "big-query" ]]; then
  if [[ -z "${BQ_PROJECT_ID}" ]]; then
    echo "❌ Error: BQ_PROJECT_ID environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ -z "${BQ_DATASET_ID}" ]]; then
    echo "❌ Error: BQ_DATASET_ID environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ -z "${BQ_SA_KEY_FILE}" ]]; then
    echo "❌ Error: BQ_SA_KEY_FILE environment variable is required when using BigQuery destination."
    exit 1
  fi
  if [[ ! -f "${BQ_SA_KEY_FILE}" ]]; then
    echo "❌ Error: BigQuery service account key file does not exist: ${BQ_SA_KEY_FILE}"
    exit 1
  fi
fi

# Determine if we need BigQuery features
FEATURES_FLAG=""
if [[ "${DESTINATION}" == "big-query" ]]; then
  FEATURES_FLAG="--features bigquery"
fi

# Validate destination option
if [[ "${DESTINATION}" != "null" && "${DESTINATION}" != "big-query" ]]; then
  echo "❌ Error: Invalid destination '${DESTINATION}'. Supported values: null, big-query"
  exit 1
fi

# Validate log target option
if [[ "${LOG_TARGET}" != "terminal" && "${LOG_TARGET}" != "file" ]]; then
  echo "❌ Error: Invalid log target '${LOG_TARGET}'. Supported values: terminal, file"
  exit 1
fi

# Build the run command
RUN_CMD="cargo bench --bench table_copies ${FEATURES_FLAG} -- --log-target ${LOG_TARGET} run --host ${DB_HOST} --port ${DB_PORT} --database ${DB_NAME} --username ${DB_USER}"
if [[ -n "${DB_PASSWORD}" && "${DB_PASSWORD}" != "" ]]; then
  RUN_CMD="${RUN_CMD} --password ${DB_PASSWORD}"
fi
RUN_CMD="${RUN_CMD} --publication-name ${PUBLICATION_NAME} --batch-max-size ${BATCH_MAX_SIZE} --batch-max-fill-ms ${BATCH_MAX_FILL_MS} --max-table-sync-workers ${MAX_TABLE_SYNC_WORKERS} --max-copy-connections-per-table ${MAX_COPY_CONNECTIONS_PER_TABLE} --table-ids ${TPCC_TABLE_IDS} --expected-row-count ${EXPECTED_ROW_COUNT}"

# Add destination-specific options
RUN_CMD="${RUN_CMD} --destination ${DESTINATION}"
if [[ "${DESTINATION}" == "big-query" ]]; then
  RUN_CMD="${RUN_CMD} --bq-project-id ${BQ_PROJECT_ID}"
  RUN_CMD="${RUN_CMD} --bq-dataset-id ${BQ_DATASET_ID}"
  RUN_CMD="${RUN_CMD} --bq-sa-key-file ${BQ_SA_KEY_FILE}"
  if [[ -n "${BQ_MAX_STALENESS_MINS}" ]]; then
    RUN_CMD="${RUN_CMD} --bq-max-staleness-mins ${BQ_MAX_STALENESS_MINS}"
  fi
  if [[ -n "${BQ_MAX_CONCURRENT_STREAMS}" ]]; then
    RUN_CMD="${RUN_CMD} --bq-max-concurrent-streams ${BQ_MAX_CONCURRENT_STREAMS}"
  fi
fi

echo ""
echo "🚀 Starting benchmark..."

# Show commands in dry-run mode
if [[ "${DRY_RUN}" == "true" ]]; then
  echo ""
  echo "📝 Command that would be executed:"
  echo "   ${RUN_CMD}"
  echo ""
  echo "ℹ️  This was a dry run. Set DRY_RUN=false to actually run the benchmark."
  exit 0
fi

# Run hyperfine
hyperfine \
  --runs "${RUNS}" \
  --show-output \
  "${RUN_CMD}"

echo "✨ Benchmark complete!"
