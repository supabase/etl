#!/usr/bin/env bash
set -euo pipefail

# Memory Profiling for Benchmarks (cargo-instruments)
#
# Runs the `etl-benchmarks` crate's benchmark under Apple Instruments for
# memory profiling. Defaults to the "Allocations" template and the
# `table_copies` bench target.
#
# Prerequisites:
# - macOS with Xcode Command Line Tools
# - cargo-instruments (install: `cargo install cargo-instruments`)
# - Postgres reachable per your env (same as other bench scripts)
#
# Environment Variables:
#   BENCH_NAME                 Bench target name. Default: table_copies
#   PACKAGE                    Cargo package. Default: etl-benchmarks
#   TEMPLATE                   Instruments template (Allocations|Leaks|VM Tracker|Time Profiler). Default: Allocations
#   OPEN_TRACE                 Open Instruments UI after run (true|false). Default: false
#   RUN_LABEL                  Run name label for trace. Default: auto timestamped
#   TRACE_DIR                  Output directory for traces. Default: target/instruments
#   LOG_TARGET                 Benchmark logs target (terminal|file). Default: terminal
#   DESTINATION                Destination (null|big-query). Default: null
#
#   Database connection (same defaults as benchmark.sh / prepare_tpcc.sh):
#     POSTGRES_USER            Default: postgres
#     POSTGRES_PASSWORD        Default: postgres
#     POSTGRES_DB              Default: bench
#     POSTGRES_PORT            Default: 5430
#     POSTGRES_HOST            Default: localhost
#   Benchmark params:
#     PUBLICATION_NAME         Default: bench_pub
#     BATCH_MAX_SIZE           Default: 1000000
#     BATCH_MAX_FILL_MS        Default: 10000
#     MAX_TABLE_SYNC_WORKERS   Default: 8
#     BQ_PROJECT_ID, BQ_DATASET_ID, BQ_SA_KEY_FILE (if DESTINATION=big-query)
#
# Examples:
#   # Profile allocations for the default bench (null destination)
#   ./etl-benchmarks/scripts/mem_profile.sh
#
#   # Open the Instruments UI afterwards
#   OPEN_TRACE=true ./etl-benchmarks/scripts/mem_profile.sh
#
#   # Use Leaks template and skip prepare
#   TEMPLATE="Leaks" ./etl-benchmarks/scripts/mem_profile.sh
#
#   # Profile BigQuery destination
#   DESTINATION=big-query \
#   BQ_PROJECT_ID=my-project \
#   BQ_DATASET_ID=my_dataset \
#   BQ_SA_KEY_FILE=/path/to/sa-key.json \
#   ./etl-benchmarks/scripts/mem_profile.sh

# --- Checks ---
# Require macOS
if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "❌ This script requires macOS (Apple Instruments)." >&2
  exit 1
fi

if ! command -v cargo-instruments >/dev/null 2>&1; then
  echo "❌ cargo-instruments not found. Install with: cargo install cargo-instruments" >&2
  exit 1
fi

# Ensure xctrace is available (part of full Xcode, not just CLT)
if ! xcrun --find xctrace >/dev/null 2>&1; then
  cat >&2 << 'EOF'
❌ xctrace not found.

Apple's xctrace is part of full Xcode (v12+). To install and make it available:
  1) Install Xcode from the App Store (not only Command Line Tools).
  2) Point the developer dir to Xcode:
       sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
  3) Run first-launch setup and accept the license:
       sudo xcodebuild -runFirstLaunch
  4) Verify:
       xcrun --find xctrace

After installing, rerun this script.
EOF
  exit 1
fi

# --- Config ---
BENCH_NAME="${BENCH_NAME:=table_copies}"
PACKAGE="${PACKAGE:=etl-benchmarks}"
TEMPLATE="${TEMPLATE:=Allocations}"
OPEN_TRACE="${OPEN_TRACE:=false}"
TRACE_DIR="${TRACE_DIR:=target/instruments}"
RUN_LABEL="${RUN_LABEL:=etl-benchmarks-${TEMPLATE// /-}-$(date +%Y%m%d%H%M%S)}"

# Database defaults
DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=bench}"
DB_PORT="${POSTGRES_PORT:=5430}"
DB_HOST="${POSTGRES_HOST:=localhost}"

# Benchmark defaults
PUBLICATION_NAME="${PUBLICATION_NAME:=bench_pub}"
BATCH_MAX_SIZE="${BATCH_MAX_SIZE:=1000000}"
BATCH_MAX_FILL_MS="${BATCH_MAX_FILL_MS:=10000}"
MAX_TABLE_SYNC_WORKERS="${MAX_TABLE_SYNC_WORKERS:=8}"
LOG_TARGET="${LOG_TARGET:=terminal}"
DESTINATION="${DESTINATION:=null}"

# Validate destination
if [[ "${DESTINATION}" != "null" && "${DESTINATION}" != "big-query" ]]; then
  echo "❌ Invalid DESTINATION='${DESTINATION}'. Supported: null, big-query" >&2
  exit 1
fi
if [[ "${LOG_TARGET}" != "terminal" && "${LOG_TARGET}" != "file" ]]; then
  echo "❌ Invalid LOG_TARGET='${LOG_TARGET}'. Supported: terminal, file" >&2
  exit 1
fi

if [[ "${DESTINATION}" == "big-query" ]]; then
  : "${BQ_PROJECT_ID:?❌ BQ_PROJECT_ID is required for DESTINATION=big-query}"
  : "${BQ_DATASET_ID:?❌ BQ_DATASET_ID is required for DESTINATION=big-query}"
  : "${BQ_SA_KEY_FILE:?❌ BQ_SA_KEY_FILE is required for DESTINATION=big-query}"
  if [[ ! -f "${BQ_SA_KEY_FILE}" ]]; then
    echo "❌ BigQuery SA key file not found: ${BQ_SA_KEY_FILE}" >&2
    exit 1
  fi
fi

echo "🧪 Memory profiling with cargo-instruments"
echo "   Template: ${TEMPLATE}"
echo "   Package:  ${PACKAGE}"
echo "   Bench:    ${BENCH_NAME}"
echo "   Label:    ${RUN_LABEL}"
echo "   Trace dir:${TRACE_DIR}"
echo "   Open UI:  ${OPEN_TRACE}"
echo "   Dest:     ${DESTINATION}"

# Build common bench arg tail
build_bench_args() {
  local args=("--log-target" "${LOG_TARGET}")
  args+=("run" "--host" "${DB_HOST}" "--port" "${DB_PORT}" "--database" "${DB_NAME}" "--username" "${DB_USER}")
  if [[ -n "${DB_PASSWORD}" ]]; then
    args+=("--password" "${DB_PASSWORD}")
  fi
  args+=("--publication-name" "${PUBLICATION_NAME}" "--batch-max-size" "${BATCH_MAX_SIZE}" "--batch-max-fill-ms" "${BATCH_MAX_FILL_MS}" "--max-table-sync-workers" "${MAX_TABLE_SYNC_WORKERS}")

  # For table_copies we require explicit table ids; fetch via psql like benchmark.sh
  echo "🔍 Fetching TPC-C table OIDs..." >&2
  local oids
  if ! command -v psql >/dev/null 2>&1; then
    echo "❌ psql not found; required to query table IDs." >&2
    exit 1
  fi
  oids=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}" -tAc "
    select string_agg(oid::text, ',')
    from pg_class
    where relname in ('customer','district','item','new_order','order_line','orders','stock','warehouse')
      and relkind = 'r';
  " 2>/dev/null || true)
  if [[ -z "${oids}" ]]; then
    echo "❌ Could not retrieve table IDs. Ensure TPC-C tables exist. Run etl-benchmarks/scripts/prepare_tpcc.sh first." >&2
    exit 1
  fi
  echo "✅ Table OIDs: ${oids}" >&2
  args+=("--table-ids" "${oids}")

  args+=("--destination" "${DESTINATION}")
  if [[ "${DESTINATION}" == "big-query" ]]; then
    args+=("--bq-project-id" "${BQ_PROJECT_ID}" "--bq-dataset-id" "${BQ_DATASET_ID}" "--bq-sa-key-file" "${BQ_SA_KEY_FILE}")
  fi
  printf '%q ' "${args[@]}"
}

# Run Instruments on the bench's run phase
echo "🚀 Launching cargo instruments (${TEMPLATE})…"
mkdir -p "${TRACE_DIR}"

# Use explicit .trace path to encode label in filename
TRACE_PATH="${TRACE_DIR}/${RUN_LABEL}.trace"

INSTR_ARGS=(cargo instruments -t "${TEMPLATE}" --package "${PACKAGE}" --bench "${BENCH_NAME}" --output "${TRACE_PATH}")
# cargo-instruments opens the trace by default; add --no-open when OPEN_TRACE=false
if [[ "${OPEN_TRACE}" != "true" ]]; then
  INSTR_ARGS+=(--no-open)
fi

BENCH_TAIL=$(build_bench_args)
echo "$ ${INSTR_ARGS[*]} -- ${BENCH_TAIL}"
eval "${INSTR_ARGS[*]}" -- ${BENCH_TAIL}

echo "✨ Trace saved to: ${TRACE_PATH}"
