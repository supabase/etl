#!/usr/bin/env bash

# Reproduces and measures an initial HyBench COPY into DuckLake on OrbStack.
#
# This runner reuses a generated HyBench source, creates a new DuckLake catalog
# for each run, deploys one resource-constrained replicator, and records
# resource samples plus Prometheus metrics under target/.

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly DEFAULT_WAREHOUSE_BENCH_DIR="${REPOSITORY_ROOT}/../warehouse-bench"
readonly DEFAULT_NAMESPACE="etl-data-plane"
readonly DEFAULT_CONTEXT="orbstack"
readonly DEFAULT_SCHEMA="hybench_sf1x"
readonly DEFAULT_SCALING_FACTOR="1"
readonly DEFAULT_SEED="8100128985715404389"
readonly DEFAULT_SAMPLE_INTERVAL_SECONDS="5"
readonly DEFAULT_METRICS_PORT="19000"
readonly DEFAULT_SOURCE_DATABASE="etl_ducklake_copy_source_baseline1"
readonly DEFAULT_PUBLICATION="etl_ducklake_copy_baseline1_pub"
readonly DEFAULT_REPLICATOR_NAME="ducklake-copy-benchmark"

usage() {
  cat <<'EOF'
Usage: scripts/benchmarks/ducklake-copy-local.sh [options]

Uses a reusable local HyBench source database and creates a fresh DuckLake
catalog database for the run. It then deploys an OrbStack replicator with
requests and limits of 1024m CPU and 256Mi memory.

Options:
  --run-id ID             Stable lowercase, digit, or underscore run identifier.
  --scaling-factor N      HyBench scaling factor (default: 1).
  --seed N                HyBench generator seed (default: 8100128985715404389).
  --prepare-source        Create the reusable HyBench source database and data.
  --resume                Reuse a failed run's already-created catalog database.
  --sample-seconds N      Stop the benchmark workload after N seconds of samples.
  --skip-image-build      Use the already-built local replicator image.
  --help                  Show this help text.

Environment:
  BENCH_PGHOST            Host-published source PostgreSQL host (default: localhost).
  BENCH_PGPORT            Host-published source PostgreSQL port (default: 5430).
  BENCH_PGUSER            Source PostgreSQL user (default: postgres).
  BENCH_PGPASSWORD        Source PostgreSQL password (default: local test credential).
  BENCH_SOURCE_DATABASE   Reusable HyBench database (default:
                          etl_ducklake_copy_source_baseline1). Normal runs never create,
                          drop, or change its generated hybench_sf1x data.
  BENCH_PUBLICATION_NAME  Publication for all fixture tables (default:
                          etl_ducklake_copy_baseline1_pub).
  BENCH_REPLICATOR_NAME   Reused isolated benchmark workload (default:
                          ducklake-copy-benchmark).
  BENCH_MAX_TABLE_SYNC_WORKERS
                          Concurrent table-copy workers (default: 4).
  BENCH_MAX_COPY_CONNECTIONS_PER_TABLE
                          Concurrent source COPY connections per table (default: 4).
  BENCH_TABLE_IDS         Optional comma-separated source table OIDs to include.
                          This changes only the replicator configuration; it does
                          not change the source publication or generated data.
  BENCH_DUCKLAKE_POOL_SIZE
                          Warm DuckDB connections (default: 4).
  BENCH_CPU_REQUEST       Replicator CPU request (default: 1024m).
  BENCH_CPU_LIMIT         Replicator CPU limit (default: 1024m).
  BENCH_MEMORY_REQUEST    Replicator memory request (default: 256Mi).
  BENCH_MEMORY_LIMIT      Replicator memory limit (default: 256Mi). Set both
                          to 512Mi for an unconstrained-copy comparison.
  BENCH_REPLICATOR_PGHOST Host that OrbStack pods use for PostgreSQL
                          (default: host.docker.internal).
  BENCH_CATALOG_URL       Full URL for the fresh DuckLake catalog database.
                          Set this when BENCH_PGPASSWORD needs URL escaping.
  BENCH_MINIO_ACCESS_KEY  Local MinIO access key (default: local Compose credential).
  BENCH_MINIO_SECRET_KEY  Local MinIO secret key (default: local Compose credential).
  BENCH_MINIO_ENDPOINT    MinIO endpoint visible to pods (default:
                          host.docker.internal:9010).
  BENCH_MINIO_BUCKET      Existing MinIO bucket (default: dev-and-test).
  WAREHOUSE_BENCH_DIR     warehouse-bench checkout (default:
                          ../warehouse-bench relative to this repository).

Results:
  target/ducklake-copy-bench/<run-id>/samples.log
  target/ducklake-copy-bench/<run-id>/port-forward.log

Run `--prepare-source` once. Normal runs only drop the dedicated `etl` metadata
schema in that database before deployment; they do not alter `hybench_sf1x`.
The runner never deletes catalog databases, S3 objects, or Kubernetes resources
after a successful run. This keeps the evidence available for inspection.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

run_id="$(date -u +%Y%m%d%H%M%S)"
scaling_factor="${DEFAULT_SCALING_FACTOR}"
seed="${DEFAULT_SEED}"
resume=false
skip_image_build=false
prepare_source=false
sample_seconds=""

while (($# > 0)); do
  case "$1" in
    --run-id)
      (($# >= 2)) || die "--run-id requires a value"
      run_id="$2"
      shift 2
      ;;
    --scaling-factor)
      (($# >= 2)) || die "--scaling-factor requires a value"
      scaling_factor="$2"
      shift 2
      ;;
    --seed)
      (($# >= 2)) || die "--seed requires a value"
      seed="$2"
      shift 2
      ;;
    --prepare-source)
      prepare_source=true
      shift
      ;;
    --sample-seconds)
      (($# >= 2)) || die "--sample-seconds requires a value"
      sample_seconds="$2"
      shift 2
      ;;
    --resume)
      resume=true
      shift
      ;;
    --skip-image-build)
      skip_image_build=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[[ "$run_id" =~ ^[a-z0-9_]+$ ]] || die "run id must contain only lowercase letters, digits, and underscores"
[[ "$scaling_factor" =~ ^[1-9][0-9]*$ ]] || die "scaling factor must be a positive integer"
[[ "$seed" =~ ^[0-9]+$ ]] || die "seed must be an unsigned integer"
[[ -z "$sample_seconds" || "$sample_seconds" =~ ^[1-9][0-9]*$ ]] || die "sample seconds must be a positive integer"

readonly namespace="${BENCH_K8S_NAMESPACE:-${DEFAULT_NAMESPACE}}"
readonly context="${BENCH_K8S_CONTEXT:-${DEFAULT_CONTEXT}}"
readonly warehouse_bench_dir="${WAREHOUSE_BENCH_DIR:-${DEFAULT_WAREHOUSE_BENCH_DIR}}"
readonly pg_host="${BENCH_PGHOST:-localhost}"
readonly pg_port="${BENCH_PGPORT:-5430}"
readonly pg_user="${BENCH_PGUSER:-postgres}"
readonly pg_password="${BENCH_PGPASSWORD:-postgres}"
readonly replicator_pg_host="${BENCH_REPLICATOR_PGHOST:-host.docker.internal}"
readonly minio_access_key="${BENCH_MINIO_ACCESS_KEY:-minio-admin}"
readonly minio_secret_key="${BENCH_MINIO_SECRET_KEY:-minio-admin-password}"
readonly minio_endpoint="${BENCH_MINIO_ENDPOINT:-host.docker.internal:9010}"
readonly minio_bucket="${BENCH_MINIO_BUCKET:-dev-and-test}"
readonly source_database="${BENCH_SOURCE_DATABASE:-${DEFAULT_SOURCE_DATABASE}}"
readonly catalog_database="etl_ducklake_copy_catalog_${run_id}"
readonly publication="${BENCH_PUBLICATION_NAME:-${DEFAULT_PUBLICATION}}"
readonly replicator_name="${BENCH_REPLICATOR_NAME:-${DEFAULT_REPLICATOR_NAME}}"
readonly data_path="s3://${minio_bucket}/etl-ducklake-copy/${run_id}"
readonly results_dir="${REPOSITORY_ROOT}/target/ducklake-copy-bench/${run_id}"
readonly samples_file="${results_dir}/samples.log"
readonly port_forward_log="${results_dir}/port-forward.log"
readonly metrics_port="${BENCH_METRICS_PORT:-${DEFAULT_METRICS_PORT}}"
readonly max_table_sync_workers="${BENCH_MAX_TABLE_SYNC_WORKERS:-4}"
readonly max_copy_connections_per_table="${BENCH_MAX_COPY_CONNECTIONS_PER_TABLE:-4}"
readonly table_ids="${BENCH_TABLE_IDS:-}"
readonly ducklake_pool_size="${BENCH_DUCKLAKE_POOL_SIZE:-4}"
readonly cpu_request="${BENCH_CPU_REQUEST:-1024m}"
readonly cpu_limit="${BENCH_CPU_LIMIT:-1024m}"
readonly memory_request="${BENCH_MEMORY_REQUEST:-256Mi}"
readonly memory_limit="${BENCH_MEMORY_LIMIT:-256Mi}"

[[ "$context" == "$DEFAULT_CONTEXT" ]] || die "this benchmark only permits the ${DEFAULT_CONTEXT} Kubernetes context"
[[ -d "$warehouse_bench_dir" ]] || die "warehouse-bench checkout not found: $warehouse_bench_dir"
[[ "$metrics_port" =~ ^[1-9][0-9]*$ ]] || die "metrics port must be a positive integer"
[[ "$max_table_sync_workers" =~ ^[1-9][0-9]*$ ]] || die "BENCH_MAX_TABLE_SYNC_WORKERS must be a positive integer"
[[ "$max_copy_connections_per_table" =~ ^[1-9][0-9]*$ ]] || die "BENCH_MAX_COPY_CONNECTIONS_PER_TABLE must be a positive integer"
[[ "$ducklake_pool_size" =~ ^[1-9][0-9]*$ ]] || die "BENCH_DUCKLAKE_POOL_SIZE must be a positive integer"
[[ -z "$table_ids" || "$table_ids" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || die "BENCH_TABLE_IDS must be a comma-separated list of table OIDs"
[[ -n "$cpu_request" ]] || die "BENCH_CPU_REQUEST must be non-empty"
[[ -n "$cpu_limit" ]] || die "BENCH_CPU_LIMIT must be non-empty"
[[ -n "$memory_request" ]] || die "BENCH_MEMORY_REQUEST must be non-empty"
[[ -n "$memory_limit" ]] || die "BENCH_MEMORY_LIMIT must be non-empty"
[[ "$source_database" =~ ^[a-z0-9_]+$ ]] || die "source database must contain only lowercase letters, digits, and underscores"
[[ "$publication" =~ ^[a-z0-9_]+$ ]] || die "publication must contain only lowercase letters, digits, and underscores"
[[ "$replicator_name" =~ ^[a-z0-9-]+$ ]] || die "replicator name must contain only lowercase letters, digits, and hyphens"

require_command cargo
require_command createdb
require_command curl
require_command kubectl
require_command psql

mkdir -p "$results_dir"

export PGHOST="$pg_host"
export PGPORT="$pg_port"
export PGUSER="$pg_user"
export PGPASSWORD="$pg_password"

psql -d postgres -v ON_ERROR_STOP=1 -Atc 'select 1' >/dev/null

create_database() {
  local database="$1"
  local exists

  exists="$(psql -d postgres -Atc "select 1 from pg_database where datname = '${database}'")"
  [[ -z "$exists" ]] || die "database already exists: ${database}; choose another --run-id"
  createdb "$database"
}

if [[ "$prepare_source" == true ]]; then
  [[ "$resume" == false ]] || die "--prepare-source cannot be combined with --resume"
  create_database "$source_database"

  echo "[$(date -u +%FT%TZ)] generating HyBench SF${scaling_factor} data in ${source_database}."
  (
    cd "$warehouse_bench_dir"
    PGDATABASE="$source_database" cargo run --release --bin hybench-dgen -- \
      --pg-schema "$DEFAULT_SCHEMA" \
      --scaling-factor "$scaling_factor" \
      --seed "$seed" \
      --no-progress
  )

  table_list="$(psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc "
    select string_agg(format('%I.%I', schemaname, tablename), ', ' order by tablename)
    from pg_tables
    where schemaname = '${DEFAULT_SCHEMA}';
")"
  [[ -n "$table_list" ]] || die "HyBench generation did not create tables in ${DEFAULT_SCHEMA}"
  psql -d "$source_database" -v ON_ERROR_STOP=1 -c "create publication \"${publication}\" for table ${table_list};" >/dev/null
fi

psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc 'select 1' >/dev/null || die "source database is unavailable: ${source_database}; run with --prepare-source once"
source_table_count="$(psql -d "$source_database" -Atc "select count(*) from pg_tables where schemaname = '${DEFAULT_SCHEMA}'")"
[[ "$source_table_count" -gt 0 ]] || die "source database has no tables in ${DEFAULT_SCHEMA}; run with --prepare-source once"
psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc "select 1 from pg_publication where pubname = '${publication}'" | grep -qx 1 || die "source publication is unavailable: ${publication}; run with --prepare-source once"

expected_ready_count="$source_table_count"
if [[ -n "$table_ids" ]]; then
  requested_table_count="$(tr ',' '\n' <<<"$table_ids" | wc -l | tr -d ' ')"
  expected_ready_count="$(psql -d "$source_database" -Atc "
    select count(*)
    from pg_class table_class
    join pg_namespace table_schema on table_schema.oid = table_class.relnamespace
    where table_schema.nspname = '${DEFAULT_SCHEMA}'
      and table_class.relkind = 'r'
      and table_class.oid = any(string_to_array('${table_ids}', ',')::oid[]);
  ")"
  [[ "$expected_ready_count" == "$requested_table_count" ]] || die "BENCH_TABLE_IDS must name existing ${DEFAULT_SCHEMA} table OIDs"
fi

if [[ "$resume" == false ]]; then
  create_database "$catalog_database"
else
  psql -d "$catalog_database" -v ON_ERROR_STOP=1 -Atc 'select 1' >/dev/null || die "cannot resume; catalog database is unavailable: ${catalog_database}"
  echo "[$(date -u +%FT%TZ)] resuming from existing DuckLake catalog ${catalog_database}."
fi

source_bytes="$(psql -d "$source_database" -Atc 'select pg_database_size(current_database())')"
echo "[$(date -u +%FT%TZ)] source ready: ${source_table_count} tables, ${source_bytes} bytes; waiting for ${expected_ready_count} selected tables."

kubectl --context "$context" -n "$namespace" scale "statefulset/${replicator_name}-statefulset" --replicas=0 >/dev/null 2>&1 || true
echo "[$(date -u +%FT%TZ)] resetting only ETL metadata in ${source_database}; ${DEFAULT_SCHEMA} is preserved."
psql -d "$source_database" -v ON_ERROR_STOP=1 -c 'drop schema if exists etl cascade;' >/dev/null

catalog_url="${BENCH_CATALOG_URL:-postgres://${pg_user}:${pg_password}@${replicator_pg_host}:${pg_port}/${catalog_database}?sslmode=disable}"

export REPLICATOR_NAME="$replicator_name"
export REPLICATOR_PIPELINE_ID="901"
export REPLICATOR_PUBLICATION_NAME="$publication"
export REPLICATOR_POSTGRES_HOST="$replicator_pg_host"
export REPLICATOR_POSTGRES_PORT="$pg_port"
export REPLICATOR_POSTGRES_DB="$source_database"
export REPLICATOR_POSTGRES_USER="$pg_user"
export REPLICATOR_POSTGRES_PASSWORD="$pg_password"
export REPLICATOR_DUCKLAKE_CATALOG_URL="$catalog_url"
export REPLICATOR_DUCKLAKE_DATA_PATH="$data_path"
export REPLICATOR_DUCKLAKE_POOL_SIZE="$ducklake_pool_size"
export REPLICATOR_MAX_TABLE_SYNC_WORKERS="$max_table_sync_workers"
export REPLICATOR_MAX_COPY_CONNECTIONS_PER_TABLE="$max_copy_connections_per_table"
if [[ -n "$table_ids" ]]; then
  export REPLICATOR_TABLE_SYNC_COPY_TABLE_IDS="$table_ids"
fi
export REPLICATOR_DUCKLAKE_S3_ACCESS_KEY_ID="$minio_access_key"
export REPLICATOR_DUCKLAKE_S3_SECRET_ACCESS_KEY="$minio_secret_key"
export REPLICATOR_DUCKLAKE_S3_REGION="us-east-1"
export REPLICATOR_DUCKLAKE_S3_ENDPOINT="$minio_endpoint"
export REPLICATOR_DUCKLAKE_S3_URL_STYLE="path"
export REPLICATOR_DUCKLAKE_S3_USE_SSL="false"
export RUST_LOG="info,etl_destinations::ducklake=debug"

deploy_args=(
  x deploy-local
  --destination ducklake
  --context "$context"
  --namespace "$namespace"
  --name "$replicator_name"
  --cpu-request "$cpu_request"
  --cpu-limit "$cpu_limit"
  --memory-request "$memory_request"
  --memory-limit "$memory_limit"
)
if [[ "$skip_image_build" == true ]]; then
  deploy_args+=(--skip-build)
fi

echo "[$(date -u +%FT%TZ)] deploying the constrained DuckLake replicator."
(
  cd "$REPOSITORY_ROOT"
  kubectl --context "$context" apply -f scripts/k8s/local >/dev/null
  cargo "${deploy_args[@]}"
)

port_forward_pid=""
cleanup_port_forward() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  fi
}
trap cleanup_port_forward EXIT

kubectl --context "$context" -n "$namespace" port-forward "service/${replicator_name}-metrics" "${metrics_port}:9000" >"$port_forward_log" 2>&1 &
port_forward_pid="$!"

for _ in {1..30}; do
  if curl --fail --silent --show-error "http://127.0.0.1:${metrics_port}/metrics" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent --show-error "http://127.0.0.1:${metrics_port}/metrics" >/dev/null

collect_sample() {
  local timestamp pod_name state_counts metrics

  timestamp="$(date -u +%FT%TZ)"
  pod_name="$(kubectl --context "$context" -n "$namespace" get pods -l "app.kubernetes.io/name=${replicator_name}" -o jsonpath='{.items[0].metadata.name}')"
  state_counts="$(psql -d "$source_database" -Atc "
    select coalesce(string_agg(state::text || ':' || count, ', ' order by state::text), 'no_state')
    from (
      select state, count(*)
      from etl.replication_state
      where pipeline_id = 901 and is_current
      group by state
    ) states;
  " 2>/dev/null || echo 'state_query_unavailable')"
  metrics="$(curl --fail --silent --show-error "http://127.0.0.1:${metrics_port}/metrics" | awk '
    /^etl_(table_copy|ducklake|ideal_batch_size)|^tokio_metrics_|^jemalloc_/ { print }
  ' || echo 'metrics_unavailable')"

  {
    echo "timestamp=${timestamp}"
    echo "pod=${pod_name:-unavailable}"
    kubectl --context "$context" -n "$namespace" top pod "$pod_name" --no-headers 2>&1 || true
    echo "table_states=${state_counts}"
    echo "$metrics"
    echo
  } >>"$samples_file"
}

echo "[$(date -u +%FT%TZ)] collecting samples in ${samples_file}."
sample_started_at="$SECONDS"
while true; do
  collect_sample

  if [[ -n "$sample_seconds" && $((SECONDS - sample_started_at)) -ge "$sample_seconds" ]]; then
    echo "[$(date -u +%FT%TZ)] sample limit reached; stopping ${replicator_name} without changing source data."
    kubectl --context "$context" -n "$namespace" scale "statefulset/${replicator_name}-statefulset" --replicas=0 >/dev/null
    break
  fi

  if [[ -n "$table_ids" ]]; then
    ready_count="$(psql -d "$source_database" -Atc "
      select count(*)
      from etl.replication_state
      where pipeline_id = 901
        and is_current
        and state = 'ready'
        and table_id = any(string_to_array('${table_ids}', ',')::oid[]);
    " 2>/dev/null || echo 0)"
  else
    ready_count="$(psql -d "$source_database" -Atc "
      select count(*)
      from etl.replication_state
      where pipeline_id = 901 and is_current and state = 'ready';
    " 2>/dev/null || echo 0)"
  fi
  if [[ "$ready_count" == "$expected_ready_count" ]]; then
    echo "[$(date -u +%FT%TZ)] copy complete: ${ready_count}/${expected_ready_count} selected tables ready."
    kubectl --context "$context" -n "$namespace" scale "statefulset/${replicator_name}-statefulset" --replicas=0 >/dev/null
    break
  fi

  sleep "$DEFAULT_SAMPLE_INTERVAL_SECONDS"
done

echo "results: ${results_dir}"
echo "replicator logs: kubectl --context ${context} -n ${namespace} logs statefulset/${replicator_name}-statefulset"
echo "catalog database: ${catalog_database}"
echo "source database: ${source_database}"
