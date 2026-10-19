#!/usr/bin/env bash

# Compares immediate and deferred DuckLake initial-copy writes with the same
# approximately 2 GiB logical PostgreSQL table and fresh local DuckLake stores.

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly DEFAULT_TARGET_BYTES=2147483648
readonly DEFAULT_PAYLOAD_BYTES=2048
readonly DEFAULT_BUFFER_TARGET_BYTES=268435456
readonly DEFAULT_BUFFER_MAX_TOTAL_BYTES=1073741824

usage() {
  cat <<'EOF'
Usage: scripts/benchmarks/ducklake-copy-compare-local.sh [options]

Creates or reuses one deterministic source table, then runs the real ETL table
copy benchmark twice against fresh PostgreSQL-backed local DuckLake catalogs:
first with immediate durability and then with deferred durability.

Options:
  --run-id ID          Unique lowercase run identifier (default: UTC timestamp).
  --target-bytes N     Approximate logical payload bytes (default: 2147483648).
  --reuse-source       Reuse and validate an existing source fixture.
  --help               Show this help text.

Environment:
  BENCH_PGHOST         PostgreSQL host (default: localhost).
  BENCH_PGPORT         PostgreSQL port (default: 5430).
  BENCH_PGUSER         PostgreSQL user (default: postgres).
  BENCH_PGPASSWORD     PostgreSQL password (default: postgres).
  BENCH_SOURCE_DB      Source fixture database (default: etl_ducklake_copy_2g).
  BENCH_BUFFER_TARGET_BYTES
                       Deferred commit target (default: 268435456).
  BENCH_BUFFER_MAX_TOTAL_BYTES
                       Process-wide deferred byte cap (default: 1073741824).

Results are retained under target/ducklake-copy-compare/<run-id>/ together with
the two catalog databases. The script never drops databases or result files.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

database_exists() {
  psql -d postgres -Atc "select 1 from pg_database where datname = '$1'" | grep -qx 1
}

run_id="$(date -u +%Y%m%d%H%M%S)"
target_bytes="${DEFAULT_TARGET_BYTES}"
reuse_source=false

while (($# > 0)); do
  case "$1" in
    --run-id)
      (($# >= 2)) || die "--run-id requires a value"
      run_id="$2"
      shift 2
      ;;
    --target-bytes)
      (($# >= 2)) || die "--target-bytes requires a value"
      target_bytes="$2"
      shift 2
      ;;
    --reuse-source)
      reuse_source=true
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
[[ "$target_bytes" =~ ^[1-9][0-9]*$ ]] || die "target bytes must be a positive integer"

readonly pg_host="${BENCH_PGHOST:-localhost}"
readonly pg_port="${BENCH_PGPORT:-5430}"
readonly pg_user="${BENCH_PGUSER:-postgres}"
readonly pg_password="${BENCH_PGPASSWORD:-postgres}"
readonly source_database="${BENCH_SOURCE_DB:-etl_ducklake_copy_2g}"
readonly publication="ducklake_copy_bench_pub"
readonly source_schema="ducklake_copy_bench"
readonly source_table="payloads"
readonly payload_bytes="${DEFAULT_PAYLOAD_BYTES}"
readonly row_count="$(((target_bytes + payload_bytes - 1) / payload_bytes))"
readonly buffer_target_bytes="${BENCH_BUFFER_TARGET_BYTES:-${DEFAULT_BUFFER_TARGET_BYTES}}"
readonly buffer_max_total_bytes="${BENCH_BUFFER_MAX_TOTAL_BYTES:-${DEFAULT_BUFFER_MAX_TOTAL_BYTES}}"
readonly baseline_catalog="etl_ducklake_copy_baseline_${run_id}"
readonly buffered_catalog="etl_ducklake_copy_buffered_${run_id}"
readonly results_dir="${REPOSITORY_ROOT}/target/ducklake-copy-compare/${run_id}"
readonly baseline_data_dir="${results_dir}/baseline-data"
readonly buffered_data_dir="${results_dir}/buffered-data"
readonly baseline_report="${results_dir}/baseline.json"
readonly buffered_report="${results_dir}/buffered.json"

[[ "$source_database" =~ ^[a-z0-9_]+$ ]] || die "source database must contain only lowercase letters, digits, and underscores"
[[ "$buffer_target_bytes" =~ ^[1-9][0-9]*$ ]] || die "buffer target bytes must be a positive integer"
[[ "$buffer_max_total_bytes" =~ ^[1-9][0-9]*$ ]] || die "buffer max total bytes must be a positive integer"
((buffer_max_total_bytes >= buffer_target_bytes)) || die "buffer max total bytes must be at least the target"

require_command cargo
require_command createdb
require_command psql

export PGHOST="$pg_host"
export PGPORT="$pg_port"
export PGUSER="$pg_user"
export PGPASSWORD="$pg_password"

psql -d postgres -v ON_ERROR_STOP=1 -Atc 'select 1' >/dev/null

if database_exists "$source_database"; then
  [[ "$reuse_source" == true ]] || die "source database already exists; pass --reuse-source after verifying it is the benchmark fixture"
else
  [[ "$reuse_source" == false ]] || die "source database does not exist and cannot be reused"
  createdb "$source_database"
  psql -d "$source_database" -v ON_ERROR_STOP=1 <<SQL
create schema ${source_schema};
create table ${source_schema}.${source_table} (
  id bigint primary key,
  payload text not null
);
insert into ${source_schema}.${source_table} (id, payload)
select id, repeat(md5(id::text), ${payload_bytes} / 32)
from generate_series(1, ${row_count}) as rows(id);
create publication ${publication} for table ${source_schema}.${source_table};
SQL
fi

actual_rows="$(psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc "select count(*) from ${source_schema}.${source_table}")"
[[ "$actual_rows" == "$row_count" ]] || die "source fixture row count mismatch: expected ${row_count}, found ${actual_rows}"
psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc "select 1 from pg_publication where pubname = '${publication}'" | grep -qx 1 || die "benchmark publication is missing"
table_id="$(psql -d "$source_database" -v ON_ERROR_STOP=1 -Atc "select '${source_schema}.${source_table}'::regclass::oid")"

database_exists "$baseline_catalog" && die "baseline catalog already exists: ${baseline_catalog}"
database_exists "$buffered_catalog" && die "buffered catalog already exists: ${buffered_catalog}"
[[ ! -e "$results_dir" ]] || die "results directory already exists: ${results_dir}"
createdb "$baseline_catalog"
createdb "$buffered_catalog"
mkdir -p "$baseline_data_dir" "$buffered_data_dir"

run_copy() {
  local pipeline_id="$1"
  local catalog_database="$2"
  local data_dir="$3"
  local report_path="$4"
  shift 4

  cargo run --release -p etl-benchmarks --features ducklake --bin table_copy -- run \
    --host "$pg_host" \
    --port "$pg_port" \
    --database "$source_database" \
    --username "$pg_user" \
    --password "$pg_password" \
    --destination ducklake \
    --ducklake-catalog-url "postgres://${pg_user}:${pg_password}@${pg_host}:${pg_port}/${catalog_database}" \
    --ducklake-data-path "file://${data_dir}" \
    --ducklake-pool-size 2 \
    --pipeline-id "$pipeline_id" \
    --publication-name "$publication" \
    --table-ids "$table_id" \
    --expected-row-count "$actual_rows" \
    --max-table-sync-workers 1 \
    --max-copy-connections-per-table 1 \
    --report-path "$report_path" \
    "$@"
}

echo "running immediate-durability baseline for approximately ${target_bytes} logical bytes"
(cd "$REPOSITORY_ROOT" && run_copy 910001 "$baseline_catalog" "$baseline_data_dir" "$baseline_report")

echo "running deferred-durability candidate with ${buffer_target_bytes}-byte target"
(cd "$REPOSITORY_ROOT" && run_copy 910002 "$buffered_catalog" "$buffered_data_dir" "$buffered_report" \
  --ducklake-copy-buffer-enabled \
  --ducklake-copy-buffer-target-bytes "$buffer_target_bytes" \
  --ducklake-copy-buffer-max-total-bytes "$buffer_max_total_bytes")

echo
echo "baseline report: ${baseline_report}"
echo "buffered report: ${buffered_report}"
if command -v jq >/dev/null 2>&1; then
  jq -n --slurpfile baseline "$baseline_report" --slurpfile buffered "$buffered_report" '{
    baseline: {
      mib_per_second: $baseline[0].estimated_mib_per_second,
      parquet_files: $baseline[0].ducklake_files.file_count,
      median_file_mib: ($baseline[0].ducklake_files.median_bytes / 1048576)
    },
    buffered: {
      mib_per_second: $buffered[0].estimated_mib_per_second,
      parquet_files: $buffered[0].ducklake_files.file_count,
      median_file_mib: ($buffered[0].ducklake_files.median_bytes / 1048576)
    },
    change: {
      throughput_percent: ((($buffered[0].estimated_mib_per_second / $baseline[0].estimated_mib_per_second) - 1) * 100),
      file_count_percent: ((($buffered[0].ducklake_files.file_count / $baseline[0].ducklake_files.file_count) - 1) * 100)
    }
  }'
fi
