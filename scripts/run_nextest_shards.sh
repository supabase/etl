#!/usr/bin/env bash
set -euo pipefail

# Runs nextest in one non-Postgres lane plus one Postgres lane per local test
# database.
# - the non-Postgres lane handles tests that do not share the source Postgres
#   cluster.
# - each Postgres lane is bound to a dedicated source Postgres cluster and a
#   disjoint nextest partition of the shared-PG tests.
#
# Environment:
#   NUM_LOCAL_DATABASES: number of local Postgres clusters to shard across.
#     Defaults to 3.
#   TESTS_DATABASE_START_PORT: base port for the first local Postgres cluster.
#     Additional cluster ports are allocated contiguously from this base.
#     Defaults to TESTS_DATABASE_PORT, POSTGRES_PORT, or 5430.
#
# Usage:
#   scripts/run_nextest_shards.sh run
#   scripts/run_nextest_shards.sh llvm-cov

mode="${1:-}"
if [[ -z "$mode" ]]; then
  echo "usage: $0 <run|llvm-cov>" >&2
  exit 1
fi
shift || true

case "$mode" in
  run)
    base_cmd=(cargo nextest run)
    prebuild_cmd=(cargo nextest run --workspace --all-features --no-run)
    ;;
  llvm-cov)
    base_cmd=(cargo llvm-cov nextest)
    prebuild_cmd=()
    ;;
  *)
    echo "unsupported mode: $mode" >&2
    exit 1
    ;;
esac

extra_args=("$@")

shared_pg_filter='binary_id(etl::main) | (binary_id(etl-destinations::main) & test(/^(bigquery_pipeline|ducklake_pipeline|iceberg_destination)::/))'
non_pg_filter="not (${shared_pg_filter})"

db_count="${NUM_LOCAL_DATABASES:-3}"
if ! [[ "$db_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "expected NUM_LOCAL_DATABASES to be a positive integer, got: $db_count" >&2
  exit 1
fi

base_port="${TESTS_DATABASE_START_PORT:-${TESTS_DATABASE_PORT:-${POSTGRES_PORT:-5430}}}"
if ! [[ "$base_port" =~ ^[0-9]+$ ]]; then
  echo "expected TESTS_DATABASE_START_PORT to be a numeric port, got: $base_port" >&2
  exit 1
fi

pg_ports=()
for ((i = 0; i < db_count; i++)); do
  pg_ports+=("$((base_port + i))")
done

common_args=(--workspace --all-features --no-fail-fast)
if [[ "$mode" == "llvm-cov" ]]; then
  common_args+=(--no-report)
fi

run_lane() {
  local lane_name="$1"
  shift
  (
    set -euo pipefail
    "$@" 2>&1 | sed -e "s/^/[${lane_name}] /"
  ) &
  lane_pids+=("$!")
}

echo "running sharded ${mode} with ${db_count} local Postgres clusters on ports: ${pg_ports[*]}." >&2

if [[ "$mode" == "llvm-cov" ]]; then
  echo "installing llvm-tools-preview before launching sharded ${mode} run." >&2
  rustup component add llvm-tools-preview
fi

if [[ "${#prebuild_cmd[@]}" -gt 0 ]]; then
  echo "prebuilding test binaries for sharded ${mode} run." >&2
  "${prebuild_cmd[@]}"
fi

lane_pids=()

non_pg_cmd=("${base_cmd[@]}" "${common_args[@]}" -E "$non_pg_filter")
if [[ "${#extra_args[@]}" -gt 0 ]]; then
  non_pg_cmd+=("${extra_args[@]}")
fi
run_lane non-pg "${non_pg_cmd[@]}"

for ((i = 0; i < db_count; i++)); do
  partition=$((i + 1))
  port="${pg_ports[$i]}"

  shared_pg_cmd=(
    env
    TESTS_DATABASE_HOST="${TESTS_DATABASE_HOST:-localhost}"
    TESTS_DATABASE_PORT="$port"
    TESTS_DATABASE_USERNAME="${TESTS_DATABASE_USERNAME:-postgres}"
    TESTS_DATABASE_PASSWORD="${TESTS_DATABASE_PASSWORD:-postgres}"
    "${base_cmd[@]}"
    "${common_args[@]}"
    -E "$shared_pg_filter"
    --partition "hash:${partition}/${db_count}"
  )
  if [[ "${#extra_args[@]}" -gt 0 ]]; then
    shared_pg_cmd+=("${extra_args[@]}")
  fi

  run_lane "shared-pg-${partition}" "${shared_pg_cmd[@]}"
done

status=0
for pid in "${lane_pids[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

exit "$status"
