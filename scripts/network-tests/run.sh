#!/usr/bin/env bash
set -euo pipefail

# Run from any directory, against explicit workspaces and an explicit local daemon.
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
: "${ETL_WORKSPACE:?set the ETL regression-test workspace}"
suite=${NETWORK_TEST_SUITE:-both}
case "$suite" in
  etl) export API_WORKSPACE=${API_WORKSPACE:-$ETL_WORKSPACE} ;;
  api|both) : "${API_WORKSPACE:?set the API regression-test workspace}" ;;
  *) echo 'NETWORK_TEST_SUITE must be etl, api, or both' >&2; exit 1 ;;
esac
: "${NETWORK_LAB_DIR:?set an absolute disposable artifact directory}"
: "${DOCKER_CONTEXT:?select a local Docker context}"
endpoint=$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')
case "$endpoint" in unix://*) ;; *) echo 'A local Unix-socket Docker context is required' >&2; exit 1 ;; esac
compose=(docker --context "$DOCKER_CONTEXT" compose -p etl-hostname-lab -f "$script_dir/compose.yaml")
if [[ -n "${NETWORK_COMPOSE_OVERRIDE:-}" ]]; then
  compose+=(-f "$NETWORK_COMPOSE_OVERRIDE")
fi

case "${1:-test}" in
  start)
    if ! test -f "$NETWORK_LAB_DIR/certs/server.key"; then
      bash "$script_dir/prepare.sh" "$NETWORK_LAB_DIR"
    fi
    "${compose[@]}" build
    "${compose[@]}" up -d --wait database
    # Recreated servers have new MAC addresses. A retained tester can otherwise
    # keep stale IPv6 neighbor entries for the fixture's manually added aliases.
    "${compose[@]}" up -d --wait --force-recreate tester
    ;;
  test|ipv4-first)
    mkdir -p "$NETWORK_LAB_DIR"
    mode=${1:-test}
    filter='test(/^hostname_connections::/)'
    extra_env=()
    if [[ "$mode" == ipv4-first ]]; then
      # This file exists only in the disposable tester container.
      "${compose[@]}" exec -T tester sh -c 'printf "precedence ::ffff:0:0/96 100\n" > /etc/gai.conf'
      filter='test(/^hostname_connections::mixed_address_fallback$/)'
      extra_env=(-e NETWORK_IPV4_FIRST=1)
    else
      "${compose[@]}" exec -T tester sh -c ': > /etc/gai.conf'
    fi
    status=0
    if [[ "$suite" != api ]]; then
      "${compose[@]}" exec -T -w /work/etl -e CARGO_TARGET_DIR=/targets/etl "${extra_env[@]}" tester \
        cargo nextest run --locked -p etl --test main --features test-utils,failpoints \
        --tool-config-file network:/work/etl/scripts/network-tests/nextest.toml \
        --run-ignored only --test-threads 1 --success-output immediate --failure-output immediate \
        -E "$filter" 2>&1 | tee "$NETWORK_LAB_DIR/etl-$mode.log" || status=1
    fi
    if [[ "$suite" != etl ]]; then
      # Use the Rust image's system linker instead of the API's optional clang/lld
      # speed optimization. This changes build tooling, not connection behavior.
      "${compose[@]}" exec -T -w /work/api -e CARGO_TARGET_DIR=/targets/api \
        -e CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=cc \
        -e CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=cc \
        -e CARGO_ENCODED_RUSTFLAGS= "${extra_env[@]}" tester \
        cargo nextest run --locked -p etl-api --test main --no-default-features \
        --tool-config-file network:/work/etl/scripts/network-tests/nextest.toml \
        --run-ignored only --test-threads 1 --success-output immediate --failure-output immediate \
        -E "$filter" 2>&1 | tee "$NETWORK_LAB_DIR/api-$mode.log" || status=1
    fi
    "${compose[@]}" logs --no-color database > "$NETWORK_LAB_DIR/database-$mode.log"
    exit "$status"
    ;;
  down)
    # Keep only reusable compiler/dependency caches; PostgreSQL data is tmpfs.
    "${compose[@]}" down
    ;;
  *) echo 'Usage: bash run.sh start|test|ipv4-first|down' >&2; exit 1 ;;
esac
