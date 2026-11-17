#!/bin/sh
# Profiles tokio lock waits in an ETL pipeline run against a throwaway Postgres.
#
# Starts a throwaway PostgreSQL cluster with logical replication enabled,
# listening only on 127.0.0.1. Builds the `lock_contention` benchmark with
# `--cfg tokio_unstable` into target/lock-contention, runs it, prints its
# report, and stops the cluster. Nothing outside target/lock-contention and one
# short socket directory under /tmp changes. Each run's cluster and logs stay in
# target/lock-contention/postgres-<timestamp> for inspection.
#
# Run from the repository, or through the flake: `nix run .#lock-contention`.
#
# Usage: lock-contention.sh [benchmark options]
#   Options go to the benchmark; see `lock-contention.sh --help`. Examples:
#   lock-contention.sh --tables 16 --writers 8 --duration-seconds 60
#   lock-contention.sh --max-table-sync-workers 8 --max-copy-connections-per-table 4
#
# Environment:
#   LOCK_CONTENTION_PG_PORT  Port of the temporary cluster (default: 5450).
#   RUSTFLAGS                Extra compiler flags, kept after `--cfg tokio_unstable`.

set -u

say() { printf '%s: %s\n' "${0##*/}" "$1" >&2; }
err() { say "error: $1"; exit 1; }
check_cmd() { command -v "$1" >/dev/null 2>&1; }
need_cmd() { check_cmd "$1" || err "need '$1' (command not found)"; }
ensure() { "$@" || err "command failed: $*"; }
ignore() { "$@"; }

# Set by start_postgres and read by stop_postgres.
PG_DATA=""

# Moves to the repository root so cargo finds the workspace.
enter_repository() {
    _root="$(git rev-parse --show-toplevel 2>/dev/null)" ||
        err "run this from inside the etl repository"
    [ -f "$_root/crates/etl-benchmarks/Cargo.toml" ] ||
        err "$_root is not the etl repository"
    ensure cd "$_root"
}

# Creates and starts a cluster with data in $1, its socket in $2, on port $3.
start_postgres() {
    _run_dir="$1"
    _socket_dir="$2"
    _port="$3"
    PG_DATA="$_run_dir/data"

    say "starting PostgreSQL on 127.0.0.1:$_port (logs: $_run_dir)"
    ensure initdb --pgdata="$PG_DATA" --username=postgres --auth=trust \
        --encoding=UTF8 --no-sync >"$_run_dir/initdb.log" 2>&1
    # Durability settings are off because the cluster is disposable; the
    # replication settings are what logical replication needs.
    pg_ctl --pgdata="$PG_DATA" --log="$_run_dir/postgres.log" --wait start \
        -o "-c listen_addresses=127.0.0.1 -c port=$_port \
            -c unix_socket_directories=$_socket_dir -c wal_level=logical \
            -c max_replication_slots=64 -c max_wal_senders=64 \
            -c max_connections=200 -c fsync=off -c synchronous_commit=off" \
        >/dev/null || err "PostgreSQL did not start; see $_run_dir/postgres.log"
}

# Stops the temporary cluster if it was started.
stop_postgres() {
    if [ -n "$PG_DATA" ] && [ -f "$PG_DATA/postmaster.pid" ]; then
        ignore pg_ctl --pgdata="$PG_DATA" --mode=fast --wait stop >/dev/null 2>&1
    fi
}

# Builds and runs the benchmark against port $1, passing the remaining args.
run_benchmark() {
    _port="$1"
    shift

    say "building and running the lock_contention benchmark"
    RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" cargo run --locked --quiet \
        --profile bench --target-dir target/lock-contention \
        -p etl-benchmarks --features lock-contention --bin lock_contention -- \
        --host 127.0.0.1 --port "$_port" --database postgres --username postgres \
        --disable-memory-backpressure "$@"
}

main() {
    need_cmd git
    need_cmd cargo
    need_cmd initdb
    need_cmd pg_ctl
    need_cmd mktemp
    need_cmd date
    need_cmd mkdir

    enter_repository

    # Help needs no database.
    for _arg in "$@"; do
        case "$_arg" in
            -h | --help)
                run_benchmark 0 --help
                return
                ;;
        esac
    done

    _port="${LOCK_CONTENTION_PG_PORT:-5450}"
    _run_dir="$PWD/target/lock-contention/postgres-$(date +%Y%m%d-%H%M%S)"
    ensure mkdir -p "$_run_dir"
    # Socket paths are limited to about 100 bytes, so the socket lives in a
    # short directory instead of next to the data.
    _socket_dir="$(mktemp -d /tmp/etl-lc.XXXXXX)" ||
        err "could not create a socket directory"

    trap stop_postgres EXIT
    trap 'stop_postgres; exit 130' INT TERM
    start_postgres "$_run_dir" "$_socket_dir" "$_port"

    run_benchmark "$_port" "$@"
    _status=$?

    stop_postgres
    say "PostgreSQL data and logs kept in $_run_dir"
    return "$_status"
}

main "$@" || exit 1
