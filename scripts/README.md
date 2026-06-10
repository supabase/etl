# Scripts

Legacy shell scripts for development workflows. All scripts are accessible through the `cargo x` task runner (see `cargo x --help`).

New development commands should be added as xtask commands in `crates/xtask/src/commands/` rather than as shell scripts here. Existing scripts will be ported to native xtask commands over time.

## Script to xtask mapping

| Script                                | xtask command             |
| ------------------------------------- | ------------------------- |
| `fmt.sh` / `fmt-check.sh`             | `cargo x fmt [--check]`   |
| `check-msrv-sync.sh`                  | `cargo x msrv`            |
| `init.sh`                             | `cargo x init`            |
| `run-migrations.sh`                   | `cargo x migrate`         |
| `deploy-local-replicator-orbstack.sh` | `cargo x deploy-local`    |
| `test-clickhouse.sh`                  | `cargo x test-clickhouse` |
| `vendor-duckdb-extensions.sh`         | `cargo x vendor-duckdb`   |
