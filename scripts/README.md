# Scripts

Legacy shell scripts for development workflows. Remaining scripts are accessible through the `cargo x` task runner (see `cargo x --help`).

New development commands should be added as xtask commands in `crates/xtask/src/commands/` rather than as shell scripts here. Existing scripts will be ported to native xtask commands over time.

## Script to xtask mapping

| Script                                | xtask command             |
| ------------------------------------- | ------------------------- |
| `bin/check-msrv-sync.sh`              | `cargo x msrv`            |
| `bin/init.sh`                         | `cargo x init`            |
| `bin/run-migrations.sh`               | `cargo x migrate`         |
| `bin/deploy-local-replicator-orbstack.sh` | `cargo x deploy-local` |
| `bin/test-clickhouse.sh`              | `cargo x test-clickhouse` |
| `bin/vendor-duckdb-extensions.sh`     | `cargo x vendor-duckdb`   |

## Directory layout

| Directory | Purpose |
| --------- | ------- |
| `bin/` | Executable development scripts. |
| `docker/` | Docker Compose file and resources mounted by the local stack. |
| `docs/` | Documentation utility scripts. |
| `k8s/local/` | Pre-defined Kubernetes resources for local OrbStack development. |
