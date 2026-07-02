# Scripts

Development workflows should be exposed through the `cargo x` task runner (see
`cargo x --help`). Shell scripts in `scripts/bin` are legacy entrypoints kept
only until their implementation is moved into native xtask commands and the
scripts can be removed.

New development commands should be added as xtask commands in
`crates/xtask/src/commands/` rather than as shell scripts here. Existing shell
scripts are being removed over time as their logic moves into xtask.

## Script to xtask mapping

| Script                                | xtask command             |
| ------------------------------------- | ------------------------- |
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
