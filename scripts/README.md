# Scripts

Development workflows should be exposed through the `cargo x` task runner (see
`cargo x --help`). Shell scripts in `scripts/bin` are not the preferred way to
add new development commands.

New development commands should be added as xtask commands in
`crates/xtask/src/commands/` rather than as shell scripts here. A shell helper
may remain when it must run in an environment where Cargo and xtask are
intentionally unavailable, such as a slim Docker build stage.

## Retained helpers

| Helper | Reason | xtask command |
| ------ | ------ | ------------- |
| `bin/vendor-duckdb-extensions.sh` | Used by Dockerfiles while fetching DuckDB extensions in a slim Debian stage without Rust or Cargo. | `cargo x vendor-duckdb` |

## Directory layout

| Directory | Purpose |
| --------- | ------- |
| `bin/` | Executable development scripts. |
| `docker/` | Docker Compose file and resources mounted by the local stack. |
| `docs/` | Documentation utility scripts. |
| `k8s/local/` | Pre-defined Kubernetes resources for local OrbStack development. |
