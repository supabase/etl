# Development Guide

How to work in this repository. Coding agents should follow
[AGENTS.md](AGENTS.md) (first-time setup and implementation rules).

The open-source project is the replication engine: the `etl` library, the
`etl-replicator` binary, and the built-in destinations. Product docs live at
[supabase.github.io/etl](https://supabase.github.io/etl/).

`etl-api` is an optional Kubernetes control-plane for deploying replicators.
You do not need it to run ETL.

## Start here

```bash
cargo x init
```

That starts local Postgres, ClickHouse, and the Iceberg REST catalog, and runs
migrations. It does not write service configuration or apply Kubernetes
resources.

Then set up the service you want:

```bash
cargo x setup api
cargo x run api

# or
cargo x setup replicator --destination clickhouse
cargo x seed
cargo x run replicator
```

Generated files in `crates/etl-api/configuration/` and
`crates/etl-replicator/configuration/` are gitignored. Re-run with `--force` to
replace them. Do not commit those files.

Need: Rust from `rust-toolchain.toml`, `psql`, SQLx CLI, and Docker Compose.
`kubectl` plus [OrbStack](https://orbstack.dev) with Kubernetes if you run the
API.

Install SQLx CLI:

```bash
cargo install --version 0.9.0-alpha.1 sqlx-cli --no-default-features --features rustls,postgres --locked
```

## Replicator

Pick a destination, then start. ClickHouse and Iceberg REST use the local Docker
services from `cargo x init`. Cloud destinations write fake identifiers and
placeholder secrets; override them with `APP_DESTINATION__*` for a real run.

| Destination | Setup | Then |
| --- | --- | --- |
| `clickhouse` | `cargo x setup replicator --destination clickhouse` | `cargo x seed && cargo x run replicator` |
| `iceberg` | `cargo x setup replicator --destination iceberg` | `cargo x seed && cargo x run replicator` |
| `bigquery` | `cargo x setup replicator --destination bigquery` | export `APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY`, then `cargo x run replicator` |
| `ducklake` | `cargo x setup replicator --destination ducklake` | export `APP_DESTINATION__DUCKLAKE__S3_*`, then `cargo x run replicator` |
| `snowflake` | `cargo x setup replicator --destination snowflake` | export `APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY`, then `cargo x run replicator` |

Iceberg is deprecated. For a Supabase catalog, add `--iceberg-catalog supabase`
and export `APP_DESTINATION__ICEBERG__SUPABASE__*`.

`cargo x setup replicator --help` lists flags. `--interactive` prompts for hosts
and names. `cargo x run replicator` compiles the destination from the generated
config. The replicator does not need Kubernetes.

## API

```bash
cargo x setup api
cargo x run api
```

`cargo x setup api` writes API configuration and applies Kubernetes resources.
If you skipped `cargo x init`, it also starts the local stack. Health is at
http://127.0.0.1:8010/health_check, Swagger at `/swagger-ui`. See
`crates/etl-api/README.md` for configuration.

## Everyday commands

`cargo x` is the task runner. `cargo x --help` lists every command.

```bash
cargo x fmt              # nightly rustfmt (pinned)
cargo x fmt --check
cargo x check            # fmt, sort, clippy
cargo x fix
cargo x migrate          # API and ETL migrations
cargo xtask nextest run  # full sharded test suite (needs Postgres)
```

## Local stack

`cargo x init` defaults:

| Service | Address |
| --- | --- |
| Postgres | `localhost:5430` (`postgres` / `postgres`) |
| Postgres replica | `localhost:6430` |
| ClickHouse | `http://localhost:8123` (`etl` / `etl`) |
| Iceberg catalog | `http://localhost:8182` |
| MinIO | `http://localhost:9010` (`minio-admin` / `minio-admin-password`) |

Override with `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`, `CLICKHOUSE_*`, or `SKIP_DOCKER=1` when
Postgres is already running. Persistent volume paths: `POSTGRES_DATA_VOLUME`,
`POSTGRES_REPLICA_DATA_VOLUME`, `CLICKHOUSE_DATA_VOLUME`.

`cargo xtask postgres start` starts only the test Postgres clusters.
`cargo xtask multigres --help` covers the optional Multigres cluster.

## Configuration

Both binaries load `configuration/base.yaml`, then
`configuration/{environment}.yaml`, then `APP_` environment variables (nested
keys use `__`). `APP_ENVIRONMENT` defaults to `prod`. `cargo x run` sets `dev`
and points `APP_CONFIG_DIR` at the generated directory.

Generated files include only required fields. Encryption keys and API keys are
random. Local Docker passwords are the published Compose defaults. Cloud
destination secrets are fake placeholders.

## Tests

After `cargo x init`:

```bash
cargo xtask nextest run
```

Unit tests that do not need Postgres:

```bash
cargo nextest run --workspace --all-features --lib
```

Integration tests need `TESTS_DATABASE_HOST`, `TESTS_DATABASE_PORT`, and
`TESTS_DATABASE_USERNAME` (local defaults: `localhost`, `5430`, `postgres`).
ClickHouse tests also need `TESTS_CLICKHOUSE_URL`, `TESTS_CLICKHOUSE_USER`, and
`TESTS_CLICKHOUSE_PASSWORD`. Iceberg tests use the local catalog and MinIO from
`cargo x init`. BigQuery tests need `TESTS_BIGQUERY_PROJECT_ID` and
`TESTS_BIGQUERY_SA_KEY_PATH`.

Debug a failing test with `ENABLE_TRACING=1` and a focused `RUST_LOG`.
Parser fuzz targets live in `fuzz/`.

## Troubleshooting

- Nothing listens on 5430: run `cargo x init` (or `SKIP_DOCKER=1` with
  `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and
  `POSTGRES_DB`).
- API exits on Kubernetes: enable OrbStack Kubernetes, then `cargo x setup api`.
- Replicator config missing: `cargo x setup replicator --destination <name>`.
