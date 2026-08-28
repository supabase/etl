<br />
<p align="center">
  <a href="https://supabase.com">
    <img alt="Supabase ETL" width="100%" src="site/public/assets/etl-logo-extended.png">
  </a>
</p>

<h1 align="center">Supabase ETL</h1>

<p align="center">
  High-performance Postgres replication, written in Rust.
  <br />
  Embed it as a library or run it as a standalone binary.
</p>

<p align="center">
  A project by <a href="https://supabase.com">Supabase</a>.
</p>

<p align="center">
  <a href="https://github.com/supabase/etl/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/supabase/etl/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://coveralls.io/github/supabase/etl?branch=main"><img alt="Coverage" src="https://coveralls.io/repos/github/supabase/etl/badge.svg?branch=main"></a>
  <a href="https://github.com/supabase/etl/actions/workflows/docs.yml"><img alt="Docs" src="https://github.com/supabase/etl/actions/workflows/docs.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/supabase/etl/actions/workflows/audit.yml"><img alt="Security audit" src="https://github.com/supabase/etl/actions/workflows/audit.yml/badge.svg?branch=main"></a>
  <a href="LICENSE"><img alt="Apache 2.0 license" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg"></a>
</p>

<p align="center">
  <a href="https://supabase.github.io/etl/"><strong>Documentation</strong></a>
  ·
  <a href="https://supabase.github.io/etl/guides/first-pipeline/"><strong>First Pipeline</strong></a>
  ·
  <a href="https://supabase.github.io/etl/guides/standalone-replicator/"><strong>Standalone Replicator</strong></a>
  ·
  <a href="crates/etl-examples/README.md"><strong>Examples</strong></a>
  ·
  <a href="https://github.com/supabase/etl/issues"><strong>Issues</strong></a>
</p>

> [!NOTE]
> Supabase ETL is under active development. APIs and setup steps may change
> before the first stable release.

Supabase ETL is a high-performance Postgres replication engine written in Rust.
Embed it in your Rust application or run it as a standalone binary. For each
published table, it performs an initial sync of existing rows, then replicates
changes to a built-in or custom destination.

This repository contains the framework, destination modules, standalone
replicator, examples, and documentation. For the managed product in the
Supabase Dashboard—including availability, pricing, and operations—use the
[Supabase Pipelines documentation](https://supabase.com/docs/guides/database/replication/pipelines).

## How It Works

```mermaid
flowchart LR
    Postgres["Postgres publication"] --> Sync["Initial sync"]
    Sync --> Replication["Ongoing replication"]
    Replication --> Destination["Destination"]
```

Supabase ETL replicates each table in two phases:

1. **Initial sync:** Copy the existing rows selected by the publication, then
   catch up changes that occurred while the copy was running.
2. **Ongoing replication:** Capture subsequent inserts, updates, deletes, and
   truncates, then deliver those changes as ordered events.

Across both phases, a store persists checkpoints, schemas, destination
metadata, and table state so replication can recover safely after a restart.
Copy and change data capture (CDC) are replication paths, not customer-visible
phases. Initial sync uses the copy path followed by CDC catch-up; ongoing
replication uses the CDC path after the table is ready.

## Start Here

| Goal | Documentation |
| --- | --- |
| Build a working pipeline | [First Pipeline](https://supabase.github.io/etl/guides/first-pipeline/) |
| Run the standalone process | [Standalone Replicator](https://supabase.github.io/etl/guides/standalone-replicator/) |
| Prepare a source database | [Configure Postgres](https://supabase.github.io/etl/guides/configure-postgres/) |
| Implement a store or destination | [Custom Implementations](https://supabase.github.io/etl/guides/custom-implementations/) |
| Understand the runtime | [Architecture](https://supabase.github.io/etl/explanation/architecture/) |
| Browse runnable destinations | [`etl-examples`](crates/etl-examples/README.md) |

ETL is installed from Git while we prepare for a crates.io release:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1", features = ["full"] }
```

## Why Supabase ETL?

- **Library or standalone binary:** Embed the engine in a Rust application or
  run the ready-made replicator.
- **High performance, small footprint:** Run one Rust process without Kafka,
  Flink, Debezium, or another coordination service.
- **Postgres-native selection:** Use publications to select tables, columns,
  rows, and operation types.
- **Flexible by design:** Implement custom destinations and durable stores with
  typed Rust APIs.
- **Recovery-aware:** Persist checkpoints and table state for safe restarts and
  at-least-once delivery.

## [Destinations](https://supabase.github.io/etl/reference/destinations/)

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `clickhouse` | ClickHouse | In progress |
| `ducklake` | DuckLake | In progress |
| `snowflake` | Snowflake | In progress |
| `iceberg` | Apache Iceberg | Deprecated |

BigQuery is the stable, recommended default. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
for maturity and limitations, and the
[`etl-examples` guide](crates/etl-examples/README.md) for runnable examples.

## Requirements

Supabase ETL supports PostgreSQL 14 through 18. PostgreSQL 15 or newer is
recommended for column and row publication filters. PostgreSQL 16 or newer is
required when the replication connection points at a physical read replica.

The source must use `wal_level = logical`, and the replication user needs the
`REPLICATION` role. See [Configure Postgres](https://supabase.github.io/etl/guides/configure-postgres/)
for the complete setup and production guidance.

## Development

```bash
cargo x init              # Docker, databases, migrations
cargo x setup api && cargo x run api
cargo x setup replicator --destination clickhouse && cargo x seed && cargo x run replicator
```

See [DEVELOPMENT.md](DEVELOPMENT.md) to start the replicator or API, pick a
destination, and run tests. The workspace uses Rust 1.95.0 from
`rust-toolchain.toml`.

## Contributing

Contributions are welcome. Before proposing a new destination, start a
[discussion or issue](https://github.com/supabase/etl/issues) so maintainership
and long-term demand can be evaluated first.

Report suspected vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
