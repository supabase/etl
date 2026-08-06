<br />
<p align="center">
  <a href="https://supabase.com">
    <img alt="Supabase ETL" width="100%" src="docs/public/assets/etl-logo-extended.png">
  </a>
</p>

<h1 align="center">Supabase ETL</h1>

<p align="center">
  Postgres replication for Rust.
  <br />
  Perform an initial sync of Postgres tables, then replicate changes to destinations.
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
  <a href="crates/etl-examples/README.md"><strong>Examples</strong></a>
  ·
  <a href="https://github.com/supabase/etl/issues"><strong>Issues</strong></a>
</p>

> [!IMPORTANT]
> Supabase ETL is under active development.

Supabase ETL is Supabase's open-source Rust framework for Postgres change data
capture. For each published table, it first performs an initial sync of existing
rows. It then replicates changes and delivers them to a built-in or
custom destination.

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

1. **Initial sync:** Copy the existing rows selected by the publication.
2. **Ongoing replication:** Capture subsequent inserts, updates, deletes, and
   truncates, then deliver those changes as ordered events.

Across both phases, a store persists checkpoints, schemas, destination
metadata, and table state so replication can recover safely after a restart.
Streaming is a transfer mode that may be used within either phase; it is not a
separate replication phase.

## Start Here

| Goal | Documentation |
| --- | --- |
| Build a working pipeline | [First Pipeline](https://supabase.github.io/etl/guides/first-pipeline/) |
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

- **Small operational footprint:** Run one Rust process without Kafka, Flink,
  Debezium, or another coordination service.
- **Postgres-native selection:** Use publications to select tables, columns,
  rows, and operation types.
- **Initial sync and ongoing replication:** Copy existing data, then replicate
  subsequent changes through one pipeline.
- **Extensible runtime:** Implement custom destinations and durable stores with
  typed Rust APIs.
- **Recovery-aware:** Persist checkpoints and table state for safe restarts and
  at-least-once delivery.

## Destinations

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `clickhouse` | ClickHouse | In progress |
| `ducklake` | DuckLake | In progress |
| `snowflake` | Snowflake | In progress |
| `iceberg` | Apache Iceberg | Deprecated for now |

BigQuery is the most mature destination module. See the
[`etl-examples` guide](crates/etl-examples/README.md) for prerequisites and
commands for each destination.

## Requirements

Supabase ETL supports PostgreSQL 14 through 18. PostgreSQL 15 or newer is
recommended for column and row publication filters. PostgreSQL 16 or newer is
required when the replication connection points at a physical read replica.

The source must use `wal_level = logical`, and the replication user needs the
`REPLICATION` role. See [Configure Postgres](https://supabase.github.io/etl/guides/configure-postgres/)
for the complete setup and production guidance.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for local setup, migrations, formatting,
linting, and tests. The workspace uses Rust 1.95.0 from `rust-toolchain.toml`.

## Contributing

Contributions are welcome. Before proposing a new destination, start a
[discussion or issue](https://github.com/supabase/etl/issues) so maintainership
and long-term demand can be evaluated first.

## License

Apache-2.0. See [LICENSE](LICENSE).

---

<p align="center">
  Made with ❤️ by the <a href="https://supabase.com">Supabase</a> team
</p>
