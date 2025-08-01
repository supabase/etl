# ETL

A Rust crate to quickly build replication solutions for Postgres. It provides building blocks to construct data pipelines which can continually copy data from Postgres to other systems. It builds abstractions on top of Postgres's [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and pushes users towards the pit of success without letting them worry about low level details of the protocol.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Examples](#examples)
- [Database Setup](#database-setup)
- [Running Tests](#running-tests)
- [Docker](#docker)
- [Architecture](#architecture)
- [Roadmap](#roadmap)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

The `etl` crate supports the following destinations:

- [x] BigQuery
- [ ] DuckDB
- [ ] MotherDuck
- [ ] Snowflake (planned)
- [ ] ClickHouse (planned)
- [ ] Many more to come...

## Installation

To use `etl` in your Rust project, add the core library and desired destinations via git dependencies in `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

The `etl` crate provides the core replication functionality, while `etl-destinations` contains destination-specific implementations. Each destination is behind a feature of the same name in the `etl-destinations` crate. The git dependency is needed for now because the crates are not yet published on crates.io.

## Quickstart

To quickly try out `etl`, you can run the BigQuery example from the `etl-examples` crate. First, create a publication in Postgres which includes the tables you want to replicate:

```sql
create publication my_publication
for table table1, table2;
```

Then run the BigQuery example:

```bash
cargo run -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name postgres \
        --db-username postgres \
        --db-password password \
        --bq-sa-key-file /path/to/your/service-account-key.json \
        --bq-project-id your-gcp-project-id \
        --bq-dataset-id your_bigquery_dataset_id \
        --publication my_publication
```

In the above example, `etl` connects to a Postgres database named `postgres` running on `localhost:5432` with a username `postgres` and password `password`.

## Examples

For code examples on how to use `etl`, please refer to the [etl-examples](https://github.com/supabase/etl/tree/main/etl-examples) crate. The main example demonstrates BigQuery integration and can be found in `etl-examples/src/main.rs`.

## Database Setup

Before running the examples, tests, or the API and replicator components, you'll need to set up a PostgreSQL database.
We provide a convenient script to help you with this setup. For detailed instructions on how to use the database setup script, please refer to our [Database Setup Guide](docs/guides/database-setup.md).

## Running Tests

To run the test suite:

```bash
cargo test
```

## Docker

The repository includes Docker support for both the `replicator` and `api` components:

```bash
# Build replicator image
docker build -f ./replicator/Dockerfile .

# Build api image
docker build -f ./api/Dockerfile .
```

## Architecture

For a detailed explanation of the ETL architecture and design decisions, please refer to our [Design Document](docs/design/etl-crate-design.md).

## Troubleshooting

### Too Many Open Files Error

If you see the following error when running tests on macOS:

```
called `Result::unwrap()` on an `Err` value: Os { code: 24, kind: Uncategorized, message: "Too many open files" }
```

Raise the limit of open files per process with:

```bash
ulimit -n 10000
```

### Performance Considerations

Currently, the data source and destinations copy table row and CDC events one at a time. This is expected to be slow. Batching and other strategies will likely improve the performance drastically. But at this early stage, the focus is on correctness rather than performance. There are also zero benchmarks at this stage, so commentary about performance is closer to speculation than reality.

## License

Distributed under the Apache-2.0 License. See `LICENSE` for more information.
