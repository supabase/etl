<br />
<p align="center">
  <a href="https://supabase.io">
        <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/supabase-logo-wordmark--dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/supabase-logo-wordmark--light.svg">
      <img alt="Supabase Logo" width="300" src="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/logo-preview.jpg">
    </picture>
  </a>

  <h1 align="center">Supabase ETL</h1>

  <p align="center">
    A Rust crate to quickly build replication solutions for Postgres. Build data pipelines which continually copy data from Postgres to other systems.
    <br />
    <a href="https://github.com/supabase/etl/tree/main/etl-examples">Examples</a>
  </p>
</p>

# ETL

This crate builds abstractions on top of Postgres's [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and pushes users towards the pit of success without letting them worry about low level details of the protocol.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Database Setup](#database-setup)
- [Running Tests](#running-tests)
- [Docker](#docker)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

The `etl` crate supports the following destinations:

- [x] BigQuery
- [ ] Apache Iceberg (planned)
- [ ] DuckDB (planned)

## Installation

To use `etl` in your Rust project, add the core library and desired destinations via git dependencies in `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

The `etl` crate provides the core replication functionality, while `etl-destinations` contains destination-specific implementations. Each destination is behind a feature of the same name in the `etl-destinations` crate. The git dependency is needed for now because the crates are not yet published on crates.io.

## Quickstart

To quickly get started with `etl`, see the [etl-examples](etl-examples/README.md) crate which contains practical examples and detailed setup instructions.

## Database Setup

Before running the examples, tests, or the API and replicator components, you'll need to set up a PostgreSQL database.
We provide a convenient script to help you with this setup. For detailed instructions on how to use the database setup script, please refer to our [Database Setup Guide](docs/guides/database-setup.md).

## Running Tests

To run the test suite:

```bash
cargo test --all-features
```

## Docker

The repository includes Docker support for both the `replicator` and `api` components:

```bash
# Build replicator image
docker build -f ./etl-replicator/Dockerfile .

# Build api image
docker build -f ./etl-api/Dockerfile .
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
