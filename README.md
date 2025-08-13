<br />
<p align="center">
  <a href="https://supabase.io">
        <picture>
      <img alt="Supabase Logo" width="100%" src="res/etl-logo-extended.png">
    </picture>
  </a>

  <h1 align="center">ETL</h1>

  <p align="center">
    Build real-time Postgres replication applications in Rust
    <br />
    <a href="https://supabase.github.io/etl"><strong>Documentation</strong></a>
    ·
    <a href="https://github.com/supabase/etl/tree/main/etl-examples"><strong>Examples</strong></a>
    ·
    <a href="https://github.com/supabase/etl/issues"><strong>Issues</strong></a>
  </p>
</p>

**ETL** is a Rust framework by [Supabase](https://supabase.com) that enables you to build high-performance, real-time data replication applications for PostgreSQL. Whether you're creating ETL pipelines, implementing CDC (Change Data Capture), or building custom data synchronization solutions, ETL provides the building blocks you need.

Built on top of PostgreSQL's [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html), ETL handles the low-level complexities of database replication while providing a clean, Rust-native API that guides you towards the pit of success.

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

**Core Capabilities:**
- 🚀 **Real-time replication**: Stream changes from PostgreSQL as they happen
- 🔄 **Multiple destinations**: Support for various data warehouses and databases (coming soon)
- 🛡️ **Fault tolerance**: Built-in error handling, retries, and recovery mechanisms
- ⚡ **High performance**: Efficient batching and parallel processing
- 🔧 **Extensible**: Plugin architecture for custom destinations

**Supported Destinations:**
- [x] **BigQuery** - Google Cloud's data warehouse
- [ ] **Apache Iceberg** (planned) - Open table format for analytics
- [ ] **DuckDB** (planned) - In-process analytical database

## Installation

Add ETL to your Rust project via git dependencies in `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
```

> **Note**: ETL is currently distributed via Git while we prepare for the initial crates.io release.

## Quickstart

Get up and running with ETL in minutes using the built-in memory destination:

```rust
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::destination::memory::MemoryDestination;
use etl::store::both::memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL connection
    let pg_connection_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "mydb".to_string(),
        username: "postgres".to_string(),
        password: Some("password".into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    // Configure the pipeline
    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: 4,
    };

    // Create in-memory store and destination for testing
    let store = MemoryStore::new();
    let destination = MemoryDestination::new();
    
    // Create and start the pipeline
    let mut pipeline = Pipeline::new(1, pipeline_config, store, destination);
    pipeline.start().await?;
    
    Ok(())
}
```

**Need production destinations?** Add the `etl-destinations` crate with specific features:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

For comprehensive examples and tutorials, visit the [etl-examples](etl-examples/README.md) crate and our [documentation](https://supabase.github.io/etl).

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

Currently, the system parallelizes the copying of different tables, but each individual table is still copied in sequential batches.
This limits performance for large tables. We plan to address this once the ETL system reaches greater stability.

## License

Distributed under the Apache-2.0 License. See `LICENSE` for more information.

---

<p align="center">
  Made with ❤️ by the <a href="https://supabase.com">Supabase</a> team
</p>
