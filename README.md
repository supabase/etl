<br />
<p align="center">
  <a href="https://supabase.com">
    <picture>
      <img alt="ETL by Supabase" width="100%" src="docs/assets/etl-logo-extended.png">
    </picture>
  </a>

  <h1 align="center">ETL</h1>

  <p align="center">
    <a href="https://github.com/supabase/etl/actions/workflows/ci.yml">
      <img alt="CI" src="https://github.com/supabase/etl/actions/workflows/ci.yml/badge.svg?branch=main">
    </a>
    <a href="https://coveralls.io/github/supabase/etl?branch=main">
      <img alt="Coverage Status" src="https://coveralls.io/repos/github/supabase/etl/badge.svg?branch=main">
    </a>
    <a href="https://github.com/supabase/etl/actions/workflows/docs.yml">
      <img alt="Docs" src="https://github.com/supabase/etl/actions/workflows/docs.yml/badge.svg?branch=main">
    </a>
    <a href="https://github.com/supabase/etl/actions/workflows/docker-build.yml">
      <img alt="Docker Build" src="https://github.com/supabase/etl/actions/workflows/docker-build.yml/badge.svg?branch=main">
    </a>
    <a href="https://github.com/supabase/etl/actions/workflows/audit.yml">
      <img alt="Security Audit" src="https://github.com/supabase/etl/actions/workflows/audit.yml/badge.svg?branch=main">
    </a>
    <a href="LICENSE">
      <img alt="License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg">
    </a>
    <br />
    Build real-time Postgres replication applications in Rust
    <br />
    <a href="https://supabase.github.io/etl"><strong>Documentation</strong></a>
    ·
    <a href="https://github.com/supabase/etl/tree/main/etl-examples"><strong>Examples</strong></a>
    ·
    <a href="https://github.com/supabase/etl/issues"><strong>Issues</strong></a>
  </p>
</p>

ETL is a Rust framework by [Supabase](https://supabase.com) for building high‑performance, real‑time data replication apps on Postgres. It sits on top of Postgres [logical replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and gives you a clean, Rust‑native API for streaming changes to your own destinations.

## Features

- **Real‑time replication**: stream changes in real time to your own destinations
- **High performance**: configurable batching and parallelism to maximize throughput
- **Fault-tolerant**: robust error handling and retry logic built-in
- **Extensible**: implement your own custom destinations and state/schema stores
- **Production destinations**: BigQuery and Apache Iceberg officially supported
- **Type-safe**: fully typed Rust API with compile-time guarantees

## Requirements

**PostgreSQL Version:** ETL officially supports and tests against **PostgreSQL 14, 15, 16, and 17**.

- **PostgreSQL 15+** is recommended for access to advanced publication features including:
  - Column-level filtering
  - Row-level filtering with `WHERE` clauses
  - `FOR ALL TABLES IN SCHEMA` syntax

For detailed configuration instructions, see the [Configure Postgres documentation](https://supabase.github.io/etl/how-to/configure-postgres/).

## Get Started

Install via Git while we prepare for a crates.io release:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
```

Quick example using the in‑memory destination:

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    destination::memory::MemoryDestination,
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg = PgConnectionConfig {
        host: "localhost".into(),
        port: 5432,
        name: "mydb".into(),
        username: "postgres".into(),
        password: Some("password".into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
    };

    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".into(),
        pg_connection: pg,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    // Start the pipeline.
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
  
    // Wait for the pipeline indefinitely.
    pipeline.wait().await?;

    Ok(())
}
```

For tutorials and deeper guidance, see the [Documentation](https://supabase.github.io/etl) or jump into the [examples](etl-examples/README.md).

## Destinations

ETL is designed to be extensible. You can implement your own destinations, and the project currently ships with the following maintained options:

- **BigQuery** – full CRUD-capable replication for analytics workloads
- **Apache Iceberg** – append-only log of operations (updates coming soon)

Enable the destinations you need through the `etl-destinations` crate:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup instructions, migration workflows, and development guidelines.

## Contributing

We welcome pull requests and GitHub issues. We currently cannot accept new custom destinations unless there is significant community demand, as each destination carries a long-term maintenance cost. We are prioritizing core stability, observability, and ergonomics. If you need a destination that is not yet supported, please start a discussion or issue so we can gauge demand before proposing an implementation.

## License

Apache‑2.0. See `LICENSE` for details.

---

<p align="center">
  Made with ❤️ by the <a href="https://supabase.com">Supabase</a> team
</p>
