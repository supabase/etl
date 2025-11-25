# ETL Documentation

**Build real-time Postgres replication applications in Rust**

ETL is a Rust framework by [Supabase](https://supabase.com) for building high‑performance, real‑time data replication apps on Postgres. It sits on top of Postgres logical replication and gives you a clean, Rust‑native API for streaming changes to your own destinations.

## Getting Started

Choose your path based on your needs:

### New to ETL?

Start with our **[Tutorials](tutorials/index.md)** to learn ETL through hands-on examples:

- [Build your first ETL pipeline](tutorials/first-pipeline.md) - Complete beginner's guide (15 minutes)
- [Build custom stores and destinations](tutorials/custom-implementations.md) - Advanced patterns (30 minutes)

### Ready to solve specific problems?

Jump to our **[How-To Guides](how-to/index.md)** for practical solutions:

- [Configure Postgres for replication](how-to/configure-postgres.md)
- More guides coming soon

### Want to understand the bigger picture?

Read our **[Explanations](explanation/index.md)** for deeper insights:

- [ETL architecture overview](explanation/architecture.md)
- More explanations coming soon

## Features

- **Real‑time replication**: stream changes in real time to your own destinations
- **High performance**: configurable batching and parallelism to maximize throughput
- **Fault-tolerant**: robust error handling and retry logic built-in
- **Extensible**: implement your own custom destinations and state/schema stores
- **Production destinations**: BigQuery and Apache Iceberg officially supported
- **Type-safe**: fully typed Rust API with compile-time guarantees

## Quick Example

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, SchemaCreationMode, TlsConfig},
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
        schema_creation_mode: SchemaCreationMode::CreateIfMissing,
    };

    // Start the pipeline.
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;

    // Wait for the pipeline indefinitely.
    pipeline.wait().await?;

    Ok(())
}
```

## Next Steps

- **First time using ETL?** → Start with [Build your first pipeline](tutorials/first-pipeline.md)
- **Need Postgres setup help?** → Check [Configure Postgres for Replication](how-to/configure-postgres.md)
- **Need technical details?** → Check the [Reference](reference/index.md)
- **Want to understand the architecture?** → Read [ETL Architecture](explanation/architecture.md)

## Contributing

We welcome pull requests and GitHub issues. We currently cannot accept new custom destinations unless there is significant community demand, as each destination carries a long-term maintenance cost. We are prioritizing core stability, observability, and ergonomics. If you need a destination that is not yet supported, please start a discussion or issue so we can gauge demand before proposing an implementation.
