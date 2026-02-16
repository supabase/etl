# ETL

**Stream Postgres changes anywhere, in real-time**

ETL is a Rust framework for building change data capture (CDC) pipelines on Postgres. Stream inserts, updates, and deletes to BigQuery, Apache Iceberg, or your own custom destinations.

## Start Here

| Your background | Recommended path |
|-----------------|------------------|
| New to Postgres logical replication | [Postgres Replication Concepts](explanation/concepts.md) |
| Ready to build | [Your First Pipeline](guides/first-pipeline.md) (15 min) |
| Need custom destinations | [Custom Stores and Destinations](guides/custom-implementations.md) (30 min) |
| Setting up Postgres | [Configure Postgres](guides/configure-postgres.md) |

## Why ETL?

- **Real-time**: Changes stream as they happen, not in batches
- **Reliable**: At-least-once delivery with automatic retries
- **Extensible**: Implement one trait to add any destination
- **Fast**: Parallel initial copy, configurable batching
- **Type-safe**: Rust API with compile-time guarantees

## How It Works

1. **Initial copy**: ETL copies existing table data to your destination
2. **Streaming**: ETL streams [events](explanation/events.md) (Insert, Update, Delete, and more) in real-time
3. **Recovery**: The [store](explanation/traits.md) persists state so pipelines resume after restarts

See [Architecture](explanation/architecture.md) for details.

## Quick Example

Add ETL to your project:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1.0", features = ["full"] }
```

Create a pipeline:

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};
use etl_destinations::bigquery::BigQueryDestination;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "mydb".to_string(),
        username: "postgres".to_string(),
        password: Some("password".to_string().into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
        keepalive: None,
    };

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    let store = MemoryStore::new();
    let destination = BigQueryDestination::new_with_key_path(
        "my-gcp-project".into(),
        "my_dataset".into(),
        "/path/to/service-account-key.json",
        None,
        1,
        1,
        store.clone(),
    )
    .await?;

    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

## Documentation

| Section | What you'll find |
|---------|------------------|
| [Guides](guides/index.md) | Step-by-step instructions to get things done |
| [Explanations](explanation/index.md) | Deep dives into concepts and architecture |

## Contributing

Pull requests and issues welcome on [GitHub](https://github.com/supabase/etl).

**New destinations**: Open an issue first to gauge interest. Each built-in destination carries long-term maintenance cost, so we only accept those with significant community demand.
