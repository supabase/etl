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
- **Extensible**: Implement four traits to add any destination
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
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::destination::memory::MemoryDestination;
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".into(),
        pg_connection: PgConnectionConfig {
            host: "localhost".into(),
            port: 5432,
            name: "mydb".into(),
            username: "postgres".into(),
            password: Some("password".into()),
            tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
            keepalive: None,
        },
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

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

We welcome pull requests and issues on [GitHub](https://github.com/supabase/etl).

We currently cannot accept new destinations unless there is significant community demand, as each carries long-term maintenance cost. If you need an unsupported destination, open an issue to gauge interest before implementing.
