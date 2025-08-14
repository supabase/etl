---
hide:
  - navigation
title: ETL Documentation
---

# ETL Documentation

**Build real-time Postgres replication applications in Rust**

ETL is a Rust framework by [Supabase](https://supabase.com) that enables you to build high-performance, real-time data replication applications for PostgreSQL. Whether you're creating ETL pipelines, implementing CDC (Change Data Capture), or building custom data synchronization solutions, ETL provides the building blocks you need.

## Getting Started

Choose your path based on your needs:

### New to ETL?
Start with our **[Tutorials](tutorials/)** to learn ETL through hands-on examples:

- [Build your first ETL pipeline](tutorials/first-pipeline/) - Complete beginner's guide (15 minutes)
- [Set up memory-based testing](tutorials/memory-destination/) - Test your pipeline locally (10 minutes)
- [Testing ETL pipelines](tutorials/testing-pipelines/) - Ensure reliability (20 minutes)

### Ready to solve specific problems?
Jump to our **[How-To Guides](how-to/)** for practical solutions:

- [Configure PostgreSQL for replication](how-to/configure-postgres/)
- [Build custom destinations](how-to/custom-destinations/)
- [Debug pipeline issues](how-to/debugging/)
- [Handle schema changes](how-to/schema-changes/)
- [Optimize performance](how-to/performance/)

### Need detailed technical information?
Consult our **[Reference](reference/)** documentation:

- API reference
- Configuration options
- Error codes and messages

### Want to understand the bigger picture?
Read our **[Explanations](explanation/)** for deeper insights:

- [ETL architecture overview](explanation/architecture/)
- [Why Postgres logical replication?](explanation/replication/)
- [Performance characteristics](explanation/performance/)
- [Design decisions](explanation/design/)

## Core Concepts

**Postgres Logical Replication** streams data changes from PostgreSQL databases in real-time using the Write-Ahead Log (WAL). ETL builds on this foundation to provide:

- 🚀 **Real-time replication** - Stream changes as they happen
- 🔄 **Multiple destinations** - BigQuery and more coming soon
- 🛡️ **Fault tolerance** - Built-in error handling and recovery
- ⚡ **High performance** - Efficient batching and parallel processing
- 🔧 **Extensible** - Plugin architecture for custom destinations

## Quick Example

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    destination::memory::MemoryDestination,
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL connection
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "mydb".to_string(),
        username: "postgres".to_string(),
        password: Some("password".to_string().into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
    };

    // Create memory-based store and destination for testing
    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

    // Configure the pipeline
    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: 4,
    };

    // Create and start the pipeline
    let mut pipeline = Pipeline::new(1, config, store, destination);
    pipeline.start().await?;

    // Pipeline will run until stopped
    pipeline.wait().await?;

    Ok(())
}
```

## Next Steps

- **First time using ETL?** → Start with [Build your first pipeline](tutorials/first-pipeline/)
- **Have a specific goal?** → Browse [How-To Guides](how-to/)  
- **Need technical details?** → Check the [Reference](reference/)
- **Want to understand ETL deeply?** → Read [Explanations](explanation/)
