---
title: ETL
description: Replicate Postgres changes anywhere in near real time.
---

**Replicate Postgres changes anywhere in near real time**

**ETL** is the open-source Rust framework for building **change data capture
(CDC)** pipelines on **Postgres**. These developer docs cover the framework,
destination modules, standalone replicator, and custom implementations. Replicate
inserts, updates, deletes, truncates, and schema events to a built-in module or
your own destination. **BigQuery** is the most mature destination module in this
library; DuckLake, ClickHouse, and Snowflake modules are also available in
`etl-destinations`, and the Iceberg module is deprecated for new deployments.

Looking for the managed product in the Supabase Dashboard? Use the canonical
[Supabase Pipelines documentation](https://supabase.com/docs/guides/database/replication/pipelines)
for product setup, availability, pricing, and operational guidance. Product
guidance lives there so these library docs can remain focused on developers who
run or embed ETL themselves.

The managed product calls the first replication phase **initial sync**. Lower-level
engine documentation and APIs may call the same phase **initial copy** or **table
copy** to match source-code names.

## Start Here

<div class="doc-path-grid">
  <a class="doc-path-card" href="/etl/guides/first-pipeline/">
    <span class="doc-path-kicker">Build</span>
    <strong>Your First Pipeline</strong>
    <span>Copy existing rows, stream changes, and implement a tiny destination.</span>
  </a>
  <a class="doc-path-card" href="/etl/guides/configure-postgres/">
    <span class="doc-path-kicker">Prepare</span>
    <strong>Configure Postgres</strong>
    <span>Set WAL, publications, slots, and retention for reliable replication.</span>
  </a>
  <a class="doc-path-card" href="/etl/guides/custom-implementations/">
    <span class="doc-path-kicker">Extend</span>
    <strong>Custom Stores and Destinations</strong>
    <span>Bring your own persistence and write replicated events anywhere.</span>
  </a>
  <a class="doc-path-card" href="/etl/explanation/concepts/">
    <span class="doc-path-kicker">Learn</span>
    <strong>Replication Concepts</strong>
    <span>Understand WAL, publications, slots, pgoutput, and replica identity.</span>
  </a>
  <a class="doc-path-card" href="/etl/explanation/schema-changes/">
    <span class="doc-path-kicker">Operate</span>
    <strong>Schema Changes</strong>
    <span>See how relation events, DDL messages, and destination diffs work.</span>
  </a>
  <a class="doc-path-card" href="https://supabase.com/docs/guides/database/replication/pipelines">
    <span class="doc-path-kicker">Supabase</span>
    <strong>Supabase Pipelines</strong>
    <span>Configure and operate the managed product in the Supabase Dashboard.</span>
  </a>
</div>

## Why ETL?

- **Near-real-time**: Continuously replicate changes with configurable batching
- **Reliable**: At-least-once delivery with automatic retries
- **Extensible**: Implement one trait to add any destination
- **Fast**: Parallel initial sync and configurable batching
- **Type-safe**: Rust API with compile-time guarantees

## How It Works

1. **Initial sync**: ETL copies existing table data to your destination
2. **Ongoing replication**: ETL batches and writes [events](/etl/explanation/events/) (Insert, Update, Delete, Relation, and more) as they arrive
3. **Recovery**: The [store](/etl/explanation/traits/) persists state so pipelines resume after restarts

See [Architecture](/etl/explanation/architecture/) for details, and
[Schema Changes](/etl/explanation/schema-changes/) for DDL semantics and
limitations.

## Quick Example

**Install ETL** in your project:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1", features = ["full"] }
```

**Create a pipeline** with a source config, store, and destination:

```rust
use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
    data::TableRow,
    destination::{
        Destination, DestinationWriteStatus, DropTableForCopyResult, WriteEventsDurability,
        WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    pipeline::Pipeline,
    schema::ReplicatedTableSchema,
    store::MemoryStore,
};

#[derive(Clone)]
struct NoopDestination;

impl Destination for NoopDestination {
    fn name() -> &'static str {
        "noop"
    }

    async fn drop_table_for_copy(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        _table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        async_result.send(Ok(DestinationWriteStatus::Durable));
        Ok(())
    }

    async fn write_events(
        &self,
        _events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        async_result.send(Ok(DestinationWriteStatus::Durable));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        hostaddr: None,
        port: 5432,
        name: "mydb".to_string(),
        username: "postgres".to_string(),
        password: Some("password".to_string().into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
        keepalive: TcpKeepaliveConfig::default(),
    };

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        store_pg_connection: None,
        batch: BatchConfig {
            max_fill_ms: 5000,
            memory_budget_ratio: 0.2,
            max_bytes: 8 * 1024 * 1024,
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
        memory_refresh_interval_ms: 100,
        replication_lag_refresh_interval_ms: 10_000,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
    };

    let store = MemoryStore::new();
    let destination = NoopDestination;

    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

This snippet intentionally shows **ETL used as a library** with your own `Destination` implementation.
Feature-gated destination modules live in `etl-destinations`: BigQuery,
DuckLake, ClickHouse, Snowflake, and the deprecated Iceberg module. The shared
pipeline, config, store, and event types should come from `etl`.

`Pipeline::start()` installs ETL's **source-side schema helpers** before
replication begins. If you use `PostgresStore` as the runtime store,
`PostgresStore::new()` separately prepares the **Postgres-backed state tables**.

## Documentation

| Section | What you'll find |
|---------|------------------|
| [Your First Pipeline](/etl/guides/first-pipeline/) | Step-by-step instructions to get things done |
| [Postgres Replication Concepts](/etl/explanation/concepts/) | Deep dives into concepts and architecture |
| [Examples](https://github.com/supabase/etl/tree/main/crates/etl-examples) | Runnable `bigquery` and `ducklake` example binaries |

## Contributing

Pull requests and issues welcome on [GitHub](https://github.com/supabase/etl).

**New destinations**: Open an issue first to gauge interest. Each built-in destination carries long-term maintenance cost, so we only accept those with significant community demand.
