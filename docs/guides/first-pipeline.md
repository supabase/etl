# Build Your First ETL Pipeline

**15 minutes**: Learn the fundamentals by building a working pipeline.

By the end of this tutorial, you'll have a complete ETL pipeline that streams data changes from Postgres to a tiny custom destination in real-time.

## What You'll Build

A real-time data pipeline that:

- Monitors a Postgres table for changes
- Streams INSERT, UPDATE, and DELETE operations
- Prints copied rows and streaming events through your own destination implementation

## Prerequisites

- Rust toolchain (1.75 or later)
- Postgres 14+ with logical replication enabled (`wal_level = logical` in `postgresql.conf`)
- Basic familiarity with Rust and SQL

New to Postgres logical replication? Read [Postgres Replication Concepts](../explanation/concepts.md) first.

## Step 1: Create the Project

```bash
cargo new etl-tutorial
cd etl-tutorial
```

Add dependencies to `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1", features = ["full"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

**Verify:** Run `cargo check` and confirm it compiles without errors.

## Step 2: Set Up Postgres

Connect to Postgres and create a test database:

```sql
CREATE DATABASE etl_tutorial;
\c etl_tutorial

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com');

CREATE PUBLICATION my_publication FOR TABLE users;
```

**Verify:** `SELECT * FROM pg_publication WHERE pubname = 'my_publication';` returns one row.

## Step 3: Write the Pipeline

Replace `src/main.rs`:

```rust
use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
    destination::{Destination, TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    error::EtlResult,
    pipeline::Pipeline,
    store::MemoryStore,
    types::{Event, ReplicatedTableSchema, TableRow},
};
use std::error::Error;

#[derive(Clone)]
struct LoggingDestination;

impl Destination for LoggingDestination {
    fn name() -> &'static str {
        "logging"
    }

    async fn truncate_table(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        println!("starting initial table copy");
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        println!("copied {} rows", rows.len());
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        println!("received {} streaming events", events.len());
        async_result.send(Ok(()));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "etl_tutorial".to_string(),
        username: "postgres".to_string(),
        password: Some("your_password".to_string().into()),
        tls: TlsConfig {
            enabled: false,
            trusted_root_certs: String::new(),
        },
        keepalive: TcpKeepaliveConfig::default(),
    };

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig {
            max_fill_ms: 5000,
            memory_budget_ratio: 0.2,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
    };

    let store = MemoryStore::new();
    let destination = LoggingDestination;

    println!("Starting pipeline...");
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

**Note:** Update the `password` field to match your Postgres credentials.

## Step 4: Run the Pipeline

```bash
RUST_LOG=info cargo run
```

You should see ETL startup logs plus messages from `LoggingDestination` as the initial rows are copied. After that, the pipeline continues running and prints the size of each streaming event batch.

## Step 5: Test Real-Time Replication

In another terminal, make changes to the database:

```sql
\c etl_tutorial

INSERT INTO users (name, email) VALUES ('Charlie Brown', 'charlie@example.com');
UPDATE users SET name = 'Alice Cooper' WHERE email = 'alice@example.com';
DELETE FROM users WHERE email = 'bob@example.com';
```

Your pipeline terminal should show new streaming batches being received in real-time.

## Cleanup

Stop the pipeline with `Ctrl+C`, then clean up the database:

```sql
-- Connect to a different database first (e.g., postgres)
\c postgres
DROP DATABASE etl_tutorial;
```

## What You Learned

- **Publications** define which tables to replicate via Postgres logical replication
- **Pipeline configuration** controls batching behavior and error retry policies
- **Custom destinations** are normal Rust types that implement the `Destination` trait
- The pipeline performs an initial table copy, then streams changes in real-time

## Next Steps

- [Custom Stores and Destinations](custom-implementations.md): Build your own components
- [Configure Postgres](configure-postgres.md): Production Postgres setup
- [Architecture](../explanation/architecture.md): How ETL works internally
