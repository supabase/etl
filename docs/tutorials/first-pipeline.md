# Build Your First ETL Pipeline

**Learn the fundamentals by building a working pipeline in 15 minutes**

By the end of this tutorial, you'll have a complete ETL pipeline that streams data changes from Postgres to a memory destination in real-time. You'll see how to set up publications, configure pipelines, and handle live data replication.

## What You'll Build

A real-time data pipeline that:

- Monitors a Postgres table for changes
- Streams INSERT, UPDATE, and DELETE operations
- Stores replicated data in memory for immediate access

## Who This Tutorial Is For

- Rust developers new to ETL
- Anyone interested in Postgres logical replication
- Developers building data synchronization tools

**Time required:** 15 minutes  
**Difficulty:** Beginner

## Safety Note

This tutorial uses an isolated test database. To clean up, simply drop the test database when finished. No production data is affected.

## Step 1: Set Up Your Environment

Create a new Rust project for this tutorial:

```bash
cargo new etl-tutorial
cd etl-tutorial
```

Add ETL to your dependencies in `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1.0", features = ["full"] }
```

**Checkpoint:** Run `cargo check` - it should compile successfully.

## Step 2: Prepare Postgres

Connect to your Postgres server and create a test database:

```sql
CREATE DATABASE etl_tutorial;
\c etl_tutorial

-- Create a sample table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com');
```

Create a publication for replication:

```sql
CREATE PUBLICATION my_publication FOR TABLE users;
```

**Checkpoint:** Verify the publication exists:

```sql
SELECT * FROM pg_publication WHERE pubname = 'my_publication';
```

You should see one row returned.

## Step 3: Configure Your Pipeline

Replace the contents of `src/main.rs`:

```rust
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::destination::memory::MemoryDestination;
use etl::store::both::memory::MemoryStore;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Configure Postgres connection.
    let pg_connection_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "postgres".to_string(),
        username: "postgres".to_string(),
        password: Some("your_password".to_string().into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    // Configure pipeline behavior.
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

    // Create stores and destination.
    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

    // We spawn a task to periodically print the content of the destination.
    let destination_clone = destination.clone();
    tokio::spawn(async move {
        loop {
            println!("Destination Contents At This Time\n");

            // Table rows are the initial rows in the table that are copied.
            for (table_id, table_rows) in destination_clone.table_rows().await {
                println!("Table ({:?}): {:?}", table_id, table_rows);
            }

            // Events are realtime events that are sent by Postgres after the table has been copied.
            for event in destination_clone.events().await {
                println!("Event: {:?}", event);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            print!("\n\n");
        }
    });

    println!("Starting ETL pipeline...");

    // Create and start the pipeline.
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    pipeline.start().await?;

    println!("Waiting for pipeline to finish...");

    // Wait for the pipeline to finish, without a shutdown signal it will continue to work until the
    // connection is closed.
    pipeline.wait().await?;

    Ok(())
}
```

**Important:** Replace `"your_password"` with your Postgres password.

## Step 4: Start Your Pipeline

Run your pipeline:

```bash
cargo run
```

You should see output like:

```
Starting ETL pipeline...
Waiting for pipeline to finish...

Destination Contents At This Time

Destination Contents At This Time

Table (TableId(32341)): [TableRow { values: [I32(1), String("Alice"), String("alice@example.com"), TimeStampTz(2025-08-05T11:14:54.400235Z)] }, TableRow { values: [I32(2), String("Bob"), String("bob@example.com"), TimeStampTz(2025-08-05T11:14:54.400235Z)] }, TableRow { values: [I32(3), String("Charlie"), String("charlie@example.com"), TimeStampTz(2025-08-05T11:14:54.400235Z)] }]
Table (TableId(245615)): [TableRow { values: [I32(1), Array(I32([Some(1), Some(2), None, Some(4)]))] }, TableRow { values: [I32(2), Array(I32([None, None, Some(3)]))] }, TableRow { values: [I32(3), Array(I32([Some(5), None]))] }, TableRow { values: [I32(4), Array(I32([None]))] }, TableRow { values: [I32(5), Null] }]
```

**Checkpoint:** Your pipeline is now running and has completed initial synchronization.

## Step 5: Test Real-Time Replication

With your pipeline running, open a new terminal and connect to Postgres:

```bash
psql -d etl_tutorial
```

Make some changes to test replication:

```sql
-- Insert a new user
INSERT INTO users (name, email) VALUES ('Charlie Brown', 'charlie@example.com');

-- Update an existing user
UPDATE users SET name = 'Alice Cooper' WHERE email = 'alice@example.com';

-- Delete a user
DELETE FROM users WHERE email = 'bob@example.com';
```

**Checkpoint:** In your pipeline terminal, you should see log messages indicating these changes were captured and processed.

## Step 6: Verify Data Replication

The data is now replicated in your memory destination. While this tutorial uses memory (perfect for testing), the same pattern works with BigQuery, DuckDB, or custom destinations.

**Checkpoint:** You've successfully built and tested a complete ETL pipeline!

## What You've Learned

You've mastered the core ETL concepts:

- **Publications** define which tables to replicate
- **Pipeline configuration** controls behavior and performance
- **Memory destinations** provide fast, local testing
- **Real-time replication** captures all data changes automatically

## Cleanup

Remove the test database:

```sql
DROP DATABASE etl_tutorial;
```

## Next Steps

Now that you understand the basics:

- **Build custom implementations** → [Custom Stores and Destinations](custom-implementations.md)
- **Configure Postgres properly** → [Configure Postgres for Replication](../how-to/configure-postgres.md)
- **Understand the architecture** → [ETL Architecture](../explanation/architecture.md)

## See Also

- [Custom Implementation Tutorial](custom-implementations.md) - Advanced patterns
- [API Reference](../reference/index.md) - Complete configuration options
- [ETL Architecture](../explanation/architecture.md) - Understand the design
