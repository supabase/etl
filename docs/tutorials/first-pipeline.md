---
type: tutorial
audience: developers
prerequisites: 
  - Rust 1.75 or later
  - PostgreSQL server (local or remote)
  - Basic Rust and SQL knowledge
version_last_tested: 0.1.0
last_reviewed: 2025-01-14
estimated_time: 15
---

# Build Your First ETL Pipeline

**Learn the fundamentals by building a working pipeline in 15 minutes**

By the end of this tutorial, you'll have a complete ETL pipeline that streams data changes from PostgreSQL to a memory destination in real-time. You'll see how to set up publications, configure pipelines, and handle live data replication.

![Pipeline outcome diagram showing data flowing from PostgreSQL through ETL to memory destination]

## What You'll Build

A real-time data pipeline that:
- Monitors a PostgreSQL table for changes
- Streams INSERT, UPDATE, and DELETE operations  
- Stores replicated data in memory for immediate access

## Who This Tutorial Is For

- Rust developers new to ETL
- Anyone interested in PostgreSQL logical replication
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
etl-config = { git = "https://github.com/supabase/etl" }
tokio = { version = "1.0", features = ["full"] }
```

**Checkpoint:** Run `cargo check` - it should compile successfully.

## Step 2: Prepare PostgreSQL

Connect to your PostgreSQL server and create a test database:

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
    // Configure PostgreSQL connection
    let pg_connection_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "etl_tutorial".to_string(),
        username: "postgres".to_string(),
        password: Some("your_password".into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    // Configure pipeline behavior
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

    // Create stores and destination
    let store = MemoryStore::new();
    let destination = MemoryDestination::new();
    
    println!("Starting ETL pipeline...");
    
    // Create and start the pipeline
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    pipeline.start().await?;
    
    Ok(())
}
```

**Important:** Replace `"your_password"` with your PostgreSQL password.

## Step 4: Start Your Pipeline

Run your pipeline:

```bash
cargo run
```

You should see output like:
```
Starting ETL pipeline...
Pipeline started successfully
Syncing table: users
Initial sync completed: 2 rows
Listening for changes...
```

**Checkpoint:** Your pipeline is now running and has completed initial synchronization.

## Step 5: Test Real-Time Replication

With your pipeline running, open a new terminal and connect to PostgreSQL:

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

Stop your pipeline with `Ctrl+C`.

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

- **Add robust testing** → [Testing ETL Pipelines](testing-pipelines/)
- **Connect to BigQuery** → [How to Set Up BigQuery Destination](../how-to/custom-destinations/)  
- **Handle production scenarios** → [How to Debug Pipeline Issues](../how-to/debugging/)
- **Understand the architecture** → [ETL Architecture](../explanation/architecture/)

## See Also

- [Memory Destination Tutorial](memory-destination/) - Deep dive into testing with memory
- [API Reference](../reference/) - Complete configuration options
- [Performance Guide](../how-to/performance/) - Optimize your pipelines