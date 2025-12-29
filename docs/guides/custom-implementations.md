# Build Custom Stores and Destinations

**30 minutes**: Implement your own stores and destinations.

**Prerequisites:** Completed [Your First Pipeline](first-pipeline.md) or familiar with ETL basics.

## Understanding the Destination Trait

ETL delivers data to destinations in two phases:

| Phase | Method | When | Data Type |
|-------|--------|------|-----------|
| Initial Copy | `write_table_rows()` | Startup | `Vec<TableRow>` |
| Streaming | `write_events()` | After copy | `Vec<Event>` |

**Important:** During initial copy, `Begin` and `Commit` events may be delivered multiple times because parallel workers consume separate replication slots. This doesn't cause data duplication - only transaction markers repeat.

## Step 1: Create the Project

```bash
cargo new etl-custom --lib
cd etl-custom
```

Update `Cargo.toml`:

```toml
[package]
name = "etl-custom"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "main"
path = "src/main.rs"

[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1.0", features = ["full"] }
reqwest = { version = "0.11", features = ["json"] }
serde_json = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
```

**Verify:** `cargo check` succeeds.

## Step 2: Implement a Custom Store

Create `src/custom_store.rs`. A store must implement three traits:

- `SchemaStore` - Table schema information
- `StateStore` - Replication progress tracking
- `CleanupStore` - Cleanup when tables are removed

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};

#[derive(Debug, Clone, Default)]
struct TableEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    tables: Arc<Mutex<HashMap<TableId, TableEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("Creating custom store");
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(table_id).and_then(|e| e.schema.clone()))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables.values().filter_map(|e| e.schema.clone()).collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        Ok(0) // In-memory store, nothing to load
    }

    async fn store_table_schema(&self, schema: TableSchema) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        let id = schema.id;
        tables.entry(id).or_default().schema = Some(Arc::new(schema));
        Ok(())
    }
}

impl StateStore for CustomStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(&table_id).and_then(|e| e.state.clone()))
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
            .collect())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        info!("Table {} -> {:?}", table_id.0, state);
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().state = Some(state);
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        _table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        todo!("Implement rollback if needed")
    }

    async fn get_table_mapping(&self, table_id: &TableId) -> EtlResult<Option<String>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(table_id).and_then(|e| e.mapping.clone()))
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .iter()
            .filter_map(|(id, e)| e.mapping.clone().map(|m| (*id, m)))
            .collect())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn store_table_mapping(
        &self,
        table_id: TableId,
        mapping: String,
    ) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().mapping = Some(mapping);
        Ok(())
    }
}

impl CleanupStore for CustomStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        tables.remove(&table_id);
        Ok(())
    }
}
```

**Verify:** `cargo check` succeeds.

## Step 3: Implement a Custom Destination

Create `src/http_destination.rs`. A destination receives data via three methods:

- `truncate_table()` - Clear table before bulk load
- `write_table_rows()` - Receive rows during initial copy
- `write_events()` - Receive streaming changes

```rust
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::types::{Event, TableId, TableRow};
use etl::{bail, etl_error};

#[derive(Debug, Clone)]
pub struct HttpDestination {
    client: Client,
    base_url: String,
}

impl HttpDestination {
    pub fn new(base_url: String) -> EtlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "HTTP client error", e))?;
        Ok(Self { client, base_url })
    }

    async fn post(&self, path: &str, body: serde_json::Value) -> EtlResult<()> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), path);

        for attempt in 1..=3 {
            match self.client.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) if resp.status().is_client_error() => {
                    bail!(ErrorKind::Unknown, "Client error", resp.status().to_string());
                }
                Ok(resp) => warn!("Attempt {}/3: status {}", attempt, resp.status()),
                Err(e) => warn!("Attempt {}/3: {}", attempt, e),
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
        bail!(ErrorKind::Unknown, "Request failed after retries", "");
    }
}

impl Destination for HttpDestination {
    fn name() -> &'static str {
        "http"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!("Truncating table {}", table_id.0);
        self.post(&format!("tables/{}/truncate", table_id.0), json!({})).await
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        info!("Writing {} rows to table {}", rows.len(), table_id.0);

        let payload = json!({
            "table_id": table_id.0,
            "rows": rows.iter().map(|r| {
                json!({ "values": r.values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>() })
            }).collect::<Vec<_>>()
        });

        self.post(&format!("tables/{}/rows", table_id.0), payload).await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        info!("Writing {} events", events.len());

        let payload = json!({
            "events": events.iter().map(|e| {
                match e {
                    Event::Insert(i) => json!({"type": "insert", "table": i.table_id.0}),
                    Event::Update(u) => json!({"type": "update", "table": u.table_id.0}),
                    Event::Delete(d) => json!({"type": "delete", "table": d.table_id.0}),
                    Event::Begin(_) => json!({"type": "begin"}),
                    Event::Commit(_) => json!({"type": "commit"}),
                    _ => json!({"type": "other"}),
                }
            }).collect::<Vec<_>>()
        });

        self.post("events", payload).await
    }
}
```

**Verify:** `cargo check` succeeds.

## Step 4: Wire It Together

Create `src/main.rs`:

```rust
mod custom_store;
mod http_destination;

use custom_store::CustomStore;
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use http_destination::HttpDestination;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    FmtSubscriber::builder().init();

    let store = CustomStore::new();
    let destination = HttpDestination::new("https://httpbin.org/post".to_string())?;

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: Some("postgres".to_string().into()),
            tls: TlsConfig {
                enabled: false,
                trusted_root_certs: String::new(),
            },
            keepalive: None,
        },
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 2,
    };

    info!("Starting pipeline");
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

**Note:** Update the Postgres credentials to match your setup.

## Step 5: Test

```bash
cargo run
```

The pipeline will connect to Postgres and start replicating. You'll see your custom store logging state transitions and your destination receiving HTTP calls.

## What You Built

- **Custom Store** - In-memory implementation of all three store traits
- **HTTP Destination** - Sends data via HTTP with retry logic
- **Working Pipeline** - Combines your components with ETL's core

## Next Steps

- [Extension Points](../explanation/traits.md): Full trait documentation
- [Event Types](../explanation/events.md): All events your destination receives
- [Configure Postgres](configure-postgres.md): Production database setup
- [Architecture](../explanation/architecture.md): How ETL works internally
