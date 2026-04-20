# Build Custom Stores and Destinations

**30 minutes**: Implement your own stores and destinations.

**Prerequisites:** Completed [Your First Pipeline](first-pipeline.md) or familiar with ETL basics.

## Understanding the Destination Trait

ETL delivers data to destinations in two phases:

| Phase | Method | When | Data Type |
|-------|--------|------|-----------|
| Initial Copy | `write_table_rows()` | Startup | `Vec<TableRow>` |
| Streaming | `write_events()` | After copy | `Vec<Event>` |

**Note:** During initial copy, parallel table sync workers each process their own replication slot, so `Begin` and `Commit` transaction markers may appear multiple times. This does not duplicate actual row data.

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

Create `src/custom_store.rs`. A store must implement three traits (see [Extension Points](../explanation/traits.md) for full details):

- `SchemaStore` - Table schema storage and retrieval
- `StateStore` - Replication progress and destination metadata tracking
- `CleanupStore` - Metadata cleanup when tables leave the publication

```rust
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use etl::error::EtlResult;
use etl::state::{
    AppliedDestinationTableMetadata, DestinationTableMetadata, TableReplicationPhase,
};
use etl::store::{CleanupStore, SchemaStore, StateStore, TableReplicationStates};
use etl::types::{SnapshotId, TableId, TableSchema};

#[derive(Debug, Clone, Default)]
struct TableEntry {
    schemas: HashMap<SnapshotId, Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    destination_metadata: Option<DestinationTableMetadata>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    tables: Arc<Mutex<HashMap<TableId, TableEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("creating custom store");
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(table_id).and_then(|entry| {
            entry
                .schemas
                .iter()
                .filter(|(sid, _)| **sid <= snapshot_id)
                .max_by_key(|(sid, _)| *sid)
                .map(|(_, schema)| Arc::clone(schema))
        }))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .values()
            .flat_map(|entry| entry.schemas.values().cloned())
            .collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn store_table_schema(&self, schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut tables = self.tables.lock().await;
        let id = schema.id;
        let snapshot_id = schema.snapshot_id;
        let schema = Arc::new(schema);
        tables
            .entry(id)
            .or_default()
            .schemas
            .insert(snapshot_id, Arc::clone(&schema));
        Ok(schema)
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
    ) -> EtlResult<TableReplicationStates> {
        let tables = self.tables.lock().await;
        Ok(Arc::new(
            tables
                .iter()
                .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
                .collect::<BTreeMap<_, _>>(),
        ))
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        for (table_id, state) in updates {
            info!("table {} -> {:?}", table_id.0, state);
            tables.entry(table_id).or_default().state = Some(state);
        }
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        _table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        todo!("Implement rollback if needed")
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .get(&table_id)
            .and_then(|e| e.destination_metadata.clone()))
    }

    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<AppliedDestinationTableMetadata>> {
        self.get_destination_table_metadata(table_id)
            .await?
            .map(|metadata| metadata.into_applied())
            .transpose()
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().destination_metadata = Some(metadata);
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

Create `src/http_destination.rs`. A destination implements the `Destination` trait with four required methods:

- `name()` - Return an identifier for logging
- `truncate_table()` - Clear table before bulk load using the current replicated table schema
- `write_table_rows()` - Receive rows during initial copy together with the current replicated table schema
- `write_events()` - Receive streaming changes (batches may span multiple tables)

There's also an optional `shutdown()` method with a default no-op implementation. Override it if your destination needs cleanup when the pipeline shuts down.

```rust
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::{
    Destination, TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::{ErrorKind, EtlResult};
use etl::types::{Event, ReplicatedTableSchema, TableRow};
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
            .map_err(|e| etl_error!(ErrorKind::Unknown, "HTTP client error", source: e))?;
        Ok(Self { client, base_url })
    }

    async fn post(&self, path: &str, body: serde_json::Value) -> EtlResult<()> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), path);

        for attempt in 1..=3 {
            match self.client.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) if resp.status().is_client_error() => {
                    bail!(ErrorKind::Unknown, "Client error", resp.status());
                }
                Ok(resp) => warn!("attempt {}/3: status {}", attempt, resp.status()),
                Err(e) => warn!("attempt {}/3: {}", attempt, e),
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
        bail!(ErrorKind::Unknown, "Request failed after retries");
    }
}

impl Destination for HttpDestination {
    fn name() -> &'static str {
        "http"
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let table_name = replicated_table_schema.get_inner().name.to_string();
        info!("truncating table {}", table_name);
        let result = self
            .post(&format!("tables/{table_name}/truncate"), json!({}))
            .await;
        async_result.send(result);
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        if rows.is_empty() {
            async_result.send(Ok(()));
            return Ok(());
        }
        let table_name = replicated_table_schema.get_inner().name.to_string();
        info!("writing {} rows to table {}", rows.len(), table_name);

        let payload = json!({
            "table_name": table_name,
            "rows": rows.iter().map(|r| {
                json!({ "values": r.values().iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>() })
            }).collect::<Vec<_>>()
        });

        let result = self.post("rows", payload).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        if events.is_empty() {
            async_result.send(Ok(()));
            return Ok(());
        }
        info!("writing {} events", events.len());

        let payload = json!({
            "events": events.iter().map(|e| {
                match e {
                    Event::Insert(i) => json!({"type": "insert", "table": i.replicated_table_schema.get_inner().name.to_string()}),
                    Event::Update(u) => json!({"type": "update", "table": u.replicated_table_schema.get_inner().name.to_string()}),
                    Event::Delete(d) => json!({"type": "delete", "table": d.replicated_table_schema.get_inner().name.to_string()}),
                    Event::Begin(_) => json!({"type": "begin"}),
                    Event::Commit(_) => json!({"type": "commit"}),
                    Event::Relation(r) => json!({"type": "relation", "table": r.replicated_table_schema.get_inner().name.to_string()}),
                    Event::Truncate(t) => json!({"type": "truncate", "tables": t.truncated_tables.iter().map(|table| table.get_inner().name.to_string()).collect::<Vec<_>>() }),
                    Event::Unsupported => json!({"type": "unsupported"}),
                }
            }).collect::<Vec<_>>()
        });

        let result = self.post("events", payload).await;
        async_result.send(result);
        Ok(())
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
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use http_destination::HttpDestination;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let store = CustomStore::new();
    let destination = HttpDestination::new("https://your-endpoint.example.com".to_string())?;

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "your_database".to_string(),
            username: "postgres".to_string(),
            password: Some("your_password".to_string().into()),
            tls: TlsConfig {
                enabled: false,
                trusted_root_certs: String::new(),
            },
            keepalive: TcpKeepaliveConfig::default(),
        },
        batch: BatchConfig {
            max_fill_ms: 5000,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
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

    println!("Starting pipeline...");
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

**Note:** Update the database name, password, and HTTP endpoint to match your setup.

## Step 5: Test

```bash
cargo run
```

The pipeline will connect to Postgres and start replicating. You'll see your custom store logging state transitions and your destination receiving HTTP calls.

## What You Built

- **Custom Store** - In-memory implementation of `SchemaStore`, `StateStore`, and `CleanupStore`
- **HTTP Destination** - Forwards replicated data via HTTP POST with retry logic
- **Working Pipeline** - Connects your custom components to the ETL core

## Next Steps

- [Extension Points](../explanation/traits.md) - Full trait API documentation
- [Event Types](../explanation/events.md) - Details on all events your destination receives
- [Configure Postgres](configure-postgres.md) - Production database setup
- [Architecture](../explanation/architecture.md) - How ETL works internally
