---
type: tutorial
audience: developers
prerequisites:
  - Complete first pipeline tutorial
  - Advanced Rust knowledge (traits, async, Arc/Mutex)
  - Understanding of ETL architecture
version_last_tested: 0.1.0
last_reviewed: 2025-01-14
estimated_time: 25
---

# Build Custom Stores and Destinations

**Learn ETL's extension patterns by implementing simple custom components**

This tutorial teaches you ETL's design patterns by implementing minimal custom stores and destinations. You'll understand the separation between state and schema storage, and learn the patterns needed for production extensions.

## What You'll Build

Simple custom implementations to understand the patterns:

- **Custom in-memory store** with logging to see the flow
- **Custom HTTP destination** with basic retry logic

**Time required:** 25 minutes  
**Difficulty:** Advanced

## Understanding ETL's Store Design

ETL is design to be a modular replication library. This means that it relies on abstractions to allow for easy extension.

One core aspect of ETL is the store. The store is composed by two parts:

- **Schema Store**: Stores table schemas.
- **State Store**: Stores replication state.

Having the store as an extension point allows you to implement your own store for your own use case. For example, you might
want to store replication state in a simple text file, or you might just want the replication state to be stored in memory. It's
important to note that the implementation of the store will significantly affect the performance and safety of the pipeline. This means
that it's your responsibility to make sure that the store is designed to be performant, thread safe and durable (in case you need persistence). The pipeline
doesn't make any assumptions about the store, it just stores data and retrieves it.

One important thing about the stores, is that they both offer `load_*` and `get_*` methods. The rationale behind this design is
that `load_*` methods are used to load data into a cache implemented within the store and `get_*` exclusively read from that cache. If there is no
need to load data into the cache, because the store implementation doesn't write data anywhere, you can just implement `load_*` as a no-op.`

### `SchemaStore`: Store for Table Schemas

The `SchemaStore` trait is responsible for storing and retrieving table schemas.

Table schemas are required by ETL since they are used to correctly parse and handle incoming data from PostgreSQL and they
also serve to correctly map tables from Postgres to the destination.

### `StateStore`: Store for Replication State

The `StateStore` trait is responsible for storing and retrieving replication state.

The state is crucial for proper pipeline operation since it's used to track the progress of replication and (if persistent)
allows a pipeline to be safely paused and later resumed.

## Step 1: Create Simple Custom Store

Create `src/custom_store.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};

#[derive(Debug, Clone)]
struct CachedEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
struct PersistentEntry {
    schema: Option<TableSchema>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    // We simulate cached entries.
    cache: Arc<Mutex<HashMap<TableId, CachedEntry>>>,
    // We simulate persistent entries.
    persistent: Arc<Mutex<HashMap<TableId, PersistentEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("Creating custom store (2 maps: cache + persistent)");
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            persistent: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn ensure_cache_slot<'a>(
        cache: &'a mut HashMap<TableId, CachedEntry>,
        id: TableId,
    ) -> &'a mut CachedEntry {
        cache
            .entry(id)
            .or_insert_with(|| CachedEntry { schema: None, state: None, mapping: None })
    }

    fn ensure_persistent_slot<'a>(
        persistent: &'a mut HashMap<TableId, PersistentEntry>,
        id: TableId,
    ) -> &'a mut PersistentEntry {
        persistent
            .entry(id)
            .or_insert_with(|| PersistentEntry { schema: None, state: None, mapping: None })
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let cache = self.cache.lock().await;
        let result = cache.get(table_id).and_then(|e| e.schema.clone());
        info!("Schema cache read for table {}: {}", table_id.0, result.is_some());
        Ok(result)
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let cache = self.cache.lock().await;
        Ok(cache
            .values()
            .filter_map(|e| e.schema.clone())
            .collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        info!("Loading schemas from 'persistent' into cache (startup)");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(schema) = &pentry.schema {
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.schema = Some(Arc::new(schema.clone()));
                loaded += 1;
            }
        }
        info!("Loaded {} schemas into cache", loaded);
        Ok(loaded)
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let id = table_schema.id;
        info!("Storing schema for table {} (dual-write)", id.0);
        
        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, id);
            p.schema = Some(table_schema.clone());
        }
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, id);
            c.schema = Some(Arc::new(table_schema));
        }
        Ok(())
    }
}

impl StateStore for CustomStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let cache = self.cache.lock().await;
        let result = cache.get(&table_id).and_then(|e| e.state.clone());
        info!("State cache read for table {}: {:?}", table_id.0, result);
        Ok(result)
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let cache = self.cache.lock().await;
        Ok(cache
            .iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
            .collect())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        info!("Loading states from 'persistent' into cache");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(state) = pentry.state.clone() {
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.state = Some(state);
                loaded += 1;
            }
        }
        info!("Loaded {} states into cache", loaded);
        Ok(loaded)
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        info!("Updating state for table {} to {:?} (dual-write)", table_id.0, state);

        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, table_id);
            p.state = Some(state.clone());
        }
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, table_id);
            c.state = Some(state);
        }
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        _table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        todo!("Implement state history tracking for rollback")
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let cache = self.cache.lock().await;
        Ok(cache.get(source_table_id).and_then(|e| e.mapping.clone()))
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let cache = self.cache.lock().await;
        Ok(cache
            .iter()
            .filter_map(|(id, e)| e.mapping.clone().map(|m| (*id, m)))
            .collect())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        info!("Loading mappings from 'persistent' into cache");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(m) = &pentry.mapping {
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.mapping = Some(m.clone());
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        info!(
            "Storing mapping: {} -> {} (dual-write)",
            source_table_id.0, destination_table_id
        );

        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, source_table_id);
            p.mapping = Some(destination_table_id.clone());
        }
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, source_table_id);
            c.mapping = Some(destination_table_id);
        }
        Ok(())
    }
}
```

## Step 2: Create Simple HTTP Destination

Create `src/http_destination.rs`:

```rust
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::types::{Event, TableId, TableRow};
use etl::{bail, etl_error};

const MAX_RETRIES: usize = 3;
const BASE_BACKOFF_MS: u64 = 500;

pub struct HttpDestination {
    client: Client,
    base_url: String,
}

impl HttpDestination {
    pub fn new(base_url: String) -> EtlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to create HTTP client", e))?;
        Ok(Self { client, base_url })
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// Small, generic sender with retry + backoff.
    async fn send_json(&self, method: Method, path: &str, body: Option<&Value>) -> EtlResult<()> {
        let url = self.url(path);

        for attempt in 0..MAX_RETRIES {
            let mut req = self.client.request(method.clone(), &url);
            if let Some(b) = body {
                req = req.json(b);
            }

            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        "HTTP {} {} succeeded (attempt {})",
                        method,
                        url,
                        attempt + 1
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status();
                    warn!(
                        "HTTP {} {} failed with {}, attempt {}",
                        method,
                        url,
                        status,
                        attempt + 1
                    );
                    // Fail-fast on 4xx
                    if !status.is_server_error() {
                        bail!(
                            ErrorKind::Unknown,
                            "HTTP client error",
                            format!("Status: {}", status)
                        );
                    }
                }
                Err(e) => warn!(
                    "HTTP {} {} network error on attempt {}: {}",
                    method,
                    url,
                    attempt + 1,
                    e
                ),
            }

            // Exponential backoff: 500ms, 1s, 2s
            let delay = Duration::from_millis(BASE_BACKOFF_MS * 2u64.pow(attempt as u32));
            tokio::time::sleep(delay).await;
        }

        bail!(
            ErrorKind::Unknown,
            "HTTP request failed after retries",
            format!("Max retries ({MAX_RETRIES}) exceeded")
        )
    }
}

impl Destination for HttpDestination {
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!("HTTP: Truncating table {}", table_id.0);
        self.send_json(
            Method::DELETE,
            &format!("tables/{}/truncate", table_id.0),
            None,
        )
            .await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        info!(
            "HTTP: Writing {} rows for table {}",
            table_rows.len(),
            table_id.0
        );

        // Simple serialization — stringify values for demo-compat.
        let rows_json: Vec<_> = table_rows
            .iter()
            .map(|row| {
                json!({
                    "values": row.values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>()
                })
            })
            .collect();

        let payload = json!({
            "table_id": table_id.0,
            "rows": rows_json
        });

        self.send_json(
            Method::POST,
            &format!("tables/{}/rows", table_id.0),
            Some(&payload),
        )
            .await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("HTTP: Writing {} events", events.len());

        let events_json: Vec<_> = events
            .iter()
            .map(|event| {
                json!({
                    "event_type": format!("{:?}", event),
                })
            })
            .collect();

        let payload = json!({ "events": events_json });

        self.send_json(Method::POST, "events", Some(&payload)).await
    }
}
```

## Step 3: Use Your Custom Components

Create `src/main.rs`:

```rust
mod custom_store;
mod http_destination;

use custom_store::CustomStore;
use http_destination::HttpDestination;
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    
    info!("Starting ETL with custom store and destination");
    
    // Create custom components
    let store = CustomStore::new();
    let destination = HttpDestination::new("https://httpbin.org/post".to_string())?;
    
    // Standard PostgreSQL config
    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: Some("your_password".to_string().into()),
            tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
        },
        batch: BatchConfig { max_size: 100, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: 2,
    };
    
    // Create pipeline with custom components
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;
    
    Ok(())
}
```

Add dependencies to `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
tokio = { version = "1.0", features = ["full"] }
reqwest = { version = "0.11", features = ["json"] }
serde_json = "1.0"
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1.0"
```

## Key Patterns You've Learned

### Store Architecture
- **Cache-first reads**: Never hit persistent storage for reads
- **Dual-write updates**: Write to persistent then cache atomically
- **Startup loading**: Load persistent data into cache once
- **Thread safety**: Arc/Mutex for concurrent worker access

### Destination Patterns
- **Retry logic**: Exponential backoff for transient failures
- **Error classification**: Retry server errors, fail fast on client errors
- **Data transformation**: Convert ETL types to API-friendly formats
- **Batching awareness**: Handle empty batches gracefully

## What You've Learned

You now understand ETL's extension patterns:

- **Storage separation**: Schema vs state concerns with different access patterns
- **Cache-first architecture**: Fast reads from memory, dual writes for consistency
- **Thread-safe design**: Arc/Mutex patterns for concurrent access
- **Retry patterns**: Exponential backoff with error classification
- **Trait contracts**: What ETL expects from custom implementations

## Next Steps

- **Test your implementations** → [Testing ETL Pipelines](testing-pipelines/)
- **Debug issues** → [Debugging Guide](../how-to/debugging/)  
- **Understand architecture** → [ETL Architecture](../explanation/architecture/)
- **See production examples** → [Custom Destinations Guide](../how-to/custom-destinations/)

## See Also

- [State management explanation](../explanation/state-management/) - Deep dive on ETL's state handling
- [Architecture overview](../explanation/architecture/) - Understanding component relationships
- [API reference](../reference/) - Complete trait documentation