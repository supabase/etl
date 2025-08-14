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
- Understanding of ETL's architectural contracts

**Time required:** 25 minutes  
**Difficulty:** Advanced

## Understanding ETL's Storage Design

ETL separates storage into two focused traits:

### SchemaStore: Table Structure Information

```rust
pub trait SchemaStore {
    // Get cached schema (fast reads from memory)
    fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>>;
    
    // Load schemas once at startup into cache
    fn load_table_schemas(&self) -> EtlResult<usize>;
    
    // Store schema in both cache and persistent store
    fn store_table_schema(&self, schema: TableSchema) -> EtlResult<()>;
}
```

### StateStore: Replication Progress Tracking  

```rust
pub trait StateStore {
    // Track replication progress (Pending → Syncing → Streaming)
    fn get_table_replication_state(&self, table_id: TableId) -> EtlResult<Option<TableReplicationPhase>>;
    
    // Update progress in cache and persistent store
    fn update_table_replication_state(&self, table_id: TableId, state: TableReplicationPhase) -> EtlResult<()>;
    
    // Map source table IDs to destination names  
    fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>>;
}
```

**Key Design Principles:**

- **Cache-first**: All reads from memory for performance
- **Dual writes**: Updates go to both cache and persistent store
- **Load-once**: Load persistent data into cache at startup only
- **Thread-safe**: Arc/Mutex for concurrent worker access

## Step 1: Create Simple Custom Store

Create `src/custom_store.rs`:

```rust
use etl_postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;

/// Educational custom store showing ETL's patterns.
///
/// This demonstrates:
/// - Cache-first design (all reads from memory)
/// - Dual-write pattern (cache + "persistent" store)
/// - Thread safety with Arc/Mutex
/// - Separation of schema vs state concerns
#[derive(Debug, Clone)]
pub struct CustomStore {
    // In-memory caches (the source of truth for reads)
    schema_cache: Arc<Mutex<HashMap<TableId, Arc<TableSchema>>>>,
    state_cache: Arc<Mutex<HashMap<TableId, TableReplicationPhase>>>,
    mapping_cache: Arc<Mutex<HashMap<TableId, String>>>,
    
    // "Persistent" storage simulation (in reality, this would be Redis, SQLite, etc.)
    persistent_schemas: Arc<Mutex<HashMap<TableId, TableSchema>>>,
    persistent_states: Arc<Mutex<HashMap<TableId, TableReplicationPhase>>>,
    persistent_mappings: Arc<Mutex<HashMap<TableId, String>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("Creating custom store with cache-first architecture");
        Self {
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
            state_cache: Arc::new(Mutex::new(HashMap::new())),
            mapping_cache: Arc::new(Mutex::new(HashMap::new())),
            persistent_schemas: Arc::new(Mutex::new(HashMap::new())),
            persistent_states: Arc::new(Mutex::new(HashMap::new())),
            persistent_mappings: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        // Always read from cache (never from persistent store)
        let cache = self.schema_cache.lock().await;
        let result = cache.get(table_id).cloned();
        info!("Schema cache read for table {}: {}", table_id.0, result.is_some());
        Ok(result)
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let cache = self.schema_cache.lock().await;
        Ok(cache.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        info!("Loading schemas from 'persistent' store into cache (startup only)");
        
        // In production: read from database/file/Redis
        let persistent = self.persistent_schemas.lock().await;
        let mut cache = self.schema_cache.lock().await;
        
        for (table_id, schema) in persistent.iter() {
            cache.insert(*table_id, Arc::new(schema.clone()));
        }
        
        let loaded_count = persistent.len();
        info!("Loaded {} schemas into cache", loaded_count);
        Ok(loaded_count)
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let table_id = table_schema.id;
        info!("Storing schema for table {} (dual-write: cache + persistent)", table_id.0);
        
        // Write to persistent store first (in production: database transaction)
        {
            let mut persistent = self.persistent_schemas.lock().await;
            persistent.insert(table_id, table_schema.clone());
        }
        
        // Then update cache
        {
            let mut cache = self.schema_cache.lock().await;
            cache.insert(table_id, Arc::new(table_schema));
        }
        
        Ok(())
    }
}

impl StateStore for CustomStore {
    async fn get_table_replication_state(&self, table_id: TableId) -> EtlResult<Option<TableReplicationPhase>> {
        let cache = self.state_cache.lock().await;
        let result = cache.get(&table_id).copied();
        info!("State cache read for table {}: {:?}", table_id.0, result);
        Ok(result)
    }

    async fn get_table_replication_states(&self) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let cache = self.state_cache.lock().await;
        Ok(cache.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        info!("Loading states from 'persistent' store into cache");
        
        let persistent = self.persistent_states.lock().await;
        let mut cache = self.state_cache.lock().await;
        
        *cache = persistent.clone();
        let loaded_count = persistent.len();
        info!("Loaded {} states into cache", loaded_count);
        Ok(loaded_count)
    }

    async fn update_table_replication_state(&self, table_id: TableId, state: TableReplicationPhase) -> EtlResult<()> {
        info!("Updating state for table {} to {:?} (dual-write)", table_id.0, state);
        
        // Write to persistent store first
        {
            let mut persistent = self.persistent_states.lock().await;
            persistent.insert(table_id, state);
        }
        
        // Then update cache
        {
            let mut cache = self.state_cache.lock().await;
            cache.insert(table_id, state);
        }
        
        Ok(())
    }

    async fn rollback_table_replication_state(&self, _table_id: TableId) -> EtlResult<TableReplicationPhase> {
        // Simplified for tutorial - in production, you'd track state history
        todo!("Implement state history tracking for rollback")
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let cache = self.mapping_cache.lock().await;
        Ok(cache.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let cache = self.mapping_cache.lock().await;
        Ok(cache.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        info!("Loading mappings from 'persistent' store into cache");
        let persistent = self.persistent_mappings.lock().await;
        let mut cache = self.mapping_cache.lock().await;
        *cache = persistent.clone();
        Ok(persistent.len())
    }

    async fn store_table_mapping(&self, source_table_id: TableId, destination_table_id: String) -> EtlResult<()> {
        info!("Storing mapping: {} -> {} (dual-write)", source_table_id.0, destination_table_id);
        
        // Write to both stores
        {
            let mut persistent = self.persistent_mappings.lock().await;
            persistent.insert(source_table_id, destination_table_id.clone());
        }
        {
            let mut cache = self.mapping_cache.lock().await;
            cache.insert(source_table_id, destination_table_id);
        }
        
        Ok(())
    }
}
```

## Step 2: Create Simple HTTP Destination

Create `src/http_destination.rs`:

```rust
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::Destination;
use etl::error::{EtlError, EtlResult};
use etl::types::{Event, TableId, TableRow};

/// Simple HTTP destination showing core patterns.
///
/// Demonstrates:
/// - Implementing the Destination trait
/// - Basic retry logic with exponential backoff
/// - Error handling (retry vs fail-fast)  
/// - Data serialization for API compatibility
pub struct HttpDestination {
    client: Client,
    base_url: String,
    max_retries: usize,
}

impl HttpDestination {
    pub fn new(base_url: String) -> EtlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| EtlError::new("Failed to create HTTP client".into(), e.into()))?;
            
        Ok(Self {
            client,
            base_url,
            max_retries: 3,
        })
    }

    /// Simple retry with exponential backoff
    async fn retry_request<F, Fut>(&self, mut operation: F) -> EtlResult<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        for attempt in 0..self.max_retries {
            match operation().await {
                Ok(response) if response.status().is_success() => {
                    info!("HTTP request succeeded on attempt {}", attempt + 1);
                    return Ok(());
                }
                Ok(response) => {
                    let status = response.status();
                    warn!("HTTP request failed with status {}, attempt {}", status, attempt + 1);
                    
                    // Retry on server errors, fail fast on client errors
                    if !status.is_server_error() {
                        return Err(EtlError::new(
                            "HTTP client error".into(),
                            anyhow::anyhow!("Status: {}", status).into(),
                        ));
                    }
                }
                Err(e) => {
                    warn!("HTTP request network error on attempt {}: {}", attempt + 1, e);
                }
            }
            
            // Exponential backoff: 500ms, 1s, 2s
            let delay = Duration::from_millis(500 * 2_u64.pow(attempt as u32));
            tokio::time::sleep(delay).await;
        }
        
        Err(EtlError::new("HTTP request failed after retries".into(), anyhow::anyhow!("Max retries exceeded").into()))
    }
}

impl Destination for HttpDestination {
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!("HTTP: Truncating table {}", table_id.0);
        
        let url = format!("{}/tables/{}/truncate", self.base_url, table_id.0);
        let operation = || self.client.delete(&url).send();
        
        self.retry_request(operation).await?;
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, table_rows: Vec<TableRow>) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }
        
        info!("HTTP: Writing {} rows for table {}", table_rows.len(), table_id.0);
        
        // Simple serialization - in production you'd handle all data types properly
        let rows_json: Vec<_> = table_rows.iter().map(|row| {
            json!({
                "values": row.values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>()
            })
        }).collect();
        
        let payload = json!({
            "table_id": table_id.0,
            "rows": rows_json
        });
        
        let url = format!("{}/tables/{}/rows", self.base_url, table_id.0);
        let operation = || self.client.post(&url).json(&payload).send();
        
        self.retry_request(operation).await?;
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        
        info!("HTTP: Writing {} events", events.len());
        
        // Simple event serialization
        let events_json: Vec<_> = events.iter().map(|event| {
            json!({
                "event_type": format!("{:?}", event),
                "timestamp": chrono::Utc::now()
            })
        }).collect();
        
        let payload = json!({
            "events": events_json
        });
        
        let url = format!("{}/events", self.base_url);
        let operation = || self.client.post(&url).json(&payload).send();
        
        self.retry_request(operation).await?;
        Ok(())
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

### Production Extensions

For real production use, extend these patterns:

```rust
// Custom Store Extensions
impl CustomStore {
    // Add connection pooling for database stores
    async fn with_database_pool() -> Self { /* ... */ }
    
    // Add metrics collection
    async fn get_cache_metrics(&self) -> Metrics { /* ... */ }
    
    // Add state history for rollbacks
    async fn track_state_history(&self, table_id: TableId, state: TableReplicationPhase) { /* ... */ }
}

// Custom Destination Extensions  
impl HttpDestination {
    // Add circuit breaker pattern
    async fn should_break_circuit(&self) -> bool { /* ... */ }
    
    // Add authentication handling
    async fn refresh_auth_token(&mut self) -> EtlResult<()> { /* ... */ }
    
    // Add request batching
    async fn batch_multiple_requests(&self, requests: Vec<Request>) -> EtlResult<()> { /* ... */ }
}
```

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