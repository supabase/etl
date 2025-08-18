
# Build Custom Stores and Destinations in 30 minutes

**Learn ETL's extension patterns by implementing working custom components**

## What You'll Build

By the end of this tutorial, you'll have:

- A **working custom in-memory store** that logs all operations for debugging
- A **custom HTTP destination** that sends data with automatic retries
- A **complete pipeline** using your custom components that processes real data

**Time required:** 30 minutes  
**Prerequisites:** Advanced Rust knowledge, running PostgreSQL, basic HTTP knowledge

## Step 1: Create Project Structure

Create a new Rust project for your custom ETL components:

```bash
cargo new etl-custom --lib
cd etl-custom
```

**Result:** You should see `Created library 'etl-custom' package` output.

## Step 2: Add Dependencies

Replace your `Cargo.toml` with the required dependencies:

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

**Result:** Running `cargo check` should download dependencies without errors.

## Step 3: Create Custom Store Implementation

Create `src/custom_store.rs` with a dual-storage implementation:

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

// Represents data stored in our in-memory cache for fast access
#[derive(Debug, Clone)]
struct CachedEntry {
    schema: Option<Arc<TableSchema>>,          // Table structure info
    state: Option<TableReplicationPhase>,      // Current replication progress
    mapping: Option<String>,                   // Source -> destination table mapping
}

// Represents data as it would be stored persistently (e.g., in files/database)
#[derive(Debug, Clone)]
struct PersistentEntry {
    schema: Option<TableSchema>,               // Not Arc-wrapped in "persistent" storage
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    // Fast in-memory cache for all read operations - this is what ETL queries
    cache: Arc<Mutex<HashMap<TableId, CachedEntry>>>,
    // Simulated persistent storage - in real implementation this might be files/database
    persistent: Arc<Mutex<HashMap<TableId, PersistentEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("Creating custom store with dual-layer architecture (cache + persistent)");
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            persistent: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Helper to ensure we have a cache entry to work with - creates if missing
    fn ensure_cache_slot<'a>(
        cache: &'a mut HashMap<TableId, CachedEntry>,
        id: TableId,
    ) -> &'a mut CachedEntry {
        cache.entry(id).or_insert_with(|| {
            // Initialize empty entry if this table hasn't been seen before
            CachedEntry { 
                schema: None, 
                state: None, 
                mapping: None 
            }
        })
    }

    // Helper to ensure we have a persistent entry to work with - creates if missing
    fn ensure_persistent_slot<'a>(
        persistent: &'a mut HashMap<TableId, PersistentEntry>,
        id: TableId,
    ) -> &'a mut PersistentEntry {
        persistent.entry(id).or_insert_with(|| {
            // Initialize empty persistent entry if this table hasn't been seen before
            PersistentEntry { 
                schema: None, 
                state: None, 
                mapping: None 
            }
        })
    }
}

// Implementation of ETL's SchemaStore trait - handles table structure information
impl SchemaStore for CustomStore {
    // ETL calls this frequently during data processing - must be fast (cache-only)
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let cache = self.cache.lock().await;
        let result = cache.get(table_id).and_then(|e| e.schema.clone());
        info!("Schema cache read for table {}: found={}", table_id.0, result.is_some());
        Ok(result)
    }

    // Return all cached schemas - used by ETL for bulk operations
    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let cache = self.cache.lock().await;
        let schemas: Vec<_> = cache.values()
            .filter_map(|e| e.schema.clone())  // Only include entries that have schemas
            .collect();
        info!("Retrieved {} schemas from cache", schemas.len());
        Ok(schemas)
    }

    // Called at startup - load persistent data into cache for fast access
    async fn load_table_schemas(&self) -> EtlResult<usize> {
        info!("Loading schemas from persistent storage into cache (startup phase)");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(schema) = &pentry.schema {
                // Move schema from persistent storage to cache, wrapping in Arc for sharing
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.schema = Some(Arc::new(schema.clone()));
                loaded += 1;
            }
        }
        info!("Loaded {} schemas into cache from persistent storage", loaded);
        Ok(loaded)
    }

    // Store new schema - implements dual-write pattern (persistent first, then cache)
    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let id = table_schema.id;
        info!("Storing schema for table {} using dual-write pattern", id.0);
        
        // First write to persistent storage (this would be a file/database in reality)
        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, id);
            p.schema = Some(table_schema.clone());
        }
        // Then update cache for immediate availability
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, id);
            c.schema = Some(Arc::new(table_schema));
        }
        Ok(())
    }
}

// Implementation of ETL's StateStore trait - handles replication progress tracking
impl StateStore for CustomStore {
    // Get current replication state for a table - cache-only for speed
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let cache = self.cache.lock().await;
        let result = cache.get(&table_id).and_then(|e| e.state.clone());
        info!("State cache read for table {}: {:?}", table_id.0, result);
        Ok(result)
    }

    // Get all replication states - used by ETL to understand overall progress
    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let cache = self.cache.lock().await;
        let states: HashMap<_, _> = cache.iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))  // Only include tables with state
            .collect();
        info!("Retrieved {} table states from cache", states.len());
        Ok(states)
    }

    // Load persistent states into cache at startup
    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        info!("Loading replication states from persistent storage into cache");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(state) = pentry.state.clone() {
                // Move state from persistent to cache
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.state = Some(state);
                loaded += 1;
            }
        }
        info!("Loaded {} replication states into cache", loaded);
        Ok(loaded)
    }

    // Update replication state - critical for tracking progress, uses dual-write
    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        info!("Updating replication state for table {} to {:?} (dual-write)", table_id.0, state);

        // First persist the state (ensures durability)
        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, table_id);
            p.state = Some(state.clone());
        }
        // Then update cache (ensures immediate availability)
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, table_id);
            c.state = Some(state);
        }
        Ok(())
    }

    // Rollback state to previous version - not implemented in this simple example
    async fn rollback_table_replication_state(
        &self,
        _table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        // In a real implementation, you'd maintain state history and rollback to previous version
        todo!("Implement state history tracking for rollback")
    }

    // Get table name mapping from source to destination
    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let cache = self.cache.lock().await;
        let mapping = cache.get(source_table_id).and_then(|e| e.mapping.clone());
        info!("Mapping lookup for table {}: {:?}", source_table_id.0, mapping);
        Ok(mapping)
    }

    // Get all table mappings - used when ETL needs to understand all table relationships
    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let cache = self.cache.lock().await;
        let mappings: HashMap<_, _> = cache.iter()
            .filter_map(|(id, e)| e.mapping.clone().map(|m| (*id, m)))  // Only include mapped tables
            .collect();
        info!("Retrieved {} table mappings from cache", mappings.len());
        Ok(mappings)
    }

    // Load persistent mappings into cache at startup
    async fn load_table_mappings(&self) -> EtlResult<usize> {
        info!("Loading table mappings from persistent storage into cache");
        let persistent = self.persistent.lock().await;
        let mut cache = self.cache.lock().await;

        let mut loaded = 0;
        for (id, pentry) in persistent.iter() {
            if let Some(m) = &pentry.mapping {
                // Load mapping into cache
                let centry = Self::ensure_cache_slot(&mut cache, *id);
                centry.mapping = Some(m.clone());
                loaded += 1;
            }
        }
        info!("Loaded {} table mappings into cache", loaded);
        Ok(loaded)
    }

    // Store a new table mapping (source table -> destination table name)
    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        info!(
            "Storing table mapping: {} -> {} (dual-write)",
            source_table_id.0, destination_table_id
        );

        // First persist the mapping
        {
            let mut persistent = self.persistent.lock().await;
            let p = Self::ensure_persistent_slot(&mut persistent, source_table_id);
            p.mapping = Some(destination_table_id.clone());
        }
        // Then update cache
        {
            let mut cache = self.cache.lock().await;
            let c = Self::ensure_cache_slot(&mut cache, source_table_id);
            c.mapping = Some(destination_table_id);
        }
        Ok(())
    }
}
```

**Result:** Your file should compile without errors when you run `cargo check`.

## Step 4: Create HTTP Destination Implementation

Create `src/http_destination.rs` with retry logic and proper error handling:

```rust
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::types::{Event, TableId, TableRow};
use etl::{bail, etl_error};

// Configuration constants for retry behavior
const MAX_RETRIES: usize = 3;      // Try up to 3 times before giving up
const BASE_BACKOFF_MS: u64 = 500;  // Start with 500ms delay, then exponential backoff

pub struct HttpDestination {
    client: Client,        // HTTP client for making requests
    base_url: String,      // Base URL for the destination API (e.g., "https://api.example.com")
}

impl HttpDestination {
    /// Create a new HTTP destination that will send data to the specified base URL
    pub fn new(base_url: String) -> EtlResult<Self> {
        // Configure HTTP client with reasonable timeout
        let client = Client::builder()
            .timeout(Duration::from_secs(10))  // 10 second timeout for each request
            .build()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to create HTTP client", e))?;
        
        info!("Created HTTP destination pointing to: {}", base_url);
        Ok(Self { client, base_url })
    }

    /// Helper to construct full URLs by combining base URL with endpoint paths
    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),    // Remove trailing slash from base
            path.trim_start_matches('/')           // Remove leading slash from path
        )
    }

    /// Generic HTTP sender with automatic retry logic and exponential backoff
    /// This handles all the complex retry logic so individual methods can focus on data formatting
    async fn send_json(&self, method: Method, path: &str, body: Option<&Value>) -> EtlResult<()> {
        let url = self.url(path);
        info!("Attempting HTTP {} to {}", method, url);

        // Retry loop with exponential backoff
        for attempt in 0..MAX_RETRIES {
            // Build the request
            let mut req = self.client.request(method.clone(), &url);
            if let Some(b) = body {
                req = req.json(b);  // Add JSON body if provided
            }

            // Send the request and handle response
            match req.send().await {
                // Success case - 2xx status codes
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        "HTTP {} {} succeeded on attempt {}/{}",
                        method, url, attempt + 1, MAX_RETRIES
                    );
                    return Ok(());
                }
                // HTTP error response (4xx/5xx)
                Ok(resp) => {
                    let status = resp.status();
                    warn!(
                        "HTTP {} {} returned status {}, attempt {}/{}",
                        method, url, status, attempt + 1, MAX_RETRIES
                    );
                    
                    // Don't retry client errors (4xx) - they won't succeed on retry
                    if status.is_client_error() {
                        bail!(
                            ErrorKind::Unknown,
                            "HTTP client error - not retrying",
                            format!("Status: {}", status)
                        );
                    }
                    // Server errors (5xx) will be retried
                }
                // Network/connection errors - these are worth retrying
                Err(e) => {
                    warn!(
                        "HTTP {} {} network error on attempt {}/{}: {}",
                        method, url, attempt + 1, MAX_RETRIES, e
                    );
                }
            }

            // If this wasn't the last attempt, wait before retrying
            if attempt + 1 < MAX_RETRIES {
                // Exponential backoff: 500ms, 1s, 2s (attempt 0, 1, 2)
                let delay = Duration::from_millis(BASE_BACKOFF_MS * 2u64.pow(attempt as u32));
                info!("Waiting {:?} before retry", delay);
                tokio::time::sleep(delay).await;
            }
        }

        // All retries failed
        bail!(
            ErrorKind::Unknown,
            "HTTP request failed after all retries",
            format!("Exhausted {} attempts to {}", MAX_RETRIES, url)
        )
    }
}

// Implementation of ETL's Destination trait - this is what ETL calls to send data
impl Destination for HttpDestination {
    /// Called when ETL needs to clear all data from a table (e.g., during full refresh)
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!("Truncating destination table: {}", table_id.0);
        
        // Send DELETE request to truncate endpoint
        self.send_json(
            Method::DELETE,
            &format!("tables/{}/truncate", table_id.0),  // e.g., "tables/users/truncate"
            None,  // No body needed for truncate
        ).await
    }

    /// Called when ETL has a batch of rows to send to the destination
    /// This is the main data flow method - gets called frequently during replication
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Skip empty batches - no work to do
        if table_rows.is_empty() {
            info!("Skipping empty batch for table {}", table_id.0);
            return Ok(());
        }

        info!(
            "Sending {} rows to destination table {}",
            table_rows.len(),
            table_id.0
        );

        // Convert ETL's internal row format to JSON that our API expects
        // In a real implementation, you'd format this according to your destination's schema
        let rows_json: Vec<_> = table_rows
            .iter()
            .map(|row| {
                json!({
                    "values": row.values.iter()
                        .map(|v| format!("{:?}", v))  // Simple string conversion for demo
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        // Create the JSON payload our API expects
        let payload = json!({
            "table_id": table_id.0,
            "rows": rows_json
        });

        // Send POST request with the row data
        self.send_json(
            Method::POST,
            &format!("tables/{}/rows", table_id.0),  // e.g., "tables/users/rows"
            Some(&payload),
        ).await
    }

    /// Called when ETL has replication events to send (e.g., transaction markers)
    /// These are metadata events about the replication process itself
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        // Skip if no events to process
        if events.is_empty() {
            return Ok(());
        }

        info!("Sending {} replication events to destination", events.len());

        // Convert events to JSON format
        let events_json: Vec<_> = events
            .iter()
            .map(|event| {
                json!({
                    "event_type": format!("{:?}", event),  // Convert event to string for demo
                })
            })
            .collect();

        let payload = json!({ "events": events_json });

        // Send events to generic events endpoint
        self.send_json(Method::POST, "events", Some(&payload)).await
    }
}
```

**Result:** Run `cargo check` again - it should compile successfully with both your store and destination implementations.

## Step 5: Create Working Pipeline Example

Create `src/main.rs` that demonstrates your custom components in action:

```rust
mod custom_store;
mod http_destination;

use custom_store::CustomStore;
use http_destination::HttpDestination;
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use tracing::{info, Level};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging so we can see what our custom components are doing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();
    
    info!("=== Starting ETL Pipeline with Custom Components ===");
    
    // Step 1: Create our custom store
    // This will handle both schema storage and replication state tracking
    info!("Creating custom dual-layer store (cache + persistent simulation)");
    let store = CustomStore::new();
    
    // Step 2: Create our custom HTTP destination
    // Using httpbin.org which echoes back what we send - perfect for testing
    info!("Creating HTTP destination with retry logic");
    let destination = HttpDestination::new(
        "https://httpbin.org/post".to_string()  // This endpoint will show us what we sent
    )?;
    
    // Step 3: Configure the PostgreSQL connection
    // Update these values to match your local PostgreSQL setup
    let pipeline_config = PipelineConfig {
        id: 1,  // Unique pipeline identifier
        publication_name: "etl_demo_pub".to_string(),  // PostgreSQL publication name
        
        // PostgreSQL connection details - CHANGE THESE to match your setup
        pg_connection: PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),          // Database name
            username: "postgres".to_string(),      // Database user
            password: Some("postgres".to_string().into()),  // Update with your password
            tls: TlsConfig { 
                enabled: false,  // Disable TLS for local development
                trusted_root_certs: String::new() 
            },
        },
        
        // Batching configuration - controls how ETL groups data for efficiency
        batch: BatchConfig { 
            max_size: 100,      // Send data when we have 100 rows
            max_fill_ms: 5000   // Or send data every 5 seconds, whichever comes first
        },
        
        // Error handling configuration
        table_error_retry_delay_ms: 10000,  // Wait 10s before retrying failed tables
        max_table_sync_workers: 2,          // Use 2 workers for parallel table syncing
    };
    
    // Step 4: Create the pipeline with our custom components
    // This combines your custom store and destination with ETL's core replication logic
    info!("Creating ETL pipeline with custom store and HTTP destination");
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    
    // Step 5: Start the pipeline
    // This will:
    // 1. Load any existing state from your custom store
    // 2. Connect to PostgreSQL and start listening for changes
    // 3. Begin replicating data through your custom destination
    info!("Starting pipeline - this will connect to PostgreSQL and begin replication");
    pipeline.start().await?;
    
    // For demo purposes, let it run for 30 seconds then gracefully shut down
    info!("Pipeline running! Watch the logs to see your custom components in action.");
    info!("Will run for 30 seconds then shut down gracefully...");
    
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    info!("Shutting down pipeline gracefully...");
    // pipeline.shutdown().await?;  // Uncomment if available in your ETL version
    
    // In production, you'd typically call:
    // pipeline.wait().await?;  // This blocks forever until manual shutdown
    
    info!("=== ETL Pipeline Demo Complete ===");
    Ok(())
}
```

**Result:** Running `cargo run` should now start your pipeline and show detailed logs from your custom components.

## Step 6: Test Your Implementation

Verify your custom components work correctly:

```bash
# Check that everything compiles
cargo check
```

**Result:** Should see "Finished dev [unoptimized + debuginfo] target(s)"

```bash
# Run the pipeline (will fail without PostgreSQL setup, but shows component initialization)
cargo run
```

**Result:** You should see logs from your custom store being created and HTTP destination being configured.

## Checkpoint: What You've Built

You now have working custom ETL components:

✅ **Custom Store**: Implements dual-layer caching with detailed logging  
✅ **HTTP Destination**: Sends data via HTTP with automatic retry logic  
✅ **Complete Pipeline**: Integrates both components with ETL's core engine  
✅ **Proper Error Handling**: Follows ETL's error patterns and logging

## Key Patterns You've Mastered

**Store Architecture:**

- Cache-first reads for performance
- Dual-write pattern for data consistency  
- Startup loading from persistent storage
- Thread-safe concurrent access with Arc/Mutex

**Destination Patterns:**

- Exponential backoff retry logic
- Smart error classification (retry 5xx, fail 4xx)
- Efficient batching and empty batch handling
- Clean data transformation from ETL to API formats

## Next Steps

- **Connect to real PostgreSQL** → [Configure PostgreSQL for Replication](../how-to/configure-postgres.md)
- **Understand the architecture** → [ETL Architecture](../explanation/architecture.md)
- **Contribute to ETL** → [Open an issue](https://github.com/supabase/etl/issues) with your custom implementations

## See Also

- [ETL Architecture](../explanation/architecture.md) - Understanding the system design
- [API Reference](../reference/index.md) - Complete trait documentation
- [Build your first pipeline](first-pipeline.md) - Start with the basics if you haven't yet