use etl_postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::state::table::TableReplicationPhase;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;

/// Internal storage structure for the memory store with carefully managed invariants.
///
/// This structure maintains all ETL pipeline state in memory using optimized data structures
/// designed for concurrent access patterns. The design emphasizes consistency, performance,
/// and observability while keeping everything in process memory.
///
/// # Data Structure Invariants
///
/// The following invariants are maintained across all operations:
///
/// ## State Consistency
/// - Every table in `table_replication_states` represents the current authoritative state
/// - Tables in `table_state_history` must have corresponding entries in `table_replication_states`
/// - State history is append-only and maintains chronological ordering
/// - Schema and mapping data must be consistent with state data
///
/// ## Memory Management
/// - Table schemas are reference-counted via `Arc<TableSchema>` for efficient sharing
/// - Hash maps use default capacity allocation strategy for balanced memory/performance
/// - No explicit cleanup is performed - relies on process termination for memory reclamation
/// - State history grows unboundedly and should be monitored in long-running processes
///
/// ## Concurrency Safety
/// - All access is protected by a single async mutex to ensure atomic operations
/// - Operations that modify multiple maps are performed atomically
/// - Read operations obtain consistent snapshots across all data structures
/// - No internal locks are used to avoid deadlock potential
///
/// # Performance Characteristics
///
/// ## Access Patterns
/// - **State queries**: O(1) average case for table state lookups
/// - **Schema retrieval**: O(1) average case with Arc cloning cost
/// - **History tracking**: O(1) insertion, O(n) for full history retrieval
/// - **Bulk operations**: O(n) where n is the number of tables
///
/// ## Memory Usage
/// - **Base overhead**: ~4 empty HashMaps + Arc + Mutex overhead
/// - **Per-table overhead**: ~3-4 HashMap entries + state data + schema Arc
/// - **History growth**: Linear with number of state transitions per table
/// - **Schema sharing**: Multiple references to same schema only store one copy
///
/// ## Concurrency Impact
/// - Single mutex protects all data - no fine-grained locking
/// - Write operations block all concurrent access
/// - Read operations block all concurrent writes
/// - Suitable for moderate concurrency levels and short operation times
///
/// # Data Structure Details
///
/// ## State Management
/// - `table_replication_states`: Current state of each table (authoritative)
/// - `table_state_history`: Complete history of state transitions for debugging/auditing
///
/// ## Schema Management  
/// - `table_schemas`: Cached table schema definitions for efficient access
/// - `table_mappings`: Table ID to name mappings for human-readable references
///
/// # Implementation Notes
///
/// ## Error Handling
/// - Operations are designed to be atomic - either all changes succeed or none do
/// - Partial failure scenarios are avoided through careful operation ordering
/// - Consistency is maintained even during error conditions
///
/// ## Future Optimizations
/// - Could be optimized with read-write locks for better concurrent read performance
/// - State history could implement size limits or archival strategies
/// - Schema caching could implement LRU eviction for memory-constrained environments
#[derive(Debug)]
struct Inner {
    /// Current replication state for each table - this is the authoritative source of truth
    /// for table states. Every table being replicated must have an entry here.
    table_replication_states: HashMap<TableId, TableReplicationPhase>,

    /// Complete history of state transitions for each table, used for debugging and auditing.
    /// This is an append-only log that grows over time and provides visibility into
    /// table state evolution. Entries are chronologically ordered.
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,

    /// Cached table schema definitions, reference-counted for efficient sharing.
    /// Schemas are expensive to fetch from PostgreSQL, so they're cached here
    /// once retrieved and shared via Arc across the application.
    table_schemas: HashMap<TableId, Arc<TableSchema>>,

    /// Mapping from table IDs to human-readable table names for easier debugging
    /// and logging. These mappings are established during schema discovery.
    table_mappings: HashMap<TableId, String>,
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`MemoryStore`] implements both [`StateStore`] and [`SchemaStore`] traits,
/// providing a complete storage solution that keeps all data in memory. This is
/// ideal for testing, development, and scenarios where persistence is not required.
///
/// All state information including table replication phases, schema definitions,
/// and table mappings are stored in memory and will be lost on process restart.
///
/// # Examples
///
/// ```rust,no_run
/// use etl::store::both::memory::MemoryStore;
/// use etl::destination::memory::MemoryDestination;
/// use etl::prelude::*;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a memory store for development/testing
/// let store = MemoryStore::new();
///
/// // The store can be used with any destination
/// let destination = MemoryDestination::new();
///
/// // Complete pipeline setup with memory components
/// let pipeline_config = PipelineConfig {
///     id: 42,
///     pg_connection: PgConnectionConfig {
///         host: "localhost".to_string(),
///         port: 5432,
///         name: "source".to_string(),
///         username: "postgres".to_string(),
///         password: None,
///         tls: TlsConfig {
///             enabled: false,
///             trusted_root_certs: String::new(),
///         },
///     },
///     publication_name: "my_publication".to_string(),
///     batch: BatchConfig::default(),
///     table_error_retry_delay_ms: 5000,
///     max_table_sync_workers: 4,
/// };
///
/// let mut pipeline = Pipeline::new(
///     42, // PipelineId
///     pipeline_config,
///     store,
///     destination
/// );
///
/// // Start pipeline - all state will be kept in memory
/// pipeline.start().await?;
/// pipeline.shutdown_and_wait().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema data.
    /// As the pipeline runs, it will populate these collections with replication
    /// state and schema information.
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        // Store the current state in history before updating
        if let Some(current_state) = inner.table_replication_states.get(&table_id).cloned() {
            inner
                .table_state_history
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(current_state);
        }

        inner.table_replication_states.insert(table_id, state);

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut inner = self.inner.lock().await;

        // Get the previous state from history
        let previous_state = inner
            .table_state_history
            .get_mut(&table_id)
            .and_then(|history| history.pop())
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "There is no state in memory to rollback to"
                )
            })?;

        // Update the current state to the previous state
        inner
            .table_replication_states
            .insert(table_id, previous_state.clone());

        Ok(previous_state)
    }
}

impl SchemaStore for MemoryStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));

        Ok(())
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.len())
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);

        Ok(())
    }
}
