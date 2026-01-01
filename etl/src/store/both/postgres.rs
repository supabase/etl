use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_postgres::replication::{schema, state, table_mappings};
use etl_postgres::types::{TableId, TableSchema};
use metrics::gauge;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::ops::DerefMut;
use std::time::Duration;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::{ETL_TABLES_TOTAL, PHASE_LABEL, PIPELINE_ID_LABEL};
use crate::state::table::{RetryPolicy, TableReplicationPhase};
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::{bail, etl_error};

/// Maximum number of connections in the pool.
///
/// Set to 2 to allow some concurrency since database operations are performed
/// before acquiring the cache lock.
const MAX_POOL_CONNECTIONS: u32 = 2;

/// Duration after which idle connections are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Creates a lazily connected pool with automatic idle connection cleanup.
///
/// This function returns immediately without establishing any connections. Connections are created
/// on-demand when queries are executed and automatically closed after being idle for the specified
/// duration.
///
/// This is ideal for the store connection since we might want a connection to be open for a while
/// and then closed when it's unnecessary since after the first table copy phase, we don't update
/// the state so often.
fn create_database_pool(config: &PgConnectionConfig) -> PgPool {
    let options = config.with_db();

    PgPoolOptions::new()
        .min_connections(0)
        .max_connections(MAX_POOL_CONNECTIONS)
        .idle_timeout(Some(IDLE_TIMEOUT))
        .connect_lazy_with(options)
}

/// Emits table-related metrics which quantify the total number of tables in each phase.
fn emit_table_metrics(pipeline_id: PipelineId, counts_by_phase: &HashMap<&'static str, u64>) {
    for (phase, count) in counts_by_phase {
        gauge!(
            ETL_TABLES_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            PHASE_LABEL => *phase
        )
        .set(*count as f64);
    }
}

/// Converts ETL table replication phases to Postgres database state format.
///
/// This conversion transforms internal ETL replication states into the format
/// used by the Postgres state store for persistence. It handles all phase
/// types except in-memory phases that cannot be persisted.
impl TryFrom<TableReplicationPhase> for state::TableReplicationState {
    type Error = EtlError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        match value {
            TableReplicationPhase::Init => Ok(state::TableReplicationState::Init),
            TableReplicationPhase::DataSync => Ok(state::TableReplicationState::DataSync),
            TableReplicationPhase::FinishedCopy => Ok(state::TableReplicationState::FinishedCopy),
            TableReplicationPhase::SyncDone { lsn } => {
                Ok(state::TableReplicationState::SyncDone { lsn })
            }
            TableReplicationPhase::Ready => Ok(state::TableReplicationState::Ready),
            TableReplicationPhase::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                // Convert ETL RetryPolicy to postgres RetryPolicy
                let db_retry_policy = match retry_policy {
                    RetryPolicy::NoRetry => state::RetryPolicy::NoRetry,
                    RetryPolicy::ManualRetry => state::RetryPolicy::ManualRetry,
                    RetryPolicy::TimedRetry { next_retry } => {
                        state::RetryPolicy::TimedRetry { next_retry }
                    }
                };

                Ok(state::TableReplicationState::Errored {
                    reason,
                    solution,
                    retry_policy: db_retry_policy,
                })
            }
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                bail!(
                    ErrorKind::InvalidState,
                    "In-memory replication phase cannot be persisted",
                    "In-memory table replication phases (SyncWait, Catchup) cannot be saved to state store"
                );
            }
        }
    }
}

/// Inner state of [`PostgresStore`].
#[derive(Debug)]
struct Inner {
    /// Count of number of tables in each phase. Used for metrics.
    phase_counts: HashMap<&'static str, u64>,
    /// Cached table replication states indexed by table ID.
    table_states: BTreeMap<TableId, TableReplicationPhase>,
    /// Cached table schemas indexed by table ID.
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
    /// Cached table mappings from source table ID to destination table name.
    table_mappings: HashMap<TableId, String>,
}

impl Inner {
    /// Initializes phase counts from an existing table states map.
    fn init_phase_counts(&mut self, table_states: &BTreeMap<TableId, TableReplicationPhase>) {
        let mut phase_counts = HashMap::new();
        for phase in table_states.values() {
            *phase_counts
                .entry(phase.as_type().as_static_str())
                .or_insert(0u64) += 1;
        }
        self.phase_counts = phase_counts;
    }

    /// Inserts or updates a table state and adjusts phase counts accordingly.
    fn set_table_state(&mut self, table_id: TableId, state: TableReplicationPhase) {
        // Decrement old phase count if the state existed.
        if let Some(old_state) = self.table_states.get(&table_id) {
            let old_phase = old_state.as_type().as_static_str();
            if let Some(count) = self.phase_counts.get_mut(old_phase) {
                *count = count.saturating_sub(1);
            }
        }

        // Increment new phase count.
        let new_phase = state.as_type().as_static_str();
        let phase_count = self.phase_counts.entry(new_phase).or_default();
        *phase_count = phase_count.saturating_add(1);

        self.table_states.insert(table_id, state);
    }

    /// Removes a table state and adjusts phase counts accordingly.
    fn remove_table_state(&mut self, table_id: TableId) {
        if let Some(old_state) = self.table_states.remove(&table_id) {
            let old_phase = old_state.as_type().as_static_str();
            if let Some(count) = self.phase_counts.get_mut(old_phase) {
                *count = count.saturating_sub(1);
            }
        }
    }
}

/// Postgres-backed storage for ETL pipeline state and schema information.
///
/// [`PostgresStore`] implements both [`StateStore`] and [`SchemaStore`] traits,
/// providing persistent storage of replication state and schema information
/// directly in the source Postgres database. This ensures durability and
/// consistency of the pipeline state across restarts.
///
/// The store maintains both in-memory cache and persistent database storage,
/// using a connection pool with automatic idle timeout to balance performance
/// and resource usage.
///
/// # Concurrency Model
///
/// Write operations follow a DB-first pattern: the database is updated first,
/// then the in-memory cache. This ensures that if a crash occurs after the DB
/// write but before the cache update, the correct state is reloaded from DB on
/// restart (the database is the source of truth).
///
/// The application-level lock is only held for brief cache updates, minimizing
/// the critical section to increase performance. This design assumes no concurrent
/// updates to the same table: the `apply_worker` and `table_sync_worker` coordinate through state
/// transitions and never race on the same table.
///
/// If this invariant is violated, there could be some execution orders that could
/// result in an inconsistent state. For example, there could be two state updates u1 and u2 being
/// run in sequence. However, then u2 lands before u1 due to network latency, which causes the in-memory
/// code to be updated to the result of u2 and then later to u1, causing an inconsistent state.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pipeline_id: PipelineId,
    pool: PgPool,
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStore {
    /// Creates a new Postgres-backed store for the given pipeline.
    ///
    /// The store uses a lazily-connected pool with automatic idle timeout.
    /// Connections are established on first use and automatically closed
    /// after [`IDLE_TIMEOUT`] of inactivity.
    pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Self {
        let pool = create_database_pool(&source_config);
        let inner = Inner {
            phase_counts: HashMap::new(),
            table_states: BTreeMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
        };

        Self {
            pipeline_id,
            pool,
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl StateStore for PostgresStore {
    /// Retrieves the replication state for a specific table from cache.
    ///
    /// This method provides fast access to table replication states by reading
    /// from the in-memory cache. The cache is populated during startup and
    /// updated as states change during replication processing.
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_states.get(&table_id).cloned())
    }

    /// Retrieves all table replication states from cache.
    ///
    /// This method returns a complete snapshot of all cached table replication
    /// states. It's useful for pipeline initialization and state inspection
    /// operations that need visibility into all tables.
    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_states.clone())
    }

    /// Loads table replication states from Postgres into memory cache.
    ///
    /// This method connects to the source database, retrieves all table
    /// replication state rows for this pipeline, deserializes the state
    /// metadata, and populates the in-memory cache. It's typically called
    /// during pipeline startup to restore state from previous runs.
    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        debug!("loading table replication states from postgres state store");

        let replication_state_rows =
            state::get_table_replication_state_rows(&self.pool, self.pipeline_id as i64).await?;

        let mut table_states: BTreeMap<TableId, TableReplicationPhase> = BTreeMap::new();
        for row in replication_state_rows {
            let table_id = TableId::new(row.table_id.0);
            let phase: TableReplicationPhase = row.try_into()?;
            table_states.insert(table_id, phase);
        }

        let table_states_len = table_states.len();

        // For performance reasons, since we load the replication states only once during startup
        // and from a single thread, we can afford to have a short critical section.
        let mut inner = self.inner.lock().await;
        inner.init_phase_counts(&table_states);
        inner.table_states = table_states;
        emit_table_metrics(self.pipeline_id, &inner.phase_counts);

        info!(
            "loaded {} table replication states from postgres state store",
            table_states_len
        );

        Ok(table_states_len)
    }

    /// Updates a table's replication state in both database and cache.
    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let db_state: state::TableReplicationState = state.clone().try_into()?;

        state::update_replication_state(&self.pool, self.pipeline_id as i64, table_id, db_state)
            .await?;

        let mut inner = self.inner.lock().await;
        inner.set_table_state(table_id, state);
        emit_table_metrics(self.pipeline_id, &inner.phase_counts);

        Ok(())
    }

    /// Rolls back a table's replication state to the previous version.
    ///
    /// Returns the restored state, or an error if no previous state exists.
    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut conn = self.pool.acquire().await?;
        let restored_row =
            state::rollback_replication_state(conn.deref_mut(), self.pipeline_id as i64, table_id)
                .await?
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::StateRollbackError,
                        "Previous table state not found",
                        "No previous state available to roll back to for this table"
                    )
                })?;

        let restored_phase: TableReplicationPhase = restored_row.try_into()?;

        let mut inner = self.inner.lock().await;
        inner.set_table_state(table_id, restored_phase.clone());
        emit_table_metrics(self.pipeline_id, &inner.phase_counts);

        Ok(restored_phase)
    }

    /// Retrieves a table mapping from source table ID to destination name.
    ///
    /// This method looks up the destination table name for a given source table
    /// ID from the cache. Table mappings define how source tables are mapped
    /// to tables in the destination system.
    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    /// Retrieves all table mappings from cache.
    ///
    /// This method returns a complete snapshot of all cached table mappings,
    /// showing how source table IDs map to destination table names. Useful
    /// for operations that need visibility into the complete mapping configuration.
    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.clone())
    }

    /// Loads table mappings from Postgres into memory cache.
    ///
    /// This method connects to the source database, retrieves all table mapping
    /// definitions for this pipeline, and populates the in-memory cache.
    /// Called during pipeline initialization to establish source-to-destination
    /// table mappings.
    async fn load_table_mappings(&self) -> EtlResult<usize> {
        debug!("loading table mappings from postgres state store");

        let table_mappings =
            table_mappings::load_table_mappings(&self.pool, self.pipeline_id as i64)
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Table mappings loading failed",
                        format!("Failed to load table mappings from PostgreSQL: {}", err)
                    )
                })?;

        let table_mappings_len = table_mappings.len();

        let mut inner = self.inner.lock().await;
        inner.table_mappings = table_mappings;

        info!(
            "loaded {} table mappings from postgres state store",
            table_mappings_len
        );

        Ok(table_mappings_len)
    }

    /// Stores a table mapping in both database and cache.
    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        debug!(
            "storing table mapping: '{}' -> '{}'",
            source_table_id, destination_table_id
        );

        table_mappings::store_table_mapping(
            &self.pool,
            self.pipeline_id as i64,
            &source_table_id,
            &destination_table_id,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Table mapping storage failed",
                format!("Failed to store table mapping in PostgreSQL: {}", err)
            )
        })?;

        let mut inner = self.inner.lock().await;
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);

        Ok(())
    }
}

impl SchemaStore for PostgresStore {
    /// Retrieves a table schema from cache by table ID.
    ///
    /// This method provides fast access to cached table schemas, which are
    /// essential for processing replication events. Schemas are loaded during
    /// startup and cached for the lifetime of the pipeline.
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    /// Retrieves all cached table schemas as a vector.
    ///
    /// This method returns all currently cached table schemas, providing a
    /// complete view of the schema information available to the pipeline.
    /// Useful for operations that need to process or analyze all table schemas.
    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    /// Loads table schemas from Postgres into memory cache.
    ///
    /// This method connects to the source database, retrieves schema information
    /// for all tables in this pipeline, and populates the in-memory cache.
    /// Called during pipeline initialization to establish the schema context
    /// needed for processing replication events.
    async fn load_table_schemas(&self) -> EtlResult<usize> {
        debug!("loading table schemas from postgres state store");

        let table_schemas = schema::load_table_schemas(&self.pool, self.pipeline_id as i64)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Table schemas loading failed",
                    format!("Failed to load table schemas from PostgreSQL: {}", err)
                )
            })?;
        let table_schemas_len = table_schemas.len();

        let mut inner = self.inner.lock().await;
        inner.table_schemas.clear();
        for table_schema in table_schemas {
            inner
                .table_schemas
                .insert(table_schema.id, Arc::new(table_schema));
        }

        info!(
            "loaded {} table schemas from postgres state store",
            table_schemas_len
        );

        Ok(table_schemas_len)
    }

    /// Stores a table schema in both database and cache.
    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        debug!("storing table schema for table '{}'", table_schema.name);

        schema::store_table_schema(&self.pool, self.pipeline_id as i64, &table_schema)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Table schema storage failed",
                    format!("Failed to store table schema in PostgreSQL: {}", err)
                )
            })?;

        let mut inner = self.inner.lock().await;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));

        Ok(())
    }
}

impl CleanupStore for PostgresStore {
    /// Removes all state for a table from both database and cache.
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut tx = self.pool.begin().await?;

        table_mappings::delete_table_mappings_for_table(
            &mut *tx,
            self.pipeline_id as i64,
            &table_id,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Table mapping deletion failed",
                format!("Failed to delete table mapping in PostgreSQL: {}", err)
            )
        })?;

        schema::delete_table_schema_for_table(&mut *tx, self.pipeline_id as i64, table_id)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Table schema deletion failed",
                    format!("Failed to delete table schema in PostgreSQL: {}", err)
                )
            })?;

        state::delete_replication_state_for_table(&mut *tx, self.pipeline_id as i64, table_id)
            .await?;

        tx.commit().await?;

        let mut inner = self.inner.lock().await;
        inner.remove_table_state(table_id);
        inner.table_schemas.remove(&table_id);
        inner.table_mappings.remove(&table_id);
        emit_table_metrics(self.pipeline_id, &inner.phase_counts);

        Ok(())
    }
}
