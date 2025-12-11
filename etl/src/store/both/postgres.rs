use std::{collections::HashMap, sync::Arc};

use etl_config::shared::PgConnectionConfig;
use etl_postgres::replication::{connect_to_source_database, schema, state, table_mappings};
use etl_postgres::types::{SnapshotId, TableId, TableSchema};
use metrics::gauge;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::{ETL_TABLES_TOTAL, PHASE_LABEL, PIPELINE_ID_LABEL};
use crate::state::table::{RetryPolicy, TableReplicationPhase};
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::{DestinationSchemaState, DestinationSchemaStateType, StateStore};
use crate::types::PipelineId;
use crate::{bail, etl_error};

const NUM_POOL_CONNECTIONS: u32 = 1;

/// Maximum number of schema snapshots to keep cached per table.
///
/// This limits memory usage by evicting older snapshots when new ones are added.
/// In practice, during a single batch of events, it's highly unlikely to need
/// more than 2 schema versions for any given table.
const MAX_CACHED_SCHEMAS_PER_TABLE: usize = 2;

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

/// Converts Postgres state rows back to ETL table replication phases.
///
/// This conversion transforms persisted database state into internal ETL
/// replication phase representations. It deserializes metadata from the
/// database row and maps database state enums to ETL phase enums.
impl TryFrom<state::TableReplicationStateRow> for TableReplicationPhase {
    type Error = EtlError;

    fn try_from(value: state::TableReplicationStateRow) -> Result<Self, Self::Error> {
        // Parse the metadata field from the row, which contains all the data we need to build the
        // replication phase
        let Some(table_replication_state) = value.deserialize_metadata().map_err(|err| {
            etl_error!(
                ErrorKind::DeserializationError,
                "Table replication state deserialization failed",
                format!(
                    "Failed to deserialize table replication state from metadata column in PostgreSQL: {}", err
                )
            )
        })?
        else {
            bail!(
                ErrorKind::InvalidState,
                "Table replication state not found",
                "Table replication state does not exist in metadata column in PostgreSQL"
            );
        };

        // Convert postgres state to phase (they are the same structs but one is meant to represent
        // only the state which can be saved in the db).
        match table_replication_state {
            state::TableReplicationState::Init => Ok(TableReplicationPhase::Init),
            state::TableReplicationState::DataSync => Ok(TableReplicationPhase::DataSync),
            state::TableReplicationState::FinishedCopy => Ok(TableReplicationPhase::FinishedCopy),
            state::TableReplicationState::SyncDone { lsn } => {
                Ok(TableReplicationPhase::SyncDone { lsn })
            }
            state::TableReplicationState::Ready => Ok(TableReplicationPhase::Ready),
            state::TableReplicationState::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                let etl_retry_policy = match retry_policy {
                    state::RetryPolicy::NoRetry => RetryPolicy::NoRetry,
                    state::RetryPolicy::ManualRetry => RetryPolicy::ManualRetry,
                    state::RetryPolicy::TimedRetry { next_retry } => {
                        RetryPolicy::TimedRetry { next_retry }
                    }
                };

                Ok(TableReplicationPhase::Errored {
                    reason,
                    solution,
                    retry_policy: etl_retry_policy,
                })
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
    table_states: HashMap<TableId, TableReplicationPhase>,
    /// Cached table schemas indexed by (table_id, snapshot_id) for versioning support.
    ///
    /// This cache is optimized for keeping the most actively used schemas in memory,
    /// not all historical snapshots. Schemas are loaded on-demand from the database
    /// when not found in cache. During normal operation, this typically contains
    /// only the latest schema version for each table, since that's what the
    /// replication pipeline actively uses.
    table_schemas: HashMap<(TableId, SnapshotId), Arc<TableSchema>>,
    /// Cached table mappings from source table ID to destination table name.
    table_mappings: HashMap<TableId, String>,
    /// Cached destination schema states indexed by table ID.
    destination_schema_states: HashMap<TableId, DestinationSchemaState>,
}

impl Inner {
    fn decrement_phase_count(&mut self, phase: &'static str) {
        let count = self.phase_counts.entry(phase).or_default();
        *count -= 1;
    }

    fn increment_phase_count(&mut self, phase: &'static str) {
        let count = self.phase_counts.entry(phase).or_default();
        *count += 1;
    }

    /// Inserts a schema into the cache and evicts older snapshots if necessary.
    ///
    /// Maintains at most [`MAX_CACHED_SCHEMAS_PER_TABLE`] snapshots per table,
    /// evicting the oldest snapshots when the limit is exceeded.
    fn insert_schema_with_eviction(&mut self, table_schema: Arc<TableSchema>) {
        let table_id = table_schema.id;
        let snapshot_id = table_schema.snapshot_id;

        // Insert the new schema
        self.table_schemas
            .insert((table_id, snapshot_id), table_schema);

        // Collect all snapshot_ids for this table
        let mut snapshots_for_table: Vec<SnapshotId> = self
            .table_schemas
            .keys()
            .filter(|(tid, _)| *tid == table_id)
            .map(|(_, sid)| *sid)
            .collect();

        // If we exceed the limit, evict oldest snapshots
        if snapshots_for_table.len() > MAX_CACHED_SCHEMAS_PER_TABLE {
            // Sort ascending so oldest are first
            snapshots_for_table.sort();

            // Remove oldest entries until we're at the limit
            let to_remove = snapshots_for_table.len() - MAX_CACHED_SCHEMAS_PER_TABLE;
            for &old_snapshot_id in snapshots_for_table.iter().take(to_remove) {
                self.table_schemas.remove(&(table_id, old_snapshot_id));
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
/// connecting to the source database as needed for state updates while
/// providing fast cached access for read operations.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pipeline_id: PipelineId,
    source_config: PgConnectionConfig,
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStore {
    /// Creates a new Postgres-backed store for the given pipeline.
    ///
    /// The store will use the provided connection configuration to access
    /// the source Postgres database for persistent storage operations.
    /// The pipeline ID ensures isolation between different pipeline instances.
    pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Self {
        let inner = Inner {
            phase_counts: HashMap::new(),
            table_states: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
            destination_schema_states: HashMap::new(),
        };

        Self {
            pipeline_id,
            source_config,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Establishes a connection to the source Postgres database.
    ///
    /// This method creates a new connection pool each time it's called rather
    /// than maintaining persistent connections. This approach trades connection
    /// setup overhead for reduced resource usage during periods of low activity.
    async fn connect_to_source(&self) -> Result<PgPool, sqlx::Error> {
        // We connect to source database each time we update because we assume that
        // these updates will be infrequent. It has some overhead to establish a
        // connection, but it's better than holding a connection open for long periods
        // when there's little activity on it.
        let pool = connect_to_source_database(
            &self.source_config,
            NUM_POOL_CONNECTIONS,
            NUM_POOL_CONNECTIONS,
        )
        .await?;

        Ok(pool)
    }
}

/// Emits table related metrics.
fn emit_table_metrics(
    pipeline_id: PipelineId,
    total_tables: usize,
    phase_counts: &HashMap<&'static str, u64>,
) {
    gauge!(ETL_TABLES_TOTAL, PIPELINE_ID_LABEL => pipeline_id.to_string()).set(total_tables as f64);

    for (phase, count) in phase_counts {
        gauge!(
            ETL_TABLES_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            PHASE_LABEL => *phase
        )
        .set(*count as f64);
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
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
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

        let pool = self.connect_to_source().await?;

        let replication_state_rows =
            state::get_table_replication_state_rows(&pool, self.pipeline_id as i64).await?;

        let mut table_states: HashMap<TableId, TableReplicationPhase> = HashMap::new();
        for row in replication_state_rows {
            let table_id = TableId::new(row.table_id.0);
            let phase: TableReplicationPhase = row.try_into()?;
            table_states.insert(table_id, phase);
        }

        let mut phase_counts = HashMap::new();
        for phase in table_states.values() {
            let entry = phase_counts
                .entry(phase.as_type().as_static_str())
                .or_insert(0u64);
            *entry += 1;
        }

        // For performance reasons, since we load the replication states only once during startup
        // and from a single thread, we can afford to have a super short critical section.
        let mut inner = self.inner.lock().await;
        inner.table_states = table_states.clone();
        inner.phase_counts = phase_counts;

        emit_table_metrics(
            self.pipeline_id,
            inner.table_states.keys().len(),
            &inner.phase_counts,
        );

        info!(
            "loaded {} table replication states from postgres state store",
            table_states.len()
        );

        Ok(table_states.len())
    }

    /// Updates a table's replication state in both database and cache.
    ///
    /// This method performs atomic updates by first acquiring the cache lock,
    /// then updating the database, and finally updating the cache. This ordering
    /// prevents inconsistencies that could occur from concurrent access during
    /// the update process.
    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let db_state: state::TableReplicationState = state.clone().try_into()?;

        let pool = self.connect_to_source().await?;

        // We lock the inner state before updating the state in the database, to make sure we are
        // consistent. If we were to lock the states only after the db state is modified, we might
        // be inconsistent since there are some interleaved executions that lead to a wrong state.
        let mut inner = self.inner.lock().await;
        state::update_replication_state(&pool, self.pipeline_id as i64, table_id, db_state).await?;

        // Compute which phases need to be increment and decremented to
        // keep table metrics updated
        let phase_to_decrement = inner
            .table_states
            .get(&table_id)
            .map(|table_state| table_state.as_type().as_static_str());
        let phase_to_increment = state.as_type().as_static_str();

        inner.table_states.insert(table_id, state);

        // Update the metrics and emit the latest values
        if let Some(phase_to_decrement) = phase_to_decrement {
            inner.decrement_phase_count(phase_to_decrement);
        }
        inner.increment_phase_count(phase_to_increment);

        emit_table_metrics(
            self.pipeline_id,
            inner.table_states.keys().len(),
            &inner.phase_counts,
        );

        Ok(())
    }

    /// Rolls back a table's replication state to the previous version.
    ///
    /// This method restores the table to its previous replication state by
    /// querying the database for the prior state entry. It updates both the
    /// persistent storage and in-memory cache to reflect the rollback.
    ///
    /// Returns the restored state on success, or an error if no previous
    /// state exists for rollback.
    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let pool = self.connect_to_source().await?;

        // Here we perform locking for the same reasons stated in `update_table_replication_state`.
        let mut inner = self.inner.lock().await;
        match state::rollback_replication_state(&pool, self.pipeline_id as i64, table_id).await? {
            Some(restored_row) => {
                // Compute which phases need to be increment and decremented to
                // keep table metrics updated
                let phase_to_decrement = inner
                    .table_states
                    .get(&table_id)
                    .map(|table_state| table_state.as_type().as_static_str());
                let restored_phase: TableReplicationPhase = restored_row.try_into()?;
                let phase_to_increment = restored_phase.as_type().as_static_str();

                inner.table_states.insert(table_id, restored_phase.clone());

                // Update the metrics and emit the latest values
                if let Some(phase_to_decrement) = phase_to_decrement {
                    inner.decrement_phase_count(phase_to_decrement);
                }
                inner.increment_phase_count(phase_to_increment);

                emit_table_metrics(
                    self.pipeline_id,
                    inner.table_states.keys().len(),
                    &inner.phase_counts,
                );

                Ok(restored_phase)
            }
            None => Err(etl_error!(
                ErrorKind::StateRollbackError,
                "Previous table state not found",
                "No previous state available to roll back to for this table"
            )),
        }
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

        let pool = self.connect_to_source().await?;

        let table_mappings = table_mappings::load_table_mappings(&pool, self.pipeline_id as i64)
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
    ///
    /// This method persists a table mapping from source table ID to destination
    /// table name in the database and updates the in-memory cache atomically.
    /// Used when establishing or updating the mapping configuration between
    /// source and destination systems.
    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        debug!(
            "storing table mapping: '{}' -> '{}'",
            source_table_id, destination_table_id
        );

        let pool = self.connect_to_source().await?;

        let mut inner = self.inner.lock().await;
        table_mappings::store_table_mapping(
            &pool,
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
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);

        Ok(())
    }

    /// Retrieves the destination schema state for a table from cache.
    ///
    /// This method provides fast access to destination schema states by reading
    /// from the in-memory cache. The cache is populated during startup via
    /// [`load_destination_schema_states`] and updated as states change.
    async fn get_destination_schema_state(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<DestinationSchemaState>> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_schema_states.get(table_id).cloned())
    }

    /// Loads destination schema states from Postgres into memory cache.
    ///
    /// This method connects to the source database, retrieves all destination
    /// schema state records for this pipeline, deserializes them, and populates
    /// the in-memory cache. It's typically called during pipeline startup.
    async fn load_destination_schema_states(&self) -> EtlResult<usize> {
        debug!("loading destination schema states from postgres state store");

        let pool = self.connect_to_source().await?;

        let state_rows = schema::load_destination_schema_states(&pool, self.pipeline_id as i64)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Destination schema states loading failed",
                    format!(
                        "Failed to load destination schema states from PostgreSQL: {}",
                        err
                    )
                )
            })?;

        let mut states: HashMap<TableId, DestinationSchemaState> = HashMap::new();
        for (table_id, row) in state_rows {
            let state_type = match row.state_type.as_str() {
                "applying" => DestinationSchemaStateType::Applying,
                "applied" => DestinationSchemaStateType::Applied,
                _ => {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "Invalid destination schema state type",
                        format!("Unknown state type '{}' in database", row.state_type)
                    ));
                }
            };
            states.insert(
                table_id,
                DestinationSchemaState {
                    state: state_type,
                    snapshot_id: row.snapshot_id,
                },
            );
        }

        let states_len = states.len();
        let mut inner = self.inner.lock().await;
        inner.destination_schema_states = states;

        info!(
            "loaded {} destination schema states from postgres state store",
            states_len
        );

        Ok(states_len)
    }

    /// Stores a destination schema state in both database and cache.
    ///
    /// This method performs atomic updates by first acquiring the cache lock,
    /// then updating the database, and finally updating the cache.
    async fn store_destination_schema_state(
        &self,
        table_id: TableId,
        state: DestinationSchemaState,
    ) -> EtlResult<()> {
        let state_type = match state.state {
            DestinationSchemaStateType::Applying => "applying",
            DestinationSchemaStateType::Applied => "applied",
        };

        let pool = self.connect_to_source().await?;

        let mut inner = self.inner.lock().await;
        schema::store_destination_schema_state(
            &pool,
            self.pipeline_id as i64,
            table_id,
            state_type,
            state.snapshot_id,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Destination schema state storage failed",
                format!(
                    "Failed to store destination schema state in PostgreSQL: {}",
                    err
                )
            )
        })?;

        inner.destination_schema_states.insert(table_id, state);

        Ok(())
    }
}

impl SchemaStore for PostgresStore {
    /// Retrieves a table schema at a specific snapshot point.
    ///
    /// Returns the schema version with the largest snapshot_id <= the requested snapshot_id.
    /// First checks the in-memory cache, then loads from the database if not found.
    /// The loaded schema is cached for subsequent requests. Note that the cache is
    /// optimized for active schemas, not historical snapshots.
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        // First, check if we have a cached schema that matches the criteria.
        //
        // We can afford to hold the lock only for this short critical section since we assume that
        // there is not really concurrency at the table level since each table is processed by exactly
        // one worker.
        {
            let inner = self.inner.lock().await;

            // Find the best matching schema in the cache (largest snapshot_id <= requested).
            let newest_table_schema = inner
                .table_schemas
                .iter()
                .filter(|((tid, sid), _)| *tid == *table_id && *sid <= snapshot_id)
                .max_by_key(|((_, sid), _)| *sid)
                .map(|(_, schema)| schema.clone());

            if newest_table_schema.is_some() {
                return Ok(newest_table_schema);
            }
        }

        debug!(
            "schema for table {} at snapshot {} not in cache, loading from database",
            table_id, snapshot_id
        );

        let pool = self.connect_to_source().await?;

        // Load the schema at the requested snapshot.
        let table_schema = schema::load_table_schema_at_snapshot(
            &pool,
            self.pipeline_id as i64,
            *table_id,
            snapshot_id,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Table schema loading failed",
                format!(
                    "Failed to load table schema for table {} at snapshot {} from PostgreSQL: {}",
                    table_id, snapshot_id, err
                )
            )
        })?;

        let Some(table_schema) = table_schema else {
            return Ok(None);
        };

        let result = {
            let mut inner = self.inner.lock().await;

            let table_schema = Arc::new(table_schema);
            inner.insert_schema_with_eviction(table_schema.clone());

            Some(table_schema)
        };

        Ok(result)
    }

    /// Retrieves all cached table schemas as a vector.
    ///
    /// This method returns all currently cached table schemas, providing a
    /// complete view of the schema information available to the pipeline.
    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    /// Loads table schemas from Postgres into memory cache.
    ///
    /// This method connects to the source database, retrieves the latest schema
    /// version for all tables in this pipeline, and populates the in-memory cache.
    /// Called during pipeline initialization to establish the schema context
    /// needed for processing replication events.
    async fn load_table_schemas(&self) -> EtlResult<usize> {
        debug!("loading table schemas from postgres state store");

        let pool = self.connect_to_source().await?;

        let table_schemas = schema::load_table_schemas(&pool, self.pipeline_id as i64)
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
            let key = (table_schema.id, table_schema.snapshot_id);
            inner.table_schemas.insert(key, Arc::new(table_schema));
        }

        info!(
            "loaded {} table schemas from postgres state store",
            table_schemas_len
        );

        Ok(table_schemas_len)
    }

    /// Stores a table schema in both database and cache.
    ///
    /// This method persists a table schema to the database and updates the
    /// in-memory cache atomically. The schema's snapshot_id determines which
    /// version this schema represents.
    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        debug!(
            "storing table schema for table '{}' at snapshot {}",
            table_schema.name, table_schema.snapshot_id
        );

        let pool = self.connect_to_source().await?;

        schema::store_table_schema(&pool, self.pipeline_id as i64, &table_schema)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Table schema storage failed",
                    format!("Failed to store table schema in PostgreSQL: {}", err)
                )
            })?;

        let mut inner = self.inner.lock().await;
        let table_schema = Arc::new(table_schema);
        inner.insert_schema_with_eviction(table_schema.clone());

        Ok(table_schema)
    }
}

impl CleanupStore for PostgresStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let pool = self.connect_to_source().await?;

        // Use a single DB transaction to keep persistent state consistent.
        let mut tx = pool.begin().await?;

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

        // Update in-memory caches and metrics.
        let mut inner = self.inner.lock().await;

        inner.table_states.remove(&table_id);
        // Remove all schema versions for this table
        inner.table_schemas.retain(|(tid, _), _| *tid != table_id);
        inner.table_mappings.remove(&table_id);

        emit_table_metrics(
            self.pipeline_id,
            inner.table_states.keys().len(),
            &inner.phase_counts,
        );

        Ok(())
    }
}
