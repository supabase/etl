use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use tokio_postgres::types::PgLsn;

use crate::{
    destination::DestinationTableMetadata,
    error::{ErrorKind, EtlResult},
    etl_error,
    replication::{WorkerType, state::TableState},
    schema::{SnapshotId, TableId, TableSchema},
    store::{
        CachedStore, DestinationTablesMetadata, SchemaStore, StateStore, TableSchemaSnapshots,
        TableStateLifecycleStore, TableStateOperation, TableStates,
    },
};

/// Unsynchronized contents and operations of an in-memory store.
///
/// [`MemoryStore`] guards one instance with a mutex. The test-only notifying
/// store guards one together with its observers under a single lock, so
/// observers see every update. Both stores share these operations so their
/// store semantics cannot drift apart.
#[derive(Debug, Default)]
pub(crate) struct MemoryStoreData {
    /// Current state for each table.
    table_states: TableStates,
    /// Replaced states for each table, oldest first, restored by rollback.
    table_state_history: HashMap<TableId, Vec<TableState>>,
    /// Stored table schema snapshots.
    table_schemas: Arc<TableSchemaSnapshots>,
    /// Destination table metadata indexed by table ID.
    destination_tables_metadata: DestinationTablesMetadata,
    /// Replication checkpoints indexed by worker type.
    replication_checkpoints: HashMap<WorkerType, PgLsn>,
}

impl MemoryStoreData {
    /// Returns the current state of a table.
    pub(crate) fn table_state(&self, table_id: TableId) -> Option<TableState> {
        self.table_states.get(&table_id).cloned()
    }

    /// Returns a snapshot of every current table state.
    pub(crate) fn table_states(&self) -> TableStates {
        Arc::clone(&self.table_states)
    }

    /// Sets table states, recording each replaced state for rollback.
    pub(crate) fn update_table_states(&mut self, updates: Vec<(TableId, TableState)>) {
        let states = Arc::make_mut(&mut self.table_states);
        for (table_id, state) in updates {
            if let Some(replaced_state) = states.insert(table_id, state) {
                self.table_state_history.entry(table_id).or_default().push(replaced_state);
            }
        }
    }

    /// Restores and returns the most recently replaced state of a table.
    pub(crate) fn rollback_table_state(&mut self, table_id: TableId) -> EtlResult<TableState> {
        let previous_state =
            self.table_state_history.get_mut(&table_id).and_then(Vec::pop).ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "No previous state available to roll back to"
                )
            })?;

        Arc::make_mut(&mut self.table_states).insert(table_id, previous_state.clone());

        Ok(previous_state)
    }

    /// Returns the persisted checkpoint for a worker.
    pub(crate) fn replication_checkpoint(&self, worker_type: WorkerType) -> Option<PgLsn> {
        self.replication_checkpoints.get(&worker_type).copied()
    }

    /// Stores a checkpoint unless a later one exists and returns the stored
    /// checkpoint.
    pub(crate) fn upsert_replication_checkpoint(
        &mut self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> PgLsn {
        let persisted_checkpoint_lsn =
            self.replication_checkpoints.entry(worker_type).or_insert(checkpoint_lsn);
        *persisted_checkpoint_lsn = (*persisted_checkpoint_lsn).max(checkpoint_lsn);

        *persisted_checkpoint_lsn
    }

    /// Removes the persisted checkpoint for a worker.
    pub(crate) fn delete_replication_checkpoint(&mut self, worker_type: WorkerType) {
        self.replication_checkpoints.remove(&worker_type);
    }

    /// Returns the destination metadata for a table.
    pub(crate) fn destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> Option<DestinationTableMetadata> {
        self.destination_tables_metadata.get(&table_id).cloned()
    }

    /// Stores the destination metadata for a table.
    pub(crate) fn store_destination_table_metadata(
        &mut self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) {
        Arc::make_mut(&mut self.destination_tables_metadata).insert(table_id, metadata);
    }

    /// Returns the newest schema of a table at or before `snapshot_id`.
    pub(crate) fn table_schema(
        &self,
        table_id: TableId,
        snapshot_id: SnapshotId,
    ) -> Option<Arc<TableSchema>> {
        self.table_schemas.get_at_or_before(table_id, snapshot_id)
    }

    /// Returns every stored schema snapshot.
    pub(crate) fn table_schemas(&self) -> Vec<Arc<TableSchema>> {
        self.table_schemas.all()
    }

    /// Stores a schema snapshot and returns the shared stored schema.
    pub(crate) fn store_table_schema(&mut self, table_schema: TableSchema) -> Arc<TableSchema> {
        Arc::make_mut(&mut self.table_schemas).insert(table_schema)
    }

    /// Removes schemas older than each table's retention snapshot and returns
    /// how many were removed.
    pub(crate) fn prune_table_schemas(
        &mut self,
        retention_snapshot_ids: &BTreeMap<TableId, SnapshotId>,
    ) -> u64 {
        Arc::make_mut(&mut self.table_schemas).prune(retention_snapshot_ids)
    }

    /// Applies a table lifecycle operation to every affected store domain.
    pub(crate) fn apply_table_state_operation(&mut self, operation: TableStateOperation) {
        match operation {
            TableStateOperation::PrepareForCopy { table_id } => {
                Arc::make_mut(&mut self.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut self.destination_tables_metadata).remove(&table_id);
                self.replication_checkpoints.remove(&WorkerType::TableSync { table_id });
            }
            TableStateOperation::ResetForResync => {
                let states = Arc::make_mut(&mut self.table_states);
                for (&table_id, state) in states.iter_mut() {
                    let previous = std::mem::replace(state, TableState::Init);
                    self.table_state_history.entry(table_id).or_default().push(previous);
                }

                self.replication_checkpoints.remove(&WorkerType::Apply);
            }
            TableStateOperation::Delete { table_id } => {
                Arc::make_mut(&mut self.table_states).remove(&table_id);
                self.table_state_history.remove(&table_id);
                Arc::make_mut(&mut self.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut self.destination_tables_metadata).remove(&table_id);
                self.replication_checkpoints.remove(&WorkerType::TableSync { table_id });
            }
        }
    }

    /// Returns the replaced states of a table, oldest first.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn table_state_history(&self, table_id: TableId) -> &[TableState] {
        self.table_state_history.get(&table_id).map(Vec::as_slice).unwrap_or_default()
    }

    /// Returns the number of stored schema snapshots for a table.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn table_schema_count(&self, table_id: TableId) -> usize {
        self.table_schemas.snapshots_count(table_id)
    }

    /// Resets a table to [`TableState::Init`] and clears its state history.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn reset_table_state(&mut self, table_id: TableId) {
        self.table_state_history.remove(&table_id);
        Arc::make_mut(&mut self.table_states).insert(table_id, TableState::Init);
    }
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`MemoryStore`] implements the store traits required by
/// [`crate::store::PipelineStore`], providing a complete storage solution that
/// keeps all data in memory. This is ideal for testing, development, and
/// scenarios where persistence is not required.
///
/// All state information, including table states, schema definitions, and
/// destination table metadata are stored in memory and will be lost on process
/// restart.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    data: Arc<Mutex<MemoryStoreData>>,
}

impl MemoryStore {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema
    /// data. As the pipeline runs, it will populate these collections with
    /// table state and schema information.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CachedStore for MemoryStore {
    async fn load_cache(&self) -> EtlResult<()> {
        Ok(())
    }
}

impl StateStore for MemoryStore {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        Ok(self.data.lock().await.table_state(table_id))
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        Ok(self.data.lock().await.table_states())
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        self.data.lock().await.update_table_states(updates);

        Ok(())
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        self.data.lock().await.rollback_table_state(table_id)
    }

    async fn get_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<Option<PgLsn>> {
        Ok(self.data.lock().await.replication_checkpoint(worker_type))
    }

    async fn upsert_replication_checkpoint(
        &self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        Ok(self.data.lock().await.upsert_replication_checkpoint(worker_type, checkpoint_lsn))
    }

    async fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> EtlResult<()> {
        self.data.lock().await.delete_replication_checkpoint(worker_type);

        Ok(())
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        Ok(self.data.lock().await.destination_table_metadata(table_id))
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        self.data.lock().await.store_destination_table_metadata(table_id, metadata);

        Ok(())
    }
}

impl SchemaStore for MemoryStore {
    /// Returns the table schema for the given table at the specified snapshot
    /// point.
    ///
    /// Returns the newest schema version at or before the requested snapshot.
    /// For [`MemoryStore`], this only looks in the in-memory cache.
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        Ok(self.data.lock().await.table_schema(*table_id, snapshot_id))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        Ok(self.data.lock().await.table_schemas())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        Ok(self.data.lock().await.store_table_schema(table_schema))
    }

    async fn prune_table_schemas(
        &self,
        retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> EtlResult<u64> {
        Ok(self.data.lock().await.prune_table_schemas(&retention_snapshot_ids))
    }
}

impl TableStateLifecycleStore for MemoryStore {
    async fn apply_table_state_operation(&self, operation: TableStateOperation) -> EtlResult<()> {
        self.data.lock().await.apply_table_state_operation(operation);

        Ok(())
    }
}
