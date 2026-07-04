use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use tokio_postgres::types::PgLsn;

use crate::{
    destination::{
        AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationWriteStreamState,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    replication::{WorkerType, state::TableState},
    schema::{SnapshotId, TableId, TableSchema},
    store::{
        DestinationTablesMetadata, DestinationWriteStreamStates, SchemaStore, StateStore,
        TableSchemaRetention, TableSchemaSnapshots, TableStateLifecycleStore, TableStateOperation,
        TableStates,
    },
};

/// Inner state of [`MemoryStore`].
#[derive(Debug)]
struct Inner {
    /// Current table state for each table - this is the authoritative
    /// source of truth for table states. Every table being replicated must
    /// have an entry here.
    table_states: TableStates,
    /// Complete history of state transitions for each table, used for debugging
    /// and auditing. This is an append-only log that grows over time and
    /// provides visibility into table state evolution. Entries are
    /// chronologically ordered.
    table_state_history: HashMap<TableId, Vec<TableState>>,
    /// Cached table schema snapshots.
    table_schemas: Arc<TableSchemaSnapshots>,
    /// Cached destination table metadata indexed by table ID.
    destination_tables_metadata: DestinationTablesMetadata,
    /// Durable write stream state indexed by source table and destination table.
    destination_write_stream_states: DestinationWriteStreamStates,
    /// Durable replication progress indexed by worker type and optional table.
    replication_progress: HashMap<WorkerType, PgLsn>,
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`MemoryStore`] implements the store traits required by
/// [`crate::store::PipelineStore`], providing a complete storage solution that
/// keeps all data in memory. This is ideal for testing, development, and
/// scenarios where persistence is not required.
///
/// All state information, including table states, schema
/// definitions, and destination table metadata are stored in memory and will be
/// lost on process restart.
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema
    /// data. As the pipeline runs, it will populate these collections with
    /// table state and schema information.
    pub fn new() -> Self {
        let inner = Inner {
            table_states: Arc::new(BTreeMap::new()),
            table_state_history: HashMap::new(),
            table_schemas: Arc::new(TableSchemaSnapshots::default()),
            destination_tables_metadata: Arc::new(BTreeMap::new()),
            destination_write_stream_states: Arc::new(BTreeMap::new()),
            replication_progress: HashMap::new(),
        };

        Self { inner: Arc::new(Mutex::new(inner)) }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStore {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_states.get(&table_id).cloned())
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        let inner = self.inner.lock().await;

        Ok(Arc::clone(&inner.table_states))
    }

    async fn load_table_states(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_states.len())
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        let mut guard = self.inner.lock().await;
        // To enable split-borrow (`Arc::make_mut()` borrows `table_states`
        // mutably, while `inner.table_state_history` borrows different field).
        let inner = &mut *guard;

        let states = Arc::make_mut(&mut inner.table_states);
        for (table_id, state) in updates {
            // Store the current state in history before updating.
            if let Some(current_state) = states.get(&table_id).cloned() {
                inner
                    .table_state_history
                    .entry(table_id)
                    .or_insert_with(Vec::new)
                    .push(current_state);
            }

            states.insert(table_id, state);
        }

        Ok(())
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        let mut inner = self.inner.lock().await;

        // Get the previous state from history.
        let previous_state =
            inner.table_state_history.get_mut(&table_id).and_then(Vec::pop).ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "No previous state available to roll back to"
                )
            })?;

        // Update the current state to the previous state.
        Arc::make_mut(&mut inner.table_states).insert(table_id, previous_state.clone());

        Ok(previous_state)
    }

    async fn get_replication_progress(&self, worker_type: WorkerType) -> EtlResult<Option<PgLsn>> {
        let inner = self.inner.lock().await;

        Ok(inner.replication_progress.get(&worker_type).copied())
    }

    async fn upsert_replication_progress(
        &self,
        worker_type: WorkerType,
        flush_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        let mut inner = self.inner.lock().await;
        let stored_lsn = inner
            .replication_progress
            .entry(worker_type)
            .and_modify(|stored_lsn| {
                if flush_lsn > *stored_lsn {
                    *stored_lsn = flush_lsn;
                }
            })
            .or_insert(flush_lsn);

        Ok(*stored_lsn)
    }

    async fn delete_replication_progress(&self, worker_type: WorkerType) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner.replication_progress.remove(&worker_type);

        Ok(())
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
    }

    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<AppliedDestinationTableMetadata>> {
        let inner = self.inner.lock().await;

        inner
            .destination_tables_metadata
            .get(&table_id)
            .cloned()
            .map(DestinationTableMetadata::into_applied)
            .transpose()
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_tables_metadata.len())
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        Arc::make_mut(&mut inner.destination_tables_metadata).insert(table_id, metadata);

        Ok(())
    }

    async fn get_destination_write_stream_state(
        &self,
        table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<Option<DestinationWriteStreamState>> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_write_stream_states.get(&(table_id, destination_table_id)).cloned())
    }

    async fn store_destination_write_stream_state(
        &self,
        table_id: TableId,
        state: DestinationWriteStreamState,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        let key = (table_id, state.destination_table_id.clone());
        if should_store_destination_write_stream_state(
            inner.destination_write_stream_states.get(&key),
            &state,
        ) {
            Arc::make_mut(&mut inner.destination_write_stream_states).insert(key, state);
        }

        Ok(())
    }

    async fn replace_destination_write_stream_state(
        &self,
        table_id: TableId,
        expected_stream_name: String,
        state: DestinationWriteStreamState,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        let key = (table_id, state.destination_table_id.clone());
        ensure_destination_write_stream_state_can_be_replaced(
            inner.destination_write_stream_states.get(&key),
            &expected_stream_name,
            &state,
        )?;
        Arc::make_mut(&mut inner.destination_write_stream_states).insert(key, state);

        Ok(())
    }

    async fn delete_destination_write_stream_state(
        &self,
        table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        Arc::make_mut(&mut inner.destination_write_stream_states)
            .remove(&(table_id, destination_table_id));

        Ok(())
    }
}

fn should_store_destination_write_stream_state(
    current: Option<&DestinationWriteStreamState>,
    next: &DestinationWriteStreamState,
) -> bool {
    match current {
        None => true,
        Some(current) => {
            current.stream_name == next.stream_name
                && current.next_offset <= next.next_offset
                && current.last_sequence_number <= next.last_sequence_number
        }
    }
}

fn ensure_destination_write_stream_state_can_be_replaced(
    current: Option<&DestinationWriteStreamState>,
    expected_stream_name: &str,
    next: &DestinationWriteStreamState,
) -> EtlResult<()> {
    let Some(current) = current else {
        return Ok(());
    };

    if current.stream_name != expected_stream_name {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "Destination write stream state changed before replacement",
            format!("Expected stream '{}', found '{}'", expected_stream_name, current.stream_name)
        ));
    }

    if current.last_sequence_number > next.last_sequence_number {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "Destination write stream sequence would move backwards",
            "The replacement state has an older append-only sequence watermark"
        ));
    }

    Ok(())
}

impl SchemaStore for MemoryStore {
    /// Returns the table schema for the given table at the specified snapshot
    /// point.
    ///
    /// Returns the schema version with the largest snapshot_id <= the requested
    /// snapshot_id. For MemoryStore, this only looks in the in-memory
    /// cache.
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get_at_or_before(*table_id, snapshot_id))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.all())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.total_snapshots_count())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.inner.lock().await;

        Ok(Arc::make_mut(&mut inner.table_schemas).insert(table_schema))
    }

    async fn prune_table_schemas(
        &self,
        table_schema_retentions: HashMap<TableId, TableSchemaRetention>,
    ) -> EtlResult<u64> {
        let mut inner = self.inner.lock().await;

        Ok(Arc::make_mut(&mut inner.table_schemas).prune(&table_schema_retentions))
    }
}

impl TableStateLifecycleStore for MemoryStore {
    async fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> EtlResult<usize> {
        match operation {
            TableStateOperation::PrepareForCopy { table_id } => {
                let mut inner = self.inner.lock().await;
                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                Arc::make_mut(&mut inner.destination_write_stream_states)
                    .retain(|(state_table_id, _), _| *state_table_id != table_id);
                inner.replication_progress.remove(&WorkerType::TableSync { table_id });

                Ok(0)
            }
            TableStateOperation::ResetForResync => {
                let mut guard = self.inner.lock().await;
                let inner = &mut *guard;

                let states = Arc::make_mut(&mut inner.table_states);
                let table_ids = states.keys().copied().collect::<Vec<_>>();
                let reset_count = table_ids.len();

                for table_id in table_ids {
                    if let Some(current_state) = states.get(&table_id).cloned() {
                        inner
                            .table_state_history
                            .entry(table_id)
                            .or_insert_with(Vec::new)
                            .push(current_state);
                    }

                    states.insert(table_id, TableState::Init);
                }

                inner.replication_progress.remove(&WorkerType::Apply);
                Arc::make_mut(&mut inner.destination_write_stream_states).clear();

                Ok(reset_count)
            }
            TableStateOperation::Delete { table_id } => {
                let mut inner = self.inner.lock().await;
                let affected_table_count = usize::from(inner.table_states.contains_key(&table_id));

                Arc::make_mut(&mut inner.table_states).remove(&table_id);
                inner.table_state_history.remove(&table_id);
                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                Arc::make_mut(&mut inner.destination_write_stream_states)
                    .retain(|(state_table_id, _), _| *state_table_id != table_id);
                inner.replication_progress.remove(&WorkerType::TableSync { table_id });

                Ok(affected_table_count)
            }
        }
    }
}
