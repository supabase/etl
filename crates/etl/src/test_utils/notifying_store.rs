use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use tokio::sync::{Notify, RwLock};
use tokio_postgres::types::PgLsn;

use crate::{
    destination::{
        AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationWriteStreamState,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    replication::{
        WorkerType,
        state::{TableState, TableStateType},
    },
    schema::{SnapshotId, TableId, TableSchema},
    store::{
        DestinationTablesMetadata, DestinationWriteStreamStates, SchemaStore, StateStore,
        TableSchemaRetention, TableSchemaSnapshots, TableStateLifecycleStore, TableStateOperation,
        TableStates,
    },
    test_utils::notify::TimedNotify,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableState,
    GetTableStates,
    LoadTableStates,
    StoreTableState,
    RollbackTableState,
}

type TableStateTypeCondition = (TableId, TableStateType, Arc<Notify>);
type TableStateCondition = (TableId, Arc<Notify>, Box<dyn Fn(&TableState) -> bool + Send + Sync>);
type TableSchemaCountCondition = (TableId, usize, Arc<Notify>);
type TableSchemaPruneCondition = Arc<Notify>;

struct Inner {
    table_states: TableStates,
    table_state_history: HashMap<TableId, Vec<TableState>>,
    table_schemas: Arc<TableSchemaSnapshots>,
    destination_tables_metadata: DestinationTablesMetadata,
    destination_write_stream_states: DestinationWriteStreamStates,
    replication_progress: HashMap<WorkerType, PgLsn>,
    table_state_type_conditions: Vec<TableStateTypeCondition>,
    table_state_conditions: Vec<TableStateCondition>,
    table_schema_count_conditions: Vec<TableSchemaCountCondition>,
    table_schema_prune_conditions: Vec<TableSchemaPruneCondition>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    fn check_conditions(&mut self) {
        let table_states = Arc::clone(&self.table_states);
        self.table_state_type_conditions.retain(|(tid, expected_state, notify)| {
            if let Some(state) = table_states.get(tid) {
                let should_retain = *expected_state != state.as_type();
                if !should_retain {
                    notify.notify_one();
                }
                should_retain
            } else {
                true
            }
        });

        self.table_state_conditions.retain(|(tid, notify, condition)| {
            if let Some(state) = table_states.get(tid) {
                let should_retain = !condition(state);
                if !should_retain {
                    notify.notify_one();
                }
                should_retain
            } else {
                true
            }
        });

        self.table_schema_count_conditions.retain(|(tid, expected_count, notify)| {
            let schemas_count = self.table_schemas.snapshots_count(*tid);
            let should_retain = schemas_count < *expected_count;
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });
    }

    fn dispatch_method_notification(&self, method: StateStoreMethod) {
        if let Some(notifiers) = self.method_call_notifiers.get(&method) {
            for notifier in notifiers {
                notifier.notify_one();
            }
        }
    }
}

/// In-memory store that notifies tests about state and schema changes.
///
/// Notification helpers only observe changes that happen after registration.
/// Register the returned [`TimedNotify`] before starting the producer that is
/// expected to satisfy it.
#[derive(Clone)]
pub struct NotifyingStore {
    inner: Arc<RwLock<Inner>>,
}

impl NotifyingStore {
    /// Creates an empty notifying store.
    pub fn new() -> Self {
        let inner = Inner {
            table_states: Arc::new(BTreeMap::new()),
            table_state_history: HashMap::new(),
            table_schemas: Arc::new(TableSchemaSnapshots::default()),
            destination_tables_metadata: Arc::new(BTreeMap::new()),
            destination_write_stream_states: Arc::new(BTreeMap::new()),
            replication_progress: HashMap::new(),
            table_state_type_conditions: Vec::new(),
            table_state_conditions: Vec::new(),
            table_schema_count_conditions: Vec::new(),
            table_schema_prune_conditions: Vec::new(),
            method_call_notifiers: HashMap::new(),
        };

        Self { inner: Arc::new(RwLock::new(inner)) }
    }

    /// Returns the current table states.
    pub async fn get_table_states(&self) -> TableStates {
        let inner = self.inner.read().await;
        Arc::clone(&inner.table_states)
    }

    /// Returns the latest schema snapshot stored for each table.
    pub async fn get_latest_table_schemas(&self) -> HashMap<TableId, TableSchema> {
        let inner = self.inner.read().await;

        let mut table_schemas = HashMap::new();
        for schema in inner.table_schemas.all() {
            table_schemas
                .entry(schema.id)
                .and_modify(|current: &mut TableSchema| {
                    if current.snapshot_id < schema.snapshot_id {
                        *current = Arc::as_ref(&schema).clone();
                    }
                })
                .or_insert_with(|| Arc::as_ref(&schema).clone());
        }

        table_schemas
    }

    /// Returns all stored schema snapshots grouped by table.
    pub async fn get_table_schemas(&self) -> HashMap<TableId, Vec<(SnapshotId, TableSchema)>> {
        let inner = self.inner.read().await;

        let mut table_schemas: HashMap<TableId, Vec<_>> = HashMap::new();
        for schema in inner.table_schemas.all() {
            table_schemas
                .entry(schema.id)
                .or_default()
                .push((schema.snapshot_id, Arc::as_ref(&schema).clone()));
        }

        table_schemas
    }

    /// Registers a notification that fires when a future state update reaches
    /// the expected table state type.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected state is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn notify_on_table_state_type(
        &self,
        table_id: TableId,
        expected_state: TableStateType,
    ) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.table_state_type_conditions.push((table_id, expected_state, Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when a future state update matches a
    /// custom condition.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the condition is not met. This prevents tests
    /// from hanging indefinitely.
    pub async fn notify_on_table_state<F>(&self, table_id: TableId, condition: F) -> TimedNotify
    where
        F: Fn(&TableState) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.table_state_conditions.push((table_id, Arc::clone(&notify), Box::new(condition)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when a future schema write brings a
    /// table to at least the expected number of stored snapshots.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected schema count is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn notify_on_table_schema_count(
        &self,
        table_id: TableId,
        expected_count: usize,
    ) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.table_schema_count_conditions.push((table_id, expected_count, Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires after a future schema prune call.
    pub async fn notify_on_table_schema_prune(&self) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.table_schema_prune_conditions.push(Arc::clone(&notify));

        TimedNotify::new(notify)
    }

    /// Resets one table to [`TableState::Init`] and clears its state
    /// history.
    pub async fn reset_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner.table_state_history.remove(&table_id);

        let states = Arc::make_mut(&mut inner.table_states);
        states.remove(&table_id);
        states.insert(table_id, TableState::Init);

        Ok(())
    }
}

impl Default for NotifyingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for NotifyingStore {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_states.get(&table_id).cloned());

        inner.dispatch_method_notification(StateStoreMethod::GetTableState);

        result
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        let inner = self.inner.read().await;
        let result = Ok(Arc::clone(&inner.table_states));

        inner.dispatch_method_notification(StateStoreMethod::GetTableStates);

        result
    }

    async fn load_table_states(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        let table_states_len = inner.table_states.len();

        inner.dispatch_method_notification(StateStoreMethod::LoadTableStates);

        Ok(table_states_len)
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        let mut guard = self.inner.write().await;
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

        guard.check_conditions();
        guard.dispatch_method_notification(StateStoreMethod::StoreTableState);

        Ok(())
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        let mut inner = self.inner.write().await;

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
        inner.check_conditions();

        inner.dispatch_method_notification(StateStoreMethod::RollbackTableState);

        Ok(previous_state)
    }

    async fn get_replication_progress(&self, worker_type: WorkerType) -> EtlResult<Option<PgLsn>> {
        let inner = self.inner.read().await;

        Ok(inner.replication_progress.get(&worker_type).copied())
    }

    async fn upsert_replication_progress(
        &self,
        worker_type: WorkerType,
        flush_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        let mut inner = self.inner.write().await;
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
        let mut inner = self.inner.write().await;
        inner.replication_progress.remove(&worker_type);

        Ok(())
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.inner.read().await;
        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
    }

    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<AppliedDestinationTableMetadata>> {
        let inner = self.inner.read().await;

        inner
            .destination_tables_metadata
            .get(&table_id)
            .cloned()
            .map(DestinationTableMetadata::into_applied)
            .transpose()
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        Ok(inner.destination_tables_metadata.len())
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        Arc::make_mut(&mut inner.destination_tables_metadata).insert(table_id, metadata);
        Ok(())
    }

    async fn get_destination_write_stream_state(
        &self,
        table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<Option<DestinationWriteStreamState>> {
        let inner = self.inner.read().await;

        Ok(inner.destination_write_stream_states.get(&(table_id, destination_table_id)).cloned())
    }

    async fn store_destination_write_stream_state(
        &self,
        table_id: TableId,
        state: DestinationWriteStreamState,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        let key = (table_id, state.destination_table_id.clone());
        if should_store_destination_write_stream_state(
            inner.destination_write_stream_states.get(&key),
            &state,
        ) {
            Arc::make_mut(&mut inner.destination_write_stream_states).insert(key, state);
        }
        inner.check_conditions();

        Ok(())
    }

    async fn replace_destination_write_stream_state(
        &self,
        table_id: TableId,
        expected_stream_name: String,
        state: DestinationWriteStreamState,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        let key = (table_id, state.destination_table_id.clone());
        ensure_destination_write_stream_state_can_be_replaced(
            inner.destination_write_stream_states.get(&key),
            &expected_stream_name,
            &state,
        )?;
        Arc::make_mut(&mut inner.destination_write_stream_states).insert(key, state);
        inner.check_conditions();

        Ok(())
    }

    async fn delete_destination_write_stream_state(
        &self,
        table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        Arc::make_mut(&mut inner.destination_write_stream_states)
            .remove(&(table_id, destination_table_id));
        inner.check_conditions();

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
                && (current.next_offset < next.next_offset
                    || (current.next_offset == next.next_offset
                        && current.last_sequence_number <= next.last_sequence_number))
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

impl SchemaStore for NotifyingStore {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.get_at_or_before(*table_id, snapshot_id))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.all())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.total_snapshots_count())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.inner.write().await;

        let table_schema = Arc::make_mut(&mut inner.table_schemas).insert(table_schema);

        inner.check_conditions();

        Ok(table_schema)
    }

    async fn prune_table_schemas(
        &self,
        table_schema_retentions: HashMap<TableId, TableSchemaRetention>,
    ) -> EtlResult<u64> {
        let mut inner = self.inner.write().await;
        let removed_count = Arc::make_mut(&mut inner.table_schemas).prune(&table_schema_retentions);

        if removed_count > 0 {
            for notify in std::mem::take(&mut inner.table_schema_prune_conditions) {
                notify.notify_one();
            }
        }

        Ok(removed_count)
    }
}

impl TableStateLifecycleStore for NotifyingStore {
    async fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> EtlResult<usize> {
        match operation {
            TableStateOperation::PrepareForCopy { table_id } => {
                let mut inner = self.inner.write().await;
                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                Arc::make_mut(&mut inner.destination_write_stream_states)
                    .retain(|(state_table_id, _), _| *state_table_id != table_id);
                inner.replication_progress.remove(&WorkerType::TableSync { table_id });
                inner.check_conditions();

                Ok(0)
            }
            TableStateOperation::ResetForResync => {
                let mut guard = self.inner.write().await;
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
                inner.check_conditions();

                Ok(reset_count)
            }
            TableStateOperation::Delete { table_id } => {
                let mut inner = self.inner.write().await;
                let affected_table_count = usize::from(inner.table_states.contains_key(&table_id));

                Arc::make_mut(&mut inner.table_states).remove(&table_id);
                inner.table_state_history.remove(&table_id);
                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                Arc::make_mut(&mut inner.destination_write_stream_states)
                    .retain(|(state_table_id, _), _| *state_table_id != table_id);
                inner.replication_progress.remove(&WorkerType::TableSync { table_id });
                inner.check_conditions();

                Ok(affected_table_count)
            }
        }
    }
}

impl fmt::Debug for NotifyingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyingStore").finish()
    }
}
