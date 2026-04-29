use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use etl_postgres::types::{SnapshotId, TableId, TableSchema};
use tokio::sync::{Notify, RwLock};

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    state::{
        destination_metadata::{AppliedDestinationTableMetadata, DestinationTableMetadata},
        table::{TableReplicationPhase, TableReplicationPhaseType},
    },
    store::{
        cleanup::CleanupStore,
        schema::{SchemaStore, TableSchemaRetention, TableSchemaSnapshots},
        state::{DestinationTablesMetadata, StateStore, TableReplicationStates},
    },
    test_utils::notify::TimedNotify,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableReplicationState,
    GetTableReplicationStates,
    LoadTableReplicationStates,
    StoreTableReplicationState,
    RollbackTableReplicationState,
}

type TableStateTypeCondition = (TableId, TableReplicationPhaseType, Arc<Notify>);
type TableStateCondition =
    (TableId, Arc<Notify>, Box<dyn Fn(&TableReplicationPhase) -> bool + Send + Sync>);
type TableSchemaCountCondition = (TableId, usize, Arc<Notify>);
type TableSchemaPruneCondition = Arc<Notify>;

struct Inner {
    table_replication_states: TableReplicationStates,
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    table_schemas: Arc<TableSchemaSnapshots>,
    destination_tables_metadata: DestinationTablesMetadata,
    table_state_type_conditions: Vec<TableStateTypeCondition>,
    table_state_conditions: Vec<TableStateCondition>,
    table_schema_count_conditions: Vec<TableSchemaCountCondition>,
    table_schema_prune_conditions: Vec<TableSchemaPruneCondition>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    fn check_conditions(&mut self) {
        let table_states = Arc::clone(&self.table_replication_states);
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
            let schemas_count = self.table_schemas.table_len(*tid);
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
            table_replication_states: Arc::new(BTreeMap::new()),
            table_state_history: HashMap::new(),
            table_schemas: Arc::new(TableSchemaSnapshots::default()),
            destination_tables_metadata: Arc::new(BTreeMap::new()),
            table_state_type_conditions: Vec::new(),
            table_state_conditions: Vec::new(),
            table_schema_count_conditions: Vec::new(),
            table_schema_prune_conditions: Vec::new(),
            method_call_notifiers: HashMap::new(),
        };

        Self { inner: Arc::new(RwLock::new(inner)) }
    }

    /// Returns the current table replication states.
    pub async fn get_table_replication_states(&self) -> TableReplicationStates {
        let inner = self.inner.read().await;
        Arc::clone(&inner.table_replication_states)
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
        expected_state: TableReplicationPhaseType,
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
        F: Fn(&TableReplicationPhase) -> bool + Send + Sync + 'static,
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

    /// Resets one table to [`TableReplicationPhase::Init`] and clears its state
    /// history.
    pub async fn reset_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner.table_state_history.remove(&table_id);

        let states = Arc::make_mut(&mut inner.table_replication_states);
        states.remove(&table_id);
        states.insert(table_id, TableReplicationPhase::Init);

        Ok(())
    }
}

impl Default for NotifyingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for NotifyingStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.get(&table_id).cloned());

        inner.dispatch_method_notification(StateStoreMethod::GetTableReplicationState);

        result
    }

    async fn get_table_replication_states(&self) -> EtlResult<TableReplicationStates> {
        let inner = self.inner.read().await;
        let result = Ok(Arc::clone(&inner.table_replication_states));

        inner.dispatch_method_notification(StateStoreMethod::GetTableReplicationStates);

        result
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        let table_replication_states_len = inner.table_replication_states.len();

        inner.dispatch_method_notification(StateStoreMethod::LoadTableReplicationStates);

        Ok(table_replication_states_len)
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        let mut guard = self.inner.write().await;
        let inner = &mut *guard;

        let states = Arc::make_mut(&mut inner.table_replication_states);
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
        guard.dispatch_method_notification(StateStoreMethod::StoreTableReplicationState);

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
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
        Arc::make_mut(&mut inner.table_replication_states).insert(table_id, previous_state.clone());
        inner.check_conditions();

        inner.dispatch_method_notification(StateStoreMethod::RollbackTableReplicationState);

        Ok(previous_state)
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

        Ok(inner.table_schemas.len())
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

impl CleanupStore for NotifyingStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;

        Arc::make_mut(&mut inner.table_replication_states).remove(&table_id);
        inner.table_state_history.remove(&table_id);
        Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
        Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);

        Ok(())
    }
}

impl fmt::Debug for NotifyingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyingStore").finish()
    }
}
