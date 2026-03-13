use std::collections::{BTreeMap, HashMap};
use std::{fmt, sync::Arc};

use etl_postgres::types::{SnapshotId, TableId, TableSchema};
use tokio::sync::{Notify, RwLock};

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::state::destination_metadata::DestinationTableMetadata;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::test_utils::notify::TimedNotify;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableReplicationState,
    GetTableReplicationStates,
    LoadTableReplicationStates,
    StoreTableReplicationState,
    RollbackTableReplicationState,
}

type TableStateTypeCondition = (TableId, TableReplicationPhaseType, Arc<Notify>);
type TableStateCondition = (
    TableId,
    Arc<Notify>,
    Box<dyn Fn(&TableReplicationPhase) -> bool + Send + Sync>,
);

struct Inner {
    table_replication_states: BTreeMap<TableId, TableReplicationPhase>,
    table_replication_states_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    table_schemas: HashMap<TableId, Vec<Arc<TableSchema>>>,
    destination_tables_metadata: HashMap<TableId, DestinationTableMetadata>,
    table_state_type_conditions: Vec<TableStateTypeCondition>,
    table_state_conditions: Vec<TableStateCondition>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    async fn check_conditions(&mut self) {
        let table_states = self.table_replication_states.clone();
        self.table_state_type_conditions
            .retain(|(tid, expected_state, notify)| {
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

        self.table_state_conditions
            .retain(|(tid, notify, condition)| {
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
    }

    async fn dispatch_method_notification(&self, method: StateStoreMethod) {
        if let Some(notifiers) = self.method_call_notifiers.get(&method) {
            for notifier in notifiers {
                notifier.notify_one();
            }
        }
    }
}

/// A state store that notifies listeners about changes to table states.
#[derive(Clone)]
pub struct NotifyingStore {
    inner: Arc<RwLock<Inner>>,
}

impl NotifyingStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: BTreeMap::new(),
            table_replication_states_history: HashMap::new(),
            table_schemas: HashMap::new(),
            destination_tables_metadata: HashMap::new(),
            table_state_type_conditions: Vec::new(),
            table_state_conditions: Vec::new(),
            method_call_notifiers: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get_table_replication_states(&self) -> BTreeMap<TableId, TableReplicationPhase> {
        let inner = self.inner.read().await;
        inner.table_replication_states.clone()
    }

    pub async fn get_latest_table_schemas(&self) -> HashMap<TableId, TableSchema> {
        let inner = self.inner.read().await;

        // Return the latest schema version for each table (last in the Vec).
        inner
            .table_schemas
            .iter()
            .filter_map(|(table_id, schemas)| {
                schemas
                    .last()
                    .map(|schema| (*table_id, Arc::as_ref(schema).clone()))
            })
            .collect()
    }

    pub async fn get_table_schemas(&self) -> HashMap<TableId, Vec<(SnapshotId, TableSchema)>> {
        let inner = self.inner.read().await;

        // Return schemas in insertion order per table.
        inner
            .table_schemas
            .iter()
            .map(|(table_id, schemas)| {
                let schemas_with_ids: Vec<_> = schemas
                    .iter()
                    .map(|schema| (schema.snapshot_id, Arc::as_ref(schema).clone()))
                    .collect();
                (*table_id, schemas_with_ids)
            })
            .collect()
    }

    /// Registers a notification that fires when a table reaches a specific state type or if the
    /// table already has that specific state type.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// expected state is not reached. This prevents tests from hanging indefinitely.
    pub async fn notify_on_table_state_type(
        &self,
        table_id: TableId,
        expected_state: TableReplicationPhaseType,
    ) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_type_conditions
            .push((table_id, expected_state, notify.clone()));

        inner.check_conditions().await;

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when a table reaches a specific state type from when
    /// this method was called.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// expected state is not reached. This prevents tests from hanging indefinitely.
    pub async fn notify_on_future_table_state_type(
        &self,
        table_id: TableId,
        expected_state: TableReplicationPhaseType,
    ) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_type_conditions
            .push((table_id, expected_state, notify.clone()));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when a table state matches a custom condition.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// condition is not met. This prevents tests from hanging indefinitely.
    pub async fn notify_on_table_state<F>(&self, table_id: TableId, condition: F) -> TimedNotify
    where
        F: Fn(&TableReplicationPhase) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_conditions
            .push((table_id, notify.clone(), Box::new(condition)));

        inner.check_conditions().await;

        TimedNotify::new(notify)
    }

    pub async fn reset_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner.table_replication_states.remove(&table_id);
        inner.table_replication_states_history.remove(&table_id);

        inner
            .table_replication_states
            .insert(table_id, TableReplicationPhase::Init);

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

        inner
            .dispatch_method_notification(StateStoreMethod::GetTableReplicationState)
            .await;

        result
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.clone());

        inner
            .dispatch_method_notification(StateStoreMethod::GetTableReplicationStates)
            .await;

        result
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        let table_replication_states_len = inner.table_replication_states.len();

        inner
            .dispatch_method_notification(StateStoreMethod::LoadTableReplicationStates)
            .await;

        Ok(table_replication_states_len)
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;

        for (table_id, state) in updates {
            // Store the current state in history before updating
            if let Some(current_state) = inner.table_replication_states.get(&table_id).cloned() {
                inner
                    .table_replication_states_history
                    .entry(table_id)
                    .or_insert_with(Vec::new)
                    .push(current_state);
            }

            inner.table_replication_states.insert(table_id, state);
        }

        inner.check_conditions().await;
        inner
            .dispatch_method_notification(StateStoreMethod::StoreTableReplicationState)
            .await;

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut inner = self.inner.write().await;

        // Get the previous state from history
        let previous_state = inner
            .table_replication_states_history
            .get_mut(&table_id)
            .and_then(|history| history.pop())
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "No previous state available to roll back to"
                )
            })?;

        // Update the current state to the previous state
        inner
            .table_replication_states
            .insert(table_id, previous_state.clone());
        inner.check_conditions().await;

        inner
            .dispatch_method_notification(StateStoreMethod::RollbackTableReplicationState)
            .await;

        Ok(previous_state)
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.inner.read().await;
        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
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
        inner.destination_tables_metadata.insert(table_id, metadata);
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

        // Find the best matching schema (largest snapshot_id <= requested).
        let best_match = inner.table_schemas.get(table_id).and_then(|schemas| {
            schemas
                .iter()
                .filter(|schema| schema.snapshot_id <= snapshot_id)
                .max_by_key(|schema| schema.snapshot_id)
                .cloned()
        });

        Ok(best_match)
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.read().await;

        Ok(inner
            .table_schemas
            .values()
            .flat_map(|schemas| schemas.iter().cloned())
            .collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        Ok(inner.table_schemas.values().map(|v| v.len()).sum())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.inner.write().await;

        let table_id = table_schema.id;
        let table_schema = Arc::new(table_schema);
        inner
            .table_schemas
            .entry(table_id)
            .or_default()
            .push(table_schema.clone());

        Ok(table_schema)
    }
}

impl CleanupStore for NotifyingStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;

        inner.table_replication_states.remove(&table_id);
        inner.table_replication_states_history.remove(&table_id);
        inner.table_schemas.remove(&table_id);
        inner.destination_tables_metadata.remove(&table_id);

        Ok(())
    }
}

impl fmt::Debug for NotifyingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyingStore").finish()
    }
}
