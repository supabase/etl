use std::{collections::HashMap, fmt, sync::Arc};

use etl_postgres::types::{TableId, TableSchema};
use tokio::sync::{Notify, RwLock};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableReplicationState,
    GetTableReplicationStates,
    LoadTableReplicationStates,
    StoreTableReplicationState,
    RollbackTableReplicationState,
}

struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
    table_mappings: HashMap<TableId, String>,
    table_state_conditions: Vec<(TableId, TableReplicationPhaseType, Arc<Notify>)>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    async fn check_conditions(&mut self) {
        let table_states = self.table_replication_states.clone();
        self.table_state_conditions
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
    }

    async fn dispatch_method_notification(&self, method: StateStoreMethod) {
        if let Some(notifiers) = self.method_call_notifiers.get(&method) {
            for notifier in notifiers {
                notifier.notify_one();
            }
        }
    }
}

/// A state store which notifying about changes to the table states
#[derive(Clone)]
pub struct NotifyingStore {
    inner: Arc<RwLock<Inner>>,
}

impl NotifyingStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
            table_state_conditions: Vec::new(),
            method_call_notifiers: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get_table_replication_states(&self) -> HashMap<TableId, TableReplicationPhase> {
        let inner = self.inner.read().await;
        inner.table_replication_states.clone()
    }

    pub async fn get_table_schemas(&self) -> HashMap<TableId, TableSchema> {
        let inner = self.inner.read().await;
        inner
            .table_schemas
            .iter()
            .map(|(id, schema)| (*id, Arc::as_ref(schema).clone()))
            .collect()
    }

    pub async fn notify_on_table_state(
        &self,
        table_id: TableId,
        expected_state: TableReplicationPhaseType,
    ) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_conditions
            .push((table_id, expected_state, notify.clone()));

        // Checking conditions here as well because it is possible that the state
        // the conditions are checking for is already reached by the time
        // this method is called, in which case this notification will not ever
        // fire if conditions are not checked here.
        inner.check_conditions().await;

        notify
    }

    pub async fn reset_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);

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
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
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

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;

        // Store the current state in history before updating
        if let Some(current_state) = inner.table_replication_states.get(&table_id).cloned() {
            inner
                .table_state_history
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(current_state);
        }

        inner.table_replication_states.insert(table_id, state);
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
        inner.check_conditions().await;

        inner
            .dispatch_method_notification(StateStoreMethod::RollbackTableReplicationState)
            .await;

        Ok(previous_state)
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let inner = self.inner.read().await;
        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.read().await;
        Ok(inner.table_mappings.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        Ok(inner.table_mappings.len())
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);
        Ok(())
    }
}

impl SchemaStore for NotifyingStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.read().await;
        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.write().await;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));

        Ok(())
    }
}

impl CleanupStore for NotifyingStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.write().await;

        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);
        inner.table_schemas.remove(&table_id);
        inner.table_mappings.remove(&table_id);

        Ok(())
    }
}

impl fmt::Debug for NotifyingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyingStore").finish()
    }
}
