use std::{collections::HashMap, fmt, sync::Arc};

use postgres::schema::TableId;
use tokio::{
    runtime::Handle,
    sync::{Notify, RwLock},
};

use crate::state::{
    store::base::{StateStore, StateStoreError},
    table::{TableReplicationPhase, TableReplicationPhaseType},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableReplicationState,
    GetTableReplicationStates,
    LoadTableReplicationStates,
    StoreTableReplicationState,
}

struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
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
pub struct NotifyingStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl NotifyingStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
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
        // the conditions are checking for is already reached by the time the
        // this method is called, in which case this notification will not ever
        // fire if conditions are not checked here.
        inner.check_conditions().await;

        notify
    }
}

impl Default for NotifyingStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for NotifyingStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.get(&table_id).cloned());

        inner
            .dispatch_method_notification(StateStoreMethod::GetTableReplicationState)
            .await;

        result
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.clone());

        inner
            .dispatch_method_notification(StateStoreMethod::GetTableReplicationStates)
            .await;

        result
    }

    async fn load_table_replication_states(&self) -> Result<usize, StateStoreError> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.clone());

        inner
            .dispatch_method_notification(StateStoreMethod::LoadTableReplicationStates)
            .await;

        result.map(|states| states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError> {
        let mut inner = self.inner.write().await;
        inner.table_replication_states.insert(table_id, state);
        inner.check_conditions().await;
        inner
            .dispatch_method_notification(StateStoreMethod::StoreTableReplicationState)
            .await;
        Ok(())
    }
}

impl fmt::Debug for NotifyingStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestStateStore")
            .field("table_replication_states", &inner.table_replication_states)
            .finish()
    }
}
