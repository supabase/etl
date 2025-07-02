use etl::v2::state::store::base::{StateStore, StateStoreError};
use etl::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use postgres::schema::TableId;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

type TableStateCondition = Box<dyn Fn(&TableReplicationPhase) -> bool + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    GetTableReplicationState,
    GetTableReplicationStates,
    LoadTableReplicationStates,
    StoreTableReplicationState,
}

struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    table_state_conditions: Vec<(TableId, TableStateCondition, Arc<Notify>)>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    async fn check_conditions(&mut self) {
        let table_states = self.table_replication_states.clone();
        self.table_state_conditions
            .retain(|(tid, condition, notify)| {
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

#[derive(Clone)]
pub struct TestStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl TestStateStore {
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

    pub async fn notify_on_replication_state<F>(
        &self,
        table_id: TableId,
        condition: F,
    ) -> Arc<Notify>
    where
        F: Fn(&TableReplicationPhase) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_conditions
            .push((table_id, Box::new(condition), notify.clone()));
        // Checking conditions here as well because it is possible that the state
        // the conditions are checking for is already reached by the time the
        // `notify_on_replication_state` method is called, in which case this
        // notification will not ever fire if conditions are not checked here.
        inner.check_conditions().await;

        notify
    }

    pub async fn notify_on_replication_phase(
        &self,
        table_id: TableId,
        phase_type: TableReplicationPhaseType,
    ) -> Arc<Notify> {
        self.notify_on_replication_state(table_id, move |state| state.as_type() == phase_type)
            .await
    }
}

impl StateStore for TestStateStore {
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

impl fmt::Debug for TestStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestStateStore")
            .field("table_replication_states", &inner.table_replication_states)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum FaultType {
    Panic,
    // Commenting out to fix clippy error because this is unused, comment in when this starts being used
    // Error,
}

#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    pub load_table_replication_state: Option<FaultType>,
    pub load_table_replication_states: Option<FaultType>,
    pub store_table_replication_state: Option<FaultType>,
}

#[derive(Debug, Clone)]
pub struct FaultInjectingStateStore<S>
where
    S: Clone,
{
    inner: S,
    config: Arc<FaultConfig>,
}

impl<S> FaultInjectingStateStore<S>
where
    S: Clone,
{
    pub fn wrap(inner: S, config: FaultConfig) -> Self {
        Self {
            inner,
            config: Arc::new(config),
        }
    }

    pub fn get_inner(&self) -> &S {
        &self.inner
    }

    fn trigger_fault(&self, fault: &Option<FaultType>) -> Result<(), StateStoreError> {
        if let Some(fault_type) = fault {
            match fault_type {
                FaultType::Panic => panic!("Fault injection: panic triggered"),
                // Commenting out to fix clippy error because this is unused, comment in when this starts being used
                // // We trigger a random error.
                // FaultType::Error => return Err(StateStoreError::TableReplicationStateNotFound),
            }
        }

        Ok(())
    }
}

impl<S> StateStore for FaultInjectingStateStore<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        self.trigger_fault(&self.config.load_table_replication_state)?;
        self.inner.get_table_replication_state(table_id).await
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        self.trigger_fault(&self.config.load_table_replication_states)?;
        self.inner.get_table_replication_states().await
    }

    async fn load_table_replication_states(&self) -> Result<usize, StateStoreError> {
        self.trigger_fault(&self.config.load_table_replication_states)?;
        self.inner.load_table_replication_states().await
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError> {
        self.trigger_fault(&self.config.store_table_replication_state)?;
        self.inner
            .update_table_replication_state(table_id, state)
            .await
    }
}
