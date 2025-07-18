use etl::state::store::base::{StateStore, StateStoreError};
use etl::state::table::TableReplicationPhase;
use postgres::schema::TableId;
use std::collections::HashMap;
use std::sync::Arc;

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
