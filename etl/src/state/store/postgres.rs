use std::{collections::HashMap, sync::Arc};

use config::shared::PgConnectionConfig;
use postgres::replication::{
    TableReplicationState, TableReplicationStateRow, connect_to_source_database,
    get_table_replication_state_rows, update_replication_state, rollback_replication_state,
};
use postgres::schema::TableId;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::state::store::StateStore;
use crate::state::table::{RetryPolicy, TableReplicationPhase};
use crate::types::PipelineId;
use crate::{bail, etl_error};

const NUM_POOL_CONNECTIONS: u32 = 3;

impl TryFrom<TableReplicationPhase> for (TableReplicationState, serde_json::Value) {
    type Error = EtlError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        let metadata = serde_json::to_value(&value).map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Serialization failed",
                format!("Failed to serialize TableReplicationPhase: {err}")
            )
        })?;
        
        let state = match value {
            TableReplicationPhase::Init => TableReplicationState::Init,
            TableReplicationPhase::DataSync => TableReplicationState::DataSync,
            TableReplicationPhase::FinishedCopy => TableReplicationState::FinishedCopy,
            TableReplicationPhase::SyncDone { .. } => TableReplicationState::SyncDone,
            TableReplicationPhase::Ready => TableReplicationState::Ready,
            TableReplicationPhase::Errored { .. } => TableReplicationState::Errored,
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                bail!(
                    ErrorKind::InvalidState,
                    "In-memory phase error",
                    "In-memory table replication phase can't be saved in the state store"
                );
            }
        };
        
        Ok((state, metadata))
    }
}

impl TryFrom<TableReplicationStateRow> for TableReplicationPhase {

    type Error = EtlError;

    fn try_from(value: TableReplicationStateRow) -> Result<Self, Self::Error> {
        if let Some(metadata) = &value.metadata {
            // Try to deserialize from the metadata JSONB
            match serde_json::from_value::<TableReplicationPhase>(metadata.clone()) {
                Ok(phase) => return Ok(phase),
                Err(e) => {
                    info!(
                        "Failed to deserialize metadata, falling back to legacy conversion: {}",
                        e
                    );
                }
            }
        }

        // Fallback to legacy conversion for backwards compatibility
        // This should only happen for very old data that was created before the metadata migration
        match value.state {
            TableReplicationState::Init => Ok(TableReplicationPhase::Init),
            TableReplicationState::DataSync => Ok(TableReplicationPhase::DataSync),
            TableReplicationState::FinishedCopy => Ok(TableReplicationPhase::FinishedCopy),
            TableReplicationState::SyncDone => {
                // For SyncDone without metadata, we can't recover the LSN, so create a default
                bail!(
                    ErrorKind::ValidationError,
                    "Missing metadata for SyncDone state",
                    "SyncDone state without metadata is not supported after migration"
                )
            },
            TableReplicationState::Ready => Ok(TableReplicationPhase::Ready),
            TableReplicationState::Errored => Ok(TableReplicationPhase::Errored {
                reason: "Legacy error state".to_string(),
                solution: Some("Check logs for more details".to_string()),
                retry_policy: RetryPolicy::NoRetry,
            }),
        }
    }
}

#[derive(Debug)]
struct Inner {
    table_states: HashMap<TableId, TableReplicationPhase>,
    pool: PgPool,
}

/// A state store which saves the replication state in the source
/// postgres database.
#[derive(Debug, Clone)]
pub struct PostgresStateStore {
    pipeline_id: PipelineId,
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStateStore {
    pub async fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Result<Self, sqlx::Error> {
        let pool = connect_to_source_database(
            &source_config,
            NUM_POOL_CONNECTIONS,
            NUM_POOL_CONNECTIONS,
        )
        .await?;

        let inner = Inner {
            table_states: HashMap::new(),
            pool,
        };

        Ok(Self {
            pipeline_id,
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl StateStore for PostgresStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        debug!("loading table replication states from postgres state store");

        // Perform database operation without holding the lock
        let pool = {
            let inner = self.inner.lock().await;
            inner.pool.clone()
        };
        let replication_state_rows = get_table_replication_state_rows(&pool, self.pipeline_id as i64).await?;
        
        let mut table_states: HashMap<TableId, TableReplicationPhase> = HashMap::new();
        for row in replication_state_rows {
            let phase = self.replication_phase_from_row(&row).await?;
            table_states.insert(TableId::new(row.table_id.0), phase);
        }
        
        // Update cache after processing all rows
        let mut inner = self.inner.lock().await;
        inner.table_states = table_states.clone();

        info!(
            "loaded {} table replication states from postgres state store",
            table_states.len()
        );

        Ok(table_states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let (table_state, metadata) = state.clone().try_into()?;
        
        // Perform database operation without holding the lock
        let pool = {
            let inner = self.inner.lock().await;
            inner.pool.clone()
        };
        update_replication_state(&pool, self.pipeline_id, table_id, table_state, metadata).await?;
        
        // Update cache after successful database operation
        let mut inner = self.inner.lock().await;
        inner.table_states.insert(table_id, state);

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        // Perform database operation without holding the lock
        let pool = {
            let inner = self.inner.lock().await;
            inner.pool.clone()
        };
        let result = rollback_replication_state(&pool, self.pipeline_id, table_id).await?;
        
        match result {
            Some(restored_row) => {
                let restored_phase = self.replication_phase_from_row(&restored_row).await?;

                // Update cache after successful database operation
                let mut inner = self.inner.lock().await;
                inner.table_states.insert(table_id, restored_phase.clone());
                
                Ok(restored_phase)
            }
            None => Err(etl_error!(
                ErrorKind::StateRollbackError,
                "No previous state found",
                "There is no previous state to rollback to for this table"
            )),
        }
    }
}
