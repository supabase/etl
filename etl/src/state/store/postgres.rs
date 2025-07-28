use std::{collections::HashMap, sync::Arc};

use config::shared::PgConnectionConfig;
use postgres::replication::{
    TableReplicationState, TableReplicationStateRow, connect_to_source_database,
    get_table_replication_state_rows, update_replication_state,
};
use postgres::schema::TableId;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::pipeline::PipelineId;
use crate::state::store::base::StateStore;
use crate::state::table::TableReplicationPhase;
use crate::{bail, etl_error};

const NUM_POOL_CONNECTIONS: u32 = 1;

impl TryFrom<TableReplicationPhase> for (TableReplicationState, Option<String>) {
    type Error = EtlError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        Ok(match value {
            TableReplicationPhase::Init => (TableReplicationState::Init, None),
            TableReplicationPhase::DataSync => (TableReplicationState::DataSync, None),
            TableReplicationPhase::FinishedCopy => (TableReplicationState::FinishedCopy, None),
            TableReplicationPhase::SyncDone { lsn } => {
                (TableReplicationState::SyncDone, Some(lsn.to_string()))
            }
            TableReplicationPhase::Ready => (TableReplicationState::Ready, None),
            TableReplicationPhase::Skipped => (TableReplicationState::Skipped, None),
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                bail!(
                    ErrorKind::InvalidState,
                    "In-memory phase error",
                    "In-memory table replication phase can't be saved in the state store"
                );
            }
        })
    }
}

#[derive(Debug)]
struct Inner {
    table_states: HashMap<TableId, TableReplicationPhase>,
}

/// A state store which saves the replication state in the source
/// postgres database.
#[derive(Debug, Clone)]
pub struct PostgresStateStore {
    pipeline_id: PipelineId,
    source_config: PgConnectionConfig,
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStateStore {
    pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Self {
        let inner = Inner {
            table_states: HashMap::new(),
        };

        Self {
            pipeline_id,
            source_config,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn connect_to_source(&self) -> Result<PgPool, sqlx::Error> {
        let pool = connect_to_source_database(
            &self.source_config,
            NUM_POOL_CONNECTIONS,
            NUM_POOL_CONNECTIONS,
        )
        .await?;

        Ok(pool)
    }

    async fn get_all_replication_state_rows(
        &self,
        pool: &PgPool,
        pipeline_id: PipelineId,
    ) -> sqlx::Result<Vec<TableReplicationStateRow>> {
        get_table_replication_state_rows(pool, pipeline_id as i64).await
    }

    async fn update_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: TableId,
        state: TableReplicationState,
        sync_done_lsn: Option<String>,
    ) -> sqlx::Result<()> {
        // We connect to source database each time we update because we assume that
        // these updates will be infrequent. It has some overhead to establish a
        // connection, but it's better than holding a connection open for long periods
        // when there's little activity on it.
        let pool = self.connect_to_source().await?;
        update_replication_state(&pool, pipeline_id, table_id, state, sync_done_lsn).await
    }

    async fn replication_phase_from_state(
        &self,
        state: &TableReplicationState,
        sync_done_lsn: Option<String>,
    ) -> EtlResult<TableReplicationPhase> {
        Ok(match state {
            TableReplicationState::Init => TableReplicationPhase::Init,
            TableReplicationState::DataSync => TableReplicationPhase::DataSync,
            TableReplicationState::FinishedCopy => TableReplicationPhase::FinishedCopy,
            TableReplicationState::SyncDone => match sync_done_lsn {
                Some(lsn_str) => {
                    let lsn = lsn_str.parse::<PgLsn>().map_err(|_| {
                        etl_error!(
                            ErrorKind::ValidationError,
                            "Invalid LSN",
                            format!(
                                "Invalid confirmed flush lsn value in state store: {}",
                                lsn_str
                            )
                        )
                    })?;
                    TableReplicationPhase::SyncDone { lsn }
                }
                None => bail!(
                    ErrorKind::ValidationError,
                    "Missing LSN",
                    "Lsn can't be missing from the state store if state is 'SyncDone'"
                ),
            },
            TableReplicationState::Ready => TableReplicationPhase::Ready,
            TableReplicationState::Skipped => TableReplicationPhase::Skipped,
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

        let pool = self.connect_to_source().await?;
        let replication_state_rows = self
            .get_all_replication_state_rows(&pool, self.pipeline_id)
            .await?;
        let mut table_states: HashMap<TableId, TableReplicationPhase> = HashMap::new();
        for row in replication_state_rows {
            let phase = self
                .replication_phase_from_state(&row.state, row.sync_done_lsn)
                .await?;
            table_states.insert(TableId::new(row.table_id.0), phase);
        }
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
        let (table_state, sync_done_lsn) = state.try_into()?;
        self.update_replication_state(self.pipeline_id, table_id, table_state, sync_done_lsn)
            .await?;

        let mut inner = self.inner.lock().await;
        inner.table_states.insert(table_id, state);

        Ok(())
    }
}
