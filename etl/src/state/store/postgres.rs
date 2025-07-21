use std::{collections::HashMap, sync::Arc};

use config::shared::{IntoConnectOptions, PgConnectionConfig};
use postgres::schema::TableId;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions, types::Oid as SqlxTableId},
    prelude::{FromRow, Type},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::{
    pipeline::PipelineId,
    state::{
        store::base::{StateStore, StateStoreError},
        table::TableReplicationPhase,
    },
};

const NUM_POOL_CONNECTIONS: u32 = 1;

#[derive(Debug, Clone, Copy, Type, PartialEq)]
#[sqlx(type_name = "etl.table_state", rename_all = "snake_case")]
pub enum TableState {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone,
    Ready,
    Skipped,
}

#[derive(Debug, Error)]
pub enum ToTableStateError {
    #[error("In-memory table replication phase can't be saved in the state store")]
    InMemoryPhase,
}

impl TryFrom<TableReplicationPhase> for (TableState, Option<String>) {
    type Error = ToTableStateError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        Ok(match value {
            TableReplicationPhase::Init => (TableState::Init, None),
            TableReplicationPhase::DataSync => (TableState::DataSync, None),
            TableReplicationPhase::FinishedCopy => (TableState::FinishedCopy, None),
            TableReplicationPhase::SyncDone { lsn } => {
                (TableState::SyncDone, Some(lsn.to_string()))
            }
            TableReplicationPhase::Ready => (TableState::Ready, None),
            TableReplicationPhase::Skipped => (TableState::Skipped, None),
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                return Err(ToTableStateError::InMemoryPhase);
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum FromTableStateError {
    #[error("Lsn can't be missing from the state store if state is SyncDone")]
    MissingSyncDoneLsn,
}

#[derive(Debug, FromRow)]
pub struct ReplicationStateRow {
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableState,
    pub sync_done_lsn: Option<String>,
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
    pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> PostgresStateStore {
        let inner = Inner {
            table_states: HashMap::new(),
        };
        PostgresStateStore {
            pipeline_id,
            source_config,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn connect_to_source(&self) -> Result<PgPool, sqlx::Error> {
        let options: PgConnectOptions = self.source_config.with_db();

        let pool = PgPoolOptions::new()
            .max_connections(NUM_POOL_CONNECTIONS)
            .min_connections(NUM_POOL_CONNECTIONS)
            .connect_with(options)
            .await?;
        Ok(pool)
    }

    async fn get_all_replication_state_rows(
        &self,
        pool: &PgPool,
        pipeline_id: PipelineId,
    ) -> sqlx::Result<Vec<ReplicationStateRow>> {
        let states = sqlx::query_as::<_, ReplicationStateRow>(
            r#"
            select pipeline_id, table_id, state, sync_done_lsn
            from etl.replication_state
            where pipeline_id = $1
            "#,
        )
        .bind(pipeline_id as i64)
        .fetch_all(pool)
        .await?;

        Ok(states)
    }

    async fn update_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: TableId,
        state: TableState,
        sync_done_lsn: Option<String>,
    ) -> sqlx::Result<()> {
        // We connect to source database each time we update because we assume that
        // these updates will be infrequent. It has some overhead to establish a
        // connection but it's better than holding a connection open for long periods
        // when there's little activity on it.
        let pool = self.connect_to_source().await?;
        sqlx::query(
            r#"
            insert into etl.replication_state (pipeline_id, table_id, state, sync_done_lsn)
            values ($1, $2, $3, $4)
            on conflict (pipeline_id, table_id)
            do update set state = $3, sync_done_lsn = $4
        "#,
        )
        .bind(pipeline_id as i64)
        .bind(SqlxTableId(table_id))
        .bind(state)
        .bind(sync_done_lsn)
        .execute(&pool)
        .await?;

        Ok(())
    }

    async fn replication_phase_from_state(
        &self,
        state: &TableState,
        sync_done_lsn: Option<String>,
    ) -> Result<TableReplicationPhase, StateStoreError> {
        Ok(match state {
            TableState::Init => TableReplicationPhase::Init,
            TableState::DataSync => TableReplicationPhase::DataSync,
            TableState::FinishedCopy => TableReplicationPhase::FinishedCopy,
            TableState::SyncDone => match sync_done_lsn {
                Some(lsn_str) => {
                    let lsn = lsn_str
                        .parse::<PgLsn>()
                        .map_err(|_| StateStoreError::InvalidConfirmedFlushLsn(lsn_str))?;
                    TableReplicationPhase::SyncDone { lsn }
                }
                None => return Err(FromTableStateError::MissingSyncDoneLsn)?,
            },
            TableState::Ready => TableReplicationPhase::Ready,
            TableState::Skipped => TableReplicationPhase::Skipped,
        })
    }
}

impl StateStore for PostgresStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.table_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.table_states.clone())
    }

    async fn load_table_replication_states(&self) -> Result<usize, StateStoreError> {
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
            table_states.insert(row.table_id.0, phase);
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
    ) -> Result<(), StateStoreError> {
        let (table_state, sync_done_lsn) = state.try_into()?;
        self.update_replication_state(self.pipeline_id, table_id, table_state, sync_done_lsn)
            .await?;
        let mut inner = self.inner.lock().await;
        inner.table_states.insert(table_id, state);
        Ok(())
    }
}
