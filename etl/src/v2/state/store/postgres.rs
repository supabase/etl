use std::{collections::HashMap, sync::Arc};

use config::shared::SourceConfig;
use postgres::schema::TableId;
use sqlx::{
    postgres::{types::Oid as SqlxOid, PgPoolOptions},
    prelude::{FromRow, Type},
    PgPool, Row,
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_postgres::types::PgLsn;

use crate::v2::{
    pipeline::PipelineId,
    replication::slot::get_slot_name,
    state::{
        store::base::{StateStore, StateStoreError},
        table::TableReplicationPhase,
    },
    workers::base::WorkerType,
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

impl TryFrom<TableReplicationPhase> for TableState {
    type Error = ToTableStateError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        Ok(match value {
            TableReplicationPhase::Init => TableState::Init,
            TableReplicationPhase::DataSync => TableState::DataSync,
            TableReplicationPhase::FinishedCopy => TableState::FinishedCopy,
            TableReplicationPhase::SyncDone { .. } => TableState::SyncDone,
            TableReplicationPhase::Ready => TableState::Ready,
            TableReplicationPhase::Skipped => TableState::Skipped,
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                return Err(ToTableStateError::InMemoryPhase)
            }
        })
    }
}

#[derive(Debug, FromRow)]
pub struct ReplicationStateRow {
    pub pipeline_id: i64,
    pub table_id: SqlxOid,
    pub state: TableState,
}

#[derive(Debug)]
struct Inner {
    table_states: HashMap<TableId, TableReplicationPhase>,
}

#[derive(Debug, Clone)]
pub struct PostgresStateStore {
    pipeline_id: PipelineId,
    source_config: SourceConfig,
    inner: Arc<RwLock<Inner>>,
}

impl PostgresStateStore {
    pub fn new(pipeline_id: PipelineId, source_config: SourceConfig) -> PostgresStateStore {
        let inner = Inner {
            table_states: HashMap::new(),
        };
        PostgresStateStore {
            pipeline_id,
            source_config,
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    async fn connect_to_source(&self) -> Result<PgPool, sqlx::Error> {
        let options = self
            .source_config
            .clone()
            .into_connection_config()
            .with_db();

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
            select pipeline_id, table_id, state
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
    ) -> sqlx::Result<()> {
        let pool = self.connect_to_source().await?;
        sqlx::query(
            r#"
            insert into etl.replication_state (pipeline_id, table_id, state)
            values ($1, $2, $3)
            on conflict (pipeline_id, table_id)
            do update set state = $3
        "#,
        )
        .bind(pipeline_id as i64)
        .bind(SqlxOid(table_id))
        .bind(state)
        .execute(&pool)
        .await?;

        Ok(())
    }

    async fn replication_phase_from_state(
        &self,
        pool: &PgPool,
        table_id: TableId,
        value: &TableState,
    ) -> Result<TableReplicationPhase, StateStoreError> {
        Ok(match value {
            TableState::Init => TableReplicationPhase::Init,
            TableState::DataSync => TableReplicationPhase::DataSync,
            TableState::FinishedCopy => TableReplicationPhase::FinishedCopy,
            TableState::SyncDone => {
                let lsn = self.get_slot_confirmed_flush_lsn(pool, table_id).await?;
                TableReplicationPhase::SyncDone { lsn }
            }
            TableState::Ready => TableReplicationPhase::Ready,
            TableState::Skipped => TableReplicationPhase::Skipped,
        })
    }

    async fn get_slot_confirmed_flush_lsn(
        &self,
        pool: &PgPool,
        table_id: TableId,
    ) -> Result<PgLsn, StateStoreError> {
        let slot_name = get_slot_name(self.pipeline_id, WorkerType::TableSync { table_id })?;

        let row = sqlx::query(
            r#"
            select confirmed_flush_lsn
            from pg_replication_slots
            where slot_name = $1;
            "#,
        )
        .bind(&slot_name)
        .fetch_optional(pool)
        .await?;

        if let Some(row) = row {
            let lsn_str: String = row.try_get("confirmed_flush_lsn")?;
            let lsn = lsn_str
                .parse::<PgLsn>()
                .map_err(|_| StateStoreError::InvalidConfirmedFlushLsn(lsn_str))?;
            Ok(lsn)
        } else {
            Err(StateStoreError::MissingSlot(slot_name))
        }
    }
}

#[async_trait::async_trait]
impl StateStore for PostgresStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.read().await;
        Ok(inner.table_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.read().await;
        Ok(inner.table_states.clone())
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        let pool = self.connect_to_source().await?;
        let replication_state_rows = self
            .get_all_replication_state_rows(&pool, self.pipeline_id)
            .await?;
        let mut table_states: HashMap<TableId, TableReplicationPhase> = HashMap::new();
        for row in replication_state_rows {
            let phase = self
                .replication_phase_from_state(&pool, row.table_id.0, &row.state)
                .await?;
            table_states.insert(row.table_id.0, phase);
        }
        let mut inner = self.inner.write().await;
        inner.table_states = table_states.clone();
        Ok(table_states)
    }

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError> {
        let table_state = state.try_into()?;
        self.update_replication_state(self.pipeline_id, table_id, table_state)
            .await?;
        let mut inner = self.inner.write().await;
        inner.table_states.insert(table_id, state);
        Ok(())
    }
}
