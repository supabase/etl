use std::{collections::HashMap, sync::Arc};

use config::shared::PgConnectionConfig;
use postgres::replication::{connect_to_source_database, schema, state};
use postgres::schema::{TableId, TableSchema};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::state::table::{RetryPolicy, TableReplicationPhase};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::{bail, etl_error};

const NUM_POOL_CONNECTIONS: u32 = 1;

impl TryFrom<TableReplicationPhase> for state::TableReplicationState {
    type Error = EtlError;

    fn try_from(value: TableReplicationPhase) -> Result<Self, Self::Error> {
        match value {
            TableReplicationPhase::Init => Ok(state::TableReplicationState::Init),
            TableReplicationPhase::DataSync => Ok(state::TableReplicationState::DataSync),
            TableReplicationPhase::FinishedCopy => Ok(state::TableReplicationState::FinishedCopy),
            TableReplicationPhase::SyncDone { lsn } => {
                Ok(state::TableReplicationState::SyncDone { lsn })
            }
            TableReplicationPhase::Ready => Ok(state::TableReplicationState::Ready),
            TableReplicationPhase::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                // Convert ETL RetryPolicy to postgres RetryPolicy
                let db_retry_policy = match retry_policy {
                    RetryPolicy::NoRetry => state::RetryPolicy::NoRetry,
                    RetryPolicy::ManualRetry => state::RetryPolicy::ManualRetry,
                    RetryPolicy::TimedRetry { next_retry } => {
                        state::RetryPolicy::TimedRetry { next_retry }
                    }
                };

                Ok(state::TableReplicationState::Errored {
                    reason,
                    solution,
                    retry_policy: db_retry_policy,
                })
            }
            TableReplicationPhase::SyncWait | TableReplicationPhase::Catchup { .. } => {
                bail!(
                    ErrorKind::InvalidState,
                    "In-memory phase error",
                    "In-memory table replication phase can't be saved in the state store"
                );
            }
        }
    }
}

impl TryFrom<state::TableReplicationStateRow> for TableReplicationPhase {
    type Error = EtlError;

    fn try_from(value: state::TableReplicationStateRow) -> Result<Self, Self::Error> {
        // Parse the metadata field from the row, which contains all the data we need to build the
        // replication phase
        let Some(table_replication_state) = value.deserialize_metadata().map_err(|err| {
            etl_error!(
                ErrorKind::DeserializationError,
                "Failed to deserialize table replication state",
                format!(
                    "Failed to deserialize table replication state from metadata column in Postgres: {err}"
                )
            )
        })?
        else {
            bail!(
                ErrorKind::InvalidState,
                "The table replication state does not exist",
                "The table replication state does not exist in the metadata column in Postgres"
            );
        };

        // Convert postgres state to phase (they are the same structs but one is meant to represent
        // only the state which can be saved in the db).
        match table_replication_state {
            state::TableReplicationState::Init => Ok(TableReplicationPhase::Init),
            state::TableReplicationState::DataSync => Ok(TableReplicationPhase::DataSync),
            state::TableReplicationState::FinishedCopy => Ok(TableReplicationPhase::FinishedCopy),
            state::TableReplicationState::SyncDone { lsn } => {
                Ok(TableReplicationPhase::SyncDone { lsn })
            }
            state::TableReplicationState::Ready => Ok(TableReplicationPhase::Ready),
            state::TableReplicationState::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                let etl_retry_policy = match retry_policy {
                    state::RetryPolicy::NoRetry => RetryPolicy::NoRetry,
                    state::RetryPolicy::ManualRetry => RetryPolicy::ManualRetry,
                    state::RetryPolicy::TimedRetry { next_retry } => {
                        RetryPolicy::TimedRetry { next_retry }
                    }
                };

                Ok(TableReplicationPhase::Errored {
                    reason,
                    solution,
                    retry_policy: etl_retry_policy,
                })
            }
        }
    }
}

#[derive(Debug)]
struct Inner {
    table_states: HashMap<TableId, TableReplicationPhase>,
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
}

/// A state store which saves the replication state in the source
/// postgres database.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pipeline_id: PipelineId,
    source_config: PgConnectionConfig,
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStore {
    pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Self {
        let inner = Inner {
            table_states: HashMap::new(),
            table_schemas: HashMap::new(),
        };

        Self {
            pipeline_id,
            source_config,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn connect_to_source(&self) -> Result<PgPool, sqlx::Error> {
        // We connect to source database each time we update because we assume that
        // these updates will be infrequent. It has some overhead to establish a
        // connection, but it's better than holding a connection open for long periods
        // when there's little activity on it.
        let pool = connect_to_source_database(
            &self.source_config,
            NUM_POOL_CONNECTIONS,
            NUM_POOL_CONNECTIONS,
        )
        .await?;

        Ok(pool)
    }
}

impl StateStore for PostgresStore {
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

        let replication_state_rows =
            state::get_table_replication_state_rows(&pool, self.pipeline_id as i64).await?;

        let mut table_states: HashMap<TableId, TableReplicationPhase> = HashMap::new();
        for row in replication_state_rows {
            let table_id = TableId::new(row.table_id.0);
            let phase: TableReplicationPhase = row.try_into()?;
            table_states.insert(table_id, phase);
        }

        // For performance reasons, since we load the replication states only once during startup
        // and from a single thread, we can afford to have a super short critical section.
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
        let db_state: state::TableReplicationState = state.clone().try_into()?;

        let pool = self.connect_to_source().await?;

        // We lock the inner state before updating the state in the database, to make sure we are
        // consistent. If we were to lock the states only after the db state is modified, we might
        // be inconsistent since there are some interleaved executions that lead to a wrong state.
        let mut inner = self.inner.lock().await;
        state::update_replication_state(&pool, self.pipeline_id as i64, table_id, db_state).await?;
        inner.table_states.insert(table_id, state);

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let pool = self.connect_to_source().await?;

        // Here we perform locking for the same reasons stated in `update_table_replication_state`.
        let mut inner = self.inner.lock().await;
        match state::rollback_replication_state(&pool, self.pipeline_id as i64, table_id).await? {
            Some(restored_row) => {
                let restored_phase: TableReplicationPhase = restored_row.try_into()?;
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

impl SchemaStore for PostgresStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        debug!("loading table schemas from postgres state store");

        let pool = self.connect_to_source().await?;

        let table_schemas = schema::load_table_schemas(&pool, self.pipeline_id as i64)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::QueryFailed,
                    "Failed to load table schemas",
                    format!("Failed to load table schemas from postgres: {err}")
                )
            })?;
        let table_schemas_len = table_schemas.len();

        // For performance reasons, since we load the table schemas only once during startup
        // and from a single thread, we can afford to have a super short critical section.
        let mut inner = self.inner.lock().await;
        inner.table_schemas.clear();
        for table_schema in table_schemas {
            inner
                .table_schemas
                .insert(table_schema.id, Arc::new(table_schema));
        }

        info!(
            "loaded {} table schemas from postgres state store",
            table_schemas_len
        );

        Ok(table_schemas_len)
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        debug!("storing table schema for table '{}'", table_schema.name);

        let pool = self.connect_to_source().await?;

        // We also lock the entire section to be consistent.
        let mut inner = self.inner.lock().await;
        schema::store_table_schema(&pool, self.pipeline_id as i64, &table_schema)
            .await
            .map_err(|err| {
                etl_error!(
                    ErrorKind::QueryFailed,
                    "Failed to store table schema",
                    format!("Failed to store table schema in postgres: {err}")
                )
            })?;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));

        Ok(())
    }
}
