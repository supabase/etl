use postgres::schema::Oid;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::v2::destination::base::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::{Worker, WorkerError, WorkerHandle};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::TableSyncWorker;

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) {}

    async fn wait(mut self) -> Result<(), WorkerError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    state_store: S,
    destination: D,
    pool: TableSyncWorkerPool,
}

impl<S, D> ApplyWorker<S, D> {
    pub fn new(state_store: S, destination: D, pool: TableSyncWorkerPool) -> Self {
        Self {
            state_store,
            destination,
            pool,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn start(self) -> Option<ApplyWorkerHandle> {
        info!("Starting apply worker");

        let apply_worker = async move {
            // We load the initial state that will be used for the apply worker.
            let pipeline_state = self.state_store.load_pipeline_state().await;
            info!(
                "Loaded initial pipeline state with LSN: {}",
                pipeline_state.last_lsn
            );

            // We start the applying loop by starting from the last LSN that we know was applied
            // by the destination.
            start_apply_loop(
                self.state_store,
                self.destination,
                Hook::new(self.pool),
                pipeline_state.last_lsn,
            )
            .await;
        };

        let handle = tokio::spawn(apply_worker);

        Some(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

#[derive(Debug)]
struct Hook {
    pool: TableSyncWorkerPool,
}

impl Hook {
    fn new(pool: TableSyncWorkerPool) -> Self {
        Self { pool }
    }
}

impl<S, D> ApplyLoopHook<S, D> for Hook
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn process_syncing_tables(&self, state_store: S, destination: D, current_lsn: PgLsn) {
        let table_replication_states = state_store.load_table_replication_states().await;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for table_replication_state in table_replication_states {
            if let TableReplicationPhase::SyncDone { lsn } = table_replication_state.phase {
                if current_lsn >= lsn {
                    let table_replication_state = table_replication_state
                        .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                    state_store
                        .store_table_replication_state(table_replication_state)
                        .await;
                }
            } else {
                {
                    let pool = self.pool.read().await;
                    if let Some(table_sync_worker_state) =
                        pool.get_worker_state(table_replication_state.id)
                    {
                        let mut catchup_started = false;
                        let mut inner = table_sync_worker_state.inner().write().await;
                        if inner.phase().as_type() == TableReplicationPhaseType::SyncWait {
                            inner
                                .set_phase_with(
                                    TableReplicationPhase::Catchup { lsn: current_lsn },
                                    state_store.clone(),
                                )
                                .await;
                            catchup_started = true;
                        }
                        drop(inner);

                        if catchup_started {
                            let _ = table_sync_worker_state
                                .wait_for_phase_type(TableReplicationPhaseType::SyncDone)
                                .await;
                        }

                        continue;
                    }
                }

                info!(
                    "Creating new sync worker for table {}",
                    table_replication_state.id
                );

                let worker = TableSyncWorker::new(
                    state_store.clone(),
                    destination.clone(),
                    table_replication_state.id,
                    self.pool.clone(),
                );

                let mut table_sync_workers = self.pool.write().await;
                table_sync_workers.start_worker(worker).await;
            }
        }
    }

    async fn should_apply_changes(&self, table_id: Oid, remote_final_lsn: PgLsn) -> bool {
        let pool = self.pool.read().await;
        let Some(table_sync_worker_state) = pool.get_worker_state(table_id) else {
            return false;
        };

        let inner = table_sync_worker_state.inner().read().await;
        match inner.phase() {
            TableReplicationPhase::Ready { .. } => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        }
    }
}
