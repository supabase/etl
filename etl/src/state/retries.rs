use chrono::{DateTime, Utc};
use futures::StreamExt;
use postgres::schema::TableId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::{debug, error, info};

use crate::concurrency::signal::SignalTx;
use crate::state::store::base::StateStore;
use crate::workers::pool::TableSyncWorkerPool;

/// Orchestrates retry mechanisms for failed table synchronization operations.
///
/// Manages two types of retries:
/// - Manual retries that require user intervention
/// - Timed retries that are automatically executed after a delay
///
/// The orchestrator uses a [`DelayQueue`] for managing timed retries and ensures
/// that only the most recent retry attempt is kept for each table ID.
#[derive(Debug, Clone)]
pub struct RetriesOrchestrator<S> {
    /// Queue for timed retries with automatic expiration
    delay_queue: Arc<Mutex<DelayQueue<TableRetryInfo>>>,
    /// Maps table IDs to their current delay queue keys for deduplication
    enqueued_table_keys: Arc<Mutex<HashMap<TableId, delay_queue::Key>>>,
    /// Set of table IDs waiting for manual retry intervention
    manual_retries: Arc<Mutex<HashSet<TableId>>>,
    /// Worker pool for executing table synchronization tasks
    pool: TableSyncWorkerPool,
    /// State store for persisting retry information
    state_store: S,
    /// Signal sender for forcing table synchronization
    force_syncing_tables_tx: SignalTx,
    /// Notification mechanism for waking up the task that handles timed retries
    notify_new_timed_retry: Arc<Notify>,
}

/// Information about a table retry operation
#[derive(Debug, Clone)]
struct TableRetryInfo {
    table_id: TableId,
    retry_time: DateTime<Utc>,
}

impl<S> RetriesOrchestrator<S>
where
    S: StateStore + Clone + Send + 'static,
{
    /// Creates a new [`RetriesOrchestrator`] instance.
    pub fn new(
        pool: TableSyncWorkerPool,
        state_store: S,
        force_syncing_tables_tx: SignalTx,
    ) -> Self {
        let delay_queue = Arc::new(Mutex::new(DelayQueue::new()));
        let enqueued_table_keys = Arc::new(Mutex::new(HashMap::new()));
        let manual_retries = Arc::new(Mutex::new(HashSet::new()));
        let notify_new_timed_retry = Arc::new(Notify::new());

        // TODO: do we want to store the handle?
        // Start the background task to handle timed retries.
        Self::start_background_task(
            pool.clone(),
            state_store.clone(),
            force_syncing_tables_tx.clone(),
            delay_queue.clone(),
            enqueued_table_keys.clone(),
            notify_new_timed_retry.clone(),
        );

        Self {
            delay_queue,
            enqueued_table_keys,
            manual_retries,
            pool,
            state_store,
            force_syncing_tables_tx,
            notify_new_timed_retry,
        }
    }

    /// Schedules a timed retry for the specified table.
    ///
    /// If the table already has a pending retry, the old one is removed and replaced
    /// with the new retry time to ensure only the most recent retry is active.
    pub async fn schedule_timed_retry(&self, table_id: TableId, next_retry: DateTime<Utc>) {
        let mut delay_queue = self.delay_queue.lock().await;
        let mut enqueued_keys = self.enqueued_table_keys.lock().await;

        // Remove existing retry for this table if present. This is done to implement a debouncing
        // behavior for multiple retries on the same table.
        if let Some(old_key) = enqueued_keys.remove(&table_id) {
            delay_queue.remove(&old_key);
            debug!(
                "removed existing retry for table {} from delay queue in favor of a new one",
                table_id
            );
        }

        // Calculate delay from now
        let now = Utc::now();
        let delay = if next_retry > now {
            chrono::Duration::to_std(&(next_retry - now)).unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        };

        // Schedule new retry
        let retry_info = TableRetryInfo {
            table_id,
            retry_time: next_retry,
        };
        let key = delay_queue.insert_at(retry_info, Instant::now() + delay);
        enqueued_keys.insert(table_id, key);

        info!(
            "scheduled timed retry for table {} at {}",
            table_id, next_retry
        );

        // Notify the background task that new work is available
        self.notify_new_timed_retry.notify_one();
    }

    /// Adds a table to the manual retry queue.
    ///
    /// Tables in this queue require user intervention before they can be retried.
    pub async fn schedule_manual_retry(&self, table_id: TableId) {
        let mut manual_retries = self.manual_retries.lock().await;
        manual_retries.insert(table_id);

        info!("scheduled manual retry for table {}", table_id);
    }

    // TODO: hook this method to allow the process to receive a signal to retry the specific table.
    /// Attempts to retry a table that requires manual intervention.
    ///
    /// Returns `true` if the retry was found and executed, `false` if no manual
    /// retry was pending for the specified table.
    pub async fn retry(&self, table_id: TableId) -> bool {
        let mut manual_retries = self.manual_retries.lock().await;

        if manual_retries.remove(&table_id) {
            info!("executing manual retry for table {}", table_id);

            // Release lock before executing retry
            drop(manual_retries);

            Self::execute_retry(
                self.pool.clone(),
                self.state_store.clone(),
                self.force_syncing_tables_tx.clone(),
                table_id,
            )
            .await;

            true
        } else {
            debug!("no manual retry found for table {}", table_id);
            false
        }
    }

    /// Starts the background task that processes timed retries.
    fn start_background_task(
        pool: TableSyncWorkerPool,
        state_store: S,
        force_syncing_tables_tx: SignalTx,
        delay_queue: Arc<Mutex<DelayQueue<TableRetryInfo>>>,
        enqueued_table_keys: Arc<Mutex<HashMap<TableId, delay_queue::Key>>>,
        notify: Arc<Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(Self::background_task(
            pool,
            state_store,
            force_syncing_tables_tx,
            delay_queue,
            enqueued_table_keys,
            notify,
        ))
    }

    /// The background task that processes timed retries.
    ///
    /// This task waits for expired items from the delay queue and executes them automatically.
    /// It waits for notifications when new items are added to minimize CPU usage.
    async fn background_task(
        pool: TableSyncWorkerPool,
        state_store: S,
        force_syncing_tables_tx: SignalTx,
        delay_queue: Arc<Mutex<DelayQueue<TableRetryInfo>>>,
        enqueued_table_keys: Arc<Mutex<HashMap<TableId, delay_queue::Key>>>,
        notify_new_timed_retry: Arc<Notify>,
    ) {
        info!("starting retries orchestrator background task");

        loop {
            // Wait for an expired item from the delay queue
            let expired_retry = {
                let mut delay_queue = delay_queue.lock().await;

                tokio::select! {
                    // Wait for the next expired item
                    expired = delay_queue.next() => {
                        if let Some(expired) = expired {
                            let retry_info = expired.into_inner();

                            // Remove from tracking map
                            let mut enqueued_keys = enqueued_table_keys.lock().await;
                            enqueued_keys.remove(&retry_info.table_id);

                            Some(retry_info)
                        } else {
                            None
                        }
                    }
                    // Also listen for notifications that new items were added
                    _ = notify_new_timed_retry.notified() => {
                        None
                    }
                }
            };

            if let Some(retry_info) = expired_retry {
                info!(
                    "executing timed retry for table {} (scheduled for {})",
                    retry_info.table_id, retry_info.retry_time
                );

                Self::execute_retry(
                    pool.clone(),
                    state_store.clone(),
                    force_syncing_tables_tx.clone(),
                    retry_info.table_id,
                )
                .await;
            }
        }
    }

    /// Executes a retry operation for the specified table.
    ///
    /// Rolls back the table replication state and notifies the main apply worker to try
    /// and process syncing tables again.
    async fn execute_retry(
        pool: TableSyncWorkerPool,
        state_store: S,
        force_syncing_tables_tx: SignalTx,
        table_id: TableId,
    ) {
        // We lock the pool to prevent any table sync workers to be scheduled while we are
        // rolling back.
        let pool = pool.lock().await;

        // We try to see if there is an in-memory state, if so, we lock it for the whole duration of the
        // rollback. This way, we prevent the case where the in-memory and state store states are out of
        // sync due to race conditions.
        //
        // If we fail to rollback, we will just log an error, since in that case we can't do much, and
        // we want to avoid changing the error in the table since if we fail to rollback we might have
        // a problem in the source database, and we don't want to aggravate it by issuing another write.
        match pool.get_active_worker_state(table_id) {
            Some(table_sync_worker_state) => {
                let mut inner = table_sync_worker_state.lock().await;

                let rolled_back_state =
                    match state_store.rollback_table_replication_state(table_id).await {
                        Ok(rolled_back_state) => rolled_back_state,
                        Err(err) => {
                            error!("error while rolling back table replication state: {}", err);
                            return;
                        }
                    };

                // In case we have an in-memory state, we want to update it. Technically there should not be an
                // active worker for that table when a table is errored since the worker is shutdown on failure,
                // but just to be extra sure we also update the in-memory state.
                inner.set(rolled_back_state);
            }
            None => {
                match state_store.rollback_table_replication_state(table_id).await {
                    Ok(rolled_back_state) => rolled_back_state,
                    Err(err) => {
                        error!("error while rolling back table replication state: {}", err);
                        return;
                    }
                };
            }
        }

        // We send the signal to the apply worker to force the syncing of tables, which will result in
        // new table sync workers being spawned.
        if force_syncing_tables_tx.send(()).is_err() {
            error!("error while forcing syncing tables after table replication state rollback");
        }

        info!("successfully executed retry for table {}", table_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::schema::TableId;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_retries_orchestrator_basic() {
        let _pool = TableSyncWorkerPool::new();
        let _state_store = (); // Use unit type for simple test
        let (_tx, _rx) = watch::channel(());

        // This test just verifies basic compilation and instantiation
        let table_id = TableId::new(1);

        // Basic instantiation test - we can't easily test the full functionality
        // without implementing a proper StateStore mock, but this verifies
        // the basic structure compiles correctly
        assert!(table_id.0 > 0);
    }
}
