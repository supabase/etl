//! A syncing table whose sync worker is running.
//!
//! [`ActiveTable`] holds the worker's state lock from observation through
//! transition. Its catchup internals are private to this module, which keeps
//! two rules out of the callers' hands:
//!
//! - Storing `Catchup` and waiting for the worker are one operation,
//!   [`ActiveTable::execute`]. A caller cannot store `Catchup` and then read
//!   more WAL, and a caller without an [`ActivityHandle`] cannot enter catchup
//!   at all.
//! - The wait runs on a [`CatchupWait`], which holds no lock. Building one
//!   consumes the [`ActiveTable`], so the apply worker cannot wait while
//!   holding the lock the table-sync worker needs to reach `SyncDone`.

use tokio_postgres::types::PgLsn;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    activity::ActivityHandle,
    error::EtlResult,
    replication::{
        WorkerType,
        apply::{
            ExitIntent,
            handoff::{
                decision::{ActiveAction, ReadyProof},
                log_ready_transition,
            },
        },
        state::{TableState, TableStateType},
    },
    runtime::{TableSyncWorkerState, TableSyncWorkerStateGuard, concurrency::ShutdownResult},
    schema::TableId,
    store::StateStore,
};

/// Why the apply worker is waiting for table-sync catchup.
#[derive(Debug)]
enum CatchupWaitReason {
    /// The apply worker just moved the table-sync worker into catchup.
    EnteredCatchup,
    /// The apply worker found the table-sync worker already in catchup.
    AlreadyInCatchup,
}

/// Wait for a table-sync worker in catchup to hand its table back.
///
/// It holds no state lock, so the table-sync worker can reach `SyncDone`.
struct CatchupWait {
    /// Table the worker synchronizes.
    table_id: TableId,
    /// Shared state of the running worker.
    worker_state: TableSyncWorkerState,
    /// LSN the worker streams up to before `SyncDone`.
    catchup_lsn: PgLsn,
    /// How the worker came to be in catchup.
    reason: CatchupWaitReason,
}

impl CatchupWait {
    /// Waits until the table-sync worker reaches `SyncDone` or `Errored`.
    ///
    /// The table-sync worker owns the table through `SyncDone`; reaching it
    /// releases the apply loop. An owned relation can rebuild the decoder
    /// using the handover boundary, while relation-less DML can restore the
    /// complete stored decoder on demand. Returns [`ExitIntent::Pause`] when
    /// shutdown interrupts the wait.
    async fn wait(
        self,
        shutdown_token: &CancellationToken,
        activity_handle: &ActivityHandle,
    ) -> Option<ExitIntent> {
        let Self { table_id, worker_state, catchup_lsn, reason } = self;
        let catchup_state = match reason {
            CatchupWaitReason::EnteredCatchup => "entered",
            CatchupWaitReason::AlreadyInCatchup => "already_in",
        };

        info!(
            worker_type = %WorkerType::Apply,
            table_id = table_id.0,
            %catchup_lsn,
            catchup_state,
            "apply worker blocking until table sync worker reaches sync_done",
        );

        // We wait for both states since if the table sync worker errors, we
        // don't want to stall forever.
        let _activity_suspension = activity_handle.suspend();
        let result = worker_state
            .wait_for_state_type(
                &[TableStateType::SyncDone, TableStateType::Errored],
                shutdown_token.clone(),
            )
            .await;

        match result {
            ShutdownResult::Ok(result) => {
                let final_state = result.table_state();
                if final_state.as_type().is_errored() {
                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %final_state.as_type(),
                        "apply worker unblocked: table sync worker errored, skipping table",
                    );

                    return None;
                }

                info!(
                    worker_type = %WorkerType::Apply,
                    table_id = table_id.0,
                    table_state_type = %final_state.as_type(),
                    "apply worker unblocked: table sync worker reached sync_done",
                );

                None
            }
            ShutdownResult::Shutdown(_) => {
                info!(
                    worker_type = %WorkerType::Apply,
                    table_id = table_id.0,
                    "apply worker unblocked: shutdown signal received while waiting for table sync worker",
                );

                Some(ExitIntent::Pause)
            }
        }
    }
}

/// A syncing table whose sync worker is running, observed under the worker's
/// state lock.
///
/// Holding the lock makes the observed state and any transition one atomic
/// step.
pub(super) struct ActiveTable {
    /// Table the worker synchronizes.
    table_id: TableId,
    /// Shared state of the running worker.
    worker_state: TableSyncWorkerState,
    /// Lock on `worker_state`, held from observation through transition.
    guard: TableSyncWorkerStateGuard,
}

impl ActiveTable {
    /// Locks the state of a running worker.
    pub(super) async fn lock(table_id: TableId, worker_state: TableSyncWorkerState) -> Self {
        let guard = worker_state.lock_owned().await;

        Self { table_id, worker_state, guard }
    }

    /// Returns the worker's current table state.
    pub(super) fn table_state(&self) -> TableState {
        self.guard.table_state()
    }

    /// Carries out a handoff action, storing any new state under the lock.
    ///
    /// A catchup action releases the lock and then waits for the table-sync
    /// worker. Returns an exit intent when shutdown interrupts that wait.
    pub(super) async fn execute<S: StateStore>(
        mut self,
        action: ActiveAction,
        store: &S,
        shutdown_token: &CancellationToken,
        activity_handle: &ActivityHandle,
    ) -> EtlResult<Option<ExitIntent>> {
        let catchup_wait = match action {
            ActiveAction::StartCatchup { snapshot_lsn, current_lsn, catchup_lsn } => {
                info!(
                    worker_type = %WorkerType::Apply,
                    table_id = self.table_id.0,
                    %current_lsn,
                    %snapshot_lsn,
                    %catchup_lsn,
                    "transitioning sync_wait -> catchup",
                );

                self.guard.set_and_store(TableState::Catchup { lsn: catchup_lsn }, store).await?;

                self.into_catchup_wait(catchup_lsn, CatchupWaitReason::EnteredCatchup)
            }
            ActiveAction::AwaitCatchup { catchup_lsn } => {
                self.into_catchup_wait(catchup_lsn, CatchupWaitReason::AlreadyInCatchup)
            }
            ActiveAction::PromoteToReady(proof) => {
                self.promote_to_ready(proof, store).await?;

                return Ok(None);
            }
            ActiveAction::Keep => return Ok(None),
        };

        Ok(catchup_wait.wait(shutdown_token, activity_handle).await)
    }

    /// Stores `Ready` under the lock.
    pub(super) async fn promote_to_ready<S: StateStore>(
        mut self,
        proof: ReadyProof,
        store: &S,
    ) -> EtlResult<()> {
        log_ready_transition(self.table_id, &proof);

        self.guard.set_and_store(TableState::Ready, store).await
    }

    /// Releases the lock and returns the catchup wait.
    fn into_catchup_wait(self, catchup_lsn: PgLsn, reason: CatchupWaitReason) -> CatchupWait {
        let Self { table_id, worker_state, guard } = self;
        drop(guard);

        CatchupWait { table_id, worker_state, catchup_lsn, reason }
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, time::Duration};

    use futures::FutureExt;
    use tokio_postgres::types::PgLsn;
    use tokio_util::sync::CancellationToken;

    use crate::{
        activity::{ActivityKind, ActivityRegistration},
        error::ErrorKind,
        etl_error,
        replication::{
            apply::{
                ExitIntent,
                handoff::{
                    active_table::ActiveTable,
                    decision::{ActiveAction, ReadinessInputs},
                },
            },
            state::{TableRetryPolicy, TableState},
        },
        runtime::TableSyncWorkerState,
        schema::TableId,
        store::{MemoryStore, StateStore},
    };

    /// Registers apply activity whose handle a catchup wait can suspend.
    fn apply_activity() -> ActivityRegistration {
        ActivityRegistration::register(ActivityKind::WalApply {
            wal_sender_timeout: Duration::from_secs(60),
        })
    }

    #[tokio::test]
    async fn catchup_blocks_until_table_sync_hands_back_without_holding_the_lock() {
        let store = MemoryStore::new();
        let shutdown_token = CancellationToken::new();
        let activity = apply_activity();
        let activity_handle = activity.handle();
        let table_id = TableId::new(1);
        let catchup_lsn = PgLsn::from(20);
        let final_states = [
            TableState::SyncDone { lsn: catchup_lsn, table_decoding_state: None },
            TableState::Errored {
                reason: "copy failed".to_owned(),
                solution: None,
                retry_policy: TableRetryPolicy::ManualRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Copy failed"),
            },
        ];

        for final_state in final_states {
            let worker_state = TableSyncWorkerState::new_for_test(
                table_id,
                TableState::SyncWait { lsn: PgLsn::from(10) },
            );
            let table = ActiveTable::lock(table_id, worker_state.clone()).await;
            let mut handoff = pin!(table.execute(
                ActiveAction::StartCatchup {
                    snapshot_lsn: PgLsn::from(10),
                    current_lsn: catchup_lsn,
                    catchup_lsn,
                },
                &store,
                &shutdown_token,
                &activity_handle,
            ));

            assert!(handoff.as_mut().now_or_never().is_none());

            // The table-sync worker must be able to take its state lock
            // while the apply worker waits.
            let mut worker =
                tokio::time::timeout(Duration::from_secs(5), worker_state.lock()).await.unwrap();
            assert_eq!(worker.table_state(), TableState::Catchup { lsn: catchup_lsn });
            worker.set(final_state);
            drop(worker);

            assert!(handoff.await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn catchup_wait_pauses_when_shutdown_interrupts_it() {
        let store = MemoryStore::new();
        let shutdown_token = CancellationToken::new();
        let activity = apply_activity();
        let activity_handle = activity.handle();
        let table_id = TableId::new(1);
        let catchup_lsn = PgLsn::from(20);
        let worker_state =
            TableSyncWorkerState::new_for_test(table_id, TableState::Catchup { lsn: catchup_lsn });
        let table = ActiveTable::lock(table_id, worker_state).await;
        let mut handoff = pin!(table.execute(
            ActiveAction::AwaitCatchup { catchup_lsn },
            &store,
            &shutdown_token,
            &activity_handle,
        ));

        assert!(handoff.as_mut().now_or_never().is_none());

        shutdown_token.cancel();

        assert!(matches!(handoff.await.unwrap(), Some(ExitIntent::Pause)));
    }

    #[tokio::test]
    async fn promotion_stores_ready_durably_and_for_waiters() {
        let store = MemoryStore::new();
        let shutdown_token = CancellationToken::new();
        let activity = apply_activity();
        let table_id = TableId::new(1);
        let sync_done_lsn = PgLsn::from(10);
        let worker_state = TableSyncWorkerState::new_for_test(
            table_id,
            TableState::SyncDone { lsn: sync_done_lsn, table_decoding_state: None },
        );
        let proof = ReadinessInputs::new(sync_done_lsn, Some(sync_done_lsn), true)
            .ready_proof(sync_done_lsn)
            .unwrap();
        let table = ActiveTable::lock(table_id, worker_state.clone()).await;

        let exit_intent = table
            .execute(
                ActiveAction::PromoteToReady(proof),
                &store,
                &shutdown_token,
                &activity.handle(),
            )
            .await
            .unwrap();

        assert!(exit_intent.is_none());
        assert_eq!(store.get_table_state(table_id).await.unwrap(), Some(TableState::Ready));
        assert_eq!(worker_state.lock().await.table_state(), TableState::Ready);
    }
}
