//! Execution of handoff actions for a syncing table whose sync worker is
//! running.
//!
//! [`execute_active_action`] receives the worker's
//! [`TableSyncWorkerStateGuard`] so state observation and transition stay
//! atomic. Waiting for catchup consumes that guard via
//! [`TableSyncWorkerStateGuard::wait_for_state_type`], releasing the lock so
//! the table-sync worker can reach `SyncDone`.

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
            handoff::{decision::HandoffAction, log_ready_transition},
        },
        state::{TableState, TableStateType},
    },
    runtime::{TableSyncWorkerStateGuard, concurrency::ShutdownResult},
    schema::TableId,
    store::StateStore,
};

/// Waits until the table-sync worker reaches `SyncDone` or `Errored`.
///
/// Consumes `guard` so the lock is released while waiting. The table-sync
/// worker owns the table through `SyncDone`; reaching it releases the apply
/// loop. Returns [`ExitIntent::Pause`] when shutdown interrupts the wait.
async fn wait_for_table_sync_worker_catchup(
    table_id: TableId,
    guard: TableSyncWorkerStateGuard<'_>,
    catchup_lsn: PgLsn,
    catchup_state: &'static str,
    shutdown_token: &CancellationToken,
    activity_handle: &ActivityHandle,
) -> Option<ExitIntent> {
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
    let result = guard
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

/// Carries out a handoff action for a table whose sync worker is running,
/// storing any new state under `guard`.
///
/// A catchup action consumes `guard` to release the lock before waiting for
/// the table-sync worker. Returns [`ExitIntent::Pause`] when shutdown
/// interrupts that wait.
pub(super) async fn execute_active_action<S: StateStore>(
    table_id: TableId,
    mut guard: TableSyncWorkerStateGuard<'_>,
    action: HandoffAction,
    store: &S,
    shutdown_token: &CancellationToken,
    activity_handle: Option<&ActivityHandle>,
) -> EtlResult<Option<ExitIntent>> {
    match action {
        HandoffAction::StartCatchup { snapshot_lsn, current_lsn, catchup_lsn } => {
            let activity_handle =
                activity_handle.expect("catchup actions only run when an activity handle exists");

            info!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                %current_lsn,
                %snapshot_lsn,
                %catchup_lsn,
                "transitioning sync_wait -> catchup",
            );

            guard.set_and_store(TableState::Catchup { lsn: catchup_lsn }, store).await?;

            Ok(wait_for_table_sync_worker_catchup(
                table_id,
                guard,
                catchup_lsn,
                "entered",
                shutdown_token,
                activity_handle,
            )
            .await)
        }
        HandoffAction::AwaitCatchup { catchup_lsn } => {
            let activity_handle =
                activity_handle.expect("catchup actions only run when an activity handle exists");

            Ok(wait_for_table_sync_worker_catchup(
                table_id,
                guard,
                catchup_lsn,
                "already_in",
                shutdown_token,
                activity_handle,
            )
            .await)
        }
        HandoffAction::PromoteToReady { sync_done_lsn, current_lsn, persisted_checkpoint_lsn } => {
            log_ready_transition(table_id, sync_done_lsn, current_lsn, persisted_checkpoint_lsn);

            guard.set_and_store(TableState::Ready, store).await?;

            Ok(None)
        }
        HandoffAction::Keep => Ok(None),
        HandoffAction::SpawnWorker => {
            unreachable!("active table never spawns a duplicate sync worker")
        }
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
                handoff::{active_table::execute_active_action, decision::HandoffAction},
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
            let guard = worker_state.lock().await;
            let mut handoff = pin!(execute_active_action(
                table_id,
                guard,
                HandoffAction::StartCatchup {
                    snapshot_lsn: PgLsn::from(10),
                    current_lsn: catchup_lsn,
                    catchup_lsn,
                },
                &store,
                &shutdown_token,
                Some(&activity_handle),
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
        let guard = worker_state.lock().await;
        let mut handoff = pin!(execute_active_action(
            table_id,
            guard,
            HandoffAction::AwaitCatchup { catchup_lsn },
            &store,
            &shutdown_token,
            Some(&activity_handle),
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
        let guard = worker_state.lock().await;

        let exit_intent = execute_active_action(
            table_id,
            guard,
            HandoffAction::PromoteToReady {
                sync_done_lsn,
                current_lsn: sync_done_lsn,
                persisted_checkpoint_lsn: sync_done_lsn,
            },
            &store,
            &shutdown_token,
            Some(&activity.handle()),
        )
        .await
        .unwrap();

        assert!(exit_intent.is_none());
        assert_eq!(store.get_table_state(table_id).await.unwrap(), Some(TableState::Ready));
        assert_eq!(worker_state.lock().await.table_state(), TableState::Ready);
    }
}
