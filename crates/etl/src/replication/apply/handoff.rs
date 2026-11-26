//! Table-sync handoff for the apply worker.
//!
//! A table-sync worker copies a table, waits in `SyncWait`, streams up to a
//! catchup LSN and stores `SyncDone`. The apply worker then takes the table
//! over and stores `Ready`. This module is the apply worker's side of that
//! exchange.
//!
//! The apply worker evaluates every syncing table after a COMMIT, after a
//! durable flush, and while quiescent. One evaluation observes the table once
//! and makes at most one transition:
//!
//! | Sync worker | Observed state | After COMMIT | After flush | Quiescent |
//! |---|---|---|---|---|
//! | running | `SyncWait(s)` | store `Catchup(max(s, current))`, wait | keep | as COMMIT |
//! | running | `Catchup(c)` | wait | keep | as COMMIT |
//! | running | `SyncDone(d)` | keep | store `Ready` if ready | as flush |
//! | running | any other | keep | keep | keep |
//! | none | `SyncDone(d)` | keep | store `Ready` if ready | as flush |
//! | none | any other | spawn worker | spawn worker | spawn worker |
//!
//! `current` is the LSN this connection has reached. "Wait" blocks the apply
//! loop until the worker stores `SyncDone` or `Errored`, or until shutdown.
//! "If ready" means [`ReadinessInputs::ready_checkpoint`] returned a persisted
//! checkpoint covering `d`.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use tokio_postgres::types::PgLsn;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    activity::ActivityHandle,
    destination::PipelineDestination,
    error::EtlResult,
    replication::{
        TableDecodingState, WorkerType,
        apply::{ApplyWorkerContext, ExitIntent, get_syncing_tables},
        state::{TableState, TableStateType},
    },
    runtime::{
        TableSyncWorker, TableSyncWorkerPool, TableSyncWorkerStateGuard,
        concurrency::ShutdownResult,
    },
    schema::TableId,
    store::{PipelineStore, StateStore},
};

/// Point at which the apply worker evaluates syncing tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// A COMMIT was processed.
    AfterCommit,
    /// A destination flush became durable.
    AfterFlush,
    /// The apply loop is quiescent.
    Quiescent,
}

impl Trigger {
    /// Returns the trigger's log label.
    fn as_str(self) -> &'static str {
        match self {
            Self::AfterCommit => "after_commit",
            Self::AfterFlush => "after_flush",
            Self::Quiescent => "quiescent",
        }
    }
}

/// Progress that decides whether a `SyncDone` table may become `Ready`.
#[derive(Debug, Clone, Copy)]
struct ReadinessInputs {
    /// Apply checkpoint persisted before this evaluation, if any.
    persisted_checkpoint_lsn: Option<PgLsn>,
    /// Whether this connection holds a complete decoder for the table.
    has_materialized_decoding_state: bool,
}

impl ReadinessInputs {
    /// Creates the readiness inputs for one table.
    fn new(persisted_checkpoint_lsn: Option<PgLsn>, has_materialized_decoding_state: bool) -> Self {
        Self { persisted_checkpoint_lsn, has_materialized_decoding_state }
    }

    /// Returns the persisted apply checkpoint when `SyncDone` at
    /// `sync_done_lsn` may be stored as `Ready`.
    ///
    /// The three checks cover separate safety obligations:
    ///
    /// - `current_lsn >= sync_done_lsn` proves that this connection reached the
    ///   boundary where apply ownership resumes.
    /// - `persisted_checkpoint_lsn >= sync_done_lsn` proves restart safety.
    ///   Requiring the stored value, not the apply loop's in-memory checkpoint,
    ///   makes `Ready(H) => persisted apply checkpoint >= H` hold across
    ///   crashes and checkpoint-write failures. Startup selects the later of
    ///   the replication-slot position and this checkpoint, so a later
    ///   connection has a bootstrap snapshot at or beyond `SyncDone`. A fresh
    ///   pgoutput connection emits relation metadata before its first row
    ///   change, so that relation can resolve the newest stored schema at or
    ///   before the bootstrap.
    /// - `has_materialized_decoding_state` proves current-connection safety.
    ///   PostgreSQL may have sent the relation before `SyncDone`, while table
    ///   sync still owned the table. Apply skipped that relation, and
    ///   PostgreSQL need not send it again on the same connection. If DML then
    ///   arrives without a local decoder, apply can recover only while
    ///   `SyncDone` still retains the exact snapshot and masks.
    ///
    /// Apply-side decoding state is cleared before table sync starts.
    /// Consequently, a later
    /// [`crate::replication::TableDecodingState::WithSchema`] proves that this
    /// connection now has a complete decoder: relation-less DML restored it
    /// from `SyncDone`, or an owned relation materialized it. Only then may
    /// `Ready` discard the durable fallback.
    ///
    /// The decoder check does not make a table that is already `Ready` safer
    /// across restart; the persisted-checkpoint condition provides that. A
    /// restarted loop which still finds `SyncDone` applies this same check
    /// only because it is now the current connection whose decoder must be
    /// established before it stores `Ready`.
    ///
    /// A handled DDL records a pending relation while retaining any previous
    /// masks. PostgreSQL's following relation materializes the new exact
    /// snapshot and masks. A relation-less row instead combines the retained
    /// masks with the stored new table schema and transitions to `WithSchema`.
    ///
    /// A table which lacks local decoding state remains in `SyncDone`; this
    /// does not block owned changes at or after the boundary. Its first
    /// relation or DML can materialize the decoder on demand.
    fn ready_checkpoint(&self, current_lsn: PgLsn, sync_done_lsn: PgLsn) -> Option<PgLsn> {
        if !self.has_materialized_decoding_state || current_lsn < sync_done_lsn {
            return None;
        }

        self.persisted_checkpoint_lsn.filter(|lsn| *lsn >= sync_done_lsn)
    }
}

/// Action the apply worker should take for one syncing table.
#[derive(Debug, PartialEq, Eq)]
enum HandoffAction {
    /// Move `SyncWait` to `Catchup` at `catchup_lsn`, then wait for `SyncDone`.
    StartCatchup {
        /// LSN of the snapshot the table was copied from.
        snapshot_lsn: PgLsn,
        /// LSN this connection had reached.
        current_lsn: PgLsn,
        /// LSN the table-sync worker must stream up to.
        catchup_lsn: PgLsn,
    },
    /// Wait for a worker already in `Catchup` to reach `SyncDone`.
    AwaitCatchup {
        /// LSN the table-sync worker is streaming up to.
        catchup_lsn: PgLsn,
    },
    /// Store `Ready`.
    PromoteToReady {
        /// `SyncDone` boundary the readiness checks covered.
        sync_done_lsn: PgLsn,
        /// LSN this connection had reached.
        current_lsn: PgLsn,
        /// Persisted apply checkpoint, at or beyond `sync_done_lsn`.
        persisted_checkpoint_lsn: PgLsn,
    },
    /// Start a table-sync worker for the table.
    SpawnWorker,
    /// Leave the table unchanged.
    Keep,
}

/// Decides the handoff action for one syncing table.
///
/// Pure function mirroring the module's transition table. `SyncWait` enters
/// catchup at the later of the copy snapshot and `current_lsn`, because the
/// worker may have started from a position ahead of this loop.
fn decide(
    trigger: Trigger,
    worker_running: bool,
    state: &TableState,
    current_lsn: PgLsn,
    readiness: Option<&ReadinessInputs>,
) -> HandoffAction {
    let promote = |sync_done_lsn: PgLsn| match readiness
        .and_then(|inputs| inputs.ready_checkpoint(current_lsn, sync_done_lsn))
    {
        Some(persisted_checkpoint_lsn) => {
            HandoffAction::PromoteToReady { sync_done_lsn, current_lsn, persisted_checkpoint_lsn }
        }
        None => HandoffAction::Keep,
    };

    match (worker_running, state, trigger) {
        (true, TableState::SyncWait { lsn }, Trigger::AfterCommit | Trigger::Quiescent) => {
            HandoffAction::StartCatchup {
                snapshot_lsn: *lsn,
                current_lsn,
                catchup_lsn: (*lsn).max(current_lsn),
            }
        }
        (true, TableState::Catchup { lsn }, Trigger::AfterCommit | Trigger::Quiescent) => {
            HandoffAction::AwaitCatchup { catchup_lsn: *lsn }
        }
        (_, TableState::SyncDone { lsn, .. }, Trigger::AfterFlush | Trigger::Quiescent) => {
            promote(*lsn)
        }
        (false, TableState::SyncDone { .. }, Trigger::AfterCommit) => HandoffAction::Keep,
        (false, _, _) => HandoffAction::SpawnWorker,
        (true, _, _) => HandoffAction::Keep,
    }
}

/// Logs one handoff decision with the inputs it was made from.
fn log_handoff_decision(
    trigger: Trigger,
    table_id: TableId,
    table_state: &TableState,
    worker_running: bool,
    current_lsn: PgLsn,
    readiness: Option<ReadinessInputs>,
    action: &HandoffAction,
) {
    debug!(
        worker_type = %WorkerType::Apply,
        table_id = table_id.0,
        table_state_type = %table_state.as_type(),
        %table_state,
        worker_running,
        trigger = trigger.as_str(),
        %current_lsn,
        ?readiness,
        ?action,
        "evaluated table sync handoff",
    );
}

/// Logs a `SyncDone -> Ready` transition.
fn log_ready_transition(
    table_id: TableId,
    sync_done_lsn: PgLsn,
    current_lsn: PgLsn,
    persisted_checkpoint_lsn: PgLsn,
) {
    info!(
        worker_type = %WorkerType::Apply,
        table_id = table_id.0,
        %sync_done_lsn,
        %current_lsn,
        %persisted_checkpoint_lsn,
        "transitioning sync_done -> ready",
    );
}

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
async fn execute_active_action<S: StateStore>(
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

/// Clears apply-side decoding state before a table-sync worker starts.
///
/// While table sync owns the table, the apply worker skips its DDL, relation,
/// and DML messages. Removing the previous decoder makes any state observed
/// after ownership returns meaningful: it was created from `SyncDone` on
/// demand or from protocol messages handled by this apply loop after the
/// handover boundary.
///
/// This also covers explicit table resets. Resetting the durable table state
/// to `Init` immediately stops apply ownership. Before the replacement copy
/// begins, this removal prevents an earlier `WithSchema` from satisfying the
/// later `SyncDone → Ready` condition. The table-sync worker then drops the
/// old destination object, clears the table's stored schemas and metadata, and
/// stores a fresh initial-copy schema.
fn reset_table_decoding_state_for_sync(
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
    table_id: TableId,
) {
    if table_decoding_states.remove(&table_id).is_some() {
        debug!(
            worker_type = %WorkerType::Apply,
            table_id = table_id.0,
            "cleared apply decoding state before starting table sync worker",
        );
    }
}

/// Starts a table sync worker and adds it to the pool.
///
/// We optimistically start the worker without checking if another one already
/// exists since it's highly likely that if we didn't find the worker state
/// during process syncing table, then the worker doesn't exist. If it were to
/// exist, the pool itself performs de-duplication in a consistent way.
///
/// This helper function uses type erasure via [`Box::pin`] to enforce `Send`
/// bounds on the future. Without this, the compiler cannot verify that the
/// recursive async call chain (ApplyLoop -> TableSyncWorker -> ApplyLoop for
/// catchup) satisfies `Send`.
fn start_table_sync_worker<S, D>(
    pool: Arc<TableSyncWorkerPool>,
    worker: TableSyncWorker<S, D>,
) -> Pin<Box<dyn Future<Output = EtlResult<()>> + Send>>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    Box::pin(async move { worker.spawn_into_pool(&pool).await })
}

/// Clears the table's apply decoder, then starts a table-sync worker.
///
/// Clearing first is part of starting a worker, so no worker can start while
/// the apply loop holds a decoder from before the copy. A worker that fails to
/// start is logged; the next evaluation finds no running worker and tries
/// again.
async fn spawn_table_sync_worker<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_id: TableId,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) where
    S: PipelineStore,
    D: PipelineDestination,
{
    reset_table_decoding_state_for_sync(table_decoding_states, table_id);

    info!(table_id = table_id.0, "creating table sync worker");
    let table_sync_worker = TableSyncWorker::new(
        ctx.pipeline_id.clone(),
        Arc::clone(&ctx.config),
        ctx.source_network_failures.clone(),
        table_id,
        ctx.store.clone(),
        ctx.destination.clone(),
        ctx.replication_lag_monitor.clone(),
        ctx.slot_creation_monitor.clone(),
        ctx.shutdown_token.clone(),
        Arc::clone(&ctx.table_sync_worker_permits),
        ctx.memory_monitor.clone(),
        ctx.batch_memory_governor.clone(),
    );
    if let Err(err) = start_table_sync_worker(Arc::clone(&ctx.pool), table_sync_worker).await {
        error!(
            worker_type = %WorkerType::Apply,
            table_id = table_id.0,
            error = %err,
            "failed to start table sync worker",
        );
    }
}

/// Executes the handoff action for a table without a running worker.
async fn execute_idle_action<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_id: TableId,
    action: HandoffAction,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    match action {
        HandoffAction::PromoteToReady { sync_done_lsn, current_lsn, persisted_checkpoint_lsn } => {
            log_ready_transition(table_id, sync_done_lsn, current_lsn, persisted_checkpoint_lsn);

            ctx.store.update_table_state(table_id, TableState::Ready).await
        }
        HandoffAction::SpawnWorker => {
            spawn_table_sync_worker(ctx, table_id, table_decoding_states).await;

            Ok(())
        }
        HandoffAction::Keep => Ok(()),
        HandoffAction::StartCatchup { .. } | HandoffAction::AwaitCatchup { .. } => {
            unreachable!("idle table never enters or awaits catchup")
        }
    }
}

/// Evaluates every syncing table for `trigger` and executes its handoff action.
async fn process_syncing_tables<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    trigger: Trigger,
    activity_handle: Option<&ActivityHandle>,
    current_lsn: PgLsn,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<Option<ExitIntent>>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    let syncing_tables = get_syncing_tables(&ctx.store).await?;
    if syncing_tables.is_empty() {
        return Ok(None);
    }

    // Read the persisted apply checkpoint only for triggers that can promote
    // `SyncDone -> Ready`. This apply loop serializes checkpoint persistence,
    // so no write can race this read before the caller persists its next
    // candidate.
    let persisted_checkpoint_lsn = match trigger {
        Trigger::AfterCommit => None,
        Trigger::AfterFlush | Trigger::Quiescent => {
            ctx.store.get_replication_checkpoint(WorkerType::Apply).await?
        }
    };

    for (table_id, stored_state) in syncing_tables {
        let readiness = match trigger {
            Trigger::AfterCommit => None,
            Trigger::AfterFlush | Trigger::Quiescent => Some(ReadinessInputs::new(
                persisted_checkpoint_lsn,
                matches!(
                    table_decoding_states.get(&table_id),
                    Some(TableDecodingState::WithSchema(_))
                ),
            )),
        };

        let exit_intent =
            if let Some(worker_state) = ctx.pool.get_active_worker_state(table_id).await {
                let guard = worker_state.lock().await;
                let table_state = guard.table_state();
                let action = decide(trigger, true, &table_state, current_lsn, readiness.as_ref());

                log_handoff_decision(
                    trigger,
                    table_id,
                    &table_state,
                    true,
                    current_lsn,
                    readiness,
                    &action,
                );

                execute_active_action(
                    table_id,
                    guard,
                    action,
                    &ctx.store,
                    &ctx.shutdown_token,
                    activity_handle,
                )
                .await?
            } else {
                let action = decide(trigger, false, &stored_state, current_lsn, readiness.as_ref());

                log_handoff_decision(
                    trigger,
                    table_id,
                    &stored_state,
                    false,
                    current_lsn,
                    readiness,
                    &action,
                );

                execute_idle_action(ctx, table_id, action, table_decoding_states).await?;

                None
            };

        if exit_intent.is_some() {
            return Ok(exit_intent);
        }
    }

    Ok(None)
}

/// Processes syncing tables after a COMMIT.
///
/// Moves `SyncWait` workers to `Catchup`, waits for workers in catchup, and
/// spawns workers for tables without one. Never stores `Ready`.
pub(super) async fn process_syncing_tables_after_commit_event<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    activity_handle: &ActivityHandle,
    current_lsn: PgLsn,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<Option<ExitIntent>>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    process_syncing_tables(
        ctx,
        Trigger::AfterCommit,
        Some(activity_handle),
        current_lsn,
        table_decoding_states,
    )
    .await
}

/// Processes syncing tables after a batch flush.
///
/// Handles `SyncDone → Ready` transitions only after the persisted apply
/// checkpoint reaches `SyncDone.lsn` and this connection has materialized the
/// table decoder, and spawns new workers. Never enters or waits for catchup.
///
/// The caller invokes this before persisting `current_lsn`, so
/// `persisted_checkpoint_lsn` is the restart frontier from an earlier
/// evaluation. When the current flush first reaches `SyncDone.lsn`, this pass
/// keeps the table in `SyncDone`; the caller then persists the new checkpoint,
/// and the next quiescent or after-flush pass can store `Ready` if the decoder
/// is also materialized. The deliberate delay makes every persisted `Ready`
/// imply both that a checkpoint at or beyond `SyncDone.lsn` was already
/// durable and that the current connection no longer needs the stored decoding
/// payload.
pub(super) async fn process_syncing_tables_after_flush<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    current_lsn: PgLsn,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    process_syncing_tables(ctx, Trigger::AfterFlush, None, current_lsn, table_decoding_states)
        .await?;

    Ok(())
}

/// Processes syncing tables while the apply loop is quiescent.
///
/// Does the COMMIT work for each table, or the flush work when the COMMIT
/// decision keeps it.
///
/// This path does not advance the apply checkpoint because no destination
/// flush completed. A quiet table may therefore remain in `SyncDone` until a
/// later durable apply flush advances the persisted checkpoint.
pub(super) async fn process_syncing_tables_when_quiescent<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    activity_handle: &ActivityHandle,
    current_lsn: PgLsn,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<Option<ExitIntent>>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    process_syncing_tables(
        ctx,
        Trigger::Quiescent,
        Some(activity_handle),
        current_lsn,
        table_decoding_states,
    )
    .await
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
                handoff::{HandoffAction, ReadinessInputs, Trigger, decide, execute_active_action},
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

    #[test]
    fn sync_wait_starts_catchup_at_max_of_snapshot_and_current_lsn() {
        let cases = [
            (PgLsn::from(10), PgLsn::from(20), PgLsn::from(20)),
            (PgLsn::from(20), PgLsn::from(20), PgLsn::from(20)),
            (PgLsn::from(30), PgLsn::from(20), PgLsn::from(30)),
        ];

        for trigger in [Trigger::AfterCommit, Trigger::Quiescent] {
            for (snapshot_lsn, current_lsn, expected_catchup_lsn) in cases {
                let state = TableState::SyncWait { lsn: snapshot_lsn };

                let action = decide(trigger, true, &state, current_lsn, None);

                assert_eq!(
                    action,
                    HandoffAction::StartCatchup {
                        snapshot_lsn,
                        current_lsn,
                        catchup_lsn: expected_catchup_lsn,
                    }
                );
            }
        }
    }

    #[test]
    fn catchup_waits_after_commit_and_when_quiescent() {
        let catchup_lsn = PgLsn::from(20);
        let state = TableState::Catchup { lsn: catchup_lsn };

        for trigger in [Trigger::AfterCommit, Trigger::Quiescent] {
            let action = decide(trigger, true, &state, PgLsn::from(10), None);

            assert_eq!(action, HandoffAction::AwaitCatchup { catchup_lsn });
        }
    }

    #[test]
    fn after_flush_never_enters_or_awaits_catchup() {
        let readiness = ReadinessInputs::new(Some(PgLsn::from(30)), true);

        for state in [
            TableState::SyncWait { lsn: PgLsn::from(10) },
            TableState::Catchup { lsn: PgLsn::from(20) },
        ] {
            let action =
                decide(Trigger::AfterFlush, true, &state, PgLsn::from(30), Some(&readiness));

            assert_eq!(action, HandoffAction::Keep);
        }
    }

    #[test]
    fn after_commit_never_promotes_sync_done() {
        let sync_done = TableState::SyncDone { lsn: PgLsn::from(10), table_decoding_state: None };
        let readiness = ReadinessInputs::new(Some(PgLsn::from(20)), true);

        for worker_running in [true, false] {
            let action = decide(
                Trigger::AfterCommit,
                worker_running,
                &sync_done,
                PgLsn::from(20),
                Some(&readiness),
            );

            assert_eq!(action, HandoffAction::Keep);
        }
    }

    #[test]
    fn sync_done_promotes_only_when_all_readiness_conditions_hold() {
        let sync_done_lsn = PgLsn::from(20);
        let state = TableState::SyncDone { lsn: sync_done_lsn, table_decoding_state: None };
        let ready_cases = [
            // Exact boundary match.
            (PgLsn::from(20), Some(PgLsn::from(20)), true, true),
            // Progress strictly beyond SyncDone.
            (PgLsn::from(30), Some(PgLsn::from(25)), true, true),
            // Current LSN behind SyncDone.
            (PgLsn::from(19), Some(PgLsn::from(20)), true, false),
            // No persisted checkpoint yet.
            (PgLsn::from(20), None, true, false),
            // Persisted checkpoint behind SyncDone.
            (PgLsn::from(20), Some(PgLsn::from(19)), true, false),
            // Local decoder not yet materialized.
            (PgLsn::from(20), Some(PgLsn::from(20)), false, false),
        ];

        for trigger in [Trigger::AfterFlush, Trigger::Quiescent] {
            for worker_running in [true, false] {
                for (current_lsn, persisted_checkpoint_lsn, has_decoder, should_promote) in
                    ready_cases
                {
                    let readiness = ReadinessInputs::new(persisted_checkpoint_lsn, has_decoder);

                    let action =
                        decide(trigger, worker_running, &state, current_lsn, Some(&readiness));

                    if should_promote {
                        assert_eq!(
                            action,
                            HandoffAction::PromoteToReady {
                                sync_done_lsn,
                                current_lsn,
                                persisted_checkpoint_lsn: persisted_checkpoint_lsn.unwrap(),
                            }
                        );
                    } else {
                        assert_eq!(action, HandoffAction::Keep);
                    }
                }
            }
        }
    }

    #[test]
    fn running_worker_in_non_handoff_states_is_kept() {
        let readiness = ReadinessInputs::new(Some(PgLsn::from(30)), true);
        let states = [
            TableState::Init,
            TableState::DataSync,
            TableState::FinishedCopy,
            TableState::Ready,
            TableState::Errored {
                reason: "copy failed".to_owned(),
                solution: None,
                retry_policy: TableRetryPolicy::NoRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Copy failed"),
            },
        ];

        for trigger in [Trigger::AfterCommit, Trigger::AfterFlush, Trigger::Quiescent] {
            for state in &states {
                let action = decide(trigger, true, state, PgLsn::from(30), Some(&readiness));

                assert_eq!(action, HandoffAction::Keep);
            }
        }
    }

    #[test]
    fn idle_table_spawns_worker_for_all_non_sync_done_states() {
        let readiness = ReadinessInputs::new(Some(PgLsn::from(30)), true);
        let states = [
            TableState::Init,
            TableState::DataSync,
            TableState::FinishedCopy,
            TableState::SyncWait { lsn: PgLsn::from(10) },
            TableState::Catchup { lsn: PgLsn::from(20) },
        ];

        for trigger in [Trigger::AfterCommit, Trigger::AfterFlush, Trigger::Quiescent] {
            for state in &states {
                let action = decide(trigger, false, state, PgLsn::from(30), Some(&readiness));

                assert_eq!(action, HandoffAction::SpawnWorker);
            }
        }
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
