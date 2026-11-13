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
//! | running | `SyncDone(d)` | keep | store `Ready` if proven | as flush |
//! | running | any other | keep | keep | keep |
//! | none | `SyncDone(d)` | keep | store `Ready` if proven | as flush |
//! | none | any other | spawn worker | spawn worker | spawn worker |
//!
//! `current` is the LSN this connection has reached. "Wait" blocks the apply
//! loop until the worker stores `SyncDone` or `Errored`, or until shutdown.
//! "If proven" means [`ReadinessInputs::ready_proof`] returned a
//! [`ReadyProof`] for `d`.
//!
//! # Layout
//!
//! - [`decision`] maps one observation to an action. Its functions are pure.
//!   Each covers one trigger column, for the rows with a running worker or for
//!   the rows without one.
//! - [`active_table`] carries out the action for a running worker, under the
//!   worker's state lock.
//! - This module runs each trigger: observe, decide, log, execute.
//!
//! # What the types rule out
//!
//! - Storing `Ready` takes a [`ReadyProof`], which only
//!   [`ReadinessInputs::ready_proof`] constructs. The COMMIT path builds no
//!   readiness inputs, so it cannot promote.
//! - Entering `Catchup` and waiting are one operation,
//!   [`ActiveTable::execute`], which takes an [`ActivityHandle`]. The flush
//!   path receives none, so it can neither enter catchup nor block.
//! - [`ActiveTable::execute`] releases the worker's state lock before it waits.
//! - Only [`IdleAction`] has a spawn variant, so a table with a running worker
//!   cannot get a second one.
//!
//! Starting a worker always clears the apply decoder first. One spawn function
//! does both, but the compiler does not enforce it.

use std::{collections::HashMap, fmt, future::Future, pin::Pin, sync::Arc};

use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info};

use crate::{
    activity::ActivityHandle,
    destination::PipelineDestination,
    error::EtlResult,
    replication::{
        TableDecodingState, WorkerType,
        apply::{
            ApplyWorkerContext, ExitIntent, get_syncing_tables,
            handoff::{
                active_table::ActiveTable,
                decision::{
                    IdleAction, ReadinessInputs, ReadyProof, active_after_commit,
                    active_after_flush, active_when_quiescent, idle_after_commit, idle_after_flush,
                },
            },
        },
        state::TableState,
    },
    runtime::{TableSyncWorker, TableSyncWorkerPool},
    schema::TableId,
    store::{PipelineStore, StateStore},
};

mod active_table;
mod decision;

/// Point at which the apply worker evaluates syncing tables.
#[derive(Debug, Clone, Copy)]
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

/// One table's handoff evaluation, as its debug line reports it.
struct Evaluation {
    /// Trigger that caused the evaluation.
    trigger: Trigger,
    /// Table being evaluated.
    table_id: TableId,
    /// LSN this connection has reached.
    current_lsn: PgLsn,
    /// Readiness inputs; `None` after a COMMIT, whose decisions take none.
    readiness: Option<ReadinessInputs>,
}

impl Evaluation {
    /// Logs the decision for a table whose sync worker is running.
    fn log_active(&self, table_state: &TableState, action: &impl fmt::Debug) {
        self.log(table_state, true, action);
    }

    /// Logs the decision for a table without a running sync worker.
    fn log_idle(&self, table_state: &TableState, action: &IdleAction) {
        self.log(table_state, false, action);
    }

    /// Logs one decision with the inputs it was made from.
    fn log(&self, table_state: &TableState, worker_running: bool, action: &impl fmt::Debug) {
        debug!(
            worker_type = %WorkerType::Apply,
            table_id = self.table_id.0,
            table_state_type = %table_state.as_type(),
            %table_state,
            worker_running,
            trigger = self.trigger.as_str(),
            current_lsn = %self.current_lsn,
            readiness = ?self.readiness,
            ?action,
            "evaluated table sync handoff",
        );
    }
}

/// A syncing table as the apply worker observed it.
enum ObservedTable {
    /// The table's sync worker is running.
    Active(ActiveTable),
    /// No sync worker is running; the state is the stored one.
    Idle(TableState),
}

/// Observes a syncing table, locking its worker's state when one is running.
async fn observe_table<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_id: TableId,
    stored_state: TableState,
) -> ObservedTable {
    match ctx.pool.get_active_worker_state(table_id).await {
        Some(worker_state) => {
            ObservedTable::Active(ActiveTable::lock(table_id, worker_state).await)
        }
        None => ObservedTable::Idle(stored_state),
    }
}

/// Returns syncing tables with the persisted apply checkpoint.
///
/// The checkpoint is not read when no table is syncing. This apply loop
/// serializes checkpoint persistence, so no apply checkpoint write can race
/// this read before the caller finishes its evaluation and persists its next
/// candidate.
async fn syncing_tables_with_checkpoint<S: StateStore>(
    store: &S,
) -> EtlResult<(Vec<(TableId, TableState)>, Option<PgLsn>)> {
    let syncing_tables = get_syncing_tables(store).await?;
    if syncing_tables.is_empty() {
        return Ok((syncing_tables, None));
    }

    let persisted_checkpoint_lsn = store.get_replication_checkpoint(WorkerType::Apply).await?;

    Ok((syncing_tables, persisted_checkpoint_lsn))
}

/// Returns the readiness inputs for one table.
fn readiness_inputs(
    table_id: TableId,
    current_lsn: PgLsn,
    persisted_checkpoint_lsn: Option<PgLsn>,
    table_decoding_states: &HashMap<TableId, TableDecodingState>,
) -> ReadinessInputs {
    let has_materialized_decoding_state =
        matches!(table_decoding_states.get(&table_id), Some(TableDecodingState::WithSchema(_)));

    ReadinessInputs::new(current_lsn, persisted_checkpoint_lsn, has_materialized_decoding_state)
}

/// Logs a `SyncDone -> Ready` transition.
fn log_ready_transition(table_id: TableId, proof: &ReadyProof) {
    info!(
        worker_type = %WorkerType::Apply,
        table_id = table_id.0,
        sync_done_lsn = %proof.sync_done_lsn(),
        current_lsn = %proof.current_lsn(),
        persisted_checkpoint_lsn = %proof.persisted_checkpoint_lsn(),
        "transitioning sync_done -> ready",
    );
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
    action: IdleAction,
    table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
) -> EtlResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    match action {
        IdleAction::PromoteToReady(proof) => {
            log_ready_transition(table_id, &proof);

            ctx.store.update_table_state(table_id, TableState::Ready).await
        }
        IdleAction::SpawnWorker => {
            spawn_table_sync_worker(ctx, table_id, table_decoding_states).await;

            Ok(())
        }
        IdleAction::Keep => Ok(()),
    }
}

/// Processes syncing tables after a COMMIT.
///
/// Moves `SyncWait` workers to `Catchup`, waits for workers in catchup, and
/// spawns workers for tables without one. The COMMIT decisions take no
/// readiness inputs, so this path never stores `Ready`.
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
    for (table_id, stored_state) in get_syncing_tables(&ctx.store).await? {
        let evaluation =
            Evaluation { trigger: Trigger::AfterCommit, table_id, current_lsn, readiness: None };

        let exit_intent = match observe_table(ctx, table_id, stored_state).await {
            ObservedTable::Active(table) => {
                let table_state = table.table_state();
                let action = active_after_commit(&table_state, current_lsn);
                evaluation.log_active(&table_state, &action);

                table.execute(action, &ctx.store, &ctx.shutdown_token, activity_handle).await?
            }
            ObservedTable::Idle(table_state) => {
                let action = idle_after_commit(&table_state);
                evaluation.log_idle(&table_state, &action);
                execute_idle_action(ctx, table_id, action, table_decoding_states).await?;

                None
            }
        };

        if exit_intent.is_some() {
            return Ok(exit_intent);
        }
    }

    Ok(None)
}

/// Processes syncing tables after a batch flush.
///
/// Handles `SyncDone → Ready` transitions only after the persisted apply
/// checkpoint reaches `SyncDone.lsn` and this connection has materialized the
/// table decoder, and spawns new workers. It never enters or waits for
/// catchup.
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
    let (syncing_tables, persisted_checkpoint_lsn) =
        syncing_tables_with_checkpoint(&ctx.store).await?;
    for (table_id, stored_state) in syncing_tables {
        let readiness = readiness_inputs(
            table_id,
            current_lsn,
            persisted_checkpoint_lsn,
            table_decoding_states,
        );
        let evaluation = Evaluation {
            trigger: Trigger::AfterFlush,
            table_id,
            current_lsn,
            readiness: Some(readiness),
        };

        match observe_table(ctx, table_id, stored_state).await {
            ObservedTable::Active(table) => {
                let table_state = table.table_state();
                let ready_proof = active_after_flush(&table_state, &readiness);
                evaluation.log_active(&table_state, &ready_proof);

                if let Some(proof) = ready_proof {
                    table.promote_to_ready(proof, &ctx.store).await?;
                }
            }
            ObservedTable::Idle(table_state) => {
                let action = idle_after_flush(&table_state, &readiness);
                evaluation.log_idle(&table_state, &action);
                execute_idle_action(ctx, table_id, action, table_decoding_states).await?;
            }
        }
    }

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
    let (syncing_tables, persisted_checkpoint_lsn) =
        syncing_tables_with_checkpoint(&ctx.store).await?;
    for (table_id, stored_state) in syncing_tables {
        let readiness = readiness_inputs(
            table_id,
            current_lsn,
            persisted_checkpoint_lsn,
            table_decoding_states,
        );
        let evaluation = Evaluation {
            trigger: Trigger::Quiescent,
            table_id,
            current_lsn,
            readiness: Some(readiness),
        };

        let exit_intent = match observe_table(ctx, table_id, stored_state).await {
            ObservedTable::Active(table) => {
                let table_state = table.table_state();
                let action = active_when_quiescent(&table_state, &readiness);
                evaluation.log_active(&table_state, &action);

                table.execute(action, &ctx.store, &ctx.shutdown_token, activity_handle).await?
            }
            ObservedTable::Idle(table_state) => {
                let action = idle_after_flush(&table_state, &readiness);
                evaluation.log_idle(&table_state, &action);
                execute_idle_action(ctx, table_id, action, table_decoding_states).await?;

                None
            }
        };

        if exit_intent.is_some() {
            return Ok(exit_intent);
        }
    }

    Ok(None)
}
