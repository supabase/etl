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
//!
//! # Layout
//!
//! - [`decision`] maps one observation to a [`HandoffAction`]. [`decide`] is
//!   pure and mirrors the transition table above.
//! - [`active_table`] carries out the action for a running worker, under the
//!   worker's state lock.
//! - This module runs each trigger: observe, decide, log, execute.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

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
                active_table::execute_active_action,
                decision::{HandoffAction, ReadinessInputs, Trigger, decide},
            },
        },
        state::TableState,
    },
    runtime::{TableSyncWorker, TableSyncWorkerPool},
    schema::TableId,
    store::PipelineStore,
};

mod active_table;
mod decision;

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
