//! Bounded, serialized cleanup of obsolete table schemas.

use std::collections::BTreeMap;

use metrics::counter;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{info, warn};

use crate::{
    observability::{
        ETL_SCHEMA_CLEANUP_ERRORS_TOTAL, ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
        ETL_SCHEMA_CLEANUP_TABLES_TOTAL, ETL_SCHEMA_CLEANUPS_TOTAL, WORKER_TYPE_LABEL,
    },
    replication::WorkerType,
    schema::{SnapshotId, TableId},
    store::SchemaStore,
};

/// Maximum number of table schema cleanups buffered per apply loop.
///
/// Each queue entry contains one table identifier and one frozen retention
/// boundary. A capacity of 1024 accommodates large bursts of relation messages
/// while keeping queue memory bounded. Queueing is non-blocking, so additional
/// candidates remain pending in the apply loop and are retried after a later
/// durable flush result.
const SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY: usize = 1024;

/// Immutable retention boundary for one asynchronous table schema cleanup.
///
/// The apply loop builds this request immediately after persisting
/// commit-boundary progress and reading destination metadata. The background
/// worker must use this frozen boundary rather than reloading newer state:
/// arbitrary queue delay can then only make the request conservative.
///
/// Concurrent schema insertion is also safe: ordered replication cannot later
/// introduce a schema at or below persisted progress, and pruning always
/// preserves every snapshot newer than the frozen boundary.
///
/// Pruning is idempotent and does not rely on request order. Each boundary
/// preserves the greatest schema snapshot at or below it and every newer
/// snapshot, so replaying a request—or processing an older request after a
/// newer one—cannot remove a schema retained by the newer boundary.
#[derive(Debug)]
pub(super) struct SchemaCleanupRequest {
    /// Table whose obsolete schema versions may be pruned.
    table_id: TableId,
    /// Inclusive retention boundary captured at the durable flush result.
    retention_snapshot_id: SnapshotId,
}

/// Runs best-effort schema cleanup until the queue closes and drains.
async fn run_schema_cleanup<S>(
    schema_store: S,
    worker_type: WorkerType,
    mut schema_cleanup_rx: mpsc::Receiver<SchemaCleanupRequest>,
) where
    S: SchemaStore,
{
    while let Some(request) = schema_cleanup_rx.recv().await {
        // Coalesce up to one bounded batch of available requests to
        // retain batched store cleanup without making the apply loop
        // wait for it.
        let mut retention_snapshot_ids = BTreeMap::new();
        retention_snapshot_ids.insert(request.table_id, request.retention_snapshot_id);

        for _ in 1..SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY {
            let Ok(request) = schema_cleanup_rx.try_recv() else {
                break;
            };

            retention_snapshot_ids.insert(request.table_id, request.retention_snapshot_id);
        }

        let table_count = retention_snapshot_ids.len() as u64;
        match schema_store.prune_table_schemas(retention_snapshot_ids).await {
            Ok(pruned_count) => {
                counter!(
                    ETL_SCHEMA_CLEANUPS_TOTAL,
                    WORKER_TYPE_LABEL => worker_type.as_str(),
                )
                .increment(1);

                counter!(
                    ETL_SCHEMA_CLEANUP_TABLES_TOTAL,
                    WORKER_TYPE_LABEL => worker_type.as_str(),
                )
                .increment(table_count);

                counter!(
                    ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
                    WORKER_TYPE_LABEL => worker_type.as_str(),
                )
                .increment(pruned_count);

                if pruned_count > 0 {
                    info!(
                        %worker_type,
                        pruned_count,
                        "obsolete table schema cleanup completed"
                    );
                }
            }
            Err(err) => {
                // Cleanup is best-effort. Do not block later requests behind a
                // permanently failing one: a later relation for the table,
                // including its first relation after restart, will enqueue a
                // fresh cleanup attempt.
                counter!(
                    ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
                    WORKER_TYPE_LABEL => worker_type.as_str(),
                )
                .increment(1);

                warn!(
                    %worker_type,
                    error = %err,
                    "failed to clean up obsolete table schemas"
                );
            }
        };
    }
}

/// Starts the worker that serially prunes requested table schema versions.
pub(super) fn spawn_schema_cleanup_task<S>(
    schema_store: S,
    worker_type: WorkerType,
) -> (mpsc::Sender<SchemaCleanupRequest>, JoinHandle<()>)
where
    S: SchemaStore + Send + 'static,
{
    let (schema_cleanup_tx, schema_cleanup_rx) = mpsc::channel(SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY);
    let task = tokio::spawn(run_schema_cleanup(schema_store, worker_type, schema_cleanup_rx));
    (schema_cleanup_tx, task)
}

/// Tries to queue a schema cleanup request for one table.
///
/// Returns `false` when the bounded queue is full or the background worker
/// has stopped. This method never waits for queue capacity.
pub(super) fn try_queue(
    schema_cleanup_tx: Option<&mpsc::Sender<SchemaCleanupRequest>>,
    table_id: TableId,
    retention_snapshot_id: SnapshotId,
) -> bool {
    let Some(schema_cleanup_tx) = schema_cleanup_tx else {
        return false;
    };

    let request = SchemaCleanupRequest { table_id, retention_snapshot_id };
    match schema_cleanup_tx.try_send(request) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => false,
        Err(mpsc::error::TrySendError::Closed(_)) => {
            warn!("schema cleanup worker stopped before accepting cleanup request");

            false
        }
    }
}

/// Joins the worker after its queue closes and accepted work finishes.
pub(super) async fn join(task: &mut JoinHandle<()>, worker_type: WorkerType) {
    // Graceful teardown finishes accepted cleanup. If teardown itself is
    // cancelled, Drop aborts the worker and unfinished cleanup can be retried.
    if let Err(err) = task.await {
        counter!(
            ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
            WORKER_TYPE_LABEL => worker_type.as_str(),
        )
        .increment(1);

        warn!(
            %worker_type,
            error = %err,
            "schema cleanup worker task failed before completing"
        );
    }
}
