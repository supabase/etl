//! Bounded, serialized cleanup of obsolete table schemas.

use std::collections::BTreeMap;

use hotpath::wrap::tokio::sync::mpsc::{Receiver, Sender};
use metrics::counter;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use tracing::{info, warn};

use crate::{
    constants::DEFAULT_CHANNEL_CAPACITY,
    observability::{
        ETL_SCHEMA_CLEANUP_ERRORS_TOTAL, ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
        ETL_SCHEMA_CLEANUP_TABLES_TOTAL, ETL_SCHEMA_CLEANUPS_TOTAL, WORKER_TYPE_LABEL,
    },
    replication::WorkerType,
    schema::{SnapshotId, TableId},
    store::SchemaStore,
};

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

/// Creates the bounded cleanup queue with a stable profiling label.
fn schema_cleanup_channel() -> (Sender<SchemaCleanupRequest>, Receiver<SchemaCleanupRequest>) {
    hotpath::channel!(mpsc::channel(DEFAULT_CHANNEL_CAPACITY), label = "schema_cleanup")
}

/// Runs best-effort schema cleanup until the queue closes and drains.
async fn run_schema_cleanup<S>(
    schema_store: S,
    worker_type: WorkerType,
    mut schema_cleanup_rx: Receiver<SchemaCleanupRequest>,
) where
    S: SchemaStore,
{
    while let Some(request) = schema_cleanup_rx.recv().await {
        // Coalesce up to one bounded batch of available requests to retain
        // batched store cleanup without making the apply loop wait for it.
        let mut retention_snapshot_ids = BTreeMap::new();
        retention_snapshot_ids.insert(request.table_id, request.retention_snapshot_id);

        for _ in 1..DEFAULT_CHANNEL_CAPACITY {
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
///
/// Each queue entry contains one table identifier and one frozen retention
/// boundary. Buffering accommodates bursts of relation messages. Queueing is
/// non-blocking, so excess candidates remain pending in the apply loop and are
/// retried after a later durable flush result.
pub(super) fn spawn_schema_cleanup_task<S>(
    schema_store: S,
    worker_type: WorkerType,
) -> (Sender<SchemaCleanupRequest>, AbortOnDropHandle<()>)
where
    S: SchemaStore + Send + 'static,
{
    let (schema_cleanup_tx, schema_cleanup_rx) = schema_cleanup_channel();
    let task = AbortOnDropHandle::new(tokio::spawn(run_schema_cleanup(
        schema_store,
        worker_type,
        schema_cleanup_rx,
    )));
    (schema_cleanup_tx, task)
}

/// Tries to queue a schema cleanup request for one table.
///
/// Returns `false` when the bounded queue is full or the background worker has
/// stopped. This method never waits for queue capacity.
pub(super) fn try_queue(
    schema_cleanup_tx: &Sender<SchemaCleanupRequest>,
    table_id: TableId,
    retention_snapshot_id: SnapshotId,
) -> bool {
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

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    #[cfg(feature = "hotpath")]
    use {
        crate::{
            replication::state::TableState,
            replication::{WorkerType, apply::tasks::schema_cleanup::run_schema_cleanup},
        },
        std::time::Duration,
    };

    use crate::{
        constants::DEFAULT_CHANNEL_CAPACITY,
        replication::apply::tasks::schema_cleanup::{SchemaCleanupRequest, schema_cleanup_channel},
        schema::{SnapshotId, TableId},
    };

    /// Preserves bounded queue behavior with profiling enabled or disabled.
    #[tokio::test]
    async fn schema_cleanup_channel_preserves_capacity_and_closure() {
        let (tx, mut rx) = schema_cleanup_channel();
        for _ in 0..DEFAULT_CHANNEL_CAPACITY {
            tx.try_send(SchemaCleanupRequest {
                table_id: TableId::new(1),
                retention_snapshot_id: SnapshotId::initial(),
            })
            .unwrap();
        }
        let rejected = tx
            .try_send(SchemaCleanupRequest {
                table_id: TableId::new(1),
                retention_snapshot_id: SnapshotId::initial(),
            })
            .unwrap_err();
        assert!(matches!(rejected, mpsc::error::TrySendError::Full(_)));

        drop(tx);
        let mut received = 0;
        while rx.recv().await.is_some() {
            received += 1;
        }
        assert_eq!(received, DEFAULT_CHANNEL_CAPACITY);
    }

    /// Exercises core synchronization and its shared Prometheus exposition
    /// without source or destination services.
    #[cfg(feature = "hotpath")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn core_profiling_exports_locks_and_channels() {
        use etl_telemetry::{metrics::init_metrics_handle, profiling};

        use crate::{
            runtime::{
                BatchMemoryGovernor, MemoryMonitor, TableSyncWorkerPool, TableSyncWorkerState,
            },
            store::MemoryStore,
            task::TaskRegistry,
        };

        /// Reads one scalar sample selected by its fixed profiling label.
        fn sample(snapshot: &str, family: &str, label: &str) -> Option<f64> {
            snapshot.lines().find_map(|line| {
                if !line.starts_with(&format!("{family}{{"))
                    || !line.contains(&format!("\"{label}\""))
                {
                    return None;
                }
                line.rsplit_once(' ')?.1.parse().ok()
            })
        }

        let _profiler = profiling::init().unwrap();
        let handle = init_metrics_handle().unwrap();
        let store = MemoryStore::new();
        let (tx, rx) = schema_cleanup_channel();
        for _ in 0..3 {
            tx.try_send(SchemaCleanupRequest {
                table_id: TableId::new(1),
                retention_snapshot_id: SnapshotId::initial(),
            })
            .unwrap();
        }
        // Deliberately leave work queued before starting the real cleanup
        // worker.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx);
        run_schema_cleanup(store, WorkerType::Apply, rx).await;

        let state = TableSyncWorkerState::new(TableId::new(1), TableState::Init);
        let held = state.lock().await;
        tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(held);
            },
            async {
                let mut state = state.lock().await;
                state.set(TableState::Init);
            }
        );

        let pool = TableSyncWorkerPool::new();
        assert!(pool.get_active_worker_state(TableId::new(1)).await.is_none());
        pool.wait_all().await.unwrap();

        let memory = MemoryMonitor::new_for_test();
        memory.set_total_memory_bytes_for_test(1024);
        let governor = BatchMemoryGovernor::new(1, memory.clone(), 0.5, 1024);
        let slots = governor.register_batch_slots(2);
        assert_eq!(governor.batch_size_target_bytes(), 256);
        drop(slots);
        memory.set_backpressure_active_for_test(true);

        let tasks = TaskRegistry::new();
        let held = tasks.drain().await.unwrap();
        tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(held);
            },
            tasks.spawn(async {})
        );
        drop(tasks.drain().await.unwrap());

        let snapshot = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = profiling::render_metrics(&handle, &[]).await.unwrap();
                if sample(&snapshot, "hotpath_channel_received_total", "schema_cleanup")
                    == Some(3.0)
                    && sample(
                        &snapshot,
                        "hotpath_mutex_wait_seconds_count",
                        "table_sync_worker_state",
                    ) == Some(2.0)
                    && sample(&snapshot, "hotpath_rwlock_acquisitions_total", "memory_snapshot")
                        == Some(2.0)
                {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();

        assert!(snapshot.contains("etl_hotpath_exporter_up 1"));
        assert!(snapshot.contains("etl_schema_cleanups_total{"));
        assert_eq!(sample(&snapshot, "hotpath_channel_sent_total", "schema_cleanup"), Some(3.0));
        assert_eq!(
            sample(&snapshot, "hotpath_channel_max_queue_size", "schema_cleanup"),
            Some(3.0)
        );
        assert_eq!(sample(&snapshot, "hotpath_channel_queue_size", "schema_cleanup"), Some(0.0));
        assert!(
            sample(&snapshot, "hotpath_channel_proc_seconds_sum", "schema_cleanup").unwrap() > 0.0
        );
        for label in [
            "table_sync_worker_state",
            "table_sync_worker_tasks",
            "memory_store",
            "batch_memory_update",
        ] {
            assert!(sample(&snapshot, "hotpath_mutex_acquisitions_total", label).unwrap() > 0.0);
        }
        for label in ["table_sync_worker_registry", "memory_snapshot"] {
            assert!(sample(&snapshot, "hotpath_rwlock_acquisitions_total", label).unwrap() > 0.0);
        }
        assert!(
            sample(&snapshot, "hotpath_mutex_wait_seconds_sum", "table_sync_worker_state").unwrap()
                > 0.0
        );
        assert!(
            sample(&snapshot, "hotpath_function_duration_seconds_sum", "task_registry_lock_wait")
                .unwrap()
                > 0.0
        );

        for line in snapshot.lines().filter(|line| {
            line.starts_with("hotpath_")
                && !line.contains("_bucket{")
                && (line.contains("schema_cleanup")
                    || line.contains("table_sync_worker_state")
                    || line.contains("task_registry_lock_wait"))
        }) {
            println!("{line}");
        }
    }
}
