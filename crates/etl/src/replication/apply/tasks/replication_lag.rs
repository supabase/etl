//! Replication lag sampling and shared progress metrics.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use metrics::gauge;
use tokio::time::MissedTickBehavior;
use tokio_postgres::types::PgLsn;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use crate::{
    observability::{
        ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES, ETL_APPLY_LOOP_END_TO_END_LAG_BYTES,
        ETL_APPLY_LOOP_FLUSH_LAG_BYTES, ETL_APPLY_LOOP_RECEIVED_LAG_BYTES, WORKER_TYPE_LABEL,
    },
    postgres::OutOfBandSourcePool,
    replication::WorkerType,
};

/// Atomic replication lag positions shared by the apply loop and sampler task.
#[derive(Debug)]
struct ReplicationLagMetricsInner {
    /// Last source WAL LSN observed by the out-of-band sampler.
    last_source_current_lsn: AtomicU64,
    /// The highest LSN received from PostgreSQL so far.
    last_received_lsn: AtomicU64,
    /// The highest LSN whose destination write completed durably.
    last_flush_lsn: AtomicU64,
    /// The highest safe frontier selected from received-or-flushed progress.
    ///
    /// PostgreSQL feedback uses this quiescent-or-flushed selection rule.
    /// Durable ETL checkpoints advance only after destination flushes.
    last_checkpoint_lsn: AtomicU64,
}

/// Tracks replication lag measurements shared with the sampler task.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationLagMetrics {
    /// Shared atomic LSN positions used for lag gauges.
    inner: Arc<ReplicationLagMetricsInner>,
}

impl ReplicationLagMetrics {
    /// Creates replication lag metrics initialized to the given LSN.
    pub(crate) fn new(initial_lsn: PgLsn) -> Self {
        let initial_lsn = u64::from(initial_lsn);

        Self {
            inner: Arc::new(ReplicationLagMetricsInner {
                last_source_current_lsn: AtomicU64::new(initial_lsn),
                last_received_lsn: AtomicU64::new(initial_lsn),
                last_flush_lsn: AtomicU64::new(initial_lsn),
                last_checkpoint_lsn: AtomicU64::new(initial_lsn),
            }),
        }
    }

    /// Updates lag metric positions derived from apply-loop progress.
    pub(crate) fn update_progress(
        &self,
        last_received_lsn: PgLsn,
        last_flush_lsn: PgLsn,
        checkpoint_lsn: PgLsn,
    ) {
        Self::update_lsn(&self.inner.last_received_lsn, last_received_lsn);
        Self::update_lsn(&self.inner.last_flush_lsn, last_flush_lsn);
        Self::update_lsn(&self.inner.last_checkpoint_lsn, checkpoint_lsn);
    }

    /// Updates the last source current LSN if it advanced.
    fn update_last_source_current_lsn(&self, lsn: PgLsn) {
        Self::update_lsn(&self.inner.last_source_current_lsn, lsn);
    }

    /// Emits lag gauges from the current atomic progress positions.
    fn emit_lag_metrics(&self, worker_type: WorkerType) {
        let last_source_current_lsn = self.inner.last_source_current_lsn.load(Ordering::Relaxed);
        let last_received_lsn = self.inner.last_received_lsn.load(Ordering::Relaxed);
        let last_flush_lsn = self.inner.last_flush_lsn.load(Ordering::Relaxed);
        let last_checkpoint_lsn = self.inner.last_checkpoint_lsn.load(Ordering::Relaxed);

        let worker_type = worker_type.as_str();

        gauge!(
            ETL_APPLY_LOOP_RECEIVED_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_source_current_lsn.saturating_sub(last_received_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_received_lsn.saturating_sub(last_checkpoint_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_FLUSH_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_received_lsn.saturating_sub(last_flush_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_END_TO_END_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_source_current_lsn.saturating_sub(last_checkpoint_lsn) as f64);
    }

    /// Updates a stored LSN monotonically.
    fn update_lsn(stored_lsn: &AtomicU64, lsn: PgLsn) {
        let new_lsn = u64::from(lsn);
        let _ = stored_lsn.try_update(Ordering::Relaxed, Ordering::Relaxed, |current_lsn| {
            (new_lsn > current_lsn).then_some(new_lsn)
        });
    }
}

/// Runs the best-effort replication lag sampler.
async fn run_replication_lag_metrics(
    out_of_band_source_pool: OutOfBandSourcePool,
    replication_lag_metrics: ReplicationLagMetrics,
    worker_type: WorkerType,
    table_sync_monitor_refresh_interval: Duration,
) {
    let mut interval = tokio::time::interval(table_sync_monitor_refresh_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        match out_of_band_source_pool.get_current_wal_lsn().await {
            Ok(source_current_lsn) => {
                replication_lag_metrics.update_last_source_current_lsn(source_current_lsn);
                replication_lag_metrics.emit_lag_metrics(worker_type);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "replication lag sampler failed to poll source database"
                );
            }
        }
    }
}

/// Starts the replication lag sampler for an apply loop.
pub(super) fn spawn_replication_lag_metrics_task(
    out_of_band_source_pool: OutOfBandSourcePool,
    replication_lag_metrics: ReplicationLagMetrics,
    worker_type: WorkerType,
    table_sync_monitor_refresh_interval: Duration,
) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(tokio::spawn(run_replication_lag_metrics(
        out_of_band_source_pool,
        replication_lag_metrics,
        worker_type,
        table_sync_monitor_refresh_interval,
    )))
}
