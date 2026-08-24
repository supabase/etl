use std::time::Duration;

use metrics::{counter, gauge};
use tokio::{sync::watch, task::JoinHandle, time::MissedTickBehavior};
use tokio_postgres::types::PgLsn;
use tracing::warn;

use crate::{
    error::ErrorKind,
    observability::{ETL_SLOT_INVALIDATIONS_TOTAL, ETL_TABLE_COPY_END_TO_END_LAG_BYTES},
    postgres::{OutOfBandSourcePool, client::SlotState},
    runtime::concurrency::ShutdownRx,
    schema::TableId,
};

/// Background monitor for a table sync worker's in-flight table copy.
///
/// Periodically reports table-copy end-to-end replication lag and checks the
/// worker's replication slot for invalidation, so the caller can abort the
/// copy instead of continuing against a slot PostgreSQL has already dropped.
#[derive(Debug)]
pub(crate) struct TableSyncMonitor {
    handle: JoinHandle<()>,
    slot_invalidated_rx: watch::Receiver<bool>,
}

impl TableSyncMonitor {
    /// Spawns a table sync monitor for `table_id`, ticking every
    /// `refresh_interval` until `shutdown_rx` fires.
    pub(crate) fn spawn(
        table_id: TableId,
        slot_name: String,
        consistent_point: PgLsn,
        out_of_band_source_pool: OutOfBandSourcePool,
        refresh_interval: Duration,
        mut shutdown_rx: ShutdownRx,
    ) -> Self {
        let (slot_invalidated_tx, slot_invalidated_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(refresh_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => {
                        break;
                    }

                    _ = ticker.tick() => {
                        emit_replication_lag_metrics(
                            table_id,
                            consistent_point,
                            &out_of_band_source_pool,
                        ).await;

                        match out_of_band_source_pool.get_slot_state(&slot_name).await {
                            Ok(SlotState::Invalidated) => {
                                counter!(ETL_SLOT_INVALIDATIONS_TOTAL).increment(1);
                                warn!(
                                    table_id = table_id.0,
                                    slot_name,
                                    "replication slot was invalidated during table copy"
                                );

                                // Ignore send errors: if the receiver was already
                                // dropped, the copy has already finished on its own.
                                let _ = slot_invalidated_tx.send(true);

                                break;
                            }
                            Ok(SlotState::NotInvalidated) => {}
                            Err(error) if error.kind() == ErrorKind::ReplicationSlotNotFound => {
                                counter!(ETL_SLOT_INVALIDATIONS_TOTAL).increment(1);
                                warn!(
                                    table_id = table_id.0,
                                    slot_name,
                                    "replication slot disappeared during table copy"
                                );

                                // A missing slot is just as unusable as an invalidated slot.
                                let _ = slot_invalidated_tx.send(true);

                                break;
                            }
                            Err(error) => {
                                warn!(
                                    table_id = table_id.0,
                                    error = %error,
                                    "table sync monitor failed to check replication slot state"
                                );
                            }
                        }
                    }
                }
            }
        });

        Self { handle, slot_invalidated_rx }
    }

    /// Resolves once the monitored replication slot is observed invalidated.
    ///
    /// Never resolves otherwise, including when the monitor task itself ends
    /// without ever observing an invalidation (e.g. from shutdown), so callers
    /// should race this against other completion conditions rather than
    /// awaiting it alone.
    pub(crate) async fn wait_for_slot_invalidated(&mut self) {
        loop {
            if self.slot_invalidated_rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }

            if *self.slot_invalidated_rx.borrow() {
                return;
            }
        }
    }

    /// Stops the monitor task, waiting for it to finish.
    pub(crate) async fn stop(mut self) {
        self.handle.abort();

        if let Err(error) = (&mut self.handle).await
            && !error.is_cancelled()
        {
            warn!(error = %error, "table sync monitor failed before completing");
        }
    }
}

/// Emits end-to-end lag metrics for a table sync while initial copy runs.
async fn emit_replication_lag_metrics(
    table_id: TableId,
    consistent_point: PgLsn,
    out_of_band_source_pool: &OutOfBandSourcePool,
) {
    match out_of_band_source_pool.get_current_wal_lsn().await {
        Ok(source_current_lsn) => {
            let source_current_lsn = u64::from(source_current_lsn);
            let consistent_point = u64::from(consistent_point);
            let table_copy_lag_bytes = source_current_lsn.saturating_sub(consistent_point);

            gauge!(ETL_TABLE_COPY_END_TO_END_LAG_BYTES).set(table_copy_lag_bytes as f64);
        }
        Err(error) => {
            warn!(
                table_id = table_id.0,
                error = %error,
                "table copy replication lag reporter failed to poll source database"
            );
        }
    }
}
