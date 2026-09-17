use std::time::Duration;

use metrics::{counter, gauge};
use tokio::{sync::watch, time::MissedTickBehavior};
use tokio_postgres::types::PgLsn;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use crate::{
    error::{ErrorKind, EtlResult},
    observability::{ETL_SLOT_INVALIDATIONS_TOTAL, ETL_TABLE_COPY_END_TO_END_LAG_BYTES},
    postgres::{OutOfBandSourcePool, client::SlotState},
    schema::TableId,
    task::abort_and_join,
};

/// Background monitor for a table sync worker's in-flight table copy.
///
/// Periodically reports table-copy end-to-end replication lag and checks the
/// worker's replication slot for invalidation, so the caller can abort the copy
/// instead of continuing against a slot PostgreSQL has already dropped.
#[derive(Debug)]
pub(crate) struct TableSyncMonitor {
    handle: AbortOnDropHandle<()>,
    slot_invalidated_rx: watch::Receiver<bool>,
}

impl TableSyncMonitor {
    /// Spawns a monitor that ticks until its copy owner stops or drops it.
    pub(crate) fn spawn(
        table_id: TableId,
        slot_name: String,
        consistent_point: PgLsn,
        out_of_band_source_pool: OutOfBandSourcePool,
        refresh_interval: Duration,
    ) -> Self {
        let (slot_invalidated_tx, slot_invalidated_rx) = watch::channel(false);

        let handle = AbortOnDropHandle::new(tokio::spawn(async move {
            // The copy owner aborts disposable sampling, including in-flight
            // queries.
            let mut ticker = tokio::time::interval(refresh_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                emit_replication_lag_metrics(table_id, consistent_point, &out_of_band_source_pool)
                    .await;

                match out_of_band_source_pool.get_slot_state(&slot_name).await {
                    Ok(SlotState::Invalidated) => {
                        counter!(ETL_SLOT_INVALIDATIONS_TOTAL).increment(1);
                        warn!(
                            table_id = table_id.0,
                            slot_name, "replication slot was invalidated during table copy"
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
                            slot_name, "replication slot disappeared during table copy"
                        );

                        // A missing slot is just as unusable as an invalidated
                        // slot.
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
        }));

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
    pub(crate) async fn stop(self) -> EtlResult<()> {
        abort_and_join(self.handle).await.map(|_| ())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};

    use crate::{
        postgres::OutOfBandSourcePool, replication::table_sync::monitor::TableSyncMonitor,
        schema::TableId,
    };

    /// Owner shutdown interrupts sampling blocked on a database handshake.
    #[tokio::test]
    async fn stop_interrupts_pending_sample() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connection = PgConnectionConfig {
            host: "127.0.0.1".into(),
            hostaddr: None,
            port: listener.local_addr().unwrap().port(),
            name: "unused".into(),
            username: "unused".into(),
            password: None,
            tls: TlsConfig::disabled(),
            keepalive: TcpKeepaliveConfig::default(),
        };
        let interval = Duration::from_secs(60);
        let pool = OutOfBandSourcePool::new(&connection, interval);
        let monitor =
            TableSyncMonitor::spawn(TableId::new(1), "unused".into(), 0.into(), pool, interval);

        // Accept the sampler's connection without answering its handshake.
        let (_connection, _) =
            tokio::time::timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), monitor.stop()).await.unwrap().unwrap();
    }
}
