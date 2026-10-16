use std::time::Duration;

use metrics::counter;
use tokio::{sync::watch, time::MissedTickBehavior};
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use crate::{
    error::{ErrorKind, EtlResult},
    observability::ETL_SLOT_INVALIDATIONS_TOTAL,
    postgres::client::SlotState,
    replication::ReplicationLagMonitor,
    schema::TableId,
    task::abort_and_join,
};

/// Background monitor for a table sync worker's in-flight table copy.
///
/// Periodically checks the worker's replication slot for invalidation, so the
/// caller can abort the copy instead of continuing against an unusable slot.
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
        replication_lag_monitor: ReplicationLagMonitor,
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

                match replication_lag_monitor.get_slot_state(&slot_name).await {
                    Ok(slot_state) => {
                        if slot_state != SlotState::Invalidated {
                            continue;
                        }

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
                            "table sync monitor failed to poll source database"
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};

    use crate::{
        replication::{ReplicationLagMonitor, table_sync::monitor::TableSyncMonitor},
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
        let pool = ReplicationLagMonitor::new(1, &connection, interval);
        let monitor = TableSyncMonitor::spawn(TableId::new(1), "unused".into(), pool, interval);

        // Accept the sampler's connection without answering its handshake.
        let (_connection, _) =
            tokio::time::timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), monitor.stop()).await.unwrap().unwrap();
    }
}
