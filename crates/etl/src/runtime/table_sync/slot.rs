//! Admission accounting for session-owned table-sync replication slots.

use std::time::Duration;

use etl_postgres::slots::EtlReplicationSlot;
use tokio::sync::OwnedSemaphorePermit;
use tracing::warn;

use crate::{
    error::EtlResult, pipeline::PipelineId, replication::ReplicationLagMonitor, schema::TableId,
};

/// Upper bound on an individual release observation, including pool
/// acquisition.
const RELEASE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
/// Initial delay between observations of an unreleased slot.
const RELEASE_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Maximum delay while a source session or unavailable source prevents release.
const MAX_RELEASE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Charges a sync attempt until PostgreSQL confirms its slot no longer exists.
///
/// Closing the client only requests session teardown. Creation may also succeed
/// on the server without returning a response, so accounting starts before the
/// creation request and ends only after observing the backend and slot absent.
/// No deletion is issued here: an unexpectedly retained or replaced slot must
/// not be destroyed.
pub(crate) struct TableSyncSlot {
    /// Deterministic slot name, also used to observe release.
    name: String,
    /// Admission capacity retained throughout the attempt and release wait.
    permit: OwnedSemaphorePermit,
    /// Creating backend, retained until neither it nor the slot exists.
    backend_pid: Option<i32>,
}

impl TableSyncSlot {
    /// Reserves accounting before an attempt can allocate a slot.
    pub(crate) fn new(
        pipeline_id: PipelineId,
        table_id: TableId,
        permit: OwnedSemaphorePermit,
    ) -> EtlResult<Self> {
        let name = EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into()?;
        Ok(Self { name, permit, backend_pid: None })
    }

    /// Returns the slot name owned by this attempt.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Marks creation as uncertain before issuing the request to PostgreSQL.
    pub(crate) fn creation_started(&mut self, backend_pid: i32) {
        self.backend_pid = Some(backend_pid);
    }

    /// Returns capacity after slot release, including during graceful shutdown.
    ///
    /// The replication client and its observers must already have been dropped.
    /// Errors and timeouts leave capacity charged and are retried with backoff.
    /// The worker pool aborts and joins this wait at the pipeline's drain
    /// deadline. Ordinary shutdown must not return before session teardown
    /// while the source is reachable.
    pub(crate) async fn release(self, monitor: &ReplicationLagMonitor) {
        let Some(backend_pid) = self.backend_pid else {
            return;
        };
        let name = self.name.clone();
        self.wait_for_release(|| monitor.table_sync_slot_is_released(&name, backend_pid)).await;
    }

    /// Observes release without confusing query failure with slot absence.
    async fn wait_for_release<F, Fut>(mut self, mut observe: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = EtlResult<bool>>,
    {
        let mut delay = RELEASE_RETRY_DELAY;
        while self.backend_pid.is_some() {
            let observation = tokio::time::timeout(RELEASE_QUERY_TIMEOUT, observe()).await;
            match observation {
                Ok(Ok(true)) => {
                    self.backend_pid = None;
                }
                Ok(Err(error)) => {
                    warn!(slot_name = self.name, error = %error, "failed to observe table sync slot release");
                }
                Err(error) => {
                    warn!(slot_name = self.name, error = %error, "table sync slot release query timed out");
                }
                Ok(Ok(false)) => {}
            }
            if self.backend_pid.is_some() {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_RELEASE_RETRY_DELAY);
            }
        }
    }
}

impl Drop for TableSyncSlot {
    fn drop(&mut self) {
        if self.backend_pid.is_some() {
            // An abort, panic outside the attempt, or state-store failure must
            // not admit replacement work while a server resource is uncounted.
            // This is synchronous admission control, never remote cleanup.
            self.permit.semaphore().close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::{Semaphore, mpsc, oneshot};

    use crate::{
        error::{ErrorKind, EtlResult},
        etl_error,
        runtime::table_sync::slot::{RELEASE_QUERY_TIMEOUT, TableSyncSlot},
        schema::TableId,
    };

    /// Unreleased and unobservable resources retain capacity until an absence
    /// observation succeeds, including after a query timeout.
    #[tokio::test(start_paused = true)]
    async fn release_requires_observed_absence() {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut slot = TableSyncSlot::new(
            1,
            TableId::new(1),
            Arc::clone(&semaphore).acquire_owned().await.unwrap(),
        )
        .unwrap();
        slot.creation_started(1);
        let (queries, mut observations) =
            mpsc::unbounded_channel::<oneshot::Sender<EtlResult<bool>>>();
        let task = tokio::spawn(async move {
            slot.wait_for_release(|| {
                let (result, receive) = oneshot::channel();
                queries.send(result).unwrap();
                async move { receive.await.unwrap() }
            })
            .await;
        });

        for result in
            [Ok(false), Err(etl_error!(ErrorKind::SourceConnectionFailed, "Source unavailable"))]
        {
            let observation = observations.recv().await.unwrap();
            assert!(semaphore.try_acquire().is_err());
            observation.send(result).unwrap();
        }

        let stalled_query = observations.recv().await.unwrap();
        tokio::time::advance(RELEASE_QUERY_TIMEOUT + Duration::from_secs(1)).await;
        let observation = observations.recv().await.unwrap();
        assert!(stalled_query.is_closed());
        assert!(semaphore.try_acquire().is_err());
        observation.send(Ok(true)).unwrap();

        task.await.unwrap();
        assert!(!semaphore.is_closed());
        assert!(semaphore.try_acquire().is_ok());
    }

    /// Deadline escalation interrupts a stalled observation without admitting
    /// attempts against resources whose release remains uncertain.
    #[tokio::test]
    async fn forced_shutdown_closes_admission_for_unconfirmed_release() {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut slot = TableSyncSlot::new(
            1,
            TableId::new(1),
            Arc::clone(&semaphore).acquire_owned().await.unwrap(),
        )
        .unwrap();
        slot.creation_started(1);
        let (started, mut observed) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            slot.wait_for_release(|| {
                started.send(()).unwrap();
                std::future::pending()
            })
            .await;
        });

        observed.recv().await.unwrap();
        task.abort();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(semaphore.is_closed());
        assert!(semaphore.try_acquire().is_err());
    }

    /// Dropping an attempt before creation returns capacity; an unconfirmed
    /// creation closes admission even if it never returned a successful result.
    #[tokio::test]
    async fn abandoned_creation_fails_closed() {
        let semaphore = Arc::new(Semaphore::new(1));
        let slot = TableSyncSlot::new(
            1,
            TableId::new(1),
            Arc::clone(&semaphore).acquire_owned().await.unwrap(),
        )
        .unwrap();
        drop(slot);
        assert!(!semaphore.is_closed());

        let mut slot = TableSyncSlot::new(
            1,
            TableId::new(1),
            Arc::clone(&semaphore).acquire_owned().await.unwrap(),
        )
        .unwrap();
        slot.creation_started(1);
        drop(slot);
        assert!(semaphore.is_closed());
    }
}
