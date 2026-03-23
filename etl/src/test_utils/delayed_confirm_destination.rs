use etl_postgres::types::TableId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::destination::flush_result::BatchFlushResult;
use crate::destination::Destination;
use crate::error::EtlResult;
use crate::store::state::StateStore;
use crate::test_utils::notify::TimedNotify;
use crate::types::{Event, EventType, TableRow};

/// Internal state for the delayed-confirm destination.
struct Inner<S> {
    state_store: S,
    /// Pending flush result senders that have not been confirmed yet.
    pending_flush_results: Vec<BatchFlushResult<()>>,
    /// Commit LSNs received across all batches (from `CommitEvent.end_lsn`).
    received_commit_lsns: Vec<PgLsn>,
    /// All events received (tracked immediately, before flush confirmation).
    events: Vec<Event>,
    /// Conditions to check after each write_events call.
    event_conditions: Vec<(Box<dyn Fn(&[Event]) -> bool + Send + Sync>, Arc<Notify>)>,
    /// Whether shutdown was called.
    shutdown_called: bool,
}

impl<S> Inner<S> {
    fn check_conditions(&mut self) {
        self.event_conditions.retain(|(condition, notify)| {
            if condition(&self.events) {
                notify.notify_one();
                false
            } else {
                true
            }
        });
    }
}

/// A test destination that accepts events immediately but defers
/// the flush confirmation until test code explicitly calls
/// [`confirm_all`](Self::confirm_all).
///
/// Events are tracked immediately upon receipt (before flush confirmation),
/// which allows tests to wait for events even when the flush result is deferred.
#[derive(Clone)]
pub struct DelayedConfirmDestination<S> {
    inner: Arc<RwLock<Inner<S>>>,
}

impl<S> DelayedConfirmDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    /// Creates a new delayed-confirm destination backed by the given state store.
    pub fn new(state_store: S) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                state_store,
                pending_flush_results: Vec::new(),
                received_commit_lsns: Vec::new(),
                events: Vec::new(),
                event_conditions: Vec::new(),
                shutdown_called: false,
            })),
        }
    }

    /// Fires all pending flush-result senders with `Ok(())`.
    pub async fn confirm_all(&self) {
        let mut inner = self.inner.write().await;
        let pending: Vec<_> = inner.pending_flush_results.drain(..).collect();
        for flush_result in pending {
            let _ = flush_result.send(Ok(()));
        }
    }

    /// Spawns a background task that continuously confirms pending flush results
    /// with a configurable delay between confirmations.
    ///
    /// Returns a `JoinHandle` that can be aborted to stop auto-confirmation.
    pub fn start_auto_confirm(&self, interval: std::time::Duration) -> tokio::task::JoinHandle<()> {
        let dest = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                dest.confirm_all().await;
            }
        })
    }

    /// Returns the commit LSNs that were tracked from `CommitEvent`s.
    pub async fn get_received_commit_lsns(&self) -> Vec<PgLsn> {
        self.inner.read().await.received_commit_lsns.clone()
    }

    /// Returns all events received by this destination.
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

    /// Returns whether shutdown was called on this destination.
    pub async fn shutdown_called(&self) -> bool {
        self.inner.read().await.shutdown_called
    }

    /// Waits for the specified number of events of each type to be received.
    ///
    /// Events are tracked immediately on receipt (before flush confirmation),
    /// so this works even when flush results are deferred.
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;

        let condition: Box<dyn Fn(&[Event]) -> bool + Send + Sync> =
            Box::new(move |events: &[Event]| {
                for (event_type, expected_count) in &conditions {
                    let actual_count = events
                        .iter()
                        .filter(|e| &e.event_type() == event_type)
                        .count() as u64;
                    if actual_count < *expected_count {
                        return false;
                    }
                }
                true
            });

        inner.event_conditions.push((condition, notify.clone()));
        inner.check_conditions();

        TimedNotify::new(notify)
    }
}

impl<S> std::fmt::Debug for DelayedConfirmDestination<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayedConfirmDestination").finish()
    }
}

impl<S> Destination for DelayedConfirmDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        "delayed_confirm"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!(table_id = table_id.0, "truncating table (delayed confirm)");
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let state_store = {
            let inner = self.inner.read().await;
            inner.state_store.clone()
        };

        state_store
            .store_table_mapping(
                table_id,
                format!("delayed_confirm_destination_table_{}", table_id.0),
            )
            .await?;

        info!(
            table_id = table_id.0,
            row_count = table_rows.len(),
            "writing table rows (delayed confirm)"
        );

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        flush_result: BatchFlushResult<()>,
    ) -> EtlResult<()> {
        let mut has_commit = false;
        let mut table_ids = HashSet::new();

        for event in &events {
            match event {
                Event::Commit(commit_event) => {
                    has_commit = true;
                    let mut inner = self.inner.write().await;
                    inner.received_commit_lsns.push(commit_event.end_lsn);
                }
                Event::Insert(e) => {
                    table_ids.insert(e.table_id);
                }
                Event::Update(e) => {
                    table_ids.insert(e.table_id);
                }
                Event::Delete(e) => {
                    table_ids.insert(e.table_id);
                }
                Event::Relation(e) => {
                    table_ids.insert(e.table_schema.id);
                }
                Event::Truncate(e) => {
                    for tid in &e.rel_ids {
                        table_ids.insert(TableId::new(*tid));
                    }
                }
                Event::Begin(_) | Event::Unsupported => {}
            }
        }

        // Record table mappings.
        {
            let inner = self.inner.read().await;
            for table_id in table_ids {
                inner
                    .state_store
                    .store_table_mapping(
                        table_id,
                        format!("delayed_confirm_destination_table_{}", table_id.0),
                    )
                    .await?;
            }
        }

        info!(
            event_count = events.len(),
            has_commit, "writing events (delayed confirm)"
        );

        // Track events immediately (before flush confirmation) so tests can wait on them.
        {
            let mut inner = self.inner.write().await;
            inner.events.extend(events);
            inner.check_conditions();
        }

        if has_commit {
            // Defer: store the flush_result sender without firing it.
            let mut inner = self.inner.write().await;
            inner.pending_flush_results.push(flush_result);
        } else {
            // No commit in this batch — confirm immediately.
            let _ = flush_result.send(Ok(()));
        }

        Ok(())
    }

    async fn shutdown(&self) -> EtlResult<()> {
        {
            let mut inner = self.inner.write().await;
            inner.shutdown_called = true;
        }

        Ok(())
    }
}
