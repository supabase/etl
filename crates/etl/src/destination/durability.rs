//! Destination APIs for splitting write acceptance from durable completion.
//!
//! Immediate destinations complete each write's async result only after the
//! submitted data is durable; that remains the default contract. Deferred
//! destinations may complete tracked write results after they have accepted
//! ownership of the batch, then prove durability through a separate signal.
//!
//! Streaming durability is reported per ETL-assigned [`DestinationBatchId`]
//! with [`DestinationDurabilityReporter`]. Table-copy durability is proven by
//! [`crate::destination::Destination::finish_table_copy`], which acts as the
//! durable-completion barrier for all accepted rows in a copy attempt.
//!
//! Deferred destinations still participate in at-least-once replication: after
//! a failure, ETL may replay accepted work that was not proven durable.

use std::num::NonZeroUsize;

use tokio::sync::mpsc;

use crate::{
    data::TableRow,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::Event,
    schema::ReplicatedTableSchema,
};

/// Identifier assigned by ETL to a streaming destination batch.
///
/// The identifier is unique within one apply loop and correlates a tracked
/// streaming write acceptance with a later [`DestinationDurabilityEvent`]. It
/// is not persisted across process restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DestinationBatchId(u64);

impl DestinationBatchId {
    /// Creates a new destination batch identifier.
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw identifier.
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

/// Identifier assigned by ETL to a table-copy destination batch.
///
/// The identifier is unique within one table-copy attempt and exists for
/// destination-side diagnostics and correlation. It is not a source-order
/// cursor and does not prove durability; table-copy durability is proven by
/// [`crate::destination::Destination::finish_table_copy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableCopyBatchId(u64);

impl TableCopyBatchId {
    /// Creates a new table-copy batch identifier.
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw identifier.
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

/// Streaming durability mode advertised by a destination.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum StreamingDurabilityMode {
    /// Write completion means durable completion.
    #[default]
    Immediate,
    /// Write completion means acceptance; durable completion is reported later.
    Deferred(DeferredStreamingConfig),
}

/// Table-copy durability mode advertised by a destination.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TableCopyDurabilityMode {
    /// Batch completion means durable completion.
    #[default]
    Immediate,
    /// Batch completion means acceptance; table-copy completion is proven by a
    /// finish barrier after all accepted copy batches have completed.
    Deferred(DeferredTableCopyConfig),
}

/// Backpressure limits for deferred streaming durability.
///
/// When streaming durability is deferred, ETL may dispatch another streaming
/// batch after the destination accepts the previous one, before that previous
/// batch is proven durable. These limits bound the accepted-but-not-yet-retired
/// streaming backlog that ETL may allow for one apply loop.
///
/// A deferred streaming batch remains in the backlog until the destination
/// reports durable completion for its [`DestinationBatchId`] and all older
/// accepted batches have also become durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredStreamingConfig {
    /// Maximum number of accepted but not yet retired streaming batches.
    pub max_inflight_batches: NonZeroUsize,
    /// Maximum estimated bytes across accepted but not yet retired streaming
    /// batches.
    pub max_inflight_bytes: usize,
}

impl DeferredStreamingConfig {
    /// Creates deferred streaming limits.
    pub fn new(max_inflight_batches: NonZeroUsize, max_inflight_bytes: usize) -> Self {
        Self { max_inflight_batches, max_inflight_bytes }
    }
}

/// Backpressure limits for deferred table-copy durability.
///
/// When table-copy durability is deferred, ETL may dispatch additional copy
/// batches after the destination accepts earlier batches, before those rows are
/// proven durable. These limits bound the accepted-but-unfinished copy backlog
/// for one table-copy stream.
///
/// Accepted copy batches remain in the backlog until their async acceptance
/// results complete and the table-copy finish barrier later proves durable
/// completion for the whole copy attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredTableCopyConfig {
    /// Maximum number of accepted but not yet finished table-copy batches.
    pub max_inflight_batches: NonZeroUsize,
    /// Maximum estimated bytes across accepted but not yet finished table-copy
    /// batches.
    pub max_inflight_bytes: usize,
}

impl DeferredTableCopyConfig {
    /// Creates deferred table-copy limits.
    pub fn new(max_inflight_batches: NonZeroUsize, max_inflight_bytes: usize) -> Self {
        Self { max_inflight_batches, max_inflight_bytes }
    }
}

/// Streaming events plus ETL-assigned durability tracking metadata.
#[derive(Debug)]
pub struct TrackedEventsBatch {
    /// Identifier used by deferred destinations to report durability.
    pub id: DestinationBatchId,
    /// Reporter that sends durability events back to the owning apply loop.
    pub durability_reporter: DestinationDurabilityReporter,
    /// Streaming events in source order.
    pub events: Vec<Event>,
    /// Approximate byte size used for upstream deferred-durability
    /// backpressure.
    pub estimated_bytes: usize,
}

impl TrackedEventsBatch {
    /// Creates tracked streaming events.
    pub(crate) fn new(
        id: DestinationBatchId,
        durability_reporter: DestinationDurabilityReporter,
        events: Vec<Event>,
        estimated_bytes: usize,
    ) -> Self {
        Self { id, durability_reporter, events, estimated_bytes }
    }
}

/// Table-copy rows plus ETL-assigned copy tracking metadata.
#[derive(Debug)]
pub struct TrackedTableRowsBatch {
    /// Identifier used for diagnostics and table-copy acceptance tracking.
    pub id: TableCopyBatchId,
    /// Destination schema for the copied table.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// Copied source rows.
    pub table_rows: Vec<TableRow>,
    /// Approximate byte size used for upstream copy backpressure.
    pub estimated_bytes: usize,
}

impl TrackedTableRowsBatch {
    /// Creates tracked table-copy rows.
    pub(crate) fn new(
        id: TableCopyBatchId,
        replicated_table_schema: ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        estimated_bytes: usize,
    ) -> Self {
        Self { id, replicated_table_schema, table_rows, estimated_bytes }
    }
}

/// Durability event sent by a deferred streaming destination.
#[derive(Debug)]
pub enum DestinationDurabilityEvent {
    /// The streaming batch is durably committed by the destination.
    Durable {
        /// Identifier of the durable batch.
        batch_id: DestinationBatchId,
    },
    /// The destination can no longer prove durability for the batch.
    Failed {
        /// Identifier of the failed batch.
        batch_id: DestinationBatchId,
        /// Destination failure that should stop the apply loop.
        error: EtlError,
    },
}

/// Reporting handle used by deferred streaming destinations.
#[derive(Debug, Clone)]
pub struct DestinationDurabilityReporter {
    tx: mpsc::UnboundedSender<DestinationDurabilityEvent>,
}

impl DestinationDurabilityReporter {
    /// Creates a reporter and its receiving side.
    pub(crate) fn channel() -> (Self, DestinationDurabilityEvents) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, DestinationDurabilityEvents { rx })
    }

    /// Reports a deferred streaming durability event.
    pub fn report(&self, event: DestinationDurabilityEvent) -> EtlResult<()> {
        self.tx.send(event).map_err(|_| {
            etl_error!(
                ErrorKind::DestinationError,
                "Deferred durability reporter closed before receiving event"
            )
        })
    }
}

/// Receiver for deferred streaming durability events.
#[derive(Debug)]
pub(crate) struct DestinationDurabilityEvents {
    rx: mpsc::UnboundedReceiver<DestinationDurabilityEvent>,
}

impl DestinationDurabilityEvents {
    /// Receives the next durability event.
    pub(crate) async fn recv(&mut self) -> Option<DestinationDurabilityEvent> {
        self.rx.recv().await
    }
}
