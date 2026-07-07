//! Destination durability protocol and configuration types.

use std::num::NonZeroUsize;

/// Status reported by a streaming destination write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationWriteStatus {
    /// The destination accepted ownership of the write, but ETL must not
    /// advance durable replication progress for it yet.
    ///
    /// Completing a result as `Accepted` consumes that result. After that
    /// point, ETL cannot wait on the same result for a later durability signal.
    /// A deferred destination should therefore return `Accepted` for a write
    /// only after it has successfully accepted a successor write and retained
    /// the successor's [`crate::destination::WriteEventsResult`] as the
    /// pending durability tail.
    ///
    /// The destination keeps a result pending by storing its
    /// [`crate::destination::WriteEventsResult`] and not calling `send`. If
    /// input stops, shutdown starts, or the apply loop reaches the streaming
    /// write limit, ETL waits on that tail. The tail must eventually complete
    /// as [`DestinationWriteStatus::Durable`] once the accepted prefix is
    /// durable, or as `Err` if durability cannot be established.
    ///
    /// If there is no successor write to use as the tail, the destination
    /// should keep the current result pending until it can complete it as
    /// [`DestinationWriteStatus::Durable`] or `Err`; it should not complete the
    /// current result as `Accepted`.
    Accepted,
    /// This write and all earlier accepted writes in the same ordered
    /// apply-loop stream are durable according to the destination contract.
    ///
    /// The cumulative meaning is required for deferred destinations. When the
    /// apply loop observes this status, it may advance durable progress through
    /// the greatest commit end LSN carried by this write or by earlier accepted
    /// writes that have not yet been retired.
    Durable,
}

/// Durability-related behavior advertised by a destination.
///
/// This is the single destination-provided configuration surface for
/// durability behavior. It currently contains only streaming write limits, but
/// can grow copy-specific durability settings without adding more methods to
/// [`crate::destination::Destination`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityConfig {
    /// Streaming write backpressure limits.
    pub streaming_write_limits: StreamingWriteLimits,
}

impl DurabilityConfig {
    /// Creates a durability configuration.
    pub fn new(streaming_write_limits: StreamingWriteLimits) -> Self {
        Self { streaming_write_limits }
    }
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self { streaming_write_limits: StreamingWriteLimits::single() }
    }
}

/// Streaming write backpressure limits advertised by a destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingWriteLimits {
    /// Maximum number of streaming writes dispatched but not yet retired as
    /// durable.
    pub max_inflight_writes: NonZeroUsize,
    /// Maximum estimated bytes across streaming writes dispatched but not yet
    /// retired as durable.
    ///
    /// A value of `0` disables the byte limit. The apply loop always allows one
    /// write through an empty window, even if that write is larger than this
    /// limit, so a single oversized batch cannot deadlock streaming.
    pub max_inflight_bytes: usize,
}

impl StreamingWriteLimits {
    /// Creates streaming write limits.
    pub fn new(max_inflight_writes: NonZeroUsize, max_inflight_bytes: usize) -> Self {
        Self { max_inflight_writes, max_inflight_bytes }
    }

    /// Returns the compatibility configuration used by immediate destinations.
    pub fn single() -> Self {
        Self {
            max_inflight_writes: NonZeroUsize::new(1)
                .expect("single streaming write limit should be nonzero"),
            max_inflight_bytes: 0,
        }
    }
}

impl Default for StreamingWriteLimits {
    fn default() -> Self {
        Self::single()
    }
}
