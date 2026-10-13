//! Shared constants for internal ETL runtime behavior.

/// Default number of small work requests buffered by an internal channel.
///
/// Buffers bursts of metadata requests without retaining row payloads. This
/// bounds queued entries, not total memory or in-flight work. Each producer
/// defines its full-queue behavior: feedback waits, while schema cleanup
/// retains excess candidates for retry.
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
