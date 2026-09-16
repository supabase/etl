/// Result type that distinguishes between normal operation and shutdown
/// scenarios.
///
/// [`ShutdownResult`] is used by operations that can be interrupted by shutdown
/// signals. It preserves both successful results and any partial data that was
/// being processed when shutdown was requested.
pub(crate) enum ShutdownResult<T, I> {
    /// Normal successful completion with result data.
    Ok(T),
    /// Operation was interrupted by shutdown, with any partial data preserved.
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    /// Returns true if this result represents a shutdown scenario.
    pub(crate) fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}
