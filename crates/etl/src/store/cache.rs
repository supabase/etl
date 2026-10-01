use crate::error::EtlResult;

/// Prepares all cached data needed by a store's readers and writers.
///
/// Every state, schema, and lifecycle store requires this contract. Callers
/// inherit it through those traits without repeating the bound. Pipeline
/// startup prepares all domains once; implementations maintain them after
/// mutations and recover interrupted operations before serving cached data.
pub trait CachedStore {
    /// Loads all cache domains together from the store's authoritative state.
    ///
    /// Repeated calls refresh the complete cache. Implementations must
    /// serialize loading with mutations and publish only a complete,
    /// consistent result. A failed or cancelled load must not expose
    /// partially loaded state; subsequent access must recover or return an
    /// error.
    ///
    /// Implementations also own recovery after interrupted mutations; callers
    /// do not need to invoke this method before every read.
    ///
    /// In-memory stores can return success immediately because their data is
    /// already authoritative and available. Wrappers should forward this method
    /// to the underlying store.
    fn load_cache(&self) -> impl Future<Output = EtlResult<()>> + Send;
}
