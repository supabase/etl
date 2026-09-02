use std::mem::{size_of, size_of_val};

/// Reports an approximate decoded in-memory size in bytes.
///
/// The estimate includes the inline value and heap allocations uniquely owned
/// by it. Shared allocations, allocator bookkeeping, and destination-derived
/// buffers are excluded. Batch accumulators sum individual estimates, so spare
/// capacity in their outer containers is also excluded from this per-value
/// contract. This estimate is used for batching and backpressure; it does not
/// represent the PostgreSQL source payload and must not be used for usage
/// accounting.
pub trait SizeHint {
    /// Returns the approximate decoded in-memory size for this value.
    fn size_hint(&self) -> usize;
}

/// Returns the estimated uniquely owned heap bytes for a value.
pub(crate) fn owned_heap_size_hint<T>(value: &T) -> usize
where
    T: SizeHint,
{
    value.size_hint().saturating_sub(size_of_val(value))
}

impl<T, E> SizeHint for Result<T, E>
where
    T: SizeHint,
{
    fn size_hint(&self) -> usize {
        let owned_heap_bytes = match self {
            Ok(value) => owned_heap_size_hint(value),
            // Batching only retains successful decoded values. Keep the error-side
            // estimate to its inline representation without requiring every error
            // type to implement SizeHint.
            Err(_) => 0,
        };

        size_of::<Self>().saturating_add(owned_heap_bytes)
    }
}

impl<T> SizeHint for Option<T>
where
    T: SizeHint,
{
    fn size_hint(&self) -> usize {
        let owned_heap_bytes = self.as_ref().map(owned_heap_size_hint).unwrap_or_default();

        size_of::<Self>().saturating_add(owned_heap_bytes)
    }
}
