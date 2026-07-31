/// Reports an approximate decoded in-memory size in bytes.
///
/// This estimate is used for batching and backpressure. It does not represent
/// the PostgreSQL source payload and must not be used for usage accounting.
pub trait SizeHint {
    /// Returns the approximate decoded in-memory size for this value.
    fn size_hint(&self) -> usize;
}

impl<T, E> SizeHint for Result<T, E>
where
    T: SizeHint,
{
    fn size_hint(&self) -> usize {
        match self {
            Ok(value) => value.size_hint(),
            Err(_) => 0,
        }
    }
}

impl<T> SizeHint for Option<T>
where
    T: SizeHint,
{
    fn size_hint(&self) -> usize {
        match self {
            Some(value) => value.size_hint(),
            None => 0,
        }
    }
}
