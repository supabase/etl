/// Reports an approximate in-memory size in bytes.
pub trait SizeHint {
    /// Returns the approximate size in bytes for this value.
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
