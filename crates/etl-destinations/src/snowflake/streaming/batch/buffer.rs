//! Byte sink with a hard cap.

use std::{fmt, io};

/// Initial allocation for a bounded buffer.
///
/// One memory page covers most rows; the buffer doubles as needed and never
/// grows past its limit.
const INITIAL_CAPACITY: usize = 4096;

/// Error returned by [`BoundedBuffer`] when a write would exceed its limit.
#[derive(Debug)]
pub(super) struct LimitExceeded;

impl fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded buffer limit exceeded")
    }
}

impl std::error::Error for LimitExceeded {}

/// Growable byte sink that refuses to exceed a fixed limit.
///
/// A write that would cross the limit is rejected before anything is copied
/// or allocated, and the attempted length is recorded so callers can tell a
/// size rejection from any other I/O failure. This is the single enforcement
/// point for request body and row frame sizes.
pub(super) struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    overflow: Option<usize>,
}

impl BoundedBuffer {
    /// Creates an empty buffer that holds at most `limit` bytes.
    pub(super) fn new(limit: usize) -> Self {
        Self { bytes: Vec::with_capacity(INITIAL_CAPACITY.min(limit)), limit, overflow: None }
    }

    /// Number of bytes written.
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Allocated capacity, never above the limit.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Length the first rejected write would have produced, if any.
    pub(super) fn overflow(&self) -> Option<usize> {
        self.overflow
    }

    /// Written bytes.
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Forgets the written bytes and any recorded overflow, keeping the
    /// allocation for reuse.
    pub(super) fn clear(&mut self) {
        self.bytes.clear();
        self.overflow = None;
    }

    /// Returns the written bytes.
    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl BoundedBuffer {
    /// Grows the allocation for a write that does not fit it, or rejects the
    /// write when it would cross the limit.
    fn write_growing(&mut self, data: &[u8], required: usize) -> io::Result<usize> {
        if required > self.limit {
            self.overflow.get_or_insert(required);
            return Err(io::Error::other(LimitExceeded));
        }
        let target = required.max(self.bytes.capacity().saturating_mul(2)).min(self.limit);
        self.bytes.reserve_exact(target - self.bytes.len());
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
}

impl io::Write for BoundedBuffer {
    #[inline]
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.write_all(data)?;
        Ok(data.len())
    }

    /// Single checked append. JSON serialization issues many small writes per
    /// row through `write_all`; the default implementation loops over
    /// [`std::io::Write::write`], so this override keeps the hot path as cheap
    /// as a plain `Vec`: one capacity compare and a copy.
    #[inline]
    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let required = self.bytes.len().saturating_add(data.len());
        // The allocation never exceeds the limit, so a write that fits it needs
        // no limit check.
        if required <= self.bytes.capacity() {
            self.bytes.extend_from_slice(data);
            return Ok(());
        }
        self.write_growing(data, required).map(drop)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn accepts_writes_up_to_the_limit() {
        let mut buffer = BoundedBuffer::new(8);
        buffer.write_all(&[1; 5]).unwrap();
        buffer.write_all(&[2; 3]).unwrap();
        assert_eq!(buffer.len(), 8);
        assert_eq!(buffer.overflow(), None);
        assert_eq!(buffer.as_slice(), &[1, 1, 1, 1, 1, 2, 2, 2]);
    }

    #[test]
    fn rejects_the_write_that_would_exceed_the_limit_without_growing() {
        let mut buffer = BoundedBuffer::new(8);
        buffer.write_all(&[1; 7]).unwrap();
        let capacity = buffer.capacity();

        assert!(buffer.write_all(&[2; 2]).is_err());

        assert_eq!(buffer.len(), 7);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer.overflow(), Some(9));
    }

    #[test]
    fn overflow_records_the_first_attempt_only() {
        let mut buffer = BoundedBuffer::new(4);
        assert!(buffer.write_all(&[0; 10]).is_err());
        assert!(buffer.write_all(&[0; 20]).is_err());
        assert_eq!(buffer.overflow(), Some(10));
    }

    #[test]
    fn capacity_never_exceeds_the_limit() {
        let mut buffer = BoundedBuffer::new(10_000);
        for _ in 0..100 {
            buffer.write_all(&[7; 100]).unwrap();
        }
        assert_eq!(buffer.len(), 10_000);
        assert!(buffer.capacity() <= 10_000);
    }

    #[test]
    fn clear_keeps_the_allocation_and_resets_overflow() {
        let mut buffer = BoundedBuffer::new(4);
        buffer.write_all(&[1; 4]).unwrap();
        assert!(buffer.write_all(&[1]).is_err());
        let capacity = buffer.capacity();

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.overflow(), None);
        assert_eq!(buffer.capacity(), capacity);
    }

    #[test]
    fn into_bytes_returns_the_written_bytes() {
        let mut buffer = BoundedBuffer::new(16);
        buffer.write_all(b"frame").unwrap();
        assert_eq!(buffer.into_bytes(), b"frame");
    }
}
