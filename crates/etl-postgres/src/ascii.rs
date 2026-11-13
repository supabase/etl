//! Stack-buffered ASCII output for formatting Postgres values.

use std::{
    fmt::{self, Write},
    str,
};

/// Bytes of formatted output batched before each formatter write.
///
/// Large enough that a value of up to 64 characters, which covers ordinary
/// numerics and every temporal value, reaches the formatter in one write.
const ASCII_OUTPUT_BUFFER_BYTES: usize = 64;

/// Batches ASCII output on the stack, so a value reaches the formatter in a
/// few writes instead of one formatting call per digit.
pub(crate) struct AsciiOutput<'a, 'b> {
    /// Destination of flushed output.
    f: &'a mut fmt::Formatter<'b>,
    /// Pending output.
    bytes: [u8; ASCII_OUTPUT_BUFFER_BYTES],
    /// Number of pending bytes.
    len: usize,
}

impl<'a, 'b> AsciiOutput<'a, 'b> {
    /// Creates an empty buffer that writes to `f`.
    pub(crate) fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self { f, bytes: [0; ASCII_OUTPUT_BUFFER_BYTES], len: 0 }
    }

    /// Appends one ASCII byte, flushing first when the buffer is full.
    pub(crate) fn push(&mut self, byte: u8) -> fmt::Result {
        debug_assert!(byte.is_ascii());

        if self.len == self.bytes.len() {
            self.flush()?;
        }
        self.bytes[self.len] = byte;
        self.len += 1;

        Ok(())
    }

    /// Appends ASCII bytes in order.
    ///
    /// Values are appended a few bytes at a time, where a loop of inlined
    /// single-byte stores measured 14% fewer instructions per numeric than a
    /// `memcpy` call per group of digits (573 against 667).
    pub(crate) fn push_bytes(&mut self, bytes: &[u8]) -> fmt::Result {
        for byte in bytes {
            self.push(*byte)?;
        }

        Ok(())
    }

    /// Appends the low `WIDTH` decimal digits of `value`, zero-padded.
    pub(crate) fn push_decimal<const WIDTH: usize>(&mut self, mut value: u32) -> fmt::Result {
        let mut digits = [b'0'; WIDTH];
        for digit in digits.iter_mut().rev() {
            // The remainder is below ten, so the cast cannot truncate.
            *digit = b'0' + (value % 10) as u8;
            value /= 10;
        }

        self.push_bytes(&digits)
    }

    /// Writes all pending output to the formatter.
    pub(crate) fn finish(mut self) -> fmt::Result {
        self.flush()
    }

    /// Writes pending output to the formatter and empties the buffer.
    fn flush(&mut self) -> fmt::Result {
        let text = str::from_utf8(&self.bytes[..self.len])
            .expect("callers append only ASCII bytes, which are valid UTF-8");
        self.f.write_str(text)?;
        self.len = 0;

        Ok(())
    }
}

impl Write for AsciiOutput<'_, '_> {
    /// Writes text that need not be ASCII, after the pending output so order
    /// is kept.
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.flush()?;
        self.f.write_str(text)
    }
}
