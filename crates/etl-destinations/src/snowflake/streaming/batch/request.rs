//! One Snowpipe request body under construction.
//!
//! A body is a sequence of zstd frames. Small rows are appended to an open
//! stream frame compressing straight into the body buffer; large rows arrive
//! as complete frames and are appended after the stream frame is closed. The
//! body buffer is capped at the request limit, so no path can produce an
//! oversized request.

use std::{io::Write, mem};

use bytes::Bytes;
use zstd::stream::Encoder;

use crate::snowflake::{
    Error, Result,
    streaming::{
        OffsetToken,
        batch::{
            OffsetRange, OffsetRangeExt, RowBatch,
            buffer::BoundedBuffer,
            limits::{BASE_COMPRESSION_LEVEL, BatchLimits},
        },
    },
};

/// Request body bytes, with or without an open stream frame on top.
enum Body {
    /// Complete frames only; the next small row opens a new stream frame.
    Idle(BoundedBuffer),
    /// Complete frames followed by an open stream frame.
    Streaming(Encoder<'static, BoundedBuffer>),
    /// Placeholder while the body moves between the other two states.
    Transitioning,
}

/// A request body being assembled.
///
/// Owns everything that is per request: the body, the row count, the offset
/// range and the unflushed stream input. The offset range is created by the
/// first row, so a non-empty request always has one.
pub(super) struct OpenRequest {
    limits: BatchLimits,
    body: Body,
    row_count: usize,
    offset_range: Option<OffsetRange>,
    unflushed: usize,
}

impl OpenRequest {
    /// Creates an empty request.
    pub(super) fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            body: Body::Idle(BoundedBuffer::new(limits.request_limit)),
            row_count: 0,
            offset_range: None,
            unflushed: 0,
        }
    }

    /// Rows admitted so far.
    pub(super) fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns whether a stream frame is open, meaning [`Self::len`] may be
    /// stale by up to one flush interval of input.
    pub(super) fn has_open_stream_frame(&self) -> bool {
        matches!(self.body, Body::Streaming(_))
    }

    /// Bytes written to the body so far.
    ///
    /// Exact right after [`Self::flush_if_due`] returned `true` or after
    /// [`Self::close_stream_frame`]; otherwise the open stream frame may hold
    /// up to one flush interval of input not yet emitted.
    pub(super) fn len(&self) -> usize {
        match &self.body {
            Body::Idle(buffer) => buffer.len(),
            Body::Streaming(encoder) => encoder.get_ref().len(),
            Body::Transitioning => unreachable!("request body is never left transitioning"),
        }
    }

    /// Allocated body capacity, for footprint checks.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn body_capacity(&self) -> usize {
        match &self.body {
            Body::Idle(buffer) => buffer.capacity(),
            Body::Streaming(encoder) => encoder.get_ref().capacity(),
            Body::Transitioning => unreachable!("request body is never left transitioning"),
        }
    }

    /// Flushes the stream frame when the pending input plus `incoming` bytes
    /// reach the flush interval, making [`Self::len`] exact.
    pub(super) fn flush_if_due(&mut self, incoming: usize) -> Result<bool> {
        if self.unflushed + incoming < self.limits.flush_interval {
            return Ok(false);
        }
        if let Body::Streaming(encoder) = &mut self.body
            && let Err(error) = encoder.flush()
        {
            let overflowed = encoder.get_ref().overflow().is_some();
            return Err(self.body_write_error("stream flush", overflowed, error));
        }
        self.unflushed = 0;
        Ok(true)
    }

    /// Appends a serialized small row to the stream frame, opening the frame
    /// when the body is idle.
    pub(super) fn write_small(&mut self, serialized: &[u8], offset: &OffsetToken) -> Result<()> {
        if let Body::Idle(_) = self.body {
            let Body::Idle(buffer) = mem::replace(&mut self.body, Body::Transitioning) else {
                unreachable!("body state checked above");
            };
            // The encoder writes nothing until the first row arrives.
            let encoder = Encoder::new(buffer, BASE_COMPRESSION_LEVEL).map_err(|error| {
                Error::Encoding(format!("Stream frame compressor start failed: {error}"))
            })?;
            self.body = Body::Streaming(encoder);
        }
        let Body::Streaming(encoder) = &mut self.body else {
            unreachable!("body is streaming after the idle check");
        };
        if let Err(error) = encoder.write_all(serialized) {
            let overflowed = encoder.get_ref().overflow().is_some();
            return Err(self.body_write_error("stream write", overflowed, error));
        }
        self.unflushed += serialized.len();
        self.admit(offset);
        Ok(())
    }

    /// Finishes the open stream frame, if any, so the body length is exact
    /// and a complete frame can follow.
    pub(super) fn close_stream_frame(&mut self) -> Result<()> {
        if !matches!(self.body, Body::Streaming(_)) {
            return Ok(());
        }
        let Body::Streaming(encoder) = mem::replace(&mut self.body, Body::Transitioning) else {
            unreachable!("body state checked above");
        };
        match encoder.try_finish() {
            Ok(buffer) => {
                self.body = Body::Idle(buffer);
                self.unflushed = 0;
                Ok(())
            }
            Err((encoder, error)) => {
                // The request is unusable after this; leave an empty body so
                // the state stays well formed for the owner to discard.
                let overflowed = encoder.get_ref().overflow().is_some();
                self.body = Body::Idle(BoundedBuffer::new(self.limits.request_limit));
                Err(self.body_write_error("stream finish", overflowed, error))
            }
        }
    }

    /// Appends a complete row frame after closing the stream frame.
    pub(super) fn append_frame(&mut self, frame: &[u8], offset: &OffsetToken) -> Result<()> {
        self.close_stream_frame()?;
        let Body::Idle(buffer) = &mut self.body else {
            unreachable!("body is idle after closing the stream frame");
        };
        if let Err(error) = buffer.write_all(frame) {
            let overflowed = buffer.overflow().is_some();
            return Err(self.body_write_error("frame append", overflowed, error));
        }
        self.admit(offset);
        Ok(())
    }

    /// Completes the request. Returns `None` when no row was admitted.
    pub(super) fn finish(mut self) -> Result<Option<RowBatch>> {
        self.close_stream_frame()?;
        let Body::Idle(buffer) = mem::replace(&mut self.body, Body::Transitioning) else {
            unreachable!("body is idle after closing the stream frame");
        };
        match self.offset_range.take() {
            Some(offset_range) if self.row_count > 0 => Ok(Some(RowBatch::new(
                Bytes::from(buffer.into_bytes()),
                self.row_count,
                offset_range,
            ))),
            _ => Ok(None),
        }
    }

    fn admit(&mut self, offset: &OffsetToken) {
        self.row_count += 1;
        self.offset_range.extend(offset);
    }

    /// Maps a body write failure. Exceeding the body cap means the admission
    /// headroom proof was violated: a request never holds more than its cap
    /// by construction, so this is an internal error, not a data condition.
    fn body_write_error(&self, stage: &str, overflowed: bool, error: std::io::Error) -> Error {
        if overflowed {
            Error::Encoding(format!(
                "Snowflake request body exceeded {} B with {} rows during {stage}; admission \
                 headroom invariant violated",
                self.limits.request_limit, self.row_count
            ))
        } else {
            Error::Encoding(format!("Snowflake request body {stage} failed: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowflake::Error;

    fn limits() -> BatchLimits {
        BatchLimits { request_limit: 64 * 1024, small_row_limit: 1024, flush_interval: 128 }
    }

    fn offset(ordinal: u64) -> OffsetToken {
        format!("{:016x}/{:016x}", 1u64, ordinal).parse().unwrap()
    }

    fn frame(line: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(line, 3).unwrap()
    }

    #[test]
    fn flush_is_due_once_a_flush_interval_of_input_is_pending() {
        let mut request = OpenRequest::new(limits());
        request.write_small(&[b'a'; 100], &offset(1)).unwrap();

        assert!(!request.flush_if_due(20).unwrap());
        assert!(request.flush_if_due(28).unwrap());
        // A flush emits the pending block, so the body now has real bytes.
        let flushed_len = request.len();
        assert!(flushed_len > 0);

        request.write_small(&[b'b'; 10], &offset(2)).unwrap();
        assert!(!request.flush_if_due(10).unwrap());
        assert_eq!(request.len(), flushed_len);
    }

    #[test]
    fn frames_append_after_the_stream_frame_is_closed_and_rows_stay_in_order() {
        let mut request = OpenRequest::new(limits());
        request.write_small(b"small 1\n", &offset(1)).unwrap();

        request.close_stream_frame().unwrap();
        let closed_len = request.len();
        assert!(closed_len > 0);

        let large = frame(b"large 2\n");
        request.append_frame(&large, &offset(2)).unwrap();
        assert_eq!(request.len(), closed_len + large.len());

        request.write_small(b"small 3\n", &offset(3)).unwrap();
        let batch = request.finish().unwrap().unwrap();

        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.end_offset(), &offset(3));
        assert_eq!(
            zstd::decode_all(batch.bytes().as_ref()).unwrap(),
            b"small 1\nlarge 2\nsmall 3\n"
        );
    }

    #[test]
    fn body_overflow_is_reported_as_an_invariant_violation() {
        let mut limits = limits();
        limits.request_limit = 32;
        let mut request = OpenRequest::new(limits);
        request.write_small(&[b'x'; 10], &offset(1)).unwrap();

        // Incompressible input past the body cap cannot be flushed.
        let incompressible: Vec<u8> = (1..=40).collect();
        let error = request
            .write_small(&incompressible, &offset(2))
            .and_then(|()| request.close_stream_frame())
            .unwrap_err();

        assert!(
            matches!(&error, Error::Encoding(message) if message.contains("invariant")),
            "{error:?}"
        );
    }
}
