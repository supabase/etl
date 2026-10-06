//! Size limits that shape Snowpipe Streaming request bodies.
//!
//! Snowflake rejects any request body over 4 MiB with HTTP 413 (measured on
//! 2026-10-02: 4,194,304 bytes accepted, 4,194,305 rejected). Everything else
//! here is derived from that limit and from zstd's worst-case output bounds so
//! that a request body can never exceed it.

/// Hard cap on a complete compressed request body, in bytes.
///
/// Snowflake documents this as "4 MB" and enforces exactly 4 MiB.
pub(crate) const REQUEST_LIMIT_BYTES: usize = 4 * 1024 * 1024;

/// Largest serialized row that shares the per-request stream frame.
///
/// Larger rows are compressed into their own frame so their size is known
/// exactly before admission. The cap bounds scratch memory and bounds the
/// pessimistic admission waste of the stream to one such row per request.
pub(crate) const SMALL_ROW_LIMIT_BYTES: usize = 256 * 1024;

/// Uncompressed input written to the stream frame between flushes.
///
/// A flush makes the compressed length exact for admission checks but ends a
/// zstd block, so flushing more often costs compression ratio.
pub(crate) const FLUSH_INTERVAL_BYTES: usize = 128 * 1024;

/// zstd frame header (6 bytes without content size or checksum) plus the end
/// block (3 bytes), rounded up.
pub(crate) const ZSTD_FRAME_OVERHEAD_BYTES: usize = 16;

/// Compression level for stream frames and row frames.
pub(crate) const BASE_COMPRESSION_LEVEL: i32 = 3;

/// zstd window log for row frames: 32 MiB of compression history.
///
/// Extends level 3's default 2 MiB history without a row-size counting pass.
/// Matches within this distance are eligible; zstd need not find every match.
/// Rows larger than the window can still fit [`REQUEST_LIMIT_BYTES`].
///
/// zstd 1.5.7 allocates about 33.5 MiB of compressor workspace per active row
/// frame at this setting, separate from the caller's source and output buffers.
/// Resident memory depends on the input and allocator, and concurrent large-row
/// compressors multiply this cost. Each frame declares the 32 MiB window;
/// Snowflake acceptance is covered by the credentialed integration tests.
pub(crate) const ROW_FRAME_WINDOW_LOG: u32 = 25;

/// Buffer between the JSON serializer and the zstd encoder on the row-frame
/// path, so JSON fragments do not each cross into the compressor.
pub(crate) const ROW_FRAME_WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// Mirrors `ZSTD_COMPRESSBOUND`: the most output zstd can emit for
/// `input_len` bytes of input, including the raw-block fallback.
pub(crate) const fn zstd_compress_bound(input_len: usize) -> usize {
    const BLOCK_BYTES: usize = 128 * 1024;
    let small_input_margin =
        if input_len < BLOCK_BYTES { (BLOCK_BYTES - input_len) >> 11 } else { 0 };
    input_len + (input_len >> 8) + small_input_margin
}

/// Limits for building one request body.
///
/// Production uses [`BatchLimits::SNOWFLAKE`]. Tests construct smaller limits
/// so boundaries can be exercised with kilobytes of data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchLimits {
    /// Hard cap on a complete request body.
    pub(crate) request_limit: usize,
    /// Largest serialized row that shares the stream frame.
    pub(crate) small_row_limit: usize,
    /// Input written to the stream frame between flushes.
    pub(crate) flush_interval: usize,
}

impl BatchLimits {
    /// Limits measured against Snowflake.
    pub(crate) const SNOWFLAKE: Self = Self {
        request_limit: REQUEST_LIMIT_BYTES,
        small_row_limit: SMALL_ROW_LIMIT_BYTES,
        flush_interval: FLUSH_INTERVAL_BYTES,
    };

    /// Bytes reserved below `request_limit` when admitting a small row.
    ///
    /// Covers the worst-case output of the unflushed input (one flush interval,
    /// or one small row when that is larger), the per-row zstd overhead of the
    /// largest small row, and the stream frame's header and end block.
    pub(crate) const fn stream_headroom(&self) -> usize {
        zstd_compress_bound(self.flush_interval)
            + (zstd_compress_bound(self.small_row_limit) - self.small_row_limit)
            + ZSTD_FRAME_OVERHEAD_BYTES
    }

    /// A small row is admitted when the exact body length plus its serialized
    /// length does not exceed this.
    pub(crate) const fn stream_admission_limit(&self) -> usize {
        self.request_limit - self.stream_headroom()
    }

    /// Returns whether a request can always hold at least one small row.
    pub(crate) const fn is_consistent(&self) -> bool {
        self.request_limit > self.stream_headroom() + self.small_row_limit
    }
}

const _: () = assert!(BatchLimits::SNOWFLAKE.is_consistent());
const _: () = assert!(BatchLimits::SNOWFLAKE.stream_headroom() == 132_624);
const _: () = assert!(BatchLimits::SNOWFLAKE.stream_admission_limit() == 4_061_680);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_bound_matches_zstd_formula() {
        // Below one block the formula adds a shrinking margin, above it only
        // 1/256.
        assert_eq!(zstd_compress_bound(1_000), 1_000 + 3 + 63);
        assert_eq!(zstd_compress_bound(131_072), 131_584);
        assert_eq!(zstd_compress_bound(262_144), 263_168);
    }

    #[test]
    fn consistency_requires_room_for_a_full_small_row() {
        let mut limits = BatchLimits::SNOWFLAKE;
        assert!(limits.is_consistent());

        limits.request_limit = limits.stream_headroom() + limits.small_row_limit;
        assert!(!limits.is_consistent());
    }
}
