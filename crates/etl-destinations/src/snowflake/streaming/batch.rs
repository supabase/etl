use std::{io::Write, mem};

use etl::types::{ColumnSchema, TableRow};
use zstd::stream::Encoder;

use crate::snowflake::{
    Error, Result,
    encoding::{CdcMeta, serialize_row},
    streaming::OffsetToken,
};

/// Snowflake Streaming API hard limit on the compressed HTTP request body.
const MAX_COMPRESSED_BYTES: usize = 4 * 1024 * 1024;

/// Split when compressed output reaches this threshold.
///
/// After a flush check decides not to split, up to [`MAX_UNFLUSHED_BYTES`]
/// (128KB) of input can arrive before the next check. So, the worst-case size
/// at `finish()` is roughly 3.8MB + 128KB ~ 3.93MB < 4MB hard limit.
///
/// The 200KB headroom exceeds the unflushed bytes limit by design.
const BATCH_SPLIT_THRESHOLD: usize = 3_800_000;

/// Max bytes written to the compressing encoder before forcing a flush.
const MAX_UNFLUSHED_BYTES: usize = 128 * 1024;

/// Max serialized (uncompressed) size of a single row.
///
/// Rejects degenerate TOAST rows before they enter the encoder.
const MAX_UNCOMPRESSED_ROW_BYTES: usize = 2 * 1024 * 1024;

/// Pre-allocated capacity for the per-row serialization scratch buffer.
///
/// One memory page covers most rows without reallocation, the buffer
/// grows automatically for larger rows and retains its high-water mark.
const SCRATCH_INITIAL_CAPACITY: usize = 4096;

/// Compression level.
///
/// Nice balance between compression and processor time spend compressing.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Batch of rows ready to be pushed to the Streaming API.
pub struct RowBatch {
    data: Vec<u8>,
    row_count: usize,
    offset: OffsetToken,
}

impl RowBatch {
    /// Payload bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// Byte length of the payload.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Number of rows in this batch.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Offset token of the last row in this batch.
    pub fn offset(&self) -> &OffsetToken {
        &self.offset
    }
}

/// Builds compressed row batches with streaming zstd compression.
///
/// Rows are serialized into a scratch buffer first, then written to the zstd
/// encoder. When the compressed output approaches `BATCH_SPLIT_THRESHOLD`,
/// the current batch is finished and a new encoder is started.
pub struct RowBatchBuilder {
    encoder: Encoder<'static, Vec<u8>>,
    scratch: Vec<u8>,
    current_row_count: usize,
    current_offset: OffsetToken,
    input_since_flush: usize,
    batches: Vec<RowBatch>,
}

impl Default for RowBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBatchBuilder {
    pub fn new() -> Self {
        Self {
            encoder: new_encoder(),
            scratch: Vec::with_capacity(SCRATCH_INITIAL_CAPACITY),
            current_row_count: 0,
            current_offset: OffsetToken::zero(),
            input_since_flush: 0,
            batches: Vec::new(),
        }
    }

    /// Append a row.
    ///
    /// Automatically creates a new batch when the compressed output approaches
    /// the API limit.
    pub fn push_row(
        &mut self,
        cols: &[ColumnSchema],
        row: &TableRow,
        cdc: CdcMeta<'_>,
        offset: &OffsetToken,
    ) -> Result<()> {
        self.scratch.clear();
        serialize_row(&mut self.scratch, cols, row, cdc)?;

        if self.scratch.len() > MAX_UNCOMPRESSED_ROW_BYTES {
            return Err(Error::Encoding(format!(
                "single row exceeds {}B limit ({}B uncompressed)",
                MAX_UNCOMPRESSED_ROW_BYTES,
                self.scratch.len()
            )));
        }

        // Flush compressing encoder and create a new batch, when necessary.
        if self.input_since_flush + self.scratch.len() >= MAX_UNFLUSHED_BYTES {
            self.encoder.flush().map_err(|e| Error::Encoding(format!("zstd flush: {e}")))?;
            self.input_since_flush = 0;

            // Wrap up the current batch and start another on threshold.
            if self.current_row_count > 0
                && self.compressed_size() + self.scratch.len() > BATCH_SPLIT_THRESHOLD
            {
                self.next_batch()?;
            }
        }

        self.encoder
            .write_all(&self.scratch)
            .map_err(|e| Error::Encoding(format!("zstd write: {e}")))?;
        self.input_since_flush += self.scratch.len();
        self.current_row_count += 1;
        self.current_offset = offset.clone();

        Ok(())
    }

    /// Wrap-up building process, produce list of batches.
    pub fn finish(mut self) -> Result<Vec<RowBatch>> {
        if self.current_row_count > 0 {
            let compressed =
                self.encoder.finish().map_err(|e| Error::Encoding(format!("zstd finish: {e}")))?;

            if compressed.len() > MAX_COMPRESSED_BYTES {
                return Err(Error::Encoding(format!(
                    "compressed batch exceeds {}B API limit ({}B compressed)",
                    MAX_COMPRESSED_BYTES,
                    compressed.len()
                )));
            }

            self.batches.push(RowBatch {
                data: compressed,
                row_count: self.current_row_count,
                offset: self.current_offset,
            });
        }

        Ok(self.batches)
    }

    fn compressed_size(&self) -> usize {
        self.encoder.get_ref().len()
    }

    fn next_batch(&mut self) -> Result<()> {
        let old_encoder = mem::replace(&mut self.encoder, new_encoder());
        let compressed =
            old_encoder.finish().map_err(|e| Error::Encoding(format!("zstd finish: {e}")))?;

        self.batches.push(RowBatch {
            data: compressed,
            row_count: self.current_row_count,
            offset: mem::take(&mut self.current_offset),
        });

        self.current_row_count = 0;
        self.input_since_flush = 0;

        Ok(())
    }
}

fn new_encoder() -> Encoder<'static, Vec<u8>> {
    Encoder::new(Vec::new(), ZSTD_COMPRESSION_LEVEL)
        .expect("hardcoded zstd compression level must be valid")
}

#[cfg(test)]
mod tests {
    use etl::types::{Cell, Type};

    use super::*;
    use crate::snowflake::encoding::{CdcMeta, CdcOperation};

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), Type::TEXT, -1, 1, None, true)
    }

    fn incompressible_string(len: usize, seed: u64) -> String {
        let mut s = String::with_capacity(len);
        let mut state = seed;
        for _ in 0..len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let ch = (state >> 33) as u8 % 94 + 33;
            s.push(ch as char);
        }
        s
    }

    #[test]
    fn single_batch() {
        let cols = [col("id"), col("name")];
        let mut builder = RowBatchBuilder::new();

        for i in 0..10 {
            let row = TableRow::new(vec![Cell::I32(i), Cell::String(format!("row_{i}"))]);
            let offset = OffsetToken::zero();
            builder
                .push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, "0"), &offset)
                .unwrap();
        }

        let batches = builder.finish().unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches.first().unwrap();
        assert_eq!(batch.row_count(), 10);
        assert!(batch.size() > 0);
        assert!(batch.size() <= MAX_COMPRESSED_BYTES);

        let decompressed = zstd::decode_all(batch.bytes()).unwrap();
        let text = String::from_utf8(decompressed).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 10);
        for line in &lines {
            serde_json::from_str::<serde_json::Value>(line).expect("valid JSON");
        }
    }

    #[test]
    fn empty_builder() {
        let builder = RowBatchBuilder::new();
        let batches = builder.finish().unwrap();
        assert!(batches.is_empty());
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn batch_splitting() {
        let cols = [col("data")];
        let mut builder = RowBatchBuilder::new();

        for i in 0..100 {
            let value = incompressible_string(100_000, i as u64);
            let row = TableRow::new(vec![Cell::String(value)]);
            let offset = OffsetToken::zero();
            builder
                .push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, &format!("{i}")), &offset)
                .unwrap();
        }

        let batches = builder.finish().unwrap();
        assert!(batches.len() > 1, "expected multiple batches, got {}", batches.len());

        for batch in &batches {
            assert!(
                batch.size() <= MAX_COMPRESSED_BYTES,
                "batch exceeds hard limit: {}",
                batch.size()
            );
            assert!(batch.row_count() > 0);

            let decompressed = zstd::decode_all(batch.bytes()).unwrap();
            let text = String::from_utf8(decompressed).unwrap();
            let lines: Vec<&str> = text.trim_end().split('\n').collect();
            assert_eq!(lines.len(), batch.row_count());
        }
    }

    #[test]
    fn oversized_row_rejected() {
        let cols = [col("data")];
        let mut builder = RowBatchBuilder::new();

        let huge_value = "x".repeat(MAX_UNCOMPRESSED_ROW_BYTES + 1);
        let row = TableRow::new(vec![Cell::String(huge_value)]);
        let err = builder
            .push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, "0"), &OffsetToken::zero())
            .unwrap_err();

        assert!(matches!(err, Error::Encoding(msg) if msg.contains("limit")));
    }

    #[test]
    fn offset_tracks_last_row() {
        let cols = [col("id")];
        let mut builder = RowBatchBuilder::new();

        let offsets = ["0000000000000001/0000000000000000", "0000000000000002/0000000000000000"];
        for token_str in &offsets {
            let row = TableRow::new(vec![Cell::I32(1)]);
            let offset: OffsetToken = token_str.parse().unwrap();
            builder
                .push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, "0"), &offset)
                .unwrap();
        }

        let batches = builder.finish().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches.first().unwrap().offset().as_ref(), offsets[1]);
    }
}
