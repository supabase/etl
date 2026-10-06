//! One-row zstd frames for rows too large to share the stream frame.
//!
//! A large row is compressed into its own complete frame before it is admitted
//! to a request, so its size is exact. The row streams from its cells through
//! the JSON serializer into the compressor; no serialized copy is allocated.
//! The row is compressed once at level 3 with a fixed 32 MiB window into a
//! buffer capped at the request limit. A row whose compressed frame exceeds
//! that limit is rejected.

use std::io::BufWriter;

use etl::{
    data::{ArrayCell, Cell, TableRow},
    schema::{ColumnSchema, TableId},
};
use metrics::counter;
use zstd::stream::Encoder;

use crate::snowflake::{
    Error, LargestColumn, Result,
    encoding::{CdcMeta, serialize_row, serialized_cell_len, serialized_row_len},
    metrics::{ETL_SNOWFLAKE_ROW_FRAMES_TOTAL, ROW_FRAME_OUTCOME_LABEL},
    streaming::batch::{
        buffer::BoundedBuffer,
        limits::{
            BASE_COMPRESSION_LEVEL, BatchLimits, ROW_FRAME_WINDOW_LOG, ROW_FRAME_WRITE_BUFFER_BYTES,
        },
    },
};

/// Metric outcome: the row frame fit the request limit.
const OUTCOME_FIT: &str = "fit";
/// Metric outcome: the row cannot fit a request.
const OUTCOME_REJECTED: &str = "rejected";

/// A complete zstd frame holding exactly one serialized row.
#[derive(Debug)]
pub(super) struct RowFrame {
    bytes: Vec<u8>,
}

impl RowFrame {
    /// Frame length in bytes.
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Frame bytes.
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

/// Result of one compression attempt into a capped buffer.
enum Attempt {
    /// The frame is complete and within the attempt's output cap.
    Complete { bytes: Vec<u8> },
    /// Output exceeded the cap; `output_lower_bound` is the length the first
    /// rejected write would have produced.
    Overflow { output_lower_bound: usize },
}

/// Lower bound on a row's serialized length, from cell lengths alone.
///
/// Used to skip the small-row scratch attempt for rows that certainly exceed
/// it. Strings and byte values dominate large rows; everything else counts as
/// zero so the hint never overestimates.
pub(super) fn serialized_len_hint(row: &TableRow) -> usize {
    row.values().iter().map(cell_len_hint).sum()
}

fn cell_len_hint(cell: &Cell) -> usize {
    match cell {
        Cell::String(text) => text.len(),
        Cell::Bytes(bytes) => bytes.len() * 2,
        Cell::Array(ArrayCell::String(items)) => items.iter().flatten().map(String::len).sum(),
        Cell::Array(ArrayCell::Bytes(items)) => {
            items.iter().flatten().map(|bytes| bytes.len() * 2).sum()
        }
        _ => 0,
    }
}

/// Compresses one row into its own zstd frame no larger than the request limit.
///
/// Returns [`Error::RowTooLarge`] when the row's level-3 frame exceeds the
/// request limit, and the row's own serialization error when its values
/// cannot be encoded.
pub(super) fn compress_row_frame(
    limits: &BatchLimits,
    table_id: TableId,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<RowFrame> {
    match attempt(limits.request_limit, cols, row, cdc)? {
        Attempt::Complete { bytes } => {
            record_outcome(OUTCOME_FIT);
            Ok(RowFrame { bytes })
        }
        Attempt::Overflow { output_lower_bound } => {
            record_outcome(OUTCOME_REJECTED);
            Err(row_too_large(limits, table_id, cols, row, cdc, output_lower_bound))
        }
    }
}

/// Serializes and compresses the row once into a capped level-3 frame with
/// the fixed row-frame window.
fn attempt(
    output_cap: usize,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<Attempt> {
    let mut encoder = Encoder::new(BoundedBuffer::new(output_cap), BASE_COMPRESSION_LEVEL)
        .map_err(|error| Error::Encoding(format!("Row frame compressor start failed: {error}")))?;
    encoder.window_log(ROW_FRAME_WINDOW_LOG).map_err(|error| {
        Error::Encoding(format!("Row frame compressor window setup failed: {error}"))
    })?;
    let mut buffered = BufWriter::with_capacity(ROW_FRAME_WRITE_BUFFER_BYTES, &mut encoder);
    let serialized = match serialize_row(&mut buffered, cols, row, cdc) {
        // `into_inner` drains the buffer without flushing the compressor, which
        // would end a zstd block early.
        Ok(()) => buffered.into_inner().map(drop).map_err(|error| {
            Error::Encoding(format!("Row frame write buffer flush failed: {}", error.error()))
        }),
        Err(error) => {
            drop(buffered);
            Err(error)
        }
    };

    if let Err(error) = serialized {
        return match encoder.get_ref().overflow() {
            Some(output_lower_bound) => Ok(Attempt::Overflow { output_lower_bound }),
            None => Err(error),
        };
    }

    match encoder.try_finish() {
        Ok(buffer) => Ok(Attempt::Complete { bytes: buffer.into_bytes() }),
        Err((encoder, error)) => match encoder.get_ref().overflow() {
            Some(output_lower_bound) => Ok(Attempt::Overflow { output_lower_bound }),
            None => Err(Error::Encoding(format!("Row frame compression finish failed: {error}"))),
        },
    }
}

/// Builds the size error for a row that cannot fit a request.
///
/// Sizes come from counting passes that allocate nothing. A row whose values
/// cannot be serialized reports that error instead, since the size is moot.
fn row_too_large(
    limits: &BatchLimits,
    table_id: TableId,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
    compressed_lower_bound: usize,
) -> Error {
    let serialized_bytes = match serialized_row_len(cols, row, cdc) {
        Ok(serialized_bytes) => serialized_bytes,
        Err(error) => return error,
    };
    let largest_column = cols
        .iter()
        .zip(row.values())
        .filter_map(|(col, cell)| {
            serialized_cell_len(cell)
                .ok()
                .map(|serialized_bytes| (col.name.as_str(), serialized_bytes))
        })
        .max_by_key(|(_, serialized_bytes)| *serialized_bytes)
        .map(|(name, serialized_bytes)| LargestColumn { name: name.to_owned(), serialized_bytes });

    Error::RowTooLarge {
        table_id,
        operation: cdc.operation,
        column_count: cols.len(),
        serialized_bytes,
        largest_column,
        compressed_lower_bound,
        request_limit: limits.request_limit,
    }
}

fn record_outcome(outcome: &'static str) {
    counter!(ETL_SNOWFLAKE_ROW_FRAMES_TOTAL, ROW_FRAME_OUTCOME_LABEL => outcome).increment(1);
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use etl::{
        data::{Cell, TableRow},
        schema::{ColumnSchema, TableId, Type},
    };

    use super::*;
    use crate::snowflake::{
        Error,
        encoding::{CdcMeta, CdcOperation, serialize_row},
    };

    fn table() -> TableId {
        TableId::new(7)
    }

    fn cols() -> [ColumnSchema; 2] {
        [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
        ]
    }

    fn row(payload: String) -> TableRow {
        TableRow::new(vec![Cell::I32(1), Cell::String(payload)])
    }

    fn cdc() -> CdcMeta<'static> {
        CdcMeta::new(CdcOperation::Insert, "0")
    }

    fn limits(request_limit: usize) -> BatchLimits {
        BatchLimits { request_limit, small_row_limit: 1024, flush_interval: 512 }
    }

    fn random_text(len: usize, seed: u64) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut state = seed;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                ALPHABET[((state >> 33) % 62) as usize] as char
            })
            .collect()
    }

    fn serialized(payload: &str) -> Vec<u8> {
        let mut line = Vec::new();
        serialize_row(&mut line, &cols(), &row(payload.to_owned()), cdc()).unwrap();
        line
    }

    #[test]
    fn compressible_row_fits_in_one_frame() {
        let payload = "a".repeat(100 * 1024);
        let frame =
            compress_row_frame(&limits(16 * 1024), table(), &cols(), &row(payload.clone()), cdc())
                .unwrap();

        assert!(frame.len() <= 16 * 1024);
        assert_eq!(zstd::decode_all(frame.as_slice()).unwrap(), serialized(&payload));
    }

    #[test]
    fn incompressible_row_over_the_cap_is_rejected() {
        let payload = random_text(64 * 1024, 1);
        let limits = limits(16 * 1024);
        let error = compress_row_frame(&limits, table(), &cols(), &row(payload.clone()), cdc())
            .unwrap_err();

        let Error::RowTooLarge {
            table_id,
            operation,
            column_count,
            serialized_bytes,
            largest_column,
            compressed_lower_bound,
            request_limit,
        } = error
        else {
            panic!("expected RowTooLarge, got {error:?}");
        };
        assert_eq!(table_id, table());
        assert_eq!(operation, CdcOperation::Insert);
        assert_eq!(column_count, 2);
        assert_eq!(serialized_bytes, serialized(&payload).len());
        let largest = largest_column.unwrap();
        assert_eq!(largest.name, "payload");
        // The JSON string includes its quotes.
        assert_eq!(largest.serialized_bytes, payload.len() + 2);
        assert!(compressed_lower_bound > limits.request_limit);
        assert_eq!(request_limit, limits.request_limit);
    }

    #[test]
    fn row_frame_accepts_the_exact_limit_and_rejects_one_byte_less() {
        let payload = random_text(64 * 1024, 3);
        let row = row(payload.clone());
        let frame = compress_row_frame(&limits(128 * 1024), table(), &cols(), &row, cdc()).unwrap();
        let frame_bytes = frame.len();

        let exact =
            compress_row_frame(&limits(frame_bytes), table(), &cols(), &row, cdc()).unwrap();
        assert_eq!(exact.as_slice(), frame.as_slice());
        assert_eq!(zstd::decode_all(exact.as_slice()).unwrap(), serialized(&payload));

        let error = compress_row_frame(&limits(frame_bytes - 1), table(), &cols(), &row, cdc())
            .unwrap_err();
        assert!(matches!(
            error,
            Error::RowTooLarge { compressed_lower_bound, request_limit, .. }
                if compressed_lower_bound == frame_bytes && request_limit == frame_bytes - 1
        ));
    }

    #[test]
    fn row_frames_declare_the_fixed_window_regardless_of_row_size() {
        let payload = "a".repeat(4 * 1024);
        let frame =
            compress_row_frame(&limits(16 * 1024), table(), &cols(), &row(payload.clone()), cdc())
                .unwrap();

        // Inspect the declared window through the decoder's memory limit
        // rather than assuming the frame header layout: one bit below the
        // fixed window must be refused, the fixed window must decode.
        let mut decoder = zstd::stream::read::Decoder::new(frame.as_slice()).unwrap();
        decoder.window_log_max(ROW_FRAME_WINDOW_LOG - 1).unwrap();
        assert!(decoder.read_to_end(&mut Vec::new()).is_err());

        let mut decoder = zstd::stream::read::Decoder::new(frame.as_slice()).unwrap();
        decoder.window_log_max(ROW_FRAME_WINDOW_LOG).unwrap();
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, serialized(&payload));
    }

    #[test]
    fn row_frames_roundtrip_json_escaping_and_binary_beyond_the_default_window() {
        let cases = [
            (Type::JSONB, Cell::Json(serde_json::json!({"text": "\0".repeat(512 * 1024)}))),
            (Type::TEXT, Cell::String("\0".repeat(512 * 1024))),
            (Type::BYTEA, Cell::Bytes(vec![0xab; 3 * 1024 * 1024 / 2])),
        ];
        for (ty, cell) in cases {
            let cols = [ColumnSchema::new("payload".into(), ty, -1, 1, false)];
            let row = TableRow::new(vec![cell]);
            let mut line = Vec::new();
            serialize_row(&mut line, &cols, &row, cdc()).unwrap();
            // JSON nesting, escaping, or hex encoding alone push these rows
            // past level 3's default 2 MiB window.
            assert!(line.len() > 2 * 1024 * 1024);
            assert!(line.len() < 4 * 1024 * 1024);

            let frame =
                compress_row_frame(&BatchLimits::SNOWFLAKE, table(), &cols, &row, cdc()).unwrap();
            assert_eq!(zstd::decode_all(frame.as_slice()).unwrap(), line);
        }
    }

    #[test]
    fn serialized_len_hint_is_a_lower_bound() {
        let text = row("hello world".repeat(100));
        let bytes = TableRow::new(vec![Cell::Bytes(vec![0xab; 500])]);
        let numbers = TableRow::new(vec![Cell::I64(1), Cell::Bool(true)]);

        assert!(serialized_len_hint(&text) <= serialized("hello world".repeat(100).as_str()).len());
        assert_eq!(serialized_len_hint(&text), 1100);
        assert_eq!(serialized_len_hint(&bytes), 1000);
        assert_eq!(serialized_len_hint(&numbers), 0);
    }
}
