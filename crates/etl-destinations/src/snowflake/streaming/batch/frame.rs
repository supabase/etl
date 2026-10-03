//! One-row zstd frames for rows too large to share the stream frame.
//!
//! A large row is compressed into its own complete frame before it is admitted
//! to a request, so its size is exact. The row streams from its cells through
//! the JSON serializer into the compressor; no serialized copy is allocated.
//! When the first attempt lands between the request limit and the escalation
//! cap, the row is compressed once more at the escalation level with a window
//! sized to the row. Only a row that fails that attempt is rejected.

use std::io::{self, BufWriter, Write};

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
            BASE_COMPRESSION_LEVEL, BatchLimits, ESCALATION_COMPRESSION_LEVEL,
            ESCALATION_WINDOW_LOG_MAX, ESCALATION_WINDOW_LOG_MIN, ROW_FRAME_WRITE_BUFFER_BYTES,
        },
    },
};

/// Metric outcome: the first attempt fit the request limit.
const OUTCOME_FIT: &str = "fit";
/// Metric outcome: the escalation attempt fit the request limit.
const OUTCOME_ESCALATED: &str = "escalated";
/// Metric outcome: the row cannot fit a request.
const OUTCOME_REJECTED: &str = "rejected";

/// A complete zstd frame holding exactly one serialized row.
#[derive(Debug)]
pub(super) struct RowFrame {
    bytes: Vec<u8>,
    serialized_bytes: usize,
    escalated: bool,
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

    /// Serialized NDJSON length of the row, including the newline.
    pub(super) fn serialized_bytes(&self) -> usize {
        self.serialized_bytes
    }

    /// Whether the escalation attempt produced this frame.
    pub(super) fn escalated(&self) -> bool {
        self.escalated
    }
}

/// Encoder settings for one compression attempt.
struct AttemptSettings {
    level: i32,
    window_log: Option<u32>,
    pledged_size: Option<u64>,
    output_cap: usize,
}

/// Result of one compression attempt into a capped buffer.
enum Attempt {
    /// The frame is complete and within the attempt's output cap.
    Complete { bytes: Vec<u8>, serialized_bytes: usize },
    /// Output exceeded the cap; `output_lower_bound` is the length the first
    /// rejected write would have produced.
    Overflow { output_lower_bound: usize },
}

/// Counts bytes passed through to the inner writer.
struct CountingWriter<W> {
    inner: W,
    written: usize,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(data)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
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
/// Returns [`Error::RowTooLarge`] when the row cannot fit even after the
/// escalation attempt, and the row's own serialization error when its values
/// cannot be encoded.
pub(super) fn compress_row_frame(
    limits: &BatchLimits,
    table_id: TableId,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<RowFrame> {
    let first = AttemptSettings {
        level: BASE_COMPRESSION_LEVEL,
        window_log: None,
        pledged_size: None,
        output_cap: limits.escalation_cap,
    };
    let serialized_bytes = match attempt(first, cols, row, cdc)? {
        Attempt::Complete { bytes, serialized_bytes } if bytes.len() <= limits.request_limit => {
            record_outcome(OUTCOME_FIT);
            return Ok(RowFrame { bytes, serialized_bytes, escalated: false });
        }
        Attempt::Complete { serialized_bytes, .. } => serialized_bytes,
        Attempt::Overflow { output_lower_bound } => {
            record_outcome(OUTCOME_REJECTED);
            return Err(row_too_large(limits, table_id, cols, row, cdc, output_lower_bound, false));
        }
    };

    let retry = AttemptSettings {
        level: ESCALATION_COMPRESSION_LEVEL,
        window_log: Some(escalation_window_log(serialized_bytes)),
        pledged_size: Some(serialized_bytes as u64),
        output_cap: limits.request_limit,
    };
    match attempt(retry, cols, row, cdc)? {
        Attempt::Complete { bytes, serialized_bytes } => {
            record_outcome(OUTCOME_ESCALATED);
            Ok(RowFrame { bytes, serialized_bytes, escalated: true })
        }
        Attempt::Overflow { output_lower_bound } => {
            record_outcome(OUTCOME_REJECTED);
            Err(row_too_large(limits, table_id, cols, row, cdc, output_lower_bound, true))
        }
    }
}

/// Window log that covers a row of `serialized_bytes`, within the escalation
/// range.
fn escalation_window_log(serialized_bytes: usize) -> u32 {
    let ceil_log2 = usize::BITS - (serialized_bytes.max(2) - 1).leading_zeros();
    ceil_log2.clamp(ESCALATION_WINDOW_LOG_MIN, ESCALATION_WINDOW_LOG_MAX)
}

/// Serializes and compresses the row once with the given settings.
fn attempt(
    settings: AttemptSettings,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<Attempt> {
    let mut encoder = Encoder::new(BoundedBuffer::new(settings.output_cap), settings.level)
        .map_err(|error| Error::Encoding(format!("Row frame compressor start failed: {error}")))?;
    if let Some(window_log) = settings.window_log {
        encoder.window_log(window_log).map_err(|error| {
            Error::Encoding(format!("Row frame compressor window setup failed: {error}"))
        })?;
    }
    if let Some(size) = settings.pledged_size {
        encoder.set_pledged_src_size(Some(size)).map_err(|error| {
            Error::Encoding(format!("Row frame compressor size pledge failed: {error}"))
        })?;
    }

    let mut counter = CountingWriter { inner: &mut encoder, written: 0 };
    let mut buffered = BufWriter::with_capacity(ROW_FRAME_WRITE_BUFFER_BYTES, &mut counter);
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
    let serialized_bytes = counter.written;

    if let Err(error) = serialized {
        return match encoder.get_ref().overflow() {
            Some(output_lower_bound) => Ok(Attempt::Overflow { output_lower_bound }),
            None => Err(error),
        };
    }

    match encoder.try_finish() {
        Ok(buffer) => Ok(Attempt::Complete { bytes: buffer.into_bytes(), serialized_bytes }),
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
    escalated: bool,
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
        escalated,
    }
}

fn record_outcome(outcome: &'static str) {
    counter!(ETL_SNOWFLAKE_ROW_FRAMES_TOTAL, ROW_FRAME_OUTCOME_LABEL => outcome).increment(1);
}

#[cfg(test)]
mod tests {
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
        BatchLimits {
            request_limit,
            small_row_limit: 1024,
            flush_interval: 512,
            escalation_cap: request_limit * 13 / 10,
        }
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

    /// Word text with a skewed vocabulary: compresses materially better at
    /// level 19 than at level 3.
    fn word_text(len: usize, seed: u64) -> String {
        let vocabulary: Vec<String> =
            (0..4096).map(|i| random_text(3 + (i % 7), seed ^ (i as u64 * 977))).collect();
        let mut state = seed;
        let mut text = String::with_capacity(len + 16);
        while text.len() < len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let skewed = ((state >> 33) % 4096) as usize;
            text.push_str(&vocabulary[skewed * skewed / 4096]);
            text.push(' ');
        }
        text.truncate(len);
        text
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
        assert!(!frame.escalated());
        assert_eq!(frame.serialized_bytes(), serialized(&payload).len());
        assert_eq!(zstd::decode_all(frame.as_slice()).unwrap(), serialized(&payload));
    }

    #[test]
    fn incompressible_row_over_the_cap_is_rejected_without_retry() {
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
            escalated,
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
        assert!(compressed_lower_bound > limits.escalation_cap);
        assert_eq!(request_limit, limits.request_limit);
        assert!(!escalated);
    }

    #[test]
    fn row_in_the_escalation_band_is_retried_at_the_higher_level() {
        let payload = word_text(300 * 1024, 3);
        let line = serialized(&payload);
        let level_3 =
            zstd::stream::encode_all(line.as_slice(), BASE_COMPRESSION_LEVEL).unwrap().len();
        let level_19 =
            zstd::stream::encode_all(line.as_slice(), ESCALATION_COMPRESSION_LEVEL).unwrap().len();
        // Precondition for the scenario: the higher level buys at least 5%.
        assert!(level_19 * 20 < level_3 * 19, "level 19 {level_19} vs level 3 {level_3}");

        let limits = limits((level_3 + level_19) / 2);
        assert!(limits.escalation_cap >= level_3);

        let frame = compress_row_frame(&limits, table(), &cols(), &row(payload), cdc()).unwrap();

        assert!(frame.escalated());
        assert!(frame.len() <= limits.request_limit);
        assert_eq!(zstd::decode_all(frame.as_slice()).unwrap(), line);
    }

    #[test]
    fn row_that_fails_the_retry_is_rejected_as_escalated() {
        let payload = word_text(300 * 1024, 5);
        let line = serialized(&payload);
        let level_3 =
            zstd::stream::encode_all(line.as_slice(), BASE_COMPRESSION_LEVEL).unwrap().len();
        let level_19 =
            zstd::stream::encode_all(line.as_slice(), ESCALATION_COMPRESSION_LEVEL).unwrap().len();
        assert!(level_19 < level_3);

        let mut limits = limits(level_19 * 9 / 10);
        limits.escalation_cap = level_3 + 1024;

        let error =
            compress_row_frame(&limits, table(), &cols(), &row(payload), cdc()).unwrap_err();

        assert!(
            matches!(error, Error::RowTooLarge { escalated: true, request_limit, .. } if request_limit == limits.request_limit),
            "{error:?}"
        );
    }

    #[test]
    fn serialization_failure_in_a_large_row_is_not_a_size_error() {
        let cols =
            [cols()[1].clone(), ColumnSchema::new("ratio".into(), Type::FLOAT8, -1, 2, true)];
        let row = TableRow::new(vec![Cell::String("a".repeat(64 * 1024)), Cell::F64(f64::NAN)]);

        let error =
            compress_row_frame(&limits(16 * 1024), table(), &cols, &row, cdc()).unwrap_err();

        assert!(matches!(error, Error::Encoding(message) if message.contains("non-finite")));
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
