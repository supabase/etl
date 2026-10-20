//! Snowpipe Streaming request bodies, built one row at a time.
//!
//! A request body is a sequence of zstd frames: one stream frame shared by
//! consecutive small rows, and one frame per large row. Every row is admitted
//! under the same rule, in arrival order: it joins the open request when the
//! body provably stays under the request limit, otherwise the open request
//! completes and the row starts the next one. Small rows are admitted under a
//! derived headroom (see [`limits`]); large rows are compressed first so their
//! exact size is known. The body buffer is capped at the limit, so no path can
//! produce an oversized request.

mod buffer;
mod frame;
mod limits;
mod request;

use std::mem;

use bytes::Bytes;
use etl::{
    data::TableRow,
    schema::{ColumnSchema, TableId},
};

use crate::snowflake::{
    Error, Result,
    encoding::{CdcMeta, serialize_row},
    streaming::{
        OffsetToken,
        batch::{
            buffer::BoundedBuffer,
            frame::{compress_row_frame, serialized_len_hint},
            limits::BatchLimits,
            request::OpenRequest,
        },
    },
};

/// Inclusive `[a, b]` offset range for a non-empty row batch.
#[derive(Debug)]
pub(super) struct OffsetRange {
    start: OffsetToken,
    end: OffsetToken,
}

/// Extension operations for an optional [`OffsetRange`].
pub(super) trait OffsetRangeExt {
    /// Extends the range with `offset`, creating it when empty.
    fn extend(&mut self, offset: &OffsetToken);
}

impl OffsetRangeExt for Option<OffsetRange> {
    fn extend(&mut self, offset: &OffsetToken) {
        match self {
            Some(range) => {
                debug_assert!(offset >= &range.end);
                range.end = offset.clone();
            }
            None => {
                *self = Some(OffsetRange { start: offset.clone(), end: offset.clone() });
            }
        }
    }
}

/// Batch of rows ready to be pushed to the Streaming API.
#[derive(Debug)]
pub struct RowBatch {
    data: Bytes,
    row_count: usize,
    offset_range: OffsetRange,
}

impl RowBatch {
    /// Wraps a complete request body.
    pub(super) fn new(data: Bytes, row_count: usize, offset_range: OffsetRange) -> Self {
        Self { data, row_count, offset_range }
    }

    /// Payload bytes.
    pub fn bytes(&self) -> &Bytes {
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

    /// Offset token of the first row in this batch.
    pub fn start_offset(&self) -> &OffsetToken {
        &self.offset_range.start
    }

    /// Offset token of the last row in this batch.
    pub fn end_offset(&self) -> &OffsetToken {
        &self.offset_range.end
    }

    /// Assigns the single Snowpipe request offset for this encoded batch.
    ///
    /// Copy batches are encoded before the channel reserves their attempt-local
    /// offset. Both request-range endpoints are set to `offset` while the
    /// encoded `_cdc_sequence_number` remains unchanged.
    pub(crate) fn with_request_offset(mut self, offset: OffsetToken) -> Self {
        self.offset_range = OffsetRange { start: offset.clone(), end: offset };
        self
    }

    /// Appends a zstd skippable frame so the body is exactly `len` bytes.
    ///
    /// Decoders ignore skippable frames, so the rows are unchanged. Tests use
    /// this to probe Snowflake's request limit at an exact byte count.
    ///
    /// # Panics
    ///
    /// Panics when `len` leaves no room for the 8-byte skippable frame header.
    #[cfg(feature = "test-utils")]
    pub fn padded_to(self, len: usize) -> Self {
        const SKIPPABLE_FRAME_MAGIC: u32 = 0x184D_2A50;
        const SKIPPABLE_FRAME_HEADER_BYTES: usize = 8;

        let padding = len
            .checked_sub(self.data.len() + SKIPPABLE_FRAME_HEADER_BYTES)
            .expect("padding target must leave room for the skippable frame header");
        let padding_len = u32::try_from(padding).expect("padding must fit a skippable frame");
        let mut body = Vec::with_capacity(len);
        body.extend_from_slice(&self.data);
        body.extend_from_slice(&SKIPPABLE_FRAME_MAGIC.to_le_bytes());
        body.extend_from_slice(&padding_len.to_le_bytes());
        body.resize(len, 0);
        Self { data: Bytes::from(body), ..self }
    }
}

/// Zero or one request completed by a push.
///
/// A push completes at most the previous request; the pushed row is always in
/// the open one. Dropping a completed request would lose its rows silently,
/// because later requests advance the channel offset past them, so the type
/// must be consumed.
#[must_use = "a completed Snowflake request must be sent before encoding more rows"]
#[derive(Debug, Default)]
pub struct Completed(Option<RowBatch>);

impl Completed {
    /// Returns whether no request completed.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl IntoIterator for Completed {
    type Item = RowBatch;
    type IntoIter = std::option::IntoIter<RowBatch>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Allocation sizes and counters of a builder, for tests and benchmarks.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuilderFootprint {
    /// Capacity of the small-row scratch buffer.
    pub scratch_capacity: usize,
    /// Capacity of the open request body.
    pub body_capacity: usize,
    /// Rows compressed into their own frame so far.
    pub row_frames: usize,
}

/// Builds Snowpipe request bodies for one table, one row at a time.
///
/// Callers must send every request returned by [`Self::push_row`] before
/// pushing more rows, and the one returned by [`Self::finish`] at the end.
/// After an error the caller must not acknowledge the failed input; the
/// builder itself stays usable only when the error was the row's own
/// serialization failure, which leaves the open request untouched.
pub struct RowBatchBuilder {
    table_id: TableId,
    limits: BatchLimits,
    scratch: BoundedBuffer,
    open: OpenRequest,
    row_frames: usize,
}

impl RowBatchBuilder {
    /// Creates a builder with the measured Snowflake limits.
    pub fn new(table_id: TableId) -> Self {
        Self::with_limits(table_id, BatchLimits::SNOWFLAKE)
    }

    /// Creates a builder with custom limits.
    ///
    /// # Panics
    ///
    /// Panics when the limits cannot hold one small row per request.
    pub(crate) fn with_limits(table_id: TableId, limits: BatchLimits) -> Self {
        assert!(
            limits.is_consistent(),
            "batch limits must leave room for one small row per request"
        );
        Self {
            table_id,
            limits,
            scratch: BoundedBuffer::new(limits.small_row_limit),
            open: OpenRequest::new(limits),
            row_frames: 0,
        }
    }

    /// Encodes one row, returning the request it completed, if any.
    pub fn push_row(
        &mut self,
        cols: &[ColumnSchema],
        row: &TableRow,
        cdc: CdcMeta<'_>,
        offset: &OffsetToken,
    ) -> Result<Completed> {
        if serialized_len_hint(row) <= self.limits.small_row_limit {
            self.scratch.clear();
            match serialize_row(&mut self.scratch, cols, row, cdc) {
                Ok(()) => return self.admit_small(offset),
                Err(error) if self.scratch.overflow().is_none() => return Err(error),
                // The row overflowed scratch: it takes the frame path below.
                Err(_) => {}
            }
        }

        let frame = compress_row_frame(&self.limits, self.table_id, cols, row, cdc)?;
        self.row_frames += 1;
        self.open.close_stream_frame()?;
        let completed = if self.open.row_count() > 0
            && self.open.len() + frame.len() > self.limits.request_limit
        {
            Some(self.rotate()?)
        } else {
            None
        };
        self.open.append_frame(frame.as_slice(), offset)?;
        Ok(Completed(completed))
    }

    /// Completes the open request, if it holds any row.
    pub fn finish(self) -> Result<Completed> {
        Ok(Completed(self.open.finish()?))
    }

    /// Current allocation sizes and counters.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn footprint(&self) -> BuilderFootprint {
        BuilderFootprint {
            scratch_capacity: self.scratch.capacity(),
            body_capacity: self.open.body_capacity(),
            row_frames: self.row_frames,
        }
    }

    /// Admits the serialized row in scratch to the stream frame.
    ///
    /// The admission check runs whenever the body length is exact: after a due
    /// flush, or when no stream frame is open (fresh request, or right after a
    /// row frame). Between checks at most one flush interval of input is
    /// pending, which the admission headroom covers.
    fn admit_small(&mut self, offset: &OffsetToken) -> Result<Completed> {
        let serialized_len = self.scratch.len();
        let exact = self.open.flush_if_due(serialized_len)? || !self.open.has_open_stream_frame();
        let completed = if exact
            && self.open.row_count() > 0
            && self.open.len() + serialized_len > self.limits.stream_admission_limit()
        {
            Some(self.rotate()?)
        } else {
            None
        };
        self.open.write_small(self.scratch.as_slice(), offset)?;
        Ok(Completed(completed))
    }

    /// Completes the open request and starts a new one.
    fn rotate(&mut self) -> Result<RowBatch> {
        let finished = mem::replace(&mut self.open, OpenRequest::new(self.limits));
        finished
            .finish()?
            .ok_or_else(|| Error::Encoding("Rotated an empty Snowflake request.".into()))
    }
}

#[cfg(test)]
mod tests {
    use etl::{
        data::{ArrayCell, Cell, TableRow},
        error::{ErrorKind, EtlError},
        schema::{ColumnSchema, PgLsn, TableId, Type},
        test_utils::property::run_property,
    };
    use proptest::{collection, prelude::*, prop_oneof};
    use serde_json::Value;

    use super::*;
    use crate::snowflake::{
        Error,
        encoding::{CdcMeta, CdcOperation, serialize_row},
        streaming::batch::limits::{BatchLimits, REQUEST_LIMIT_BYTES},
    };

    const REQUEST_LIMIT: usize = 64 * 1024;
    const SMALL_ROW_LIMIT: usize = 2048;

    fn limits() -> BatchLimits {
        BatchLimits {
            request_limit: REQUEST_LIMIT,
            small_row_limit: SMALL_ROW_LIMIT,
            flush_interval: 1024,
        }
    }

    fn builder() -> RowBatchBuilder {
        RowBatchBuilder::with_limits(TableId::new(7), limits())
    }

    fn cols() -> [ColumnSchema; 2] {
        [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
        ]
    }

    fn offset(ordinal: u64) -> OffsetToken {
        format!("{:016x}/{:016x}", 1u64, ordinal).parse().unwrap()
    }

    fn row(ordinal: u64, payload: String) -> TableRow {
        TableRow::new(vec![Cell::I32(ordinal as i32), Cell::String(payload)])
    }

    fn push(builder: &mut RowBatchBuilder, ordinal: u64, payload: String) -> Result<Completed> {
        let offset = offset(ordinal);
        builder.push_row(
            &cols(),
            &row(ordinal, payload),
            CdcMeta::new(CdcOperation::Insert, offset.as_ref()),
            &offset,
        )
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

    /// 256-byte records with 88 random letters each: realistic structured text
    /// with a compression ratio near 4.
    fn structured_text(len: usize, seed: u64) -> String {
        let random = random_text((len / 256 + 1) * 88, seed);
        let mut payload = String::with_capacity(len + 256);
        for chunk in random.as_bytes().chunks_exact(88) {
            let start = payload.len();
            payload.push_str("{\"token\":\"");
            payload.push_str(std::str::from_utf8(chunk).unwrap());
            payload.push_str("\",\"padding\":\"");
            while payload.len() < start + 253 {
                payload.push('0');
            }
            payload.push_str("\"}\n");
            if payload.len() >= len {
                break;
            }
        }
        payload.truncate(len);
        payload
    }

    fn serialized_len(ordinal: u64, payload: &str) -> usize {
        let mut line = Vec::new();
        serialize_row(
            &mut line,
            &cols(),
            &row(ordinal, payload.to_owned()),
            CdcMeta::new(CdcOperation::Insert, offset(ordinal).as_ref()),
        )
        .unwrap();
        line.len()
    }

    fn decode(batch: &RowBatch) -> Vec<Value> {
        let text = String::from_utf8(zstd::decode_all(batch.bytes().as_ref()).unwrap()).unwrap();
        text.lines().map(|line| serde_json::from_str(line).unwrap()).collect()
    }

    fn total_rows(batches: &[RowBatch]) -> usize {
        batches.iter().map(RowBatch::row_count).sum()
    }

    /// Checks that `batches` hold rows 1..=expected in order, each within
    /// `limit`, with contiguous offset ranges covering every row once.
    fn assert_rows_in_order(batches: &[RowBatch], expected: usize, limit: usize) {
        let mut ordinal = 1u64;
        for batch in batches {
            assert!(batch.size() <= limit, "request of {} B over {limit}", batch.size());
            assert!(batch.row_count() > 0);
            assert_eq!(batch.start_offset(), &offset(ordinal));
            let rows = decode(batch);
            assert_eq!(rows.len(), batch.row_count());
            for row in rows {
                assert_eq!(row["id"], Value::from(ordinal));
                assert_eq!(row["_cdc_operation"], Value::from("insert"));
                assert_eq!(row["_cdc_sequence_number"], Value::from(offset(ordinal).as_ref()));
                ordinal += 1;
            }
            assert_eq!(batch.end_offset(), &offset(ordinal - 1));
        }
        assert_eq!(ordinal - 1, expected as u64);
    }

    /// Pushes rows until the first request completes; returns it with the
    /// number of rows pushed so far.
    fn push_until_complete(
        builder: &mut RowBatchBuilder,
        payload: impl Fn(u64) -> String,
    ) -> (RowBatch, u64) {
        for ordinal in 1..1000 {
            if let Some(request) =
                push(builder, ordinal, payload(ordinal)).unwrap().into_iter().next()
            {
                return (request, ordinal);
            }
        }
        panic!("no request completed");
    }

    #[test]
    fn small_rows_share_one_request() {
        let mut builder = builder();
        for ordinal in 1..=10 {
            assert!(push(&mut builder, ordinal, format!("row_{ordinal}")).unwrap().is_empty());
        }

        let batches: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count(), 10);
        assert_rows_in_order(&batches, 10, REQUEST_LIMIT);
    }

    #[test]
    fn empty_builder_finishes_to_nothing() {
        assert!(builder().finish().unwrap().is_empty());
    }

    #[test]
    fn row_at_the_small_limit_streams_and_one_byte_more_becomes_a_frame() {
        let overhead = serialized_len(1, "");

        let mut streamed = builder();
        assert!(push(&mut streamed, 1, "a".repeat(SMALL_ROW_LIMIT - overhead)).unwrap().is_empty());
        assert_eq!(streamed.footprint().row_frames, 0);

        let mut framed = builder();
        assert!(
            push(&mut framed, 1, "a".repeat(SMALL_ROW_LIMIT - overhead + 1)).unwrap().is_empty()
        );
        assert_eq!(framed.footprint().row_frames, 1);
    }

    #[test]
    fn stream_requests_fill_to_the_guaranteed_minimum() {
        let mut builder = builder();
        let (request, pushed) =
            push_until_complete(&mut builder, |ordinal| random_text(1500, ordinal));

        assert!(request.size() <= REQUEST_LIMIT);
        assert!(
            request.size() >= limits().stream_admission_limit() - SMALL_ROW_LIMIT,
            "request of {} B is under the fill guarantee",
            request.size()
        );
        assert_eq!(builder.footprint().row_frames, 0);

        let mut batches = vec![request];
        batches.extend(builder.finish().unwrap());
        assert_rows_in_order(&batches, pushed as usize, REQUEST_LIMIT);
    }

    #[test]
    fn row_frames_pack_requests_exactly() {
        let payload_len = 8 * 1024;
        let mut single = builder();
        assert!(push(&mut single, 1, random_text(payload_len, 1)).unwrap().is_empty());
        let frame_len = single.finish().unwrap().into_iter().next().unwrap().size();

        let mut builder = builder();
        let (request, pushed) =
            push_until_complete(&mut builder, |ordinal| random_text(payload_len, ordinal));

        assert!(request.size() <= REQUEST_LIMIT);
        // Exact admission: the frame that completed this request would not have
        // fit. Frames of equal input length differ by a few bytes at most.
        assert!(
            request.size() + frame_len + 64 > REQUEST_LIMIT,
            "request of {} B left room for a {frame_len} B frame",
            request.size()
        );
        assert_eq!(builder.footprint().row_frames, pushed as usize);

        let mut batches = vec![request];
        batches.extend(builder.finish().unwrap());
        assert_rows_in_order(&batches, pushed as usize, REQUEST_LIMIT);
    }

    #[test]
    fn small_large_small_share_one_request() {
        let mut builder = builder();
        assert!(push(&mut builder, 1, "small".into()).unwrap().is_empty());
        assert!(push(&mut builder, 2, "a".repeat(100 * 1024)).unwrap().is_empty());
        assert!(push(&mut builder, 3, "small".into()).unwrap().is_empty());

        let batches: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(total_rows(&batches), 3);
        assert_rows_in_order(&batches, 3, REQUEST_LIMIT);
        assert_eq!(decode(&batches[0])[1]["payload"].as_str().unwrap().len(), 100 * 1024);
    }

    #[test]
    fn frame_that_does_not_fit_completes_the_previous_request_and_stays_open() {
        let mut builder = builder();
        assert!(push(&mut builder, 1, random_text(50 * 1024, 1)).unwrap().is_empty());

        let completed: Vec<RowBatch> =
            push(&mut builder, 2, random_text(50 * 1024, 2)).unwrap().into_iter().collect();

        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].row_count(), 1);
        assert_eq!(completed[0].end_offset(), &offset(1));
        let rest: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].start_offset(), &offset(2));
        let mut batches = completed;
        batches.extend(rest);
        assert_rows_in_order(&batches, 2, REQUEST_LIMIT);
    }

    #[test]
    fn small_row_after_a_large_frame_is_checked_even_without_a_flush() {
        // A long flush interval means small rows alone never trigger a flush;
        // the exact length right after a frame must still be checked.
        let mut limits = limits();
        limits.flush_interval = 32 * 1024;
        assert!(limits.is_consistent());
        let mut builder = RowBatchBuilder::with_limits(TableId::new(7), limits);

        assert!(push(&mut builder, 1, random_text(76 * 1024, 1)).unwrap().is_empty());
        let completed: Vec<RowBatch> =
            push(&mut builder, 2, "small".into()).unwrap().into_iter().collect();

        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].row_count(), 1);
        assert!(completed[0].size() > limits.stream_admission_limit());
        let rest: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();
        let mut batches = completed;
        batches.extend(rest);
        assert_rows_in_order(&batches, 2, REQUEST_LIMIT);
    }

    #[test]
    fn serialization_error_in_a_large_row_leaves_the_builder_usable() {
        let mut builder = builder();
        assert!(push(&mut builder, 1, "before".into()).unwrap().is_empty());

        let cols = [
            cols()[0].clone(),
            cols()[1].clone(),
            ColumnSchema::new("ratio".into(), Type::FLOAT8, -1, 3, true),
        ];
        let bad = TableRow::new(vec![
            Cell::I32(2),
            Cell::String("a".repeat(16 * 1024)),
            Cell::F64(f64::NAN),
        ]);
        let error = builder
            .push_row(&cols, &bad, CdcMeta::new(CdcOperation::Insert, "x"), &offset(2))
            .unwrap_err();
        assert!(matches!(&error, Error::Encoding(message) if message.contains("non-finite")));

        assert!(push(&mut builder, 2, "after".into()).unwrap().is_empty());
        let batches: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();
        assert_rows_in_order(&batches, 2, REQUEST_LIMIT);
    }

    #[test]
    fn oversized_row_is_an_unsupported_value_error() {
        let mut builder = builder();
        let error = push(&mut builder, 1, random_text(200 * 1024, 9)).unwrap_err();

        assert!(matches!(&error, Error::RowTooLarge { column_count: 2, .. }), "{error:?}");
        assert_eq!(EtlError::from(error).kind(), ErrorKind::UnsupportedValueInDestination);
    }

    #[test]
    fn column_count_mismatch_is_rejected_without_touching_the_request() {
        let mut builder = builder();
        let error = builder
            .push_row(
                &cols(),
                &TableRow::new(vec![Cell::I32(1)]),
                CdcMeta::new(CdcOperation::Insert, "0"),
                &offset(1),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Encoding(message)
                if message == "Row value count (1) does not match column count (2)."
        ));
        assert!(builder.finish().unwrap().is_empty());
    }

    /// A rejected JSON array changes neither buffered rows nor their offsets,
    /// on both the small-row and independent-frame paths.
    #[test]
    fn sql_null_json_array_element_does_not_advance_request() {
        for padding in [0, SMALL_ROW_LIMIT * 2] {
            let mut builder = builder();
            assert!(push(&mut builder, 1, "accepted".to_owned()).unwrap().is_empty());
            let columns = [
                ColumnSchema::new("padding".to_owned(), Type::TEXT, -1, 1, true),
                ColumnSchema::new("payload".to_owned(), Type::JSONB_ARRAY, -1, 2, true),
            ];
            // The size hint counts text cells, but deliberately ignores JSON.
            let row = TableRow::new(vec![
                Cell::String("x".repeat(padding)),
                Cell::Array(etl::data::ArrayCell::Json(vec![Some(Value::Null), None])),
            ]);
            let rejected_offset = offset(2);
            let error = builder
                .push_row(
                    &columns,
                    &row,
                    CdcMeta::new(CdcOperation::Update, rejected_offset.as_ref()),
                    &rejected_offset,
                )
                .unwrap_err();
            assert!(matches!(error, Error::NullJsonArrayElement { element_index: 1, .. }));
            let batches: Vec<_> = builder.finish().unwrap().into_iter().collect();
            assert_rows_in_order(&batches, 1, REQUEST_LIMIT);
        }
    }

    #[test]
    fn footprint_stays_within_the_limits() {
        let mut builder = builder();
        let mut batches = Vec::new();
        for ordinal in 1..=40 {
            let payload =
                if ordinal % 4 == 0 { "a".repeat(100 * 1024) } else { random_text(1500, ordinal) };
            batches.extend(push(&mut builder, ordinal, payload).unwrap());
            let footprint = builder.footprint();
            assert!(footprint.scratch_capacity <= SMALL_ROW_LIMIT);
            assert!(footprint.body_capacity <= REQUEST_LIMIT);
        }
        batches.extend(builder.finish().unwrap());
        assert_eq!(total_rows(&batches), 40);
        assert_rows_in_order(&batches, 40, REQUEST_LIMIT);
    }

    #[test]
    fn random_small_rows_never_overflow_the_request_cap() {
        let mut state = 42u64;
        let mut next = move || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 33
        };
        for iteration in 0..40 {
            let mut builder = builder();
            let mut batches = Vec::new();
            let rows = 20 + next() % 60;
            for ordinal in 1..=rows {
                let len = 1 + next() as usize % (SMALL_ROW_LIMIT - 64);
                let payload =
                    if next() % 2 == 0 { random_text(len, next()) } else { "z".repeat(len) };
                batches.extend(
                    push(&mut builder, ordinal, payload)
                        .unwrap_or_else(|error| panic!("iteration {iteration}: {error}")),
                );
            }
            batches.extend(builder.finish().unwrap());
            assert_rows_in_order(&batches, rows as usize, REQUEST_LIMIT);
        }
    }

    #[test]
    fn production_limits_accept_a_thirteen_megabyte_compressible_row() {
        let mut builder = RowBatchBuilder::new(TableId::new(1));
        let payload = structured_text(13_600_000, 1);

        assert!(push(&mut builder, 1, payload.clone()).unwrap().is_empty());
        let batches: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();

        assert_eq!(batches.len(), 1);
        assert!(batches[0].size() <= REQUEST_LIMIT_BYTES);
        assert_eq!(batches[0].row_count(), 1);
        assert_eq!(decode(&batches[0])[0]["payload"].as_str().unwrap().len(), payload.len());
    }

    #[test]
    fn production_limits_accept_repeated_blocks_in_text_and_json_rows() {
        let payload = random_text(3 * 1024 * 1024, 71).repeat(5);
        for (ty, cell) in [
            (Type::TEXT, Cell::String(payload.clone())),
            (Type::JSONB, Cell::Json(serde_json::json!({"text": payload}))),
        ] {
            let cols = [ColumnSchema::new("payload".into(), ty, -1, 1, false)];
            let row = TableRow::new(vec![cell]);
            let offset = offset(1);
            let cdc = CdcMeta::new(CdcOperation::Insert, offset.as_ref());
            let mut builder = RowBatchBuilder::new(TableId::new(7));
            assert!(builder.push_row(&cols, &row, cdc, &offset).unwrap().is_empty());
            let batches: Vec<RowBatch> = builder.finish().unwrap().into_iter().collect();

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].row_count(), 1);
            assert!(batches[0].size() <= REQUEST_LIMIT_BYTES);
            let mut line = Vec::new();
            serialize_row(&mut line, &cols, &row, cdc).unwrap();
            assert_eq!(zstd::decode_all(batches[0].bytes().as_ref()).unwrap(), line);
        }
    }

    #[test]
    fn production_limits_reject_an_incompressible_eight_mebibyte_row() {
        let mut builder = RowBatchBuilder::new(TableId::new(1));
        let error = push(&mut builder, 1, random_text(8 * 1024 * 1024, 2)).unwrap_err();
        assert!(matches!(error, Error::RowTooLarge { .. }), "{error:?}");
    }

    #[test]
    fn production_limits_pack_one_mebibyte_rows_to_at_least_ninety_percent() {
        let mut builder = RowBatchBuilder::new(TableId::new(1));
        let mut batches = Vec::new();
        for ordinal in 1..=32 {
            batches.extend(
                push(&mut builder, ordinal, structured_text(1024 * 1024, ordinal)).unwrap(),
            );
        }
        batches.extend(builder.finish().unwrap());

        assert!(batches.len() >= 2);
        for request in &batches[..batches.len() - 1] {
            assert!(request.size() <= REQUEST_LIMIT_BYTES);
            assert!(request.size() * 10 >= REQUEST_LIMIT_BYTES * 9, "fill {} B", request.size());
        }
        assert_rows_in_order(&batches, 32, REQUEST_LIMIT_BYTES);
    }

    /// Text fill of a generated row payload.
    #[derive(Debug, Clone, Copy)]
    enum Fill {
        /// One repeated byte, which compresses to almost nothing.
        Repeated,
        /// Random alphanumerics, which compress poorly.
        Random,
    }

    /// Value that makes a generated row fail to serialize, if any.
    #[derive(Debug, Clone, Copy)]
    enum Defect {
        /// The row serializes.
        None,
        /// A NaN `float8`, which Snowflake JSON cannot carry.
        NanRatio,
        /// A SQL NULL `jsonb[]` element, which JSON cannot tell from JSON null.
        NullJsonElement,
    }

    /// One generated row of the packing property.
    #[derive(Debug, Clone)]
    struct PackedRow {
        /// Payload length in bytes.
        len: usize,
        /// Payload fill.
        fill: Fill,
        /// Seed for the random fill and the numeric columns.
        seed: u64,
        /// Serialization failure to inject.
        defect: Defect,
    }

    /// Columns of the packing property: text payload plus the two value kinds
    /// that can fail serialization.
    fn packing_cols() -> [ColumnSchema; 4] {
        [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("ratio".into(), Type::FLOAT8, -1, 3, true),
            ColumnSchema::new("doc".into(), Type::JSONB_ARRAY, -1, 4, true),
        ]
    }

    /// Builds the row for `generated`.
    fn packing_row(ordinal: u64, generated: &PackedRow) -> TableRow {
        let payload = match generated.fill {
            Fill::Repeated => "z".repeat(generated.len),
            Fill::Random => random_text(generated.len, generated.seed),
        };
        let ratio = match generated.defect {
            Defect::NanRatio => f64::NAN,
            _ => generated.seed as f64,
        };
        let last_element = match generated.defect {
            Defect::NullJsonElement => None,
            _ => Some(Value::Null),
        };
        TableRow::new(vec![
            Cell::I32(ordinal as i32),
            Cell::String(payload),
            Cell::F64(ratio),
            Cell::Array(ArrayCell::Json(vec![
                Some(Value::from(generated.seed % 1000)),
                last_element,
            ])),
        ])
    }

    /// Consistent limits, including flush intervals both below and far above
    /// the small-row limit, and requests with little room beyond the headroom.
    fn consistent_limits() -> impl Strategy<Value = BatchLimits> {
        let flush_interval = prop_oneof![1usize..=256, 256usize..=16 * 1024];
        (64usize..=4096, flush_interval, 1usize..=32 * 1024).prop_map(
            |(small_row_limit, flush_interval, spare)| {
                let mut limits = BatchLimits { request_limit: 0, small_row_limit, flush_interval };
                limits.request_limit = limits.stream_headroom() + small_row_limit + spare;
                limits
            },
        )
    }

    /// Rows mixing small, boundary and large payloads with failing rows.
    fn packed_rows(limits: BatchLimits) -> impl Strategy<Value = Vec<PackedRow>> {
        let small = limits.small_row_limit;
        // The serialized row adds roughly 150 bytes of id, ratio, doc and CDC
        // fields, so this band straddles the stream/frame boundary.
        let len = prop_oneof![
            6 => 0..small,
            2 => small.saturating_sub(256)..small + 64,
            1 => small..limits.request_limit * 2,
        ];
        let fill = prop_oneof![Just(Fill::Repeated), Just(Fill::Random)];
        let defect = prop_oneof![
            8 => Just(Defect::None),
            1 => Just(Defect::NanRatio),
            1 => Just(Defect::NullJsonElement),
        ];
        collection::vec(
            (len, fill, any::<u64>(), defect).prop_map(|(len, fill, seed, defect)| PackedRow {
                len,
                fill,
                seed,
                defect,
            }),
            1..80,
        )
    }

    /// Every request fits the limit, and the requests carry exactly the
    /// accepted rows' serialized lines, in order, with matching row counts and
    /// contiguous offset ranges. Only rows that cannot serialize, or whose own
    /// frame exceeds the limit, are rejected.
    #[test]
    fn requests_pack_accepted_rows_exactly_under_random_limits() {
        let strategy =
            consistent_limits().prop_flat_map(|limits| (Just(limits), packed_rows(limits)));
        run_property("snowflake request packing", &strategy, |(limits, rows)| {
            let cols = packing_cols();
            let mut builder = RowBatchBuilder::with_limits(TableId::new(7), *limits);
            let mut batches = Vec::new();
            let mut accepted: Vec<(OffsetToken, Vec<u8>)> = Vec::new();
            for (index, generated) in rows.iter().enumerate() {
                let ordinal = index as u64;
                let offset = OffsetToken::new(PgLsn::from(10 + ordinal / 4), ordinal % 4);
                let row = packing_row(ordinal, generated);
                let cdc = CdcMeta::new(CdcOperation::Insert, offset.as_ref());
                let mut line = Vec::new();
                let serializable = serialize_row(&mut line, &cols, &row, cdc).is_ok();
                match builder.push_row(&cols, &row, cdc, &offset) {
                    Ok(completed) => {
                        prop_assert!(serializable, "row {index} cannot serialize but was accepted");
                        batches.extend(completed);
                        accepted.push((offset, line));
                    }
                    Err(Error::RowTooLarge { compressed_lower_bound, request_limit, .. }) => {
                        prop_assert!(serializable, "row {index} reported too large, not invalid");
                        prop_assert_eq!(request_limit, limits.request_limit);
                        prop_assert!(compressed_lower_bound > request_limit);
                        // Rows here are shorter than either compressor's
                        // window, so an independent level-3 frame is within a
                        // header's bytes of the builder's frame.
                        let independent = zstd::bulk::compress(&line, 3).unwrap().len();
                        prop_assert!(
                            independent + 64 > request_limit,
                            "row {index} rejected as too large but compresses to {independent} B"
                        );
                    }
                    Err(error) => {
                        prop_assert!(!serializable, "row {index} serializes but failed: {error}");
                    }
                }
            }
            let last = builder
                .finish()
                .map_err(|error| TestCaseError::fail(format!("finish failed: {error}")))?;
            batches.extend(last);

            let mut next = 0;
            for (index, batch) in batches.iter().enumerate() {
                prop_assert!(
                    batch.size() <= limits.request_limit,
                    "request {index} is {} B, over {}",
                    batch.size(),
                    limits.request_limit
                );
                prop_assert!(batch.row_count() > 0);
                prop_assert!(
                    next + batch.row_count() <= accepted.len(),
                    "request {index} has extra rows"
                );
                let rows = &accepted[next..next + batch.row_count()];
                prop_assert_eq!(batch.start_offset(), &rows[0].0);
                prop_assert_eq!(batch.end_offset(), &rows[rows.len() - 1].0);
                let decoded = zstd::decode_all(batch.bytes().as_ref()).unwrap();
                let expected =
                    rows.iter().flat_map(|(_, line)| line.iter().copied()).collect::<Vec<_>>();
                prop_assert!(
                    decoded == expected,
                    "request {index} body differs from its rows' lines"
                );
                next += batch.row_count();
            }
            prop_assert_eq!(next, accepted.len(), "accepted rows missing from requests");
            Ok(())
        });
    }
}
