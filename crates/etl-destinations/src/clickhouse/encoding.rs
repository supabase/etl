use std::fmt::{self, Write};

use chrono::{DateTime, NaiveDate};
use etl::{
    data::{ArrayCell, Cell, Date, Timestamp},
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use uuid::Uuid;

// RowBinary bytes are written directly via `Client::insert_formatted_with`,
// bypassing the typed `Inserter<T>` / serde path because:
// - `Insert::new` panics on empty `COLUMN_NAMES` (via `join_column_names`) even
//   with validation disabled.
// - The RowBinary serde serializer wraps `BufMut` with a fresh `&mut` on every
//   `serialize_some`, telescoping `&mut &mut ... BytesMut` on nullable array
//   elements and overflowing the compiler recursion limit.
//
// Direct byte-writing has no generics and no type-level recursion. Cells are
// encoded straight into the row buffer; text-formatted values go through one
// reused scratch string, so encoding a row allocates nothing.

/// Lowercase hex digits, indexed by nibble.
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Trailing CDC column values of one row, by table engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcColumns {
    /// MergeTree: `cdc_operation` (String), `cdc_lsn` (UInt64 commit LSN), and
    /// `cdc_tx_ordinal` (UInt64 transaction ordinal).
    MergeTree {
        /// Operation name, such as `INSERT`.
        operation: &'static str,
        /// Commit LSN of the source transaction.
        commit_lsn: u64,
        /// Ordinal of the change within its transaction.
        tx_ordinal: u64,
    },
    /// ReplacingMergeTree: `_etl_version` (UInt128 packed `EventPosition`)
    /// and `_etl_deleted` (UInt8 tombstone flag).
    ReplacingMergeTree {
        /// Packed event position; later events have larger versions.
        version: u128,
        /// Whether the row is a delete tombstone.
        deleted: bool,
    },
}

impl CdcColumns {
    /// Returns the number of trailing CDC columns.
    fn len(self) -> usize {
        match self {
            Self::MergeTree { .. } => 3,
            Self::ReplacingMergeTree { .. } => 2,
        }
    }
}

/// One destination row: user cells in source column order, then the CDC
/// columns.
#[derive(Debug)]
pub(crate) struct ClickHouseRow {
    /// User column values in source column order.
    pub(crate) cells: Vec<Cell>,
    /// Trailing CDC column values.
    pub(crate) cdc: CdcColumns,
}

/// RowBinary encoding shape of one destination column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnEncoding {
    /// `Nullable(T)`: a null-indicator byte precedes every value.
    Nullable,
    /// Non-nullable scalar `T`: NULL cannot be encoded.
    Required,
    /// Non-nullable `Array(...)`. ClickHouse rejects `Nullable(Array(...))`,
    /// so a top-level NULL is written as an empty array, matching BigQuery.
    Array,
}

impl ColumnEncoding {
    /// Classifies a ClickHouse column type as reported by `system.columns`.
    pub(crate) fn from_type_name(type_name: &str) -> Self {
        if type_name.starts_with("Nullable(") {
            Self::Nullable
        } else if type_name.starts_with("Array(") {
            Self::Array
        } else {
            Self::Required
        }
    }
}

/// Returns the error for an infinite date or timestamp, which ClickHouse's
/// finite wire integers cannot represent.
fn infinite_temporal_error() -> EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Cannot encode infinite temporal value",
        "ClickHouse dates and timestamps use finite wire integers"
    )
}

/// Rejects infinite dates and timestamps, including array elements.
///
/// Callers run this over a whole batch before inserting it, so such a value
/// fails the batch before any INSERT statement is sent.
pub(crate) fn ensure_finite_temporals(cells: &[Cell]) -> EtlResult<()> {
    let finite = cells.iter().all(|cell| match cell {
        Cell::Date(date) => matches!(date, Date::Value(_)),
        Cell::Timestamp(timestamp) => matches!(timestamp, Timestamp::Value(_)),
        Cell::TimestampTz(timestamp) => matches!(timestamp, Timestamp::Value(_)),
        Cell::Array(ArrayCell::Date(values)) => {
            values.iter().flatten().all(|date| matches!(date, Date::Value(_)))
        }
        Cell::Array(ArrayCell::Timestamp(values)) => {
            values.iter().flatten().all(|timestamp| matches!(timestamp, Timestamp::Value(_)))
        }
        Cell::Array(ArrayCell::TimestampTz(values)) => {
            values.iter().flatten().all(|timestamp| matches!(timestamp, Timestamp::Value(_)))
        }
        _ => true,
    });

    if finite { Ok(()) } else { Err(infinite_temporal_error()) }
}

/// Converts a [`NaiveDate`] to a ClickHouse `Date32` day offset (signed days
/// from 1970-01-01).
fn date_to_date32_days(d: NaiveDate) -> i32 {
    i32::try_from(d.signed_duration_since(unix_epoch()).num_days())
        .expect("chrono date offset should fit in the Date32 wire integer")
}

fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date")
}

/// Encodes `v` as LEB128 for ClickHouse string/array lengths.
pub(crate) fn rb_varint(mut v: usize, buf: &mut Vec<u8>) {
    loop {
        // LEB128: bottom 7 bits per byte, MSB set on all but the last.
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Encodes a length-prefixed RowBinary string.
fn rb_string(value: &[u8], buf: &mut Vec<u8>) {
    rb_varint(value.len(), buf);
    buf.extend_from_slice(value);
}

/// Encodes a value's display text as a RowBinary string, formatting it into
/// `scratch` first because the length prefix comes before the text.
fn rb_display(value: impl fmt::Display, scratch: &mut String, buf: &mut Vec<u8>) {
    scratch.clear();
    write!(scratch, "{value}").expect("formatting into a String cannot fail");
    rb_string(scratch.as_bytes(), buf);
}

/// Encodes bytes as a RowBinary string of lowercase hex digits.
fn rb_hex(bytes: &[u8], buf: &mut Vec<u8>) {
    rb_varint(bytes.len() * 2, buf);
    buf.reserve(bytes.len() * 2);
    for &byte in bytes {
        buf.push(HEX_DIGITS[usize::from(byte >> 4)]);
        buf.push(HEX_DIGITS[usize::from(byte & 0x0f)]);
    }
}

/// Encodes a UUID as ClickHouse RowBinary: the high `u64`, then the low
/// `u64`, each little-endian.
fn rb_uuid(value: &Uuid, buf: &mut Vec<u8>) {
    let (high, low) = value.as_u64_pair();
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
}

/// Encodes a date as `Date32`: days from the Unix epoch, little-endian.
fn rb_date(value: &Date<NaiveDate>, buf: &mut Vec<u8>) -> EtlResult<()> {
    let Date::Value(date) = value else {
        return Err(infinite_temporal_error());
    };
    buf.extend_from_slice(&date_to_date32_days(*date).to_le_bytes());

    Ok(())
}

/// Encodes microseconds since the Unix epoch as `DateTime64(6)`.
fn rb_micros<T>(
    value: &Timestamp<T>,
    micros: impl FnOnce(&T) -> i64,
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    let Timestamp::Value(timestamp) = value else {
        return Err(infinite_temporal_error());
    };
    buf.extend_from_slice(&micros(timestamp).to_le_bytes());

    Ok(())
}

/// Encodes one `Nullable(T)` array element: a null byte, then the value.
fn rb_element<T>(
    value: &Option<T>,
    buf: &mut Vec<u8>,
    encode: impl FnOnce(&T, &mut Vec<u8>) -> EtlResult<()>,
) -> EtlResult<()> {
    match value {
        None => buf.push(1),
        Some(value) => {
            buf.push(0);
            encode(value, buf)?;
        }
    }

    Ok(())
}

/// Encodes array elements, which ClickHouse always stores as
/// `Array(Nullable(T))`.
fn rb_elements<T>(
    values: &[Option<T>],
    buf: &mut Vec<u8>,
    mut encode: impl FnMut(&T, &mut Vec<u8>) -> EtlResult<()>,
) -> EtlResult<()> {
    rb_varint(values.len(), buf);
    for value in values {
        rb_element(value, buf, &mut encode)?;
    }

    Ok(())
}

/// Encodes an array cell.
fn rb_array(array: &ArrayCell, scratch: &mut String, buf: &mut Vec<u8>) -> EtlResult<()> {
    /// Encodes a fixed-width little-endian element.
    fn le<const N: usize>(bytes: [u8; N], buf: &mut Vec<u8>) -> EtlResult<()> {
        buf.extend_from_slice(&bytes);
        Ok(())
    }

    match array {
        ArrayCell::Bool(values) => rb_elements(values, buf, |v, buf| le([u8::from(*v)], buf)),
        ArrayCell::String(values) => rb_elements(values, buf, |v, buf| {
            rb_string(v.as_bytes(), buf);
            Ok(())
        }),
        ArrayCell::I16(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::I32(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::I64(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::U32(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::F32(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::F64(values) => rb_elements(values, buf, |v, buf| le(v.to_le_bytes(), buf)),
        ArrayCell::Numeric(values) => rb_elements(values, buf, |v, buf| {
            rb_display(v, scratch, buf);
            Ok(())
        }),
        ArrayCell::Date(values) => rb_elements(values, buf, rb_date),
        ArrayCell::Time(values) => rb_elements(values, buf, |v, buf| {
            rb_display(v, scratch, buf);
            Ok(())
        }),
        ArrayCell::TimeTz(values) => rb_elements(values, buf, |v, buf| {
            rb_display(v, scratch, buf);
            Ok(())
        }),
        ArrayCell::Timestamp(values) => {
            rb_elements(values, buf, |v, buf| rb_micros(v, |t| t.and_utc().timestamp_micros(), buf))
        }
        ArrayCell::TimestampTz(values) => {
            rb_elements(values, buf, |v, buf| rb_micros(v, DateTime::timestamp_micros, buf))
        }
        ArrayCell::Uuid(values) => rb_elements(values, buf, |v, buf| {
            rb_uuid(v, buf);
            Ok(())
        }),
        ArrayCell::Json(values) => rb_elements(values, buf, |v, buf| {
            rb_display(v, scratch, buf);
            Ok(())
        }),
        ArrayCell::Bytes(values) => rb_elements(values, buf, |v, buf| {
            rb_hex(v, buf);
            Ok(())
        }),
    }
}

/// Encodes a non-null cell for a column without a null indicator byte.
///
/// TEXT, NUMERIC, TIME, TIMETZ, and JSON are written as strings, BYTEA as hex
/// text, dates as `Date32`, and timestamps as `DateTime64(6)`.
fn rb_value(cell: &Cell, scratch: &mut String, buf: &mut Vec<u8>) -> EtlResult<()> {
    match cell {
        Cell::Null => {
            // The physical ClickHouse column is non-nullable, so encoding NULL
            // would require a lossy fallback value.
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "NULL value for non-nullable ClickHouse column"
            ));
        }
        Cell::Bool(value) => buf.push(u8::from(*value)),
        Cell::I16(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::I32(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::I64(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::U32(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::F32(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::F64(value) => buf.extend_from_slice(&value.to_le_bytes()),
        Cell::String(value) => rb_string(value.as_bytes(), buf),
        Cell::Numeric(value) => rb_display(value, scratch, buf),
        Cell::Date(value) => rb_date(value, buf)?,
        Cell::Time(value) => rb_display(value, scratch, buf),
        Cell::TimeTz(value) => rb_display(value, scratch, buf),
        Cell::Timestamp(value) => rb_micros(value, |t| t.and_utc().timestamp_micros(), buf)?,
        Cell::TimestampTz(value) => rb_micros(value, DateTime::timestamp_micros, buf)?,
        Cell::Uuid(value) => rb_uuid(value, buf),
        Cell::Json(value) => rb_display(value, scratch, buf),
        Cell::Bytes(value) => rb_hex(value, buf),
        Cell::Array(array) => rb_array(array, scratch, buf)?,
    }

    Ok(())
}

/// Encodes one cell with its column's encoding.
pub(crate) fn encode_cell(
    cell: &Cell,
    encoding: ColumnEncoding,
    scratch: &mut String,
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    match (encoding, cell) {
        (ColumnEncoding::Nullable, Cell::Null) => buf.push(1),
        (ColumnEncoding::Nullable, cell) => {
            buf.push(0);
            rb_value(cell, scratch, buf)?;
        }
        // An empty array is a single zero length varint.
        (ColumnEncoding::Array, Cell::Null) => rb_varint(0, buf),
        (ColumnEncoding::Array | ColumnEncoding::Required, cell) => rb_value(cell, scratch, buf)?,
    }

    Ok(())
}

/// Encodes one fixed-width CDC value, with a null indicator byte if its
/// column is nullable.
fn encode_cdc_value(bytes: &[u8], encoding: ColumnEncoding, buf: &mut Vec<u8>) {
    if encoding == ColumnEncoding::Nullable {
        buf.push(0);
    }
    buf.extend_from_slice(bytes);
}

/// Encodes the trailing CDC columns with their column encodings.
fn encode_cdc_columns(cdc: CdcColumns, encodings: &[ColumnEncoding], buf: &mut Vec<u8>) {
    match (cdc, encodings) {
        (
            CdcColumns::MergeTree { operation, commit_lsn, tx_ordinal },
            &[operation_encoding, lsn_encoding, ordinal_encoding],
        ) => {
            if operation_encoding == ColumnEncoding::Nullable {
                buf.push(0);
            }
            rb_string(operation.as_bytes(), buf);
            encode_cdc_value(&commit_lsn.to_le_bytes(), lsn_encoding, buf);
            encode_cdc_value(&tx_ordinal.to_le_bytes(), ordinal_encoding, buf);
        }
        (
            CdcColumns::ReplacingMergeTree { version, deleted },
            &[version_encoding, deleted_encoding],
        ) => {
            encode_cdc_value(&version.to_le_bytes(), version_encoding, buf);
            encode_cdc_value(&[u8::from(deleted)], deleted_encoding, buf);
        }
        _ => unreachable!("the caller checked that the encodings cover every CDC column"),
    }
}

/// Encodes a complete row into `buf`, selecting the encoding per column.
///
/// `scratch` holds formatted text between calls; pass the same string for
/// every row of a statement so formatting stops allocating once it has grown.
pub(crate) fn encode_row(
    row: &ClickHouseRow,
    column_encodings: &[ColumnEncoding],
    scratch: &mut String,
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    let value_count = row.cells.len() + row.cdc.len();
    if value_count != column_encodings.len() {
        return Err(etl_error!(
            ErrorKind::ConversionError,
            "ClickHouse RowBinary row width mismatch",
            format!(
                "values length {} does not match column encodings length {}",
                value_count,
                column_encodings.len()
            )
        ));
    }

    let (cell_encodings, cdc_encodings) = column_encodings.split_at(row.cells.len());
    for (cell, &encoding) in row.cells.iter().zip(cell_encodings) {
        encode_cell(cell, encoding, scratch, buf)?;
    }
    encode_cdc_columns(row.cdc, cdc_encodings, buf);

    Ok(())
}

#[cfg(kani)]
mod verification;

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use etl::data::{Cell, Date, Timestamp};
    use uuid::Uuid;

    use super::*;

    /// MergeTree CDC columns used where a row needs a trailer.
    const MERGE_TREE_CDC: CdcColumns =
        CdcColumns::MergeTree { operation: "INSERT", commit_lsn: 5, tx_ordinal: 9 };

    /// Encodings of the MergeTree CDC columns.
    const MERGE_TREE_CDC_ENCODINGS: [ColumnEncoding; 3] = [ColumnEncoding::Required; 3];

    /// Encodes one cell with a fresh buffer.
    fn encode(cell: &Cell, encoding: ColumnEncoding) -> EtlResult<Vec<u8>> {
        let mut buf = Vec::new();
        encode_cell(cell, encoding, &mut String::new(), &mut buf)?;

        Ok(buf)
    }

    /// Returns `value` as a RowBinary string.
    fn row_binary_string(value: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        rb_string(value.as_bytes(), &mut buf);

        buf
    }

    /// Returns the RowBinary bytes of [`MERGE_TREE_CDC`].
    fn merge_tree_cdc_bytes() -> Vec<u8> {
        let mut bytes = row_binary_string("INSERT");
        bytes.extend_from_slice(&5u64.to_le_bytes());
        bytes.extend_from_slice(&9u64.to_le_bytes());

        bytes
    }

    #[test]
    fn scalars_encode_as_little_endian_values() {
        assert_eq!(encode(&Cell::Bool(true), ColumnEncoding::Required).unwrap(), [1]);
        assert_eq!(
            encode(&Cell::I32(-1), ColumnEncoding::Required).unwrap(),
            (-1i32).to_le_bytes()
        );
        assert_eq!(
            encode(&Cell::I64(i64::MAX), ColumnEncoding::Required).unwrap(),
            i64::MAX.to_le_bytes()
        );
        assert_eq!(
            encode(&Cell::U32(u32::MAX), ColumnEncoding::Required).unwrap(),
            u32::MAX.to_le_bytes()
        );
        assert_eq!(
            encode(&Cell::String("hi".to_owned()), ColumnEncoding::Required).unwrap(),
            [2, b'h', b'i']
        );
    }

    #[test]
    fn dates_encode_as_signed_days_from_the_epoch() {
        for (date, days) in [((1970, 1, 1), 0i32), ((1970, 1, 2), 1), ((1969, 12, 31), -1)] {
            let date = NaiveDate::from_ymd_opt(date.0, date.1, date.2).unwrap();

            let bytes = encode(&Cell::Date(Date::Value(date)), ColumnEncoding::Required).unwrap();

            assert_eq!(bytes, days.to_le_bytes());
        }
    }

    #[test]
    fn dates_outside_the_clickhouse_domain_are_not_validated() {
        for date in [
            NaiveDate::from_ymd_opt(1899, 12, 31).unwrap(),
            NaiveDate::from_ymd_opt(2300, 1, 1).unwrap(),
        ] {
            let expected =
                i32::try_from(date.signed_duration_since(unix_epoch()).num_days()).unwrap();

            let bytes = encode(&Cell::Date(Date::Value(date)), ColumnEncoding::Required).unwrap();

            assert_eq!(bytes, expected.to_le_bytes());
        }
    }

    #[test]
    fn timestamps_encode_as_epoch_microseconds() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();

        let bytes =
            encode(&Cell::Timestamp(Timestamp::Value(epoch)), ColumnEncoding::Required).unwrap();

        assert_eq!(bytes, 0i64.to_le_bytes());
    }

    #[test]
    fn uuids_encode_high_then_low_half_little_endian() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let bytes = encode(&Cell::Uuid(uuid), ColumnEncoding::Required).unwrap();

        let high = u64::from_be_bytes(uuid.as_bytes()[0..8].try_into().unwrap());
        let low = u64::from_be_bytes(uuid.as_bytes()[8..16].try_into().unwrap());
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn bytes_encode_as_lowercase_hex_strings() {
        for (bytes, hex) in [
            (vec![], ""),
            (vec![0x00], "00"),
            (vec![0xff], "ff"),
            (vec![0xde, 0xad, 0xbe, 0xef], "deadbeef"),
        ] {
            assert_eq!(
                encode(&Cell::Bytes(bytes), ColumnEncoding::Required).unwrap(),
                row_binary_string(hex)
            );
        }
    }

    #[test]
    fn nullable_columns_prefix_a_null_indicator() {
        assert_eq!(encode(&Cell::Null, ColumnEncoding::Nullable).unwrap(), [1]);

        let mut expected = vec![0u8];
        expected.extend_from_slice(&42i32.to_le_bytes());
        assert_eq!(encode(&Cell::I32(42), ColumnEncoding::Nullable).unwrap(), expected);
    }

    #[test]
    fn varint_encoding() {
        let mut buf = Vec::new();
        rb_varint(0, &mut buf);
        assert_eq!(buf, [0x00]);

        buf.clear();
        rb_varint(127, &mut buf);
        assert_eq!(buf, [0x7f]);

        buf.clear();
        rb_varint(128, &mut buf);
        assert_eq!(buf, [0x80, 0x01]);

        buf.clear();
        rb_varint(300, &mut buf);
        assert_eq!(buf, [0xac, 0x02]);
    }

    /// NULL for a non-nullable column is a conversion error that writes no
    /// bytes.
    #[test]
    fn null_for_required_column_is_rejected_without_writing() {
        let mut buf = Vec::new();

        let error =
            encode_cell(&Cell::Null, ColumnEncoding::Required, &mut String::new(), &mut buf)
                .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ConversionError);
        assert!(buf.is_empty());
    }

    /// A top-level NULL array is stored as an empty array, while the row's
    /// other columns keep their own encodings.
    #[test]
    fn row_writes_null_array_as_empty_array() {
        let row = ClickHouseRow {
            cells: vec![Cell::Null, Cell::Null, Cell::I32(7)],
            cdc: MERGE_TREE_CDC,
        };
        let mut encodings =
            vec![ColumnEncoding::Array, ColumnEncoding::Nullable, ColumnEncoding::Required];
        encodings.extend(MERGE_TREE_CDC_ENCODINGS);
        let mut buf = Vec::new();

        encode_row(&row, &encodings, &mut String::new(), &mut buf).unwrap();

        let mut expected = vec![0x00, 0x01, 0x07, 0x00, 0x00, 0x00];
        expected.extend(merge_tree_cdc_bytes());
        assert_eq!(buf, expected);
    }

    /// The CDC columns follow the user cells, each with its column's encoding.
    #[test]
    fn row_appends_cdc_columns_for_each_engine() {
        let mut buf = Vec::new();
        let merge_tree = ClickHouseRow { cells: vec![Cell::Bool(true)], cdc: MERGE_TREE_CDC };
        let mut encodings = vec![ColumnEncoding::Required];
        encodings.extend(MERGE_TREE_CDC_ENCODINGS);

        encode_row(&merge_tree, &encodings, &mut String::new(), &mut buf).unwrap();

        let mut expected = vec![1];
        expected.extend(merge_tree_cdc_bytes());
        assert_eq!(buf, expected);

        buf.clear();
        let replacing = ClickHouseRow {
            cells: vec![Cell::Bool(true)],
            cdc: CdcColumns::ReplacingMergeTree { version: u128::MAX - 1, deleted: true },
        };
        let encodings =
            [ColumnEncoding::Required, ColumnEncoding::Required, ColumnEncoding::Nullable];

        encode_row(&replacing, &encodings, &mut String::new(), &mut buf).unwrap();

        let mut expected = vec![1];
        expected.extend_from_slice(&(u128::MAX - 1).to_le_bytes());
        expected.extend_from_slice(&[0, 1]);
        assert_eq!(buf, expected);
    }

    /// Only array columns turn NULL into a value; a required scalar column
    /// still rejects it.
    #[test]
    fn row_rejects_null_for_required_column() {
        let row = ClickHouseRow { cells: vec![Cell::Null], cdc: MERGE_TREE_CDC };
        let mut encodings = vec![ColumnEncoding::Required];
        encodings.extend(MERGE_TREE_CDC_ENCODINGS);

        let error = encode_row(&row, &encodings, &mut String::new(), &mut Vec::new()).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ConversionError);
    }

    /// A row narrower or wider than its layout is rejected before any byte is
    /// written. The width counts the CDC columns.
    #[test]
    fn row_width_mismatch_is_rejected_without_writing() {
        let row = ClickHouseRow { cells: vec![Cell::I32(1)], cdc: MERGE_TREE_CDC };

        for (encoding_count, detail) in [
            (5, "values length 4 does not match column encodings length 5"),
            (3, "values length 4 does not match column encodings length 3"),
        ] {
            let mut buf = vec![0xaa];
            let encodings = vec![ColumnEncoding::Required; encoding_count];

            let error = encode_row(&row, &encodings, &mut String::new(), &mut buf).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::ConversionError);
            assert_eq!(error.description(), Some("ClickHouse RowBinary row width mismatch"));
            assert_eq!(error.detail(), Some(detail));
            assert_eq!(buf, [0xaa]);
        }
    }

    #[test]
    fn special_temporal_values_require_faithful_wire_encodings() {
        for cell in [
            Cell::Date(Date::PosInfinity),
            Cell::Date(Date::NegInfinity),
            Cell::Timestamp(Timestamp::PosInfinity),
            Cell::TimestampTz(Timestamp::NegInfinity),
            Cell::Array(ArrayCell::Date(vec![None, Some(Date::PosInfinity)])),
            Cell::Array(ArrayCell::Timestamp(vec![None, Some(Timestamp::NegInfinity)])),
            Cell::Array(ArrayCell::TimestampTz(vec![None, Some(Timestamp::PosInfinity)])),
        ] {
            assert!(encode(&cell, ColumnEncoding::Required).is_err());
            assert!(ensure_finite_temporals(std::slice::from_ref(&cell)).is_err());
        }

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        ensure_finite_temporals(&[
            Cell::Date(Date::Value(epoch)),
            Cell::Array(ArrayCell::Date(vec![None, Some(Date::Value(epoch))])),
        ])
        .unwrap();

        assert_eq!(
            encode(&Cell::Time(etl::data::PgTime::EndOfDay), ColumnEncoding::Required).unwrap(),
            row_binary_string("24:00:00")
        );
    }

    /// Generated rows decoded with ClickHouse's documented RowBinary layout.
    mod properties {
        use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
        use etl::{
            data::{ArrayCell, Cell, Date, Timestamp},
            test_utils::property::{any_f32, any_f64, pg_text, run_property},
        };
        use proptest::{collection::vec, prelude::*};
        use uuid::Uuid;

        use crate::clickhouse::encoding::{CdcColumns, ClickHouseRow, ColumnEncoding, encode_row};

        /// Reads RowBinary bytes as the ClickHouse format documentation
        /// describes them.
        struct Reader<'a>(&'a [u8]);

        impl Reader<'_> {
            fn take<const N: usize>(&mut self) -> Result<[u8; N], String> {
                let (head, rest) = self.0.split_first_chunk::<N>().ok_or("row ended early")?;
                self.0 = rest;
                Ok(*head)
            }

            /// Reads an unsigned LEB128 length.
            fn length(&mut self) -> Result<usize, String> {
                let mut value = 0usize;
                for shift in (0..64).step_by(7) {
                    let [byte] = self.take::<1>()?;
                    value |= usize::from(byte & 0x7f) << shift;
                    if byte & 0x80 == 0 {
                        return Ok(value);
                    }
                }
                Err("length varint too long".to_owned())
            }

            fn string(&mut self) -> Result<String, String> {
                let length = self.length()?;
                let bytes = self.0.get(..length).ok_or("string ended early")?;
                self.0 = &self.0[length..];
                String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())
            }
        }

        /// Hex text of `bytes`, written independently of the encoder.
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        /// Array elements as scalar cells, `None` as SQL NULL.
        fn array_elements(array: &ArrayCell) -> Vec<Cell> {
            fn cells<T: Clone>(values: &[Option<T>], cell: fn(T) -> Cell) -> Vec<Cell> {
                values.iter().map(|value| value.clone().map_or(Cell::Null, cell)).collect()
            }
            match array {
                ArrayCell::Bool(values) => cells(values, Cell::Bool),
                ArrayCell::String(values) => cells(values, Cell::String),
                ArrayCell::I64(values) => cells(values, Cell::I64),
                ArrayCell::F64(values) => cells(values, Cell::F64),
                ArrayCell::Date(values) => cells(values, Cell::Date),
                ArrayCell::Uuid(values) => cells(values, Cell::Uuid),
                other => panic!("generator produced unexpected array {other:?}"),
            }
        }

        /// Reads one non-null value and checks that it encodes `cell`.
        fn check_value(reader: &mut Reader<'_>, cell: &Cell) -> Result<(), String> {
            let ok = match cell {
                Cell::Bool(value) => reader.take::<1>()? == [u8::from(*value)],
                Cell::I16(value) => i16::from_le_bytes(reader.take()?) == *value,
                Cell::I32(value) => i32::from_le_bytes(reader.take()?) == *value,
                Cell::I64(value) => i64::from_le_bytes(reader.take()?) == *value,
                Cell::U32(value) => u32::from_le_bytes(reader.take()?) == *value,
                Cell::F32(value) => f32::from_le_bytes(reader.take()?).to_bits() == value.to_bits(),
                Cell::F64(value) => f64::from_le_bytes(reader.take()?).to_bits() == value.to_bits(),
                Cell::String(value) => reader.string()? == *value,
                Cell::Bytes(value) => reader.string()? == hex(value),
                Cell::Date(Date::Value(date)) => {
                    let days = i32::from_le_bytes(reader.take()?);
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_signed(chrono::TimeDelta::days(i64::from(days)))
                        == Some(*date)
                }
                Cell::Timestamp(Timestamp::Value(timestamp)) => {
                    let micros = i64::from_le_bytes(reader.take()?);
                    DateTime::from_timestamp_micros(micros).map(|value| value.naive_utc())
                        == Some(*timestamp)
                }
                Cell::TimestampTz(Timestamp::Value(timestamp)) => {
                    DateTime::from_timestamp_micros(i64::from_le_bytes(reader.take()?))
                        == Some(*timestamp)
                }
                Cell::Uuid(value) => {
                    let high = u64::from_le_bytes(reader.take()?);
                    let low = u64::from_le_bytes(reader.take()?);
                    Uuid::from_u128(u128::from(high) << 64 | u128::from(low)) == *value
                }
                Cell::Array(array) => {
                    let elements = array_elements(array);
                    if reader.length()? != elements.len() {
                        return Err(format!("array length differs for {cell:?}"));
                    }
                    for element in &elements {
                        check_nullable(reader, element)?;
                    }
                    true
                }
                other => return Err(format!("generator produced unexpected cell {other:?}")),
            };
            if ok { Ok(()) } else { Err(format!("value differs for {cell:?}")) }
        }

        /// Reads a `Nullable(T)` value: a null byte, then the value if present.
        fn check_nullable(reader: &mut Reader<'_>, cell: &Cell) -> Result<(), String> {
            let [null] = reader.take::<1>()?;
            match (null, cell) {
                (1, Cell::Null) => Ok(()),
                (0, cell) if !matches!(cell, Cell::Null) => check_value(reader, cell),
                _ => Err(format!("null byte {null} does not match {cell:?}")),
            }
        }

        /// Dates across chrono's whole range, which includes every Postgres
        /// date.
        fn date() -> impl Strategy<Value = NaiveDate> {
            (NaiveDate::MIN.num_days_from_ce()..=NaiveDate::MAX.num_days_from_ce())
                .prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())
        }

        /// Microsecond timestamps across chrono's whole range.
        fn timestamp() -> impl Strategy<Value = NaiveDateTime> {
            let min = NaiveDate::MIN.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
            let max = NaiveDate::MAX
                .and_hms_micro_opt(23, 59, 59, 999_999)
                .unwrap()
                .and_utc()
                .timestamp_micros();
            (min..=max)
                .prop_map(|micros| DateTime::from_timestamp_micros(micros).unwrap().naive_utc())
        }

        /// Text long enough to need multi-byte LEB128 lengths.
        fn text() -> impl Strategy<Value = String> {
            prop_oneof![pg_text(), "[a-z]{120,400}"]
        }

        /// Arrays of 0 to 200 elements with SQL NULL mixed in.
        fn array<T: Clone + std::fmt::Debug + 'static>(
            element: impl Strategy<Value = T> + 'static,
            wrap: fn(Vec<Option<T>>) -> ArrayCell,
        ) -> BoxedStrategy<Cell> {
            let element = element.boxed();
            prop_oneof![
                vec(proptest::option::of(element.clone()), 0..4),
                vec(proptest::option::of(element), 120..200)
            ]
            .prop_map(move |values| Cell::Array(wrap(values)))
            .boxed()
        }

        /// A non-null cell of any type the encoder supports.
        fn value() -> BoxedStrategy<Cell> {
            prop_oneof![
                any::<bool>().prop_map(Cell::Bool),
                any::<i16>().prop_map(Cell::I16),
                any::<i32>().prop_map(Cell::I32),
                any::<i64>().prop_map(Cell::I64),
                any::<u32>().prop_map(Cell::U32),
                any_f32().prop_map(Cell::F32),
                any_f64().prop_map(Cell::F64),
                text().prop_map(Cell::String),
                vec(any::<u8>(), 0..80).prop_map(Cell::Bytes),
                date().prop_map(|date| Cell::Date(Date::Value(date))),
                timestamp().prop_map(|value| Cell::Timestamp(Timestamp::Value(value))),
                timestamp().prop_map(|value| Cell::TimestampTz(Timestamp::Value(value.and_utc()))),
                any::<u128>().prop_map(|value| Cell::Uuid(Uuid::from_u128(value))),
            ]
            .boxed()
        }

        /// A column: how ClickHouse stores it and the cell written to it.
        fn column() -> impl Strategy<Value = (ColumnEncoding, Cell)> {
            let arrays = prop_oneof![
                array(any::<bool>(), ArrayCell::Bool),
                array(text(), ArrayCell::String),
                array(any::<i64>(), ArrayCell::I64),
                array(any_f64(), ArrayCell::F64),
                array(date().prop_map(Date::Value), ArrayCell::Date),
                array(any::<u128>().prop_map(Uuid::from_u128), ArrayCell::Uuid),
            ];
            prop_oneof![
                3 => (value(), any::<bool>()).prop_map(|(cell, null)| {
                    (ColumnEncoding::Nullable, if null { Cell::Null } else { cell })
                }),
                3 => value().prop_map(|cell| (ColumnEncoding::Required, cell)),
                2 => (arrays, proptest::bool::weighted(0.2)).prop_map(|(cell, null)| {
                    (ColumnEncoding::Array, if null { Cell::Null } else { cell })
                }),
            ]
        }

        /// Trailing CDC columns for either engine.
        fn cdc_columns() -> impl Strategy<Value = CdcColumns> {
            prop_oneof![
                (
                    prop_oneof![Just("INSERT"), Just("UPDATE"), Just("DELETE")],
                    any::<u64>(),
                    any::<u64>()
                )
                    .prop_map(|(operation, commit_lsn, tx_ordinal)| {
                        CdcColumns::MergeTree { operation, commit_lsn, tx_ordinal }
                    }),
                (any::<u128>(), any::<bool>()).prop_map(|(version, deleted)| {
                    CdcColumns::ReplacingMergeTree { version, deleted }
                }),
            ]
        }

        /// Reads the trailing CDC columns and checks that they encode `cdc`.
        fn check_cdc(reader: &mut Reader<'_>, cdc: CdcColumns) -> Result<(), String> {
            let ok = match cdc {
                CdcColumns::MergeTree { operation, commit_lsn, tx_ordinal } => {
                    reader.string()? == operation
                        && u64::from_le_bytes(reader.take()?) == commit_lsn
                        && u64::from_le_bytes(reader.take()?) == tx_ordinal
                }
                CdcColumns::ReplacingMergeTree { version, deleted } => {
                    u128::from_le_bytes(reader.take()?) == version
                        && reader.take::<1>()? == [u8::from(deleted)]
                }
            };
            if ok { Ok(()) } else { Err(format!("CDC columns differ for {cdc:?}")) }
        }

        /// Every generated row encodes to exactly the RowBinary bytes the
        /// ClickHouse documentation describes, with nothing left over.
        #[test]
        fn rows_decode_with_documented_row_binary_layout() {
            let strategy = (vec(column(), 1..6), cdc_columns());
            run_property("clickhouse row binary layout", &strategy, |(columns, cdc)| {
                let cdc_column_count = match cdc {
                    CdcColumns::MergeTree { .. } => 3,
                    CdcColumns::ReplacingMergeTree { .. } => 2,
                };
                let encodings: Vec<_> = columns
                    .iter()
                    .map(|(encoding, _)| *encoding)
                    .chain(std::iter::repeat_n(ColumnEncoding::Required, cdc_column_count))
                    .collect();
                let row = ClickHouseRow {
                    cells: columns.iter().map(|(_, cell)| cell.clone()).collect(),
                    cdc: *cdc,
                };
                let mut bytes = Vec::new();
                encode_row(&row, &encodings, &mut String::new(), &mut bytes).unwrap();

                let mut reader = Reader(&bytes);
                for (encoding, cell) in columns {
                    let checked = match (encoding, cell) {
                        (ColumnEncoding::Nullable, cell) => check_nullable(&mut reader, cell),
                        (ColumnEncoding::Array, Cell::Null) => match reader.length() {
                            Ok(0) => Ok(()),
                            other => Err(format!("NULL array encoded as {other:?}")),
                        },
                        (_, cell) => check_value(&mut reader, cell),
                    };
                    checked.map_err(TestCaseError::fail)?;
                }
                check_cdc(&mut reader, *cdc).map_err(TestCaseError::fail)?;
                prop_assert!(reader.0.is_empty(), "{} trailing bytes", reader.0.len());
                Ok(())
            });
        }
    }
}
