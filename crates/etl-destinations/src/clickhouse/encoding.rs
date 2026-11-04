use chrono::NaiveDate;
use etl::{
    data::{ArrayCell, Cell, Date, Timestamp},
    error::{ErrorKind, EtlResult},
    etl_error,
};

// RowBinary bytes are written directly via `Client::insert_formatted_with`,
// bypassing the typed `Inserter<T>` / serde path because:
// - `Insert::new` panics on empty `COLUMN_NAMES` (via `join_column_names`) even
//   with validation disabled.
// - The RowBinary serde serializer wraps `BufMut` with a fresh `&mut` on every
//   `serialize_some`, telescoping `&mut &mut ... BytesMut` on nullable array
//   elements and overflowing the compiler recursion limit.
//
// Direct byte-writing has no generics and no type-level recursion.

/// Owned ClickHouse-compatible value, moved (not cloned) from a [`Cell`].
pub(crate) enum ClickHouseValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    /// Unsigned 8-bit integer, used for the ReplacingMergeTree `_etl_deleted`
    /// tombstone.
    UInt8(u8),
    UInt32(u32),
    /// Unsigned 64-bit integer, used for MergeTree ordering metadata.
    UInt64(u64),
    /// Unsigned 128-bit integer, used for the ReplacingMergeTree `_etl_version`
    /// column (the packed `EventPosition`).
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    /// TEXT, NUMERIC, TIME, TIMETZ, INTERVAL, JSON, and BYTEA as strings.
    String(String),
    /// Days from Unix epoch (ClickHouse `Date32` on wire = Int32 LE).
    Date32(i32),
    /// Microseconds since Unix epoch (ClickHouse `DateTime64(6)` on wire =
    /// Int64 LE)
    DateTime64(i64),
    /// UUID in standard 16-byte big-endian order (converted to ClickHouse wire
    /// format on encode)
    Uuid([u8; 16]),
    Array(Vec<ClickHouseValue>),
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

/// Converts a [`Cell`] to a [`ClickHouseValue`], consuming it (no clone).
pub(crate) fn cell_to_clickhouse_value(cell: Cell) -> EtlResult<ClickHouseValue> {
    Ok(match cell {
        Cell::Null => ClickHouseValue::Null,
        Cell::Bool(b) => ClickHouseValue::Bool(b),
        Cell::I16(v) => ClickHouseValue::Int16(v),
        Cell::I32(v) => ClickHouseValue::Int32(v),
        Cell::I64(v) => ClickHouseValue::Int64(v),
        Cell::U32(v) => ClickHouseValue::UInt32(v),
        Cell::F32(v) => ClickHouseValue::Float32(v),
        Cell::F64(v) => ClickHouseValue::Float64(v),
        Cell::Numeric(n) => ClickHouseValue::String(n.to_string()),
        Cell::Date(Date::Value(d)) => ClickHouseValue::Date32(date_to_date32_days(d)),
        Cell::Time(t) => ClickHouseValue::String(t.to_string()),
        Cell::TimeTz(t) => ClickHouseValue::String(t.to_string()),
        Cell::Timestamp(Timestamp::Value(dt)) => {
            ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(Timestamp::Value(dt)) => {
            ClickHouseValue::DateTime64(dt.timestamp_micros())
        }
        Cell::Uuid(u) => ClickHouseValue::Uuid(*u.as_bytes()),
        Cell::Json(j) => ClickHouseValue::String(j.to_string()),
        Cell::Bytes(b) => ClickHouseValue::String(bytes_to_hex(&b)),
        Cell::String(s) => ClickHouseValue::String(s),
        Cell::Array(array_cell) => {
            ClickHouseValue::Array(array_cell_to_clickhouse_values(array_cell)?)
        }
        Cell::Date(_) | Cell::Timestamp(_) | Cell::TimestampTz(_) => {
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "Cannot encode infinite temporal value",
                "ClickHouse dates and timestamps use finite wire integers"
            ));
        }
    })
}

/// Converts an [`ArrayCell`] to a flat `Vec<ClickHouseValue>`, mapping each
/// `Some(x)` to the matching scalar variant and each `None` to
/// [`ClickHouseValue::Null`]. Per-element conversions mirror
/// [`cell_to_clickhouse_value`].
fn array_cell_to_clickhouse_values(array_cell: ArrayCell) -> EtlResult<Vec<ClickHouseValue>> {
    Ok(match array_cell {
        ArrayCell::Bool(v) => map_array(v, ClickHouseValue::Bool),
        ArrayCell::String(v) => map_array(v, ClickHouseValue::String),
        ArrayCell::I16(v) => map_array(v, ClickHouseValue::Int16),
        ArrayCell::I32(v) => map_array(v, ClickHouseValue::Int32),
        ArrayCell::I64(v) => map_array(v, ClickHouseValue::Int64),
        ArrayCell::U32(v) => map_array(v, ClickHouseValue::UInt32),
        ArrayCell::F32(v) => map_array(v, ClickHouseValue::Float32),
        ArrayCell::F64(v) => map_array(v, ClickHouseValue::Float64),
        ArrayCell::Numeric(v) => map_array(v, |n| ClickHouseValue::String(n.to_string())),
        ArrayCell::Date(v) => return map_temporal_array(v, Cell::Date),
        ArrayCell::Time(v) => map_array(v, |t| ClickHouseValue::String(t.to_string())),
        ArrayCell::TimeTz(v) => map_array(v, |t| ClickHouseValue::String(t.to_string())),
        ArrayCell::Timestamp(v) => return map_temporal_array(v, Cell::Timestamp),
        ArrayCell::TimestampTz(v) => return map_temporal_array(v, Cell::TimestampTz),
        ArrayCell::Uuid(v) => map_array(v, |u| ClickHouseValue::Uuid(*u.as_bytes())),
        ArrayCell::Json(v) => map_array(v, |j| ClickHouseValue::String(j.to_string())),
        ArrayCell::Bytes(v) => map_array(v, |b| ClickHouseValue::String(bytes_to_hex(&b))),
    })
}

/// Uses the scalar encoder for temporal array elements and preserves nulls.
fn map_temporal_array<T>(
    values: Vec<Option<T>>,
    cell: impl Fn(T) -> Cell,
) -> EtlResult<Vec<ClickHouseValue>> {
    values
        .into_iter()
        .map(|value| cell_to_clickhouse_value(value.map_or(Cell::Null, &cell)))
        .collect()
}

/// Maps a `Vec<Option<T>>` to `Vec<ClickHouseValue>`, applying `f` to each
/// `Some` and substituting [`ClickHouseValue::Null`] for each `None`.
fn map_array<T, F>(v: Vec<Option<T>>, mut f: F) -> Vec<ClickHouseValue>
where
    F: FnMut(T) -> ClickHouseValue,
{
    v.into_iter()
        .map(|o| match o {
            Some(t) => f(t),
            None => ClickHouseValue::Null,
        })
        .collect()
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

/// Lowercase hex-encodes `bytes` into a fresh `String`.
fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
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

/// Encodes a value for a `Nullable(T)` column (1-byte null indicator + value).
pub(crate) fn rb_encode_nullable(val: ClickHouseValue, buf: &mut Vec<u8>) -> EtlResult<()> {
    match val {
        ClickHouseValue::Null => buf.push(1),
        v => {
            buf.push(0);
            rb_encode_value(v, buf)?;
        }
    }
    Ok(())
}

/// Encodes a value for a non-nullable column (no null indicator byte).
pub(crate) fn rb_encode_value(val: ClickHouseValue, buf: &mut Vec<u8>) -> EtlResult<()> {
    match val {
        ClickHouseValue::Null => {
            // The physical ClickHouse column is non-nullable, so encoding NULL
            // would require a lossy fallback value.
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "NULL value for non-nullable ClickHouse column"
            ));
        }
        ClickHouseValue::Bool(b) => buf.push(u8::from(b)),
        ClickHouseValue::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt8(v) => buf.push(v),
        ClickHouseValue::UInt32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt128(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::String(s) => {
            rb_varint(s.len(), buf);
            buf.extend_from_slice(s.as_bytes());
        }
        ClickHouseValue::Date32(days) => buf.extend_from_slice(&days.to_le_bytes()),
        ClickHouseValue::DateTime64(micros) => buf.extend_from_slice(&micros.to_le_bytes()),
        ClickHouseValue::Uuid(bytes) => {
            // ClickHouse RowBinary UUID = high u64 (LE) then low u64 (LE). Our
            // bytes are in standard UUID big-endian order; reinterpret as a
            // u128 to split halves cleanly.
            let n = u128::from_be_bytes(bytes);
            buf.extend_from_slice(&((n >> 64) as u64).to_le_bytes());
            buf.extend_from_slice(&(n as u64).to_le_bytes());
        }
        // Array elements are always Nullable in ClickHouse: Array(Nullable(T)).
        ClickHouseValue::Array(items) => {
            rb_varint(items.len(), buf);
            for item in items {
                rb_encode_nullable(item, buf)?;
            }
        }
    }
    Ok(())
}

/// Encodes a complete row into `buf`, selecting the encoding per column.
pub(crate) fn encode_to_row_binary(
    values: Vec<ClickHouseValue>,
    column_encodings: &[ColumnEncoding],
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    if values.len() != column_encodings.len() {
        return Err(etl_error!(
            ErrorKind::ConversionError,
            "ClickHouse RowBinary row width mismatch",
            format!(
                "values length {} does not match column encodings length {}",
                values.len(),
                column_encodings.len()
            )
        ));
    }

    for (val, &encoding) in values.into_iter().zip(column_encodings.iter()) {
        match (encoding, val) {
            (ColumnEncoding::Nullable, val) => rb_encode_nullable(val, buf)?,
            // An empty array is a single zero length varint.
            (ColumnEncoding::Array, ClickHouseValue::Null) => rb_varint(0, buf),
            (ColumnEncoding::Array | ColumnEncoding::Required, val) => rb_encode_value(val, buf)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use etl::data::{Cell, Date, Timestamp};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn cell_to_clickhouse_value_null() {
        assert!(matches!(cell_to_clickhouse_value(Cell::Null).unwrap(), ClickHouseValue::Null));
    }

    #[test]
    fn cell_to_clickhouse_value_bool() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Bool(true)).unwrap(),
            ClickHouseValue::Bool(true)
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_i32() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::I32(42)).unwrap(),
            ClickHouseValue::Int32(42)
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_string() {
        if let ClickHouseValue::String(s) =
            cell_to_clickhouse_value(Cell::String("hello".to_owned())).unwrap()
        {
            assert_eq!(s, "hello");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn cell_to_clickhouse_value_date() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(Date::Value(epoch))).unwrap(),
            ClickHouseValue::Date32(0)
        ));

        let day1 = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(Date::Value(day1))).unwrap(),
            ClickHouseValue::Date32(1)
        ));

        // Pre-1970 dates round-trip through Date32 as a negative offset rather
        // than being silently clamped to the epoch.
        let pre_epoch = NaiveDate::from_ymd_opt(1969, 12, 31).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(Date::Value(pre_epoch))).unwrap(),
            ClickHouseValue::Date32(-1)
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_does_not_validate_date_domain() {
        let too_old = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
        let expected_too_old =
            i32::try_from(too_old.signed_duration_since(unix_epoch()).num_days()).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(Date::Value(too_old))).unwrap(),
            ClickHouseValue::Date32(days) if days == expected_too_old
        ));

        let too_new = NaiveDate::from_ymd_opt(2300, 1, 1).unwrap();
        let expected_too_new =
            i32::try_from(too_new.signed_duration_since(unix_epoch()).num_days()).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(Date::Value(too_new))).unwrap(),
            ClickHouseValue::Date32(days) if days == expected_too_new
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_timestamp() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Timestamp(Timestamp::Value(epoch))).unwrap(),
            ClickHouseValue::DateTime64(0)
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_uuid() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let expected_bytes = *u.as_bytes();
        if let ClickHouseValue::Uuid(bytes) = cell_to_clickhouse_value(Cell::Uuid(u)).unwrap() {
            assert_eq!(bytes, expected_bytes);
        } else {
            panic!("expected Uuid variant");
        }
    }

    #[test]
    fn cell_to_clickhouse_value_bytes_hex() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        if let ClickHouseValue::String(s) = cell_to_clickhouse_value(Cell::Bytes(bytes)).unwrap() {
            assert_eq!(s, "deadbeef");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn rb_encode_value_scalars() {
        let mut buf = Vec::new();

        buf.clear();
        rb_encode_value(ClickHouseValue::Bool(true), &mut buf).unwrap();
        assert_eq!(buf, [1u8]);

        buf.clear();
        rb_encode_value(ClickHouseValue::Int32(-1), &mut buf).unwrap();
        assert_eq!(buf, (-1i32).to_le_bytes());

        buf.clear();
        rb_encode_value(ClickHouseValue::UInt64(u64::MAX), &mut buf).unwrap();
        assert_eq!(buf, u64::MAX.to_le_bytes());

        buf.clear();
        rb_encode_value(ClickHouseValue::String("hi".to_owned()), &mut buf).unwrap();
        assert_eq!(buf, [2, b'h', b'i']); // varint(2) + bytes

        buf.clear();
        rb_encode_value(ClickHouseValue::Date32(1), &mut buf).unwrap();
        assert_eq!(buf, 1i32.to_le_bytes());

        buf.clear();
        rb_encode_value(ClickHouseValue::Date32(-1), &mut buf).unwrap();
        assert_eq!(buf, (-1i32).to_le_bytes());
    }

    #[test]
    fn rb_encode_uuid_wire_format() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let val = ClickHouseValue::Uuid(*u.as_bytes());
        let mut buf = Vec::new();
        rb_encode_value(val, &mut buf).unwrap();

        assert_eq!(buf.len(), 16);
        let bytes = u.as_bytes();
        let high = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let low = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn encode_nullable() {
        let mut buf = Vec::new();

        rb_encode_nullable(ClickHouseValue::Null, &mut buf).unwrap();
        assert_eq!(buf, [1u8]);

        buf.clear();
        rb_encode_nullable(ClickHouseValue::Int32(42), &mut buf).unwrap();
        let mut expected = vec![0u8];
        expected.extend_from_slice(&42i32.to_le_bytes());
        assert_eq!(buf, expected);
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

    #[test]
    fn hex_encoding() {
        assert_eq!(bytes_to_hex(&[]), "");
        assert_eq!(bytes_to_hex(&[0x00]), "00");
        assert_eq!(bytes_to_hex(&[0xff]), "ff");
        assert_eq!(bytes_to_hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    /// NULL for a non-nullable column is a conversion error that writes no
    /// bytes.
    #[test]
    fn rb_encode_value_rejects_null_for_non_nullable_column() {
        // GIVEN: an empty buffer.
        let mut buf = Vec::new();

        // WHEN: NULL is encoded without a null marker.
        let result = rb_encode_value(ClickHouseValue::Null, &mut buf);

        // THEN: encoding fails and the buffer stays empty.
        assert!(result.is_err(), "NULL in non-nullable column must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert!(buf.is_empty(), "no bytes should be written on error");
    }

    /// A top-level NULL array is stored as an empty array, while the row's
    /// other columns keep their own encodings.
    #[test]
    fn encode_to_row_binary_writes_null_array_as_empty_array() {
        let encodings = [ColumnEncoding::Array, ColumnEncoding::Nullable, ColumnEncoding::Required];
        let mut buf = Vec::new();
        encode_to_row_binary(
            vec![ClickHouseValue::Null, ClickHouseValue::Null, ClickHouseValue::Int32(7)],
            &encodings,
            &mut buf,
        )
        .unwrap();

        let mut expected = Vec::new();
        encode_to_row_binary(
            vec![
                ClickHouseValue::Array(Vec::new()),
                ClickHouseValue::Null,
                ClickHouseValue::Int32(7),
            ],
            &encodings,
            &mut expected,
        )
        .unwrap();
        assert_eq!(buf, expected);
        assert_eq!(buf, [0x00, 0x01, 0x07, 0x00, 0x00, 0x00]);
    }

    /// Only array columns turn NULL into a value; a required scalar column
    /// still rejects it.
    #[test]
    fn encode_to_row_binary_rejects_null_for_required_column() {
        let error = encode_to_row_binary(
            vec![ClickHouseValue::Null],
            &[ColumnEncoding::Required],
            &mut Vec::new(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ConversionError);
    }

    /// A row narrower than its layout is rejected before any byte is written.
    #[test]
    fn encode_to_row_binary_rejects_fewer_values_than_column_encodings() {
        let mut buf = vec![0xaa];
        let result = encode_to_row_binary(
            vec![ClickHouseValue::Int32(1)],
            &[ColumnEncoding::Required, ColumnEncoding::Required],
            &mut buf,
        );

        assert!(result.is_err(), "row width mismatch must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert_eq!(err.description(), Some("ClickHouse RowBinary row width mismatch"));
        assert_eq!(err.detail(), Some("values length 1 does not match column encodings length 2"));
        assert_eq!(buf, vec![0xaa], "no bytes should be written on error");
    }

    /// A row wider than its layout is rejected before any byte is written.
    #[test]
    fn encode_to_row_binary_rejects_more_values_than_column_encodings() {
        let mut buf = vec![0xaa];
        let result = encode_to_row_binary(
            vec![ClickHouseValue::Int32(1), ClickHouseValue::Int32(2)],
            &[ColumnEncoding::Required],
            &mut buf,
        );

        assert!(result.is_err(), "row width mismatch must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert_eq!(err.description(), Some("ClickHouse RowBinary row width mismatch"));
        assert_eq!(err.detail(), Some("values length 2 does not match column encodings length 1"));
        assert_eq!(buf, vec![0xaa], "no bytes should be written on error");
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
            assert!(cell_to_clickhouse_value(cell).is_err());
        }
        let value = cell_to_clickhouse_value(Cell::Time(etl::data::PgTime::EndOfDay)).unwrap();
        assert!(matches!(value, ClickHouseValue::String(value) if value == "24:00:00"));
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

        use crate::clickhouse::encoding::{
            ColumnEncoding, cell_to_clickhouse_value, encode_to_row_binary,
        };

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

        /// Every generated row encodes to exactly the RowBinary bytes the
        /// ClickHouse documentation describes, with nothing left over.
        #[test]
        fn rows_decode_with_documented_row_binary_layout() {
            run_property("clickhouse row binary layout", &vec(column(), 1..6), |columns| {
                let encodings: Vec<_> = columns.iter().map(|(encoding, _)| *encoding).collect();
                let values = columns
                    .iter()
                    .map(|(_, cell)| cell_to_clickhouse_value(cell.clone()))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let mut bytes = Vec::new();
                encode_to_row_binary(values, &encodings, &mut bytes).unwrap();

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
                prop_assert!(reader.0.is_empty(), "{} trailing bytes", reader.0.len());
                Ok(())
            });
        }
    }
}
