use chrono::NaiveDate;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ArrayCell, Cell},
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
    UInt32(u32),
    /// Unsigned 64-bit integer, used for CDC LSN metadata.
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    /// TEXT, NUMERIC (string), TIME (string), JSON, BYTEA (hex-encoded)
    String(String),
    /// Days from Unix epoch (ClickHouse `Date32` on wire = Int32 LE). The
    /// signed offset lets us represent pre-1970 dates that ClickHouse `Date`
    /// (UInt16) would have silently clamped to 1970-01-01.
    Date32(i32),
    /// Microseconds since Unix epoch (ClickHouse `DateTime64(6)` on wire =
    /// Int64 LE)
    DateTime64(i64),
    /// UUID in standard 16-byte big-endian order (converted to ClickHouse wire
    /// format on encode)
    Uuid([u8; 16]),
    Array(Vec<ClickHouseValue>),
}

/// Converts a [`Cell`] to a [`ClickHouseValue`], consuming it (no clone).
///
/// Returns [`ErrorKind::ConversionError`] when a value cannot be represented
/// in ClickHouse without loss, e.g. a Postgres `date` outside ClickHouse
/// `Date32`'s `1900-01-01..=2299-12-31` range.
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
        Cell::Date(d) => ClickHouseValue::Date32(date_to_date32_days(d)?),
        Cell::Time(t) => ClickHouseValue::String(t.to_string()),
        Cell::Timestamp(dt) => ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros()),
        Cell::TimestampTz(dt) => ClickHouseValue::DateTime64(dt.timestamp_micros()),
        Cell::Uuid(u) => ClickHouseValue::Uuid(*u.as_bytes()),
        Cell::Json(j) => ClickHouseValue::String(j.to_string()),
        Cell::Bytes(b) => ClickHouseValue::String(bytes_to_hex(&b)),
        Cell::String(s) => ClickHouseValue::String(s),
        Cell::Array(array_cell) => {
            ClickHouseValue::Array(array_cell_to_clickhouse_values(array_cell)?)
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
        ArrayCell::Date(v) => {
            try_map_array(v, |d| Ok(ClickHouseValue::Date32(date_to_date32_days(d)?)))?
        }
        ArrayCell::Time(v) => map_array(v, |t| ClickHouseValue::String(t.to_string())),
        ArrayCell::Timestamp(v) => {
            map_array(v, |dt| ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros()))
        }
        ArrayCell::TimestampTz(v) => {
            map_array(v, |dt| ClickHouseValue::DateTime64(dt.timestamp_micros()))
        }
        ArrayCell::Uuid(v) => map_array(v, |u| ClickHouseValue::Uuid(*u.as_bytes())),
        ArrayCell::Json(v) => map_array(v, |j| ClickHouseValue::String(j.to_string())),
        ArrayCell::Bytes(v) => map_array(v, |b| ClickHouseValue::String(bytes_to_hex(&b))),
    })
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

/// Fallible variant of [`map_array`] for element converters that can fail.
fn try_map_array<T, F>(v: Vec<Option<T>>, mut f: F) -> EtlResult<Vec<ClickHouseValue>>
where
    F: FnMut(T) -> EtlResult<ClickHouseValue>,
{
    v.into_iter()
        .map(|o| match o {
            Some(t) => f(t),
            None => Ok(ClickHouseValue::Null),
        })
        .collect()
}

/// Converts a [`NaiveDate`] to a ClickHouse `Date32` day offset (signed days
/// from 1970-01-01).
///
/// Returns [`ErrorKind::ConversionError`] when the date falls outside
/// ClickHouse `Date32`'s `1900-01-01..=2299-12-31` range. Silent clamping
/// would corrupt historical or far-future values, so we fail the batch
/// instead.
fn date_to_date32_days(d: NaiveDate) -> EtlResult<i32> {
    if d < date32_min() || d > date32_max() {
        return Err(etl_error!(
            ErrorKind::ConversionError,
            "date out of ClickHouse Date32 range",
            format!("{d} is outside the supported range {}..={}", date32_min(), date32_max())
        ));
    }
    // The bounds check above guarantees the day count fits in i32.
    Ok(d.signed_duration_since(unix_epoch()).num_days() as i32)
}

fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date")
}

fn date32_min() -> NaiveDate {
    NaiveDate::from_ymd_opt(1900, 1, 1).expect("valid date")
}

fn date32_max() -> NaiveDate {
    NaiveDate::from_ymd_opt(2299, 12, 31).expect("valid date")
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
            // The Postgres schema says this column is NOT NULL, but a NULL arrived.
            // If this proves too strict (e.g. transient schema mismatches), we could
            // downgrade to writing a zero-length string as a silent fallback.
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "NULL value for non-nullable ClickHouse column"
            ));
        }
        ClickHouseValue::Bool(b) => buf.push(b as u8),
        ClickHouseValue::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt64(v) => buf.extend_from_slice(&v.to_le_bytes()),
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

/// Encodes a complete row into `buf`, selecting nullable vs non-nullable
/// encoding per column.
pub(crate) fn encode_to_row_binary(
    values: Vec<ClickHouseValue>,
    nullable_flags: &[bool],
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    if values.len() != nullable_flags.len() {
        return Err(etl_error!(
            ErrorKind::ConversionError,
            "ClickHouse RowBinary row width mismatch",
            format!(
                "values length {} does not match nullable flags length {}",
                values.len(),
                nullable_flags.len()
            )
        ));
    }

    for (val, &is_nullable) in values.into_iter().zip(nullable_flags.iter()) {
        if is_nullable {
            rb_encode_nullable(val, buf)?;
        } else {
            rb_encode_value(val, buf)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use etl::types::Cell;
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
            cell_to_clickhouse_value(Cell::Date(epoch)).unwrap(),
            ClickHouseValue::Date32(0)
        ));

        let day1 = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(day1)).unwrap(),
            ClickHouseValue::Date32(1)
        ));

        // Pre-1970 dates round-trip through Date32 as a negative offset rather
        // than being silently clamped to the epoch.
        let pre_epoch = NaiveDate::from_ymd_opt(1969, 12, 31).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(pre_epoch)).unwrap(),
            ClickHouseValue::Date32(-1)
        ));
    }

    #[test]
    fn cell_to_clickhouse_value_date_out_of_range_errors() {
        let too_old = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
        assert!(cell_to_clickhouse_value(Cell::Date(too_old)).is_err());

        let too_new = NaiveDate::from_ymd_opt(2300, 1, 1).unwrap();
        assert!(cell_to_clickhouse_value(Cell::Date(too_new)).is_err());
    }

    #[test]
    fn cell_to_clickhouse_value_timestamp() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Timestamp(epoch)).unwrap(),
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

    /// # GIVEN
    /// A NULL ClickHouseValue passed to the non-nullable encoder.
    ///
    /// # WHEN
    /// `rb_encode_value` is called.
    ///
    /// # THEN
    /// It returns a ConversionError rather than writing invalid RowBinary.
    #[test]
    fn rb_encode_value_rejects_null_for_non_nullable_column() {
        let mut buf = Vec::new();
        let result = rb_encode_value(ClickHouseValue::Null, &mut buf);

        assert!(result.is_err(), "NULL in non-nullable column must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert!(buf.is_empty(), "no bytes should be written on error");
    }

    #[test]
    fn encode_to_row_binary_rejects_fewer_values_than_nullable_flags() {
        let mut buf = vec![0xaa];
        let result =
            encode_to_row_binary(vec![ClickHouseValue::Int32(1)], &[false, false], &mut buf);

        assert!(result.is_err(), "row width mismatch must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert_eq!(err.description(), Some("ClickHouse RowBinary row width mismatch"));
        assert_eq!(err.detail(), Some("values length 1 does not match nullable flags length 2"));
        assert_eq!(buf, vec![0xaa], "no bytes should be written on error");
    }

    #[test]
    fn encode_to_row_binary_rejects_more_values_than_nullable_flags() {
        let mut buf = vec![0xaa];
        let result = encode_to_row_binary(
            vec![ClickHouseValue::Int32(1), ClickHouseValue::Int32(2)],
            &[false],
            &mut buf,
        );

        assert!(result.is_err(), "row width mismatch must error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert_eq!(err.description(), Some("ClickHouse RowBinary row width mismatch"));
        assert_eq!(err.detail(), Some("values length 2 does not match nullable flags length 1"));
        assert_eq!(buf, vec![0xaa], "no bytes should be written on error");
    }
}
