use std::fmt;

use chrono::NaiveDate;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{ArrayCell, Cell};

// ── RowBinary encoding ────────────────────────────────────────────────────────
//
// We bypass the `Row` / `Inserter` API entirely and write RowBinary bytes directly
// via `Client::insert_formatted_with("INSERT INTO \"t\" FORMAT RowBinary")`.
//
// This avoids two fatal issues with the `Inserter<T>` path:
//
// 1. `Insert::new` always calls `join_column_names::<T>().expect(…)`, which panics
//    when `COLUMN_NAMES = &[]` regardless of whether validation is enabled.
//
// 2. The RowBinary serde serializer wraps its `BufMut` writer in a fresh `&mut` at
//    every `serialize_some` call, telescoping the type to `&mut &mut … BytesMut` for
//    nullable array elements and overflowing the compiler's recursion limit.
//
// Direct binary encoding has neither problem: it is a simple recursive function that
// writes bytes to a `Vec<u8>` with no generics and no type-level recursion.

// ── ClickHouseValue ───────────────────────────────────────────────────────────

/// Owned ClickHouse-compatible value, moved (not cloned) from a [`Cell`].
pub(crate) enum ClickHouseValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    Float32(f32),
    Float64(f64),
    /// TEXT, NUMERIC (string), TIME (string), JSON, BYTEA (hex-encoded)
    String(String),
    /// Days since Unix epoch (ClickHouse `Date` on wire = UInt16 LE)
    Date(u16),
    /// Microseconds since Unix epoch (ClickHouse `DateTime64(6)` on wire = Int64 LE)
    DateTime64(i64),
    /// UUID in standard 16-byte big-endian order (converted to ClickHouse wire format on encode)
    Uuid([u8; 16]),
    Array(Vec<ClickHouseValue>),
}

// ── Cell → ClickHouseValue conversion ────────────────────────────────────────

/// Converts a [`Cell`] to a [`ClickHouseValue`], consuming it (no clone).
pub(crate) fn cell_to_clickhouse_value(cell: Cell) -> ClickHouseValue {
    match cell {
        Cell::Null => ClickHouseValue::Null,
        Cell::Bool(b) => ClickHouseValue::Bool(b),
        Cell::I16(v) => ClickHouseValue::Int16(v),
        Cell::I32(v) => ClickHouseValue::Int32(v),
        Cell::I64(v) => ClickHouseValue::Int64(v),
        Cell::U32(v) => ClickHouseValue::UInt32(v),
        Cell::F32(v) => ClickHouseValue::Float32(v),
        Cell::F64(v) => ClickHouseValue::Float64(v),
        Cell::Numeric(n) => ClickHouseValue::String(n.to_string()),
        Cell::Date(d) => {
            let days = d
                .signed_duration_since(unix_epoch())
                .num_days()
                .clamp(0, i64::from(u16::MAX)) as u16;
            ClickHouseValue::Date(days)
        }
        Cell::Time(t) => ClickHouseValue::String(t.to_string()),
        Cell::Timestamp(dt) => ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros()),
        Cell::TimestampTz(dt) => ClickHouseValue::DateTime64(dt.timestamp_micros()),
        Cell::Uuid(u) => ClickHouseValue::Uuid(u.to_bytes_le()),
        Cell::Json(j) => ClickHouseValue::String(j.to_string()),
        Cell::Bytes(b) => ClickHouseValue::String(bytes_to_hex(b)),
        Cell::String(s) => ClickHouseValue::String(s),
        Cell::Array(array_cell) => {
            ClickHouseValue::Array(array_cell_to_clickhouse_values(array_cell))
        }
    }
}

fn array_cell_to_clickhouse_values(array_cell: ArrayCell) -> Vec<ClickHouseValue> {
    match array_cell {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Bool))
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::String))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int16))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int32))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int64))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::UInt32))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Float32))
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Float64))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |n| {
                    ClickHouseValue::String(n.to_string())
                })
            })
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |d| {
                    let days = d
                        .signed_duration_since(unix_epoch())
                        .num_days()
                        .clamp(0, i64::from(u16::MAX)) as u16;
                    ClickHouseValue::Date(days)
                })
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |t| {
                    ClickHouseValue::String(t.to_string())
                })
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |dt| {
                    ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |dt| {
                    ClickHouseValue::DateTime64(dt.timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |u| {
                    ClickHouseValue::Uuid(*u.as_bytes())
                })
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |j| {
                    ClickHouseValue::String(j.to_string())
                })
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |b| {
                    ClickHouseValue::String(bytes_to_hex(b))
                })
            })
            .collect(),
    }
}

fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date")
}

fn bytes_to_hex(bytes: Vec<u8>) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ── RowBinary wire encoding ───────────────────────────────────────────────────

/// Encodes a variable-length integer (LEB128) used by ClickHouse for string/array lengths.
pub(crate) fn rb_varint(mut v: usize, buf: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Encodes a value for a `Nullable(T)` column (1-byte null indicator + value if present).
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
            // A non-nullable column unexpectedly received NULL (data quality issue from
            // Postgres). Write a zero-length string as the least-harmful fallback.
            buf.push(0); // varint 0 = empty string
        }
        ClickHouseValue::Bool(b) => buf.push(b as u8),
        ClickHouseValue::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::String(s) => {
            rb_varint(s.len(), buf);
            buf.extend_from_slice(s.as_bytes());
        }
        ClickHouseValue::Date(days) => buf.extend_from_slice(&days.to_le_bytes()),
        ClickHouseValue::DateTime64(micros) => buf.extend_from_slice(&micros.to_le_bytes()),
        ClickHouseValue::Uuid(bytes) => {
            // ClickHouse RowBinary UUID = two little-endian u64 (high bits then low bits).
            // Our bytes are in standard UUID big-endian order, so we split into two u64
            // and write each in little-endian.
            let high = u64::from_be_bytes(bytes[0..8].try_into().map_err(|e: std::array::TryFromSliceError| {
                etl_error!(ErrorKind::ConversionError, "UUID high-half conversion failed", e)
            })?);
            let low = u64::from_be_bytes(bytes[8..16].try_into().map_err(|e: std::array::TryFromSliceError| {
                etl_error!(ErrorKind::ConversionError, "UUID low-half conversion failed", e)
            })?);
            buf.extend_from_slice(&high.to_le_bytes());
            buf.extend_from_slice(&low.to_le_bytes());
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

/// Encodes a complete row into `buf`, selecting nullable vs non-nullable encoding per column.
pub(crate) fn rb_encode_row(
    values: Vec<ClickHouseValue>,
    nullable_flags: &[bool],
    buf: &mut Vec<u8>,
) -> EtlResult<()> {
    for (val, &is_nullable) in values.into_iter().zip(nullable_flags.iter()) {
        if is_nullable {
            rb_encode_nullable(val, buf)?;
        } else {
            rb_encode_value(val, buf)?;
        }
    }
    Ok(())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use etl::types::Cell;
    use uuid::Uuid;

    #[test]
    fn test_cell_to_clickhouse_value_null() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Null),
            ClickHouseValue::Null
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_bool() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Bool(true)),
            ClickHouseValue::Bool(true)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_i32() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::I32(42)),
            ClickHouseValue::Int32(42)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_string() {
        if let ClickHouseValue::String(s) =
            cell_to_clickhouse_value(Cell::String("hello".to_string()))
        {
            assert_eq!(s, "hello");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn test_cell_to_clickhouse_value_date() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(epoch)),
            ClickHouseValue::Date(0)
        ));

        let day1 = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(day1)),
            ClickHouseValue::Date(1)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_timestamp() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Timestamp(epoch)),
            ClickHouseValue::DateTime64(0)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_uuid() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let expected_bytes = u.to_bytes_le();
        if let ClickHouseValue::Uuid(bytes) = cell_to_clickhouse_value(Cell::Uuid(u)) {
            assert_eq!(bytes, expected_bytes);
        } else {
            panic!("expected Uuid variant");
        }
    }

    #[test]
    fn test_cell_to_clickhouse_value_bytes_hex() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        if let ClickHouseValue::String(s) = cell_to_clickhouse_value(Cell::Bytes(bytes)) {
            assert_eq!(s, "deadbeef");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn test_rb_encode_value_scalars() {
        let mut buf = Vec::new();

        buf.clear();
        rb_encode_value(ClickHouseValue::Bool(true), &mut buf).unwrap();
        assert_eq!(buf, [1u8]);

        buf.clear();
        rb_encode_value(ClickHouseValue::Int32(-1), &mut buf).unwrap();
        assert_eq!(buf, (-1i32).to_le_bytes());

        buf.clear();
        rb_encode_value(ClickHouseValue::String("hi".to_string()), &mut buf).unwrap();
        assert_eq!(buf, [2, b'h', b'i']); // varint(2) + bytes

        buf.clear();
        rb_encode_value(ClickHouseValue::Date(1), &mut buf).unwrap();
        assert_eq!(buf, 1u16.to_le_bytes());
    }

    #[test]
    fn test_rb_encode_uuid_wire_format() {
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
    fn test_rb_encode_nullable() {
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
    fn test_rb_varint() {
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
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex([].to_vec()), "");
        assert_eq!(bytes_to_hex([0x00].to_vec()), "00");
        assert_eq!(bytes_to_hex([0xff].to_vec()), "ff");
        assert_eq!(bytes_to_hex([0xde, 0xad, 0xbe, 0xef].to_vec()), "deadbeef");
    }
}
