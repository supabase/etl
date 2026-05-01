use std::fmt;

use bytes::{BufMut, BytesMut};
use etl::types::{
    ArrayCell, Cell, ColumnSchema, DATE_FORMAT, PgNumeric, TIME_FORMAT, TIMESTAMP_FORMAT,
    TIMESTAMPTZ_FORMAT_HH_MM, TableRow,
};
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq, Serializer},
};

use crate::snowflake::Error;

/// Single-buffer batch accumulator for Snowflake Streaming NDJSON.
///
/// Serializes `Cell` values directly into a pre-allocated buffer.
#[derive(Debug)]
pub struct RowBatch {
    /// Write-optimized buffer, provides the `io::Write` adapter that
    /// `serde_json::to_writer` needs.
    data: BytesMut,

    /// Count of encoded rows.
    row_count: usize,
}

impl RowBatch {
    /// Pre-allocate `bytes` of buffer capacity.
    pub fn with_capacity(bytes: usize) -> Self {
        Self { data: BytesMut::with_capacity(bytes), row_count: 0 }
    }

    /// Serialize one row as a JSON line.
    ///
    /// Errors on non-finite floats or NaN numerics.
    pub fn push_row(
        &mut self,
        cols: &[ColumnSchema],
        row: &TableRow,
    ) -> std::result::Result<(), Error> {
        let serializable = RowSerializer { cols, cells: row.values() };
        serde_json::to_writer((&mut self.data).writer(), &serializable)
            .map_err(|e| Error::Encoding(e.to_string()))?;

        self.data.put_u8(b'\n');
        self.row_count += 1;

        Ok(())
    }

    /// NDJSON size in bytes (pre-compression).
    pub fn byte_size(&self) -> usize {
        self.data.len()
    }

    /// Number of rows serialized so far.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// True when no rows have been pushed.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Raw NDJSON bytes, ready for zstd compression.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

struct RowSerializer<'a> {
    cols: &'a [ColumnSchema],
    cells: &'a [Cell],
}

impl Serialize for RowSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = ser.serialize_map(Some(self.cols.len()))?;
        for (col, cell) in self.cols.iter().zip(self.cells) {
            map.serialize_entry(col.name.as_str(), &CellSerializer(cell))?;
        }
        map.end()
    }
}

struct CellSerializer<'a>(&'a Cell);

impl Serialize for CellSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            Cell::Null => ser.serialize_none(),
            Cell::Bool(b) => ser.serialize_bool(*b),
            Cell::String(s) => ser.serialize_str(s),
            Cell::I16(n) => ser.serialize_i16(*n),
            Cell::I32(n) => ser.serialize_i32(*n),
            Cell::U32(n) => ser.serialize_u32(*n),
            Cell::I64(n) => ser.serialize_i64(*n),
            Cell::F32(f) => {
                reject_non_finite(*f as f64)?;
                ser.serialize_f32(*f)
            }
            Cell::F64(f) => {
                reject_non_finite(*f)?;
                ser.serialize_f64(*f)
            }
            Cell::Numeric(n) => serialize_pg_numeric(n, ser),
            // collect_str: Display::fmt writes directly into the JSON serializer's
            // output buffer, avoiding an intermediate String allocation.
            Cell::Date(d) => ser.collect_str(&d.format(DATE_FORMAT)),
            Cell::Time(t) => ser.collect_str(&t.format(TIME_FORMAT)),
            Cell::Timestamp(dt) => ser.collect_str(&dt.format(TIMESTAMP_FORMAT)),
            Cell::TimestampTz(dt) => ser.collect_str(&dt.format(TIMESTAMPTZ_FORMAT_HH_MM)),
            Cell::Uuid(u) => ser.collect_str(u),
            Cell::Json(v) => v.serialize(ser),
            Cell::Bytes(b) => ser.collect_str(&HexDisplay(b)),
            Cell::Array(arr) => ArrayCellSerializer(arr).serialize(ser),
        }
    }
}

fn reject_non_finite<E: serde::ser::Error>(f: f64) -> std::result::Result<(), E> {
    if f.is_nan() || f.is_infinite() {
        return Err(E::custom(format!(
            "Snowflake does not support NaN/Infinity float values: {f}"
        )));
    }
    Ok(())
}

fn serialize_pg_numeric<S: Serializer>(
    n: &PgNumeric,
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    match n {
        PgNumeric::NaN => Err(serde::ser::Error::custom("Snowflake NUMBER does not support NaN")),
        PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => {
            Err(serde::ser::Error::custom("Snowflake NUMBER does not support Infinity"))
        }
        // PgNumeric `Display` writes directly via `collect_str`, no extra alloc.
        PgNumeric::Value { .. } => ser.collect_str(n),
    }
}

/// Zero-allocation hex formatter for byte slices.
///
/// Implements `Display` so it can be used with `collect_str()` to write hex
/// directly into the JSON buffer, avoiding the `String` that `hex::encode`
/// would allocate.
///
/// For arrays, wrap in `CollectStr(HexDisplay(b))`.
struct HexDisplay<'a>(&'a [u8]);

impl fmt::Display for HexDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Display to Serialize adapter.
///
/// Types that implement Display trait, get Serialize via this adapter.
///
/// For instance, `Uuid` only implements `Display`, so this wrapper bridges the
/// gap: its `Serialize` implementation calls `collect_str`, thus keeping the
/// same zero-allocation path.
struct CollectStr<T>(T);

impl<T: fmt::Display> Serialize for CollectStr<T> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        ser.collect_str(&self.0)
    }
}

struct ValidatedF32(f32);

impl Serialize for ValidatedF32 {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        reject_non_finite(self.0 as f64)?;
        ser.serialize_f32(self.0)
    }
}

struct ValidatedF64(f64);

impl Serialize for ValidatedF64 {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        reject_non_finite(self.0)?;
        ser.serialize_f64(self.0)
    }
}

struct NumericElement<'a>(&'a PgNumeric);

impl Serialize for NumericElement<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        serialize_pg_numeric(self.0, ser)
    }
}

struct ArrayCellSerializer<'a>(&'a ArrayCell);

impl Serialize for ArrayCellSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            // Primitives: Option<T> already implements Serialize correctly.
            ArrayCell::Bool(v) => serialize_array(v, ser),
            ArrayCell::String(v) => serialize_array(v, ser),
            ArrayCell::I16(v) => serialize_array(v, ser),
            ArrayCell::I32(v) => serialize_array(v, ser),
            ArrayCell::U32(v) => serialize_array(v, ser),
            ArrayCell::I64(v) => serialize_array(v, ser),
            ArrayCell::Json(v) => serialize_array(v, ser),
            // Validated floats: reject NaN/Infinity during serialization.
            ArrayCell::F32(v) => serialize_array_with(v, ser, |f| ValidatedF32(*f)),
            ArrayCell::F64(v) => serialize_array_with(v, ser, |f| ValidatedF64(*f)),
            // Custom formatting via collect_str wrappers.
            ArrayCell::Numeric(v) => serialize_array_with(v, ser, NumericElement),
            ArrayCell::Date(v) => {
                serialize_array_with(v, ser, |d| CollectStr(d.format(DATE_FORMAT)))
            }
            ArrayCell::Time(v) => {
                serialize_array_with(v, ser, |t| CollectStr(t.format(TIME_FORMAT)))
            }
            ArrayCell::Timestamp(v) => {
                serialize_array_with(v, ser, |dt| CollectStr(dt.format(TIMESTAMP_FORMAT)))
            }
            ArrayCell::TimestampTz(v) => {
                serialize_array_with(v, ser, |dt| CollectStr(dt.format(TIMESTAMPTZ_FORMAT_HH_MM)))
            }
            ArrayCell::Uuid(v) => serialize_array_with(v, ser, |u| CollectStr(u)),
            ArrayCell::Bytes(v) => serialize_array_with(v, ser, |b| CollectStr(HexDisplay(b))),
        }
    }
}

/// Serialize a slice of `Serialize` items as a JSON array.
///
/// Used for primitive array variants where `Option<T>: Serialize`.
fn serialize_array<T: Serialize, S: Serializer>(
    items: &[T],
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut seq = ser.serialize_seq(Some(items.len()))?;
    for item in items {
        seq.serialize_element(item)?;
    }
    seq.end()
}

/// Like `serialize_array`, but wraps each `Some` element via `wrap`.
///
/// Used for array variants needing custom serialization (dates, validated
/// floats, etc.).
fn serialize_array_with<'a, T: 'a, S, R>(
    items: &'a [Option<T>],
    ser: S,
    wrap: impl Fn(&'a T) -> R,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
    R: Serialize,
{
    let mut seq = ser.serialize_seq(Some(items.len()))?;
    for item in items {
        match item {
            Some(v) => seq.serialize_element(&wrap(v))?,
            None => seq.serialize_element(&None::<()>)?,
        }
    }
    seq.end()
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::types::Type;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::*;

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_string(), Type::TEXT, -1, 1, None, true)
    }

    fn push_single_cell(cell: Cell) -> std::result::Result<Value, Error> {
        let cols = [col("v")];
        let mut batch = RowBatch::with_capacity(1024);
        let row = TableRow::new(vec![cell]);
        batch.push_row(&cols, &row)?;
        let line = std::str::from_utf8(batch.as_bytes()).unwrap().trim();
        let map: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();
        Ok(map.into_iter().next().unwrap().1)
    }

    #[test]
    fn cell_serialization_ok() {
        let d = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
        let t = NaiveTime::from_hms_micro_opt(10, 30, 0, 123456).unwrap();

        let cases: Vec<(Cell, Value)> = vec![
            (Cell::Null, Value::Null),
            (Cell::Bool(true), Value::Bool(true)),
            (Cell::Bool(false), Value::Bool(false)),
            (Cell::String("hello".into()), json!("hello")),
            (Cell::I16(42), json!(42i16)),
            (Cell::I32(i32::MAX), json!(i32::MAX)),
            (Cell::U32(u32::MAX), json!(u32::MAX)),
            (Cell::I64(i64::MAX), json!(i64::MAX)),
            (Cell::F32(1.5), json!(1.5f64)),
            (Cell::F64(2.5), json!(2.5)),
            (Cell::Numeric(PgNumeric::default()), json!(PgNumeric::default().to_string())),
            (Cell::Date(d), json!("2026-04-29")),
            (Cell::Time(t), json!(t.format(TIME_FORMAT).to_string())),
            (
                Cell::Timestamp(NaiveDateTime::new(d, t)),
                json!(NaiveDateTime::new(d, t).format(TIMESTAMP_FORMAT).to_string()),
            ),
            (
                Cell::TimestampTz(Utc.with_ymd_and_hms(2026, 4, 29, 10, 30, 0).unwrap()),
                json!(
                    Utc.with_ymd_and_hms(2026, 4, 29, 10, 30, 0)
                        .unwrap()
                        .format(TIMESTAMPTZ_FORMAT_HH_MM)
                        .to_string()
                ),
            ),
            (Cell::Uuid(Uuid::nil()), json!("00000000-0000-0000-0000-000000000000")),
            (Cell::Json(json!({"key": [1, 2, 3]})), json!({"key": [1, 2, 3]})),
            (Cell::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]), json!("deadbeef")),
        ];
        for (cell, expected) in cases {
            let dbg = format!("{cell:?}");
            assert_eq!(push_single_cell(cell).unwrap(), expected, "cell: {dbg}");
        }
    }

    #[test]
    fn rejects_non_finite() {
        let cases: Vec<Cell> = vec![
            Cell::F64(f64::NAN),
            Cell::F64(f64::INFINITY),
            Cell::F64(f64::NEG_INFINITY),
            Cell::F32(f32::NAN),
            Cell::F32(f32::INFINITY),
            Cell::F32(f32::NEG_INFINITY),
            Cell::Numeric(PgNumeric::NaN),
            Cell::Numeric(PgNumeric::PositiveInfinity),
            Cell::Numeric(PgNumeric::NegativeInfinity),
        ];
        for cell in cases {
            let dbg = format!("{cell:?}");
            assert!(push_single_cell(cell).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn array_serialization_ok() {
        let cases: Vec<(Cell, Value)> = vec![
            (Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])), json!([1, null, 3])),
            (Cell::Array(ArrayCell::I32(vec![])), json!([])),
            (Cell::Array(ArrayCell::Bool(vec![Some(true), None])), json!([true, null])),
            (Cell::Array(ArrayCell::String(vec![Some("a".into()), None])), json!(["a", null])),
            (Cell::Array(ArrayCell::Bytes(vec![Some(vec![0xFF]), None])), json!(["ff", null])),
            (
                Cell::Array(ArrayCell::Uuid(vec![Some(Uuid::nil())])),
                json!(["00000000-0000-0000-0000-000000000000"]),
            ),
            (Cell::Array(ArrayCell::Json(vec![Some(json!(1)), None])), json!([1, null])),
            (
                Cell::Array(ArrayCell::F64(vec![Some(1.5), None, Some(2.5)])),
                json!([1.5, null, 2.5]),
            ),
            (
                Cell::Array(ArrayCell::Date(vec![
                    Some(NaiveDate::from_ymd_opt(2026, 4, 29).unwrap()),
                    None,
                ])),
                json!(["2026-04-29", null]),
            ),
        ];
        for (cell, expected) in cases {
            let dbg = format!("{cell:?}");
            assert_eq!(push_single_cell(cell).unwrap(), expected, "cell: {dbg}");
        }
    }

    #[test]
    fn array_rejects_non_finite() {
        let cases: Vec<Cell> = vec![
            Cell::Array(ArrayCell::F64(vec![Some(f64::NAN)])),
            Cell::Array(ArrayCell::F32(vec![Some(f32::INFINITY)])),
            Cell::Array(ArrayCell::Numeric(vec![Some(PgNumeric::NaN)])),
        ];
        for cell in cases {
            let dbg = format!("{cell:?}");
            assert!(push_single_cell(cell).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn multi_column_row() {
        let cols = [col("id"), col("name")];
        let mut batch = RowBatch::with_capacity(1024);
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("hello".into())]);
        batch.push_row(&cols, &row).unwrap();

        let line = std::str::from_utf8(batch.as_bytes()).unwrap().trim();
        let map: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();
        assert_eq!(map.get("id").unwrap(), &json!(1));
        assert_eq!(map.get("name").unwrap(), &json!("hello"));
    }

    #[test]
    fn byte_size_is_exact() {
        let cols = [col("id"), col("name")];
        let mut batch = RowBatch::with_capacity(1024);

        let row = TableRow::new(vec![Cell::I32(42), Cell::String("hello".into())]);
        batch.push_row(&cols, &row).unwrap();
        assert_eq!(batch.byte_size(), batch.as_bytes().len());
    }

    #[test]
    fn multi_row_ndjson() {
        let cols = [col("id")];

        let mut batch = RowBatch::with_capacity(1024);
        batch.push_row(&cols, &TableRow::new(vec![Cell::I32(1)])).unwrap();
        batch.push_row(&cols, &TableRow::new(vec![Cell::I32(2)])).unwrap();
        assert_eq!(batch.row_count(), 2);

        let text = std::str::from_utf8(batch.as_bytes()).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap(), json!({"id": 1}));
        assert_eq!(serde_json::from_str::<Value>(lines[1]).unwrap(), json!({"id": 2}));
    }

    #[test]
    fn empty_batch() {
        let batch = RowBatch::with_capacity(1024);
        assert!(batch.is_empty());
        assert_eq!(batch.row_count(), 0);
        assert_eq!(batch.byte_size(), 0);
    }
}
