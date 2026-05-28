use std::{fmt, io::Write};

use etl::types::{ArrayCell, Cell, ColumnSchema, PgNumeric, TableRow, Type};
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq, Serializer},
};
use serde_json::value::RawValue;

use crate::snowflake::{
    Error, Result,
    schema::{CDC_OPERATION_COLUMN, CDC_SEQUENCE_COLUMN},
};

/// CDC operation type appended to every row sent via Snowpipe Streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
}

impl CdcOperation {
    /// Returns string written into the `_cdc_operation` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// CDC metadata attached to every row in a batch.
#[derive(Debug, Clone, Copy)]
pub struct CdcMeta<'a> {
    /// Operation performed.
    pub(crate) operation: CdcOperation,
    /// WAL sequence identifier obtained from `OffsetToken`.
    pub(crate) sequence: &'a str,
}

impl<'a> CdcMeta<'a> {
    /// Create new CDC meta.
    pub fn new(operation: CdcOperation, sequence: &'a str) -> Self {
        Self { operation, sequence }
    }
}

/// Serialize a single row as an NDJSON line into any `Write` sink.
pub(crate) fn serialize_row(
    writer: &mut impl Write,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<()> {
    if cols.len() != row.values().len() {
        return Err(Error::Encoding(format!(
            "Row width mismatch: {} columns but {} cells",
            cols.len(),
            row.values().len()
        )));
    }

    let serializable = RowSerializer {
        cols,
        cells: row.values(),
        operation: cdc.operation.as_str(),
        sequence: cdc.sequence,
    };
    serde_json::to_writer(&mut *writer, &serializable)
        .map_err(|e| Error::Encoding(e.to_string()))?;
    writer.write_all(b"\n").map_err(|e| Error::Encoding(e.to_string()))?;
    Ok(())
}

struct RowSerializer<'a> {
    cols: &'a [ColumnSchema],
    cells: &'a [Cell],
    operation: &'a str,
    sequence: &'a str,
}

impl Serialize for RowSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = ser.serialize_map(Some(self.cols.len() + 2))?;
        for (col, cell) in self.cols.iter().zip(self.cells) {
            map.serialize_entry(col.name.as_str(), &CellSerializer { typ: &col.typ, cell })?;
        }
        map.serialize_entry(CDC_OPERATION_COLUMN, self.operation)?;
        map.serialize_entry(CDC_SEQUENCE_COLUMN, self.sequence)?;
        map.end()
    }
}

struct CellSerializer<'a> {
    typ: &'a Type,
    cell: &'a Cell,
}

impl Serialize for CellSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        match self.cell {
            Cell::Null => ser.serialize_none(),
            Cell::Bool(b) => ser.serialize_bool(*b),
            // JSON and JSONB are string-backed in the shared cell model. The
            // column type tells Snowpipe to ingest the text as VARIANT JSON.
            Cell::String(s) if is_json_type(self.typ) => serialize_json_text(s, ser),
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
            Cell::Date(d) => ser.collect_str(d),
            Cell::Time(t) => ser.collect_str(t),
            Cell::Timestamp(dt) => ser.collect_str(dt),
            Cell::TimestampTz(dt) => ser.collect_str(dt),
            Cell::Uuid(u) => ser.collect_str(u),
            Cell::Bytes(b) => ser.collect_str(&HexDisplay(b)),
            Cell::Array(array) => ArrayCellSerializer { typ: self.typ, array }.serialize(ser),
        }
    }
}

fn is_json_type(typ: &Type) -> bool {
    matches!(*typ, Type::JSON | Type::JSONB)
}

fn is_json_array_type(typ: &Type) -> bool {
    matches!(*typ, Type::JSON_ARRAY | Type::JSONB_ARRAY)
}

fn serialize_json_text<S: Serializer>(value: &str, ser: S) -> std::result::Result<S::Ok, S::Error> {
    let value = serde_json::from_str::<&RawValue>(value).map_err(serde::ser::Error::custom)?;
    value.serialize(ser)
}

fn reject_non_finite<E: serde::ser::Error>(f: f64) -> std::result::Result<(), E> {
    if f.is_nan() || f.is_infinite() {
        return Err(E::custom(format!(
            "Snowpipe NDJSON cannot represent NaN/Infinity float values: {f}"
        )));
    }
    Ok(())
}

fn serialize_pg_numeric<S: Serializer>(
    n: &PgNumeric,
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    // Postgres NUMERIC maps to Snowflake VARCHAR to avoid NUMBER precision and
    // special-value loss, so preserve the PostgreSQL text representation.
    ser.collect_str(n)
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

struct JsonElement<'a>(&'a str);

impl Serialize for JsonElement<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        serialize_json_text(self.0, ser)
    }
}

struct ArrayCellSerializer<'a> {
    typ: &'a Type,
    array: &'a ArrayCell,
}

impl Serialize for ArrayCellSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        match self.array {
            // Primitives: Option<T> already implements Serialize correctly.
            ArrayCell::Bool(v) => serialize_array(v, ser),
            // JSON arrays are stored as string elements by the shared cell
            // model, while Snowflake ARRAY can carry nested VARIANT values.
            ArrayCell::String(v) if is_json_array_type(self.typ) => {
                serialize_array_with(v, ser, |value| JsonElement(value))
            }
            ArrayCell::String(v) => serialize_array(v, ser),
            ArrayCell::I16(v) => serialize_array(v, ser),
            ArrayCell::I32(v) => serialize_array(v, ser),
            ArrayCell::U32(v) => serialize_array(v, ser),
            ArrayCell::I64(v) => serialize_array(v, ser),
            // Validated floats: reject NaN/Infinity during serialization.
            ArrayCell::F32(v) => serialize_array_with(v, ser, |f| ValidatedF32(*f)),
            ArrayCell::F64(v) => serialize_array_with(v, ser, |f| ValidatedF64(*f)),
            // Custom formatting via collect_str wrappers.
            ArrayCell::Numeric(v) => serialize_array_with(v, ser, NumericElement),
            ArrayCell::Date(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::Time(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::Timestamp(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::TimestampTz(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::Uuid(v) => serialize_array_with(v, ser, CollectStr),
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
    use etl::types::{PgDate, PgTime, PgTimestamp, PgTimestampTz, Type};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::*;

    fn col(name: &str, typ: Type) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, 1, None, true)
    }

    fn push_single_cell_line(cell: Cell, typ: Type) -> std::result::Result<String, Error> {
        let cols = [col("v", typ)];
        let mut buf = Vec::new();

        let row = TableRow::new(vec![cell]);
        serialize_row(&mut buf, &cols, &row, CdcMeta::new(CdcOperation::Insert, "0"))?;

        Ok(std::str::from_utf8(&buf).unwrap().trim().to_owned())
    }

    fn push_single_cell(cell: Cell, typ: Type) -> std::result::Result<Value, Error> {
        let line = push_single_cell_line(cell, typ)?;
        let map: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();

        Ok(map.get("v").unwrap().clone())
    }

    #[test]
    fn row_width_mismatches_are_rejected() {
        let cols = [col("id", Type::INT4), col("name", Type::TEXT)];

        for row in [
            TableRow::new(vec![Cell::I32(1)]),
            TableRow::new(vec![
                Cell::I32(1),
                Cell::String("Alice".into()),
                Cell::String("extra".into()),
            ]),
        ] {
            let mut buf = Vec::new();
            let err = serialize_row(&mut buf, &cols, &row, CdcMeta::new(CdcOperation::Insert, "0"))
                .unwrap_err();

            assert!(
                matches!(err, Error::Encoding(ref message) if message.contains("Row width mismatch")),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn cell_serialization_ok() {
        let d = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
        let t = NaiveTime::from_hms_micro_opt(10, 30, 0, 123456).unwrap();
        let timestamp = NaiveDateTime::new(d, t);
        let timestamptz = Utc.with_ymd_and_hms(2026, 4, 29, 10, 30, 0).unwrap();

        let cases: Vec<(Cell, Type, Value)> = vec![
            (Cell::Null, Type::TEXT, Value::Null),
            (Cell::Bool(true), Type::BOOL, Value::Bool(true)),
            (Cell::Bool(false), Type::BOOL, Value::Bool(false)),
            (Cell::String("hello".into()), Type::TEXT, json!("hello")),
            (Cell::I16(42), Type::INT2, json!(42i16)),
            (Cell::I32(i32::MAX), Type::INT4, json!(i32::MAX)),
            (Cell::U32(u32::MAX), Type::OID, json!(u32::MAX)),
            (Cell::I64(i64::MAX), Type::INT8, json!(i64::MAX)),
            (Cell::F32(1.5), Type::FLOAT4, json!(1.5f64)),
            (Cell::F64(2.5), Type::FLOAT8, json!(2.5)),
            (
                Cell::Numeric(PgNumeric::default()),
                Type::NUMERIC,
                json!(PgNumeric::default().to_string()),
            ),
            (Cell::Date(PgDate::from(d)), Type::DATE, json!("2026-04-29")),
            (Cell::Time(PgTime::from(t)), Type::TIME, json!(PgTime::from(t).to_string())),
            (
                Cell::Timestamp(PgTimestamp::from(timestamp)),
                Type::TIMESTAMP,
                json!(PgTimestamp::from(timestamp).to_string()),
            ),
            (
                Cell::TimestampTz(PgTimestampTz::from(timestamptz)),
                Type::TIMESTAMPTZ,
                json!(PgTimestampTz::from(timestamptz).to_string()),
            ),
            (Cell::Uuid(Uuid::nil()), Type::UUID, json!("00000000-0000-0000-0000-000000000000")),
            (Cell::String(r#"{"key":[1,2,3]}"#.to_owned()), Type::JSONB, json!({"key": [1, 2, 3]})),
            (Cell::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]), Type::BYTEA, json!("deadbeef")),
        ];
        for (cell, typ, expected) in cases {
            let dbg = format!("{cell:?}");
            assert_eq!(push_single_cell(cell, typ).unwrap(), expected, "cell: {dbg}");
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
        ];
        for cell in cases {
            let dbg = format!("{cell:?}");
            assert!(push_single_cell(cell, Type::TEXT).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn numeric_special_values_are_preserved_as_text() {
        let cases = [
            (PgNumeric::NaN, "NaN"),
            (PgNumeric::PositiveInfinity, "Infinity"),
            (PgNumeric::NegativeInfinity, "-Infinity"),
        ];
        for (numeric, expected) in cases {
            assert_eq!(
                push_single_cell(Cell::Numeric(numeric), Type::NUMERIC).unwrap(),
                json!(expected)
            );
        }

        assert_eq!(
            push_single_cell(
                Cell::Array(ArrayCell::Numeric(vec![
                    Some(PgNumeric::NaN),
                    Some(PgNumeric::PositiveInfinity),
                    Some(PgNumeric::NegativeInfinity),
                    None,
                ])),
                Type::NUMERIC_ARRAY,
            )
            .unwrap(),
            json!(["NaN", "Infinity", "-Infinity", null])
        );
    }

    #[test]
    fn temporal_special_values_are_preserved_as_source_text() {
        assert_eq!(
            push_single_cell(Cell::Date(PgDate::PosInfinity), Type::DATE).unwrap(),
            json!("infinity")
        );
        assert_eq!(
            push_single_cell(Cell::Time(PgTime::TwentyFourHour), Type::TIME).unwrap(),
            json!("24:00:00")
        );
        assert_eq!(
            push_single_cell(Cell::Timestamp(PgTimestamp::NegInfinity), Type::TIMESTAMP).unwrap(),
            json!("-infinity")
        );
        assert_eq!(
            push_single_cell(Cell::TimestampTz(PgTimestampTz::PosInfinity), Type::TIMESTAMPTZ,)
                .unwrap(),
            json!("infinity")
        );
    }

    #[test]
    fn string_cells_use_column_type_for_json_encoding() {
        let json_text = r#"{"key":[1,2,3]}"#.to_owned();

        assert_eq!(
            push_single_cell(Cell::String(json_text.clone()), Type::JSONB).unwrap(),
            json!({"key": [1, 2, 3]})
        );
        assert_eq!(
            push_single_cell(Cell::String(json_text), Type::TEXT).unwrap(),
            json!(r#"{"key":[1,2,3]}"#)
        );
        assert!(push_single_cell(Cell::String("not json".to_owned()), Type::JSONB).is_err());

        assert_eq!(
            push_single_cell(
                Cell::Array(ArrayCell::String(vec![
                    Some("1".to_owned()),
                    Some(r#"{"nested":true}"#.to_owned()),
                    None,
                ])),
                Type::JSONB_ARRAY,
            )
            .unwrap(),
            json!([1, {"nested": true}, null])
        );
        assert_eq!(
            push_single_cell(
                Cell::Array(ArrayCell::String(vec![Some("1".to_owned())])),
                Type::TEXT_ARRAY,
            )
            .unwrap(),
            json!(["1"])
        );
        assert!(
            push_single_cell(
                Cell::Array(ArrayCell::String(vec![Some("not json".to_owned())])),
                Type::JSONB_ARRAY,
            )
            .is_err()
        );
    }

    #[test]
    fn json_encoding_preserves_raw_json_text() {
        let line =
            push_single_cell_line(Cell::String(r#"{"dup":1,"dup":2}"#.to_owned()), Type::JSONB)
                .unwrap();

        assert!(line.contains(r#""v":{"dup":1,"dup":2}"#), "line: {line}");
    }

    #[test]
    fn array_serialization_ok() {
        let cases: Vec<(Cell, Type, Value)> = vec![
            (
                Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
                Type::INT4_ARRAY,
                json!([1, null, 3]),
            ),
            (Cell::Array(ArrayCell::I32(vec![])), Type::INT4_ARRAY, json!([])),
            (
                Cell::Array(ArrayCell::Bool(vec![Some(true), None])),
                Type::BOOL_ARRAY,
                json!([true, null]),
            ),
            (
                Cell::Array(ArrayCell::String(vec![Some("a".into()), None])),
                Type::TEXT_ARRAY,
                json!(["a", null]),
            ),
            (
                Cell::Array(ArrayCell::Bytes(vec![Some(vec![0xFF]), None])),
                Type::BYTEA_ARRAY,
                json!(["ff", null]),
            ),
            (
                Cell::Array(ArrayCell::Uuid(vec![Some(Uuid::nil())])),
                Type::UUID_ARRAY,
                json!(["00000000-0000-0000-0000-000000000000"]),
            ),
            (
                Cell::Array(ArrayCell::String(vec![Some("1".to_owned()), None])),
                Type::JSONB_ARRAY,
                json!([1, null]),
            ),
            (
                Cell::Array(ArrayCell::F64(vec![Some(1.5), None, Some(2.5)])),
                Type::FLOAT8_ARRAY,
                json!([1.5, null, 2.5]),
            ),
            (
                Cell::Array(ArrayCell::Date(vec![
                    Some(PgDate::from(NaiveDate::from_ymd_opt(2026, 4, 29).unwrap())),
                    None,
                ])),
                Type::DATE_ARRAY,
                json!(["2026-04-29", null]),
            ),
        ];
        for (cell, typ, expected) in cases {
            let dbg = format!("{cell:?}");
            assert_eq!(push_single_cell(cell, typ).unwrap(), expected, "cell: {dbg}");
        }
    }

    #[test]
    fn array_rejects_non_finite() {
        let cases: Vec<Cell> = vec![
            Cell::Array(ArrayCell::F64(vec![Some(f64::NAN)])),
            Cell::Array(ArrayCell::F32(vec![Some(f32::INFINITY)])),
        ];
        for cell in cases {
            let dbg = format!("{cell:?}");
            assert!(push_single_cell(cell, Type::TEXT).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn multi_column_row() {
        let cols = [col("id", Type::INT4), col("name", Type::TEXT)];
        let mut buf = Vec::new();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("hello".into())]);
        serialize_row(&mut buf, &cols, &row, CdcMeta::new(CdcOperation::Insert, "0")).unwrap();

        let line = std::str::from_utf8(&buf).unwrap().trim();
        let map: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();
        assert_eq!(map.get("id").unwrap(), &json!(1));
        assert_eq!(map.get("name").unwrap(), &json!("hello"));
    }

    #[test]
    fn multi_row_ndjson() {
        let cols = [col("id", Type::INT4)];
        let mut buf = Vec::new();

        serialize_row(
            &mut buf,
            &cols,
            &TableRow::new(vec![Cell::I32(1)]),
            CdcMeta::new(CdcOperation::Insert, "0"),
        )
        .unwrap();
        serialize_row(
            &mut buf,
            &cols,
            &TableRow::new(vec![Cell::I32(2)]),
            CdcMeta::new(CdcOperation::Insert, "0"),
        )
        .unwrap();

        let text = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let v0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["id"], json!(1));
        let v1: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["id"], json!(2));
    }

    #[test]
    fn cdc_columns() {
        let cols = [col("id", Type::INT4), col("name", Type::TEXT)];
        let mut buf = Vec::new();

        let row = TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]);
        serialize_row(&mut buf, &cols, &row, CdcMeta::new(CdcOperation::Insert, "0000/0000"))
            .unwrap();

        let line = std::str::from_utf8(&buf).unwrap().trim();
        let map: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();
        assert_eq!(map.get("id").unwrap(), &json!(1));
        assert_eq!(map.get("name").unwrap(), &json!("Alice"));
        assert_eq!(map.get("_cdc_operation").unwrap(), &json!("insert"));
        assert_eq!(map.get("_cdc_sequence_number").unwrap(), &json!("0000/0000"));
    }
}
