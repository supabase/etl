use std::{fmt, io::Write};

use etl::{
    data::{
        ArrayCell, Cell, OldTableRow, PgNumeric, TableRow, UpdatedTableRow, format_date,
        format_timestamp, format_timestamptz,
    },
    schema::{ColumnSchema, ReplicatedTableSchema},
};
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq, Serializer},
};

use crate::snowflake::{
    Error, Result,
    schema::{CDC_MISSING_COLUMNS_COLUMN, CDC_OPERATION_COLUMN, CDC_SEQUENCE_COLUMN, is_json_type},
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

impl fmt::Display for CdcOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Byte sink that counts what is written and stores nothing.
struct CountingSink(usize);

impl Write for CountingSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0 += data.len();
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialized NDJSON length of one row, including the newline, without
/// allocating.
pub(crate) fn serialized_row_len(
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<usize> {
    let mut sink = CountingSink(0);
    serialize_row(&mut sink, cols, row, cdc)?;
    Ok(sink.0)
}

/// Serialized JSON length of one cell value, without allocating.
pub(crate) fn serialized_cell_len(cell: &Cell) -> Result<usize> {
    let mut sink = CountingSink(0);
    serde_json::to_writer(&mut sink, &CellSerializer(cell))
        .map_err(|e| Error::Encoding(e.to_string()))?;
    Ok(sink.0)
}

/// CDC metadata attached to every row in a batch.
#[derive(Debug, Clone, Copy)]
pub struct CdcMeta<'a> {
    /// Operation performed.
    pub(crate) operation: CdcOperation,
    /// WAL sequence identifier obtained from `OffsetToken`.
    pub(crate) sequence: &'a str,
    /// Missing replicated-column positions, empty for complete records.
    missing_columns: &'a [usize],
}

impl<'a> CdcMeta<'a> {
    /// Create new CDC meta.
    pub fn new(operation: CdcOperation, sequence: &'a str) -> Self {
        Self { operation, sequence, missing_columns: &[] }
    }

    /// Attach a missing-column mask whose positions have been validated against
    /// the row schema.
    pub(super) fn with_missing_columns(mut self, missing_columns: &'a [usize]) -> Self {
        self.missing_columns = missing_columns;
        self
    }
}

/// Serialize a single row as an NDJSON line into any `Write` sink.
pub(crate) fn serialize_row(
    writer: &mut impl Write,
    cols: &[ColumnSchema],
    row: &TableRow,
    cdc: CdcMeta<'_>,
) -> Result<()> {
    let cells = row.values();
    if cells.len() != cols.len() {
        return Err(Error::Encoding(format!(
            "Row value count ({}) does not match column count ({}).",
            cells.len(),
            cols.len()
        )));
    }

    // JSON has only one null token, so an array element cannot encode SQL NULL
    // separately from JSON null. Reject before writing any part of the row.
    for (column, cell) in cols.iter().zip(cells) {
        if let Cell::Array(ArrayCell::Json(elements)) = cell
            && let Some(element_index) = elements.iter().position(Option::is_none)
        {
            return Err(Error::NullJsonArrayElement {
                column_name: column.name.clone(),
                element_index,
            });
        }
    }

    let serializable = RowSerializer {
        cols,
        cells,
        operation: cdc.operation.as_str(),
        sequence: cdc.sequence,
        missing_columns: cdc.missing_columns,
    };
    serde_json::to_writer(&mut *writer, &serializable)
        .map_err(|e| Error::Encoding(e.to_string()))?;
    writer.write_all(b"\n").map_err(|e| Error::Encoding(e.to_string()))?;
    Ok(())
}

/// Serializes stable PostgreSQL attribute numbers without allocating a second
/// mask.
struct MissingColumns<'a> {
    /// Columns in replicated order.
    cols: &'a [ColumnSchema],
    /// Missing positions in that order.
    indexes: &'a [usize],
}

impl Serialize for MissingColumns<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = ser.serialize_seq(Some(self.indexes.len()))?;
        for &index in self.indexes {
            let column = self.cols.get(index).ok_or_else(|| {
                serde::ser::Error::custom("Missing column position exceeds row width")
            })?;
            seq.serialize_element(&column.ordinal_position)?;
        }
        seq.end()
    }
}

struct RowSerializer<'a> {
    cols: &'a [ColumnSchema],
    cells: &'a [Cell],
    operation: &'a str,
    sequence: &'a str,
    missing_columns: &'a [usize],
}

impl Serialize for RowSerializer<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = ser.serialize_map(None)?;
        for (col, cell) in self.cols.iter().zip(self.cells) {
            // On the default pipe an omitted VARIANT field is SQL NULL, while
            // an explicit null token is JSON null. Channel setup removes the
            // column defaults that would otherwise override this omission.
            if is_json_type(&col.typ) && matches!(cell, Cell::Null) {
                continue;
            }
            map.serialize_entry(col.name.as_str(), &CellSerializer(cell))?;
        }
        map.serialize_entry(CDC_OPERATION_COLUMN, self.operation)?;
        map.serialize_entry(CDC_SEQUENCE_COLUMN, self.sequence)?;
        map.serialize_entry(
            CDC_MISSING_COLUMNS_COLUMN,
            &MissingColumns { cols: self.cols, indexes: self.missing_columns },
        )?;
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
                reject_non_finite(f64::from(*f))?;
                ser.serialize_f32(*f)
            }
            Cell::F64(f) => {
                reject_non_finite(*f)?;
                ser.serialize_f64(*f)
            }
            Cell::Numeric(n) => serialize_pg_numeric(n, ser),
            // collect_str: Display::fmt writes directly into the JSON serializer's output buffer,
            // avoiding an intermediate String allocation.
            Cell::Date(d) => ser.collect_str(&format_date(d)),
            Cell::Time(t) => ser.collect_str(t),
            Cell::TimeTz(t) => ser.collect_str(t),
            Cell::Timestamp(dt) => ser.collect_str(&format_timestamp(dt)),
            Cell::TimestampTz(dt) => ser.collect_str(&format_timestamptz(dt)),
            Cell::Uuid(u) => ser.collect_str(u),
            Cell::Json(v) => v.serialize(ser),
            Cell::Bytes(b) => ser.collect_str(&HexDisplay(b)),
            Cell::Array(arr) => ArrayCellSerializer(arr).serialize(ser),
        }
    }
}

fn reject_non_finite<E: serde::ser::Error>(f: f64) -> std::result::Result<(), E> {
    if f.is_nan() || f.is_infinite() {
        return Err(E::custom(
            "Snowflake JSON ingestion cannot encode non-finite float values without changing them",
        ));
    }
    Ok(())
}

fn serialize_pg_numeric<S: Serializer>(
    n: &PgNumeric,
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    // PostgreSQL NUMERIC columns are stored as Snowflake VARCHAR, including
    // special values, so formatting preserves the source value exactly.
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
        reject_non_finite(f64::from(self.0))?;
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
            ArrayCell::Date(v) => serialize_array_with(v, ser, |d| CollectStr(format_date(d))),
            ArrayCell::Time(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::TimeTz(v) => serialize_array_with(v, ser, CollectStr),
            ArrayCell::Timestamp(v) => {
                serialize_array_with(v, ser, |dt| CollectStr(format_timestamp(dt)))
            }
            ArrayCell::TimestampTz(v) => {
                serialize_array_with(v, ser, |dt| CollectStr(format_timestamptz(dt)))
            }
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

/// Expands a dense partial tuple without cloning its present payloads.
fn expand_partial_row(
    schema: &ReplicatedTableSchema,
    row: TableRow,
    missing: &[usize],
) -> Result<TableRow> {
    let width = schema.column_schemas().len();
    if row.values().len().checked_add(missing.len()) != Some(width)
        || missing.last().is_some_and(|&index| index >= width)
        || missing.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(Error::UnsupportedLayout(
            "Partial row positions do not match the replicated layout".to_owned(),
        ));
    }
    let mut present = row.into_values().into_iter();
    let mut absent = missing.iter().copied().peekable();
    let mut values = Vec::with_capacity(width);
    for index in 0..width {
        if absent.peek() == Some(&index) {
            absent.next();
            values.push(Cell::Null);
        } else {
            // The validated width and ordered mask guarantee one present value.
            values.push(
                present
                    .next()
                    .expect("validated partial row has a value at every present position"),
            );
        }
    }
    let row = TableRow::new(values);
    Ok(row)
}

/// Expands key-only deletes using the actual replica identity, including an
/// alternative unique index. Materialization queries never carry forward values
/// from deletes.
pub(super) fn normalize_delete_row(
    schema: &ReplicatedTableSchema,
    row: Option<OldTableRow>,
) -> Result<(TableRow, Vec<usize>)> {
    let row =
        row.ok_or_else(|| Error::UnsupportedIdentity("Delete has no old tuple".to_owned()))?;
    match row {
        OldTableRow::Full(row) => {
            if row.values().len() != schema.column_schemas().len() {
                return Err(Error::UnsupportedLayout(
                    "Old row width does not match the replicated layout".to_owned(),
                ));
            }
            Ok((row, Vec::new()))
        }
        OldTableRow::Key(row) => {
            let mut identity = schema.identity_column_schemas().peekable();
            let missing = schema
                .column_schemas()
                .enumerate()
                .filter_map(|(index, column)| {
                    if identity
                        .peek()
                        .is_some_and(|key| key.ordinal_position == column.ordinal_position)
                    {
                        identity.next();
                        None
                    } else {
                        Some(index)
                    }
                })
                .collect::<Vec<_>>();
            Ok((expand_partial_row(schema, row, &missing)?, missing))
        }
    }
}

/// Normalizes updates while preserving existing complete-row admission.
///
/// Partial rows require a supported, nondeferrable primary key with every key
/// column replicated. An old tuple is allowed when its key has the same wire
/// representation; no extra tombstone or event offset is introduced for a
/// changed key.
pub(super) fn normalize_update_row(
    schema: &ReplicatedTableSchema,
    row: UpdatedTableRow,
    old: Option<OldTableRow>,
) -> Result<(TableRow, Vec<usize>)> {
    match row {
        UpdatedTableRow::Full(row) => Ok((row, Vec::new())),
        UpdatedTableRow::Partial(row) => {
            crate::snowflake::schema::validate_primary_key_identity(schema)?;
            if row.total_columns() != schema.column_schemas().len() {
                return Err(Error::UnsupportedLayout(
                    "Partial row width does not match the replicated layout".to_owned(),
                ));
            }
            let (present, missing) = row.into_parts();
            let row = expand_partial_row(schema, present, &missing)?;
            if schema
                .column_schemas()
                .zip(row.values())
                .any(|(column, value)| column.primary_key() && matches!(value, Cell::Null))
            {
                return Err(Error::UnsupportedLayout(
                    "Every primary-key value must be present and nonnull".to_owned(),
                ));
            }
            if let Some(old) = old {
                let (old, _) = normalize_delete_row(schema, Some(old))?;
                for ((column, before), after) in
                    schema.column_schemas().zip(old.values()).zip(row.values())
                {
                    if column.primary_key() {
                        let before = serde_json::to_vec(&CellSerializer(before))
                            .map_err(|error| Error::Encoding(error.to_string()))?;
                        let after = serde_json::to_vec(&CellSerializer(after))
                            .map_err(|error| Error::Encoding(error.to_string()))?;
                        if before != after {
                            return Err(Error::UnsupportedIdentity(
                                "Partial updates cannot change a primary-key value".to_owned(),
                            ));
                        }
                    }
                }
            }
            Ok((row, missing))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::{
        data::{
            Date, PartialTableRow, PgTime, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM,
            Timestamp,
        },
        error::{ErrorKind, EtlError},
        schema::{IdentityMask, PgLsn, ReplicationMask, TableId, TableName, TableSchema, Type},
        test_utils::property::run_property,
    };
    use proptest::{collection, prelude::*, sample::Index};
    use serde_json::{Map, Value, json};
    use uuid::Uuid;

    use super::*;
    use crate::snowflake::{
        OffsetToken, RowBatchBuilder, SNOWFLAKE_COLUMN_NAME_MAPPING, materialization_query,
    };

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), Type::TEXT, -1, 1, true)
    }

    fn push_single_cell(cell: Cell) -> std::result::Result<Value, Error> {
        let cols = [col("v")];
        let mut buf = Vec::new();

        let row = TableRow::new(vec![cell]);
        serialize_row(&mut buf, &cols, &row, CdcMeta::new(CdcOperation::Insert, "0"))?;

        let line = std::str::from_utf8(&buf).unwrap().trim();
        let map: serde_json::Map<String, Value> = serde_json::from_str(line).unwrap();

        Ok(map.get("v").unwrap().clone())
    }

    /// Serializes a JSON cell while checking row sizing and CDC metadata.
    fn serialize_json_cell(typ: Type, cell: Cell, operation: CdcOperation) -> Value {
        let cols = [ColumnSchema::new("v".to_owned(), typ, -1, 1, true)];
        let row = TableRow::new(vec![cell]);
        let cdc = CdcMeta::new(operation, "0");
        let mut bytes = Vec::new();
        serialize_row(&mut bytes, &cols, &row, cdc).unwrap();

        assert_eq!(serialized_row_len(&cols, &row, cdc).unwrap(), bytes.len());
        assert_eq!(bytes.last(), Some(&b'\n'));
        let document: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(document.get(CDC_OPERATION_COLUMN), Some(&json!(operation.as_str())));
        assert_eq!(document.get(CDC_SEQUENCE_COLUMN), Some(&json!("0")));
        assert_eq!(document.get(CDC_MISSING_COLUMNS_COLUMN), Some(&json!([])));
        document
    }

    /// SQL NULL omits a scalar JSON field; JSON null remains an explicit token.
    #[test]
    fn scalar_json_null_kinds_are_distinct() {
        for typ in [Type::JSON, Type::JSONB] {
            for operation in [CdcOperation::Insert, CdcOperation::Update, CdcOperation::Delete] {
                let sql_null = serialize_json_cell(typ.clone(), Cell::Null, operation);
                assert_eq!(sql_null.get("v"), None);
                assert_eq!(sql_null.as_object().unwrap().len(), 3);
                for expected in [
                    Value::Null,
                    json!("null"),
                    json!({"nested": null}),
                    json!([null]),
                    json!({"ordinary": 1}),
                    json!(42),
                    json!(true),
                ] {
                    let actual =
                        serialize_json_cell(typ.clone(), Cell::Json(expected.clone()), operation);
                    assert_eq!(actual.get("v"), Some(&expected));
                }
            }
            // Omitted JSON values also omit the wire field, but retain their
            // presence metadata so they cannot be mistaken for explicit SQL
            // NULL.
            let cols = [ColumnSchema::new("v".into(), typ, -1, 1, true)];
            let row = TableRow::new(vec![Cell::Null]);
            let mut bytes = Vec::new();
            serialize_row(
                &mut bytes,
                &cols,
                &row,
                CdcMeta::new(CdcOperation::Update, "0").with_missing_columns(&[0]),
            )
            .unwrap();
            let omitted: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(omitted.get("v"), None);
            assert_eq!(omitted[CDC_MISSING_COLUMNS_COLUMN], json!([1]));
        }
    }

    /// Whole SQL-null arrays, empty arrays and JSON-null elements remain valid.
    #[test]
    fn json_arrays_preserve_supported_null_shapes() {
        for typ in [Type::JSON_ARRAY, Type::JSONB_ARRAY] {
            for (cell, expected) in [
                (Cell::Null, Value::Null),
                (Cell::Array(ArrayCell::Json(vec![])), json!([])),
                (
                    Cell::Array(ArrayCell::Json(vec![
                        Some(Value::Null),
                        Some(json!("null")),
                        Some(json!({"nested": null})),
                    ])),
                    json!([null, "null", {"nested": null}]),
                ),
            ] {
                let actual = serialize_json_cell(typ.clone(), cell, CdcOperation::Insert);
                assert_eq!(actual.get("v"), Some(&expected));
            }
        }
    }

    /// Invalid JSON-array elements fail before any bytes reach the writer.
    #[test]
    fn json_arrays_reject_sql_null_elements() {
        for typ in [Type::JSON_ARRAY, Type::JSONB_ARRAY] {
            for operation in [CdcOperation::Insert, CdcOperation::Update, CdcOperation::Delete] {
                for elements in [vec![None], vec![Some(Value::Null), None]] {
                    let element_index = elements.len() - 1;
                    let cols = [ColumnSchema::new("payload".to_owned(), typ.clone(), -1, 1, true)];
                    let row = TableRow::new(vec![Cell::Array(ArrayCell::Json(elements))]);
                    let cdc = CdcMeta::new(operation, "0");
                    let mut bytes = Vec::new();
                    let error = serialize_row(&mut bytes, &cols, &row, cdc).unwrap_err();
                    assert!(
                        matches!(error, Error::NullJsonArrayElement { column_name, element_index: index }
                        if column_name == "payload" && index == element_index)
                    );
                    assert!(bytes.is_empty());
                    assert!(matches!(
                        serialized_row_len(&cols, &row, cdc),
                        Err(Error::NullJsonArrayElement { .. })
                    ));
                }
            }
        }
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
            (Cell::Date(Date::Value(d)), json!("2026-04-29")),
            (Cell::Date(Date::PosInfinity), json!("infinity")),
            (Cell::Timestamp(Timestamp::NegInfinity), json!("-infinity")),
            (Cell::Time(PgTime::EndOfDay), json!("24:00:00")),
            (Cell::Time(PgTime::Value(t)), json!(t.format(TIME_FORMAT).to_string())),
            (
                Cell::Timestamp(Timestamp::Value(NaiveDateTime::new(d, t))),
                json!(NaiveDateTime::new(d, t).format(TIMESTAMP_FORMAT).to_string()),
            ),
            (
                Cell::TimestampTz(Timestamp::Value(
                    Utc.with_ymd_and_hms(2026, 4, 29, 10, 30, 0).unwrap(),
                )),
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
    fn rejects_non_finite_floats() {
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
            assert!(push_single_cell(cell).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn serializes_non_finite_numerics_as_strings() {
        for numeric in [PgNumeric::NaN, PgNumeric::PositiveInfinity, PgNumeric::NegativeInfinity] {
            let expected = json!(numeric.to_string());
            assert_eq!(push_single_cell(Cell::Numeric(numeric)).unwrap(), expected);
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
            (
                Cell::Array(ArrayCell::Json(vec![Some(json!(1)), Some(Value::Null)])),
                json!([1, null]),
            ),
            (
                Cell::Array(ArrayCell::F64(vec![Some(1.5), None, Some(2.5)])),
                json!([1.5, null, 2.5]),
            ),
            (
                Cell::Array(ArrayCell::Date(vec![
                    Some(Date::Value(NaiveDate::from_ymd_opt(2026, 4, 29).unwrap())),
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
    fn array_rejects_non_finite_floats() {
        let cases: Vec<Cell> = vec![
            Cell::Array(ArrayCell::F64(vec![Some(f64::NAN)])),
            Cell::Array(ArrayCell::F32(vec![Some(f32::INFINITY)])),
        ];
        for cell in cases {
            let dbg = format!("{cell:?}");
            assert!(push_single_cell(cell).is_err(), "should reject: {dbg}");
        }
    }

    #[test]
    fn array_serializes_non_finite_numerics_as_strings() {
        let numerics = vec![
            Some(PgNumeric::NaN),
            Some(PgNumeric::PositiveInfinity),
            Some(PgNumeric::NegativeInfinity),
            None,
        ];
        let expected = json!(["NaN", "Infinity", "-Infinity", null]);

        assert_eq!(push_single_cell(Cell::Array(ArrayCell::Numeric(numerics))).unwrap(), expected);
    }

    #[test]
    fn multi_column_row() {
        let cols = [col("id"), col("name")];
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
        let cols = [col("id")];
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
        let cols = [col("id"), col("name")];
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

    /// Builds a schema with a non-leading key and two independently missing
    /// values.
    fn partial_update_schema() -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".into(), "events".into()),
            vec![
                ColumnSchema::new("payload".into(), Type::TEXT, -1, 1, true),
                ColumnSchema::new("id".into(), Type::INT4, -1, 2, false).with_primary_key(1),
                ColumnSchema::new("counter".into(), Type::INT4, -1, 7, true),
            ],
        )))
    }

    /// Rejects malformed rows before they can become Snowflake requests.
    #[test]
    fn malformed_partial_rows_and_missing_keys_fail_locally() {
        let schema = partial_update_schema();
        for (width, values, missing) in [
            (4, vec![Cell::I32(1)], vec![0, 2]),
            (3, vec![Cell::I32(1)], vec![0, 3]),
            (3, vec![Cell::I32(1)], vec![2, 0]),
            (3, vec![Cell::I32(1)], vec![0, 0]),
            (3, vec![Cell::I32(1)], vec![0]),
            (3, vec![Cell::Null, Cell::Null], vec![1]),
            (3, vec![Cell::Null, Cell::Null, Cell::Null], vec![]),
        ] {
            let error = normalize_update_row(
                &schema,
                UpdatedTableRow::Partial(PartialTableRow::new(
                    width,
                    TableRow::new(values),
                    missing,
                )),
                None,
            )
            .unwrap_err();
            assert_eq!(EtlError::from(error).kind(), ErrorKind::DestinationLayoutUnsupported);
        }
        assert_eq!(
            EtlError::from(normalize_delete_row(&schema, None).unwrap_err()).kind(),
            ErrorKind::SourceReplicaIdentityError
        );
    }

    /// Preserves source attribute numbers, including gaps, through the
    /// large-row encoder.
    #[test]
    fn partial_update_large_row_frame_includes_missing_mask() {
        let schema = partial_update_schema();
        let columns = schema.column_schemas().cloned().collect::<Vec<_>>();
        let partial = PartialTableRow::new(
            3,
            TableRow::new(vec![Cell::String("x".repeat(512 * 1024)), Cell::I32(1)]),
            vec![2],
        );
        let (row, missing) =
            normalize_update_row(&schema, UpdatedTableRow::Partial(partial), None).unwrap();
        let offset = OffsetToken::new(10_u64.into(), 1);
        let mut builder = RowBatchBuilder::new(schema.id());
        let mut batches = builder
            .push_row(
                &columns,
                &row,
                CdcMeta::new(CdcOperation::Update, offset.as_ref()).with_missing_columns(&missing),
                &offset,
            )
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(builder.footprint().row_frames, 1);
        batches.extend(builder.finish().unwrap());
        assert_eq!(batches.len(), 1);
        let data = zstd::decode_all(batches[0].bytes().as_ref()).unwrap();
        let encoded: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(encoded["_cdc_missing_columns"], json!([7]));
        assert_eq!(encoded["payload"].as_str().unwrap().len(), 512 * 1024);
    }

    /// Complete keyless FULL rows remain writable despite partial-update
    /// restrictions.
    #[test]
    fn keyless_full_identity_accepts_complete_updates_only() {
        let schema = partial_update_schema();
        let mut table = schema.inner().clone();
        table.column_schemas[1].primary_key_ordinal_position = None;
        let full = ReplicatedTableSchema::from_masks(
            Arc::new(table),
            schema.replication_mask().clone(),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );
        let row = TableRow::new(vec![Cell::Null, Cell::I32(1), Cell::Null]);
        let (actual, missing) = normalize_update_row(
            &full,
            UpdatedTableRow::Full(row.clone()),
            Some(OldTableRow::Full(row.clone())),
        )
        .unwrap();
        assert_eq!(actual, row);
        assert!(missing.is_empty());
        let partial =
            PartialTableRow::new(3, TableRow::new(vec![Cell::I32(1), Cell::Null]), vec![0]);
        assert_eq!(
            EtlError::from(
                normalize_update_row(&full, UpdatedTableRow::Partial(partial), None).unwrap_err()
            )
            .kind(),
            ErrorKind::SourceReplicaIdentityError
        );
        assert!(materialization_query("db", "schema", "table", &full).is_err());
    }

    /// Accepts matching old keys and rejects changed keys or malformed old
    /// rows.
    #[test]
    fn partial_updates_validate_old_keys() {
        let schema = partial_update_schema();
        let row = || {
            UpdatedTableRow::Partial(PartialTableRow::new(
                3,
                TableRow::new(vec![Cell::I32(7), Cell::Null]),
                vec![0],
            ))
        };
        for old in [
            OldTableRow::Key(TableRow::new(vec![Cell::I32(7)])),
            OldTableRow::Full(TableRow::new(vec![
                Cell::String("old".into()),
                Cell::I32(7),
                Cell::Null,
            ])),
        ] {
            assert!(normalize_update_row(&schema, row(), Some(old)).is_ok());
        }
        assert_eq!(
            EtlError::from(
                normalize_update_row(
                    &schema,
                    row(),
                    Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(8)]))),
                )
                .unwrap_err()
            )
            .kind(),
            ErrorKind::SourceReplicaIdentityError
        );
        assert_eq!(
            EtlError::from(
                normalize_update_row(
                    &schema,
                    row(),
                    Some(OldTableRow::Full(TableRow::new(vec![]))),
                )
                .unwrap_err()
            )
            .kind(),
            ErrorKind::DestinationLayoutUnsupported
        );
    }

    /// Alternative replica identities remain valid for deletes, but not partial
    /// updates.
    #[test]
    fn alternative_identity_allows_deletes_but_rejects_partial_updates() {
        let schema = partial_update_schema();
        let alternative = ReplicatedTableSchema::from_masks(
            Arc::new(schema.inner().clone()),
            schema.replication_mask().clone(),
            IdentityMask::from_bytes(vec![1, 0, 0]),
        );
        let (row, missing) = normalize_delete_row(
            &alternative,
            Some(OldTableRow::Key(TableRow::new(vec![Cell::String("identity".into())]))),
        )
        .unwrap();
        assert_eq!(row.values(), &[Cell::String("identity".into()), Cell::Null, Cell::Null]);
        assert_eq!(missing, vec![1, 2]);
        let partial =
            PartialTableRow::new(3, TableRow::new(vec![Cell::I32(1), Cell::Null]), vec![0]);
        assert_eq!(
            EtlError::from(
                normalize_update_row(&alternative, UpdatedTableRow::Partial(partial), None)
                    .unwrap_err()
            )
            .kind(),
            ErrorKind::SourceReplicaIdentityError
        );
    }

    /// Query-only reserved names must not restrict which rows the writer
    /// accepts.
    #[test]
    fn partial_writer_accepts_names_reserved_only_by_materialization_query() {
        let schema = partial_update_schema();
        let mut table = schema.inner().clone();
        table.column_schemas[2].name = "_cdc_version".into();
        let schema = ReplicatedTableSchema::all(Arc::new(table));
        let row = UpdatedTableRow::Partial(PartialTableRow::new(
            3,
            TableRow::new(vec![Cell::I32(7), Cell::I32(1)]),
            vec![0],
        ));
        let (row, missing) = normalize_update_row(&schema, row, None).unwrap();
        assert_eq!(missing, vec![0]);
        assert_eq!(row.values()[2], Cell::I32(1));
        assert!(materialization_query("db", "schema", "table", &schema).is_err());
    }

    /// Key column types the materialization query accepts.
    const HISTORY_KEY_TYPES: [Type; 6] =
        [Type::INT4, Type::INT8, Type::TEXT, Type::NUMERIC, Type::BYTEA, Type::UUID];

    /// Value column types, including both JSON types and an array.
    const HISTORY_VALUE_TYPES: [Type; 9] = [
        Type::INT4,
        Type::FLOAT8,
        Type::BOOL,
        Type::TEXT,
        Type::BYTEA,
        Type::NUMERIC,
        Type::JSON,
        Type::JSONB,
        Type::TEXT_ARRAY,
    ];

    /// Most columns one generated table can have.
    const HISTORY_MAX_COLUMNS: usize = 8;

    /// One generated column: type index, attnum gap and a placement or
    /// publication choice.
    #[derive(Debug, Clone)]
    struct HistoryColumn {
        /// Index into [`HISTORY_KEY_TYPES`] or [`HISTORY_VALUE_TYPES`].
        type_index: usize,
        /// Attribute numbers skipped before this column, as after dropped
        /// columns.
        attnum_gap: i32,
    }

    /// One generated history step for one key.
    #[derive(Debug, Clone)]
    struct HistoryStep {
        /// Key slot the step touches.
        key: usize,
        /// Operation choice for a live key.
        action: u8,
        /// Value seed per column position.
        values: Vec<u8>,
        /// Which TOAST-able unchanged columns a partial update omits.
        omit: u16,
        /// Whether a partial update carries its unchanged old key.
        with_old_key: bool,
        /// Whether the step starts a new transaction.
        new_tx: bool,
        /// LSN distance to the new transaction's commit.
        lsn_gap: u64,
    }

    /// One generated table history.
    #[derive(Debug, Clone)]
    struct HistoryCase {
        /// Primary-key columns with their placement among value columns and
        /// key-ordinal seed.
        keys: Vec<(HistoryColumn, Index, u8)>,
        /// Value columns with whether a column-list publication includes them.
        values: Vec<(HistoryColumn, bool)>,
        /// `REPLICA IDENTITY FULL` instead of the primary key.
        full_identity: bool,
        /// Bit set of key slots present in the initial copy.
        copied: u8,
        /// Streamed steps in commit order.
        steps: Vec<HistoryStep>,
    }

    /// Generated tables with one to three key columns in any position, gaps
    /// in attribute numbers, and histories over a small key domain.
    fn history_case() -> impl Strategy<Value = HistoryCase> {
        let column = |types: usize| {
            (0..types, 0i32..=2)
                .prop_map(|(type_index, attnum_gap)| HistoryColumn { type_index, attnum_gap })
        };
        let step = (
            0usize..6,
            any::<u8>(),
            collection::vec(0u8..12, HISTORY_MAX_COLUMNS),
            any::<u16>(),
            any::<bool>(),
            any::<bool>(),
            1u64..=40,
        )
            .prop_map(|(key, action, values, omit, with_old_key, new_tx, lsn_gap)| HistoryStep {
                key,
                action,
                values,
                omit,
                with_old_key,
                new_tx,
                lsn_gap,
            });
        (
            collection::vec((column(HISTORY_KEY_TYPES.len()), any::<Index>(), any::<u8>()), 1..=3),
            collection::vec(
                (column(HISTORY_VALUE_TYPES.len()), proptest::bool::weighted(0.85)),
                0..=5,
            ),
            any::<bool>(),
            any::<u8>(),
            collection::vec(step, 1..60),
        )
            .prop_map(|(keys, values, full_identity, copied, steps)| HistoryCase {
                keys,
                values,
                full_identity,
                copied,
                steps,
            })
    }

    /// Builds the replicated schema of `case`.
    fn history_schema(case: &HistoryCase) -> ReplicatedTableSchema {
        // Place key columns among value columns, then number them with gaps.
        let mut layout: Vec<(Type, Option<u8>, i32, bool)> = case
            .values
            .iter()
            .map(|(column, published)| {
                (
                    HISTORY_VALUE_TYPES[column.type_index].clone(),
                    None,
                    column.attnum_gap,
                    *published,
                )
            })
            .collect();
        for (column, position, order) in &case.keys {
            let index = position.index(layout.len() + 1);
            layout.insert(
                index,
                (
                    HISTORY_KEY_TYPES[column.type_index].clone(),
                    Some(*order),
                    column.attnum_gap,
                    true,
                ),
            );
        }
        // Primary-key ordinals follow the order seeds, not column positions.
        let mut key_positions =
            layout.iter().enumerate().filter_map(|(i, c)| c.1.map(|o| (o, i))).collect::<Vec<_>>();
        key_positions.sort();
        let mut attnum = 0;
        let columns = layout
            .iter()
            .enumerate()
            .map(|(index, (typ, order, gap, _))| {
                attnum += 1 + gap;
                let column = ColumnSchema::new(
                    format!("c{attnum}"),
                    typ.clone(),
                    -1,
                    attnum,
                    order.is_none(),
                );
                match key_positions.iter().position(|(_, i)| *i == index) {
                    Some(rank) => column.with_primary_key(rank as i32 + 1),
                    None => column,
                }
            })
            .collect::<Vec<_>>();
        // A FULL identity publication must include every column.
        let replicated =
            layout.iter().map(|c| u8::from(case.full_identity || c.3)).collect::<Vec<_>>();
        let identity = if case.full_identity {
            replicated.clone()
        } else {
            layout.iter().map(|c| u8::from(c.1.is_some())).collect()
        };
        let table =
            TableSchema::new(TableId::new(1), TableName::new("public".into(), "t".into()), columns);
        ReplicatedTableSchema::from_masks(
            Arc::new(table),
            ReplicationMask::from_bytes(replicated),
            IdentityMask::from_bytes(identity),
        )
    }

    /// Key cell of `typ` for base-3 digit `digit` of a key slot. Digits map to
    /// values Postgres considers distinct, including case-only text changes.
    fn history_key_cell(typ: &Type, digit: usize) -> Cell {
        match *typ {
            Type::INT4 => Cell::I32(digit as i32 - 1),
            Type::INT8 => Cell::I64(digit as i64 * 1_000_000_000_000),
            Type::TEXT => Cell::String(["a", "A", "ä"][digit].to_owned()),
            Type::NUMERIC => Cell::Numeric(["0", "1.5", "-2.00"][digit].parse().unwrap()),
            Type::BYTEA => Cell::Bytes(vec![digit as u8; digit]),
            Type::UUID => Cell::Uuid(Uuid::from_u128(digit as u128)),
            _ => unreachable!("key types come from HISTORY_KEY_TYPES"),
        }
    }

    /// Value cell of `typ` for `seed`, covering SQL NULL, JSON null and the
    /// string `"null"`.
    fn history_value_cell(typ: &Type, seed: u8) -> Cell {
        if seed == 0 {
            return Cell::Null;
        }
        match *typ {
            Type::INT4 => Cell::I32(i32::from(seed) - 6),
            Type::FLOAT8 => Cell::F64(f64::from(seed) / 4.0),
            Type::BOOL => Cell::Bool(seed.is_multiple_of(2)),
            Type::TEXT => Cell::String(
                ["", "null", "x", "X"][usize::from(seed) % 4].repeat(usize::from(seed)),
            ),
            Type::BYTEA => Cell::Bytes(vec![seed; usize::from(seed % 4)]),
            Type::NUMERIC => Cell::Numeric(format!("{seed}.{}", seed % 3).parse().unwrap()),
            Type::JSON | Type::JSONB => Cell::Json(match seed {
                1 => Value::Null,
                2 => json!("null"),
                3 => json!({"nested": null}),
                4 => json!([null]),
                _ => json!(seed),
            }),
            Type::TEXT_ARRAY => Cell::Array(ArrayCell::String(match seed % 3 {
                0 => vec![],
                1 => vec![None],
                _ => vec![Some(seed.to_string()), None],
            })),
            _ => unreachable!("value types come from HISTORY_VALUE_TYPES"),
        }
    }

    /// Returns whether Postgres can store `typ` out of line, so an unchanged
    /// value may be omitted from an update.
    fn toastable(typ: &Type) -> bool {
        matches!(
            *typ,
            Type::TEXT | Type::BYTEA | Type::NUMERIC | Type::JSON | Type::JSONB | Type::TEXT_ARRAY
        )
    }

    /// One source event, in the shape the apply loop hands the destination.
    #[derive(Debug)]
    enum HistoryEvent {
        /// An inserted row.
        Insert { row: TableRow },
        /// An update and its optional old tuple.
        Update { row: UpdatedTableRow, old: Option<OldTableRow> },
        /// A delete's old tuple.
        Delete { old: OldTableRow },
    }

    /// Where an event sits in the WAL: `None` for an initial-copy row, else
    /// whether it starts a new transaction and the LSN distance to that
    /// transaction's commit.
    type HistoryPosition = Option<(bool, u64)>;

    /// Turns `case` into source events Postgres could emit, applying each to
    /// `state` in order. Returns the events with their WAL placement.
    fn history_events(
        case: &HistoryCase,
        schema: &ReplicatedTableSchema,
        state: &mut BTreeMap<usize, Vec<Cell>>,
    ) -> Vec<(HistoryEvent, HistoryPosition)> {
        let columns = schema.column_schemas().collect::<Vec<_>>();
        let key_count = columns.iter().filter(|c| c.primary_key()).count() as u32;
        let key_slots = 3usize.pow(key_count).min(6);
        let key_cells = |slot: usize| {
            let mut rank = 0;
            columns
                .iter()
                .map(|column| {
                    column.primary_key().then(|| {
                        let digit = slot / 3usize.pow(rank) % 3;
                        rank += 1;
                        history_key_cell(&column.typ, digit)
                    })
                })
                .collect::<Vec<_>>()
        };
        let full_row = |slot: usize, seeds: &[u8]| {
            key_cells(slot)
                .into_iter()
                .zip(columns.iter().zip(seeds))
                .map(|(key, (column, seed))| {
                    key.unwrap_or_else(|| history_value_cell(&column.typ, *seed))
                })
                .collect::<Vec<_>>()
        };
        let key_only = |slot: usize| TableRow::new(key_cells(slot).into_iter().flatten().collect());

        let mut events = Vec::new();
        for slot in 0..key_slots {
            if case.copied & (1 << slot) != 0 {
                let row = full_row(slot, &[slot as u8; HISTORY_MAX_COLUMNS]);
                state.insert(slot, row.clone());
                events.push((HistoryEvent::Insert { row: TableRow::new(row) }, None));
            }
        }
        for step in &case.steps {
            let position = Some((step.new_tx, step.lsn_gap));
            let slot = step.key % key_slots;
            let Some(current) = state.get(&slot).cloned() else {
                let row = full_row(slot, &step.values);
                state.insert(slot, row.clone());
                events.push((HistoryEvent::Insert { row: TableRow::new(row) }, position));
                continue;
            };
            let full_old = || OldTableRow::Full(TableRow::new(current.clone()));
            let delete_old =
                || if case.full_identity { full_old() } else { OldTableRow::Key(key_only(slot)) };
            match step.action % 4 {
                0 => {
                    let row = full_row(slot, &step.values);
                    state.insert(slot, row.clone());
                    let old = case.full_identity.then(full_old);
                    let row = UpdatedTableRow::Full(TableRow::new(row));
                    events.push((HistoryEvent::Update { row, old }, position));
                }
                1 => {
                    // Only unchanged out-of-line values can be omitted, and
                    // those are never NULL.
                    let missing = columns
                        .iter()
                        .enumerate()
                        .filter(|(i, column)| {
                            !column.primary_key()
                                && toastable(&column.typ)
                                && !matches!(current[*i], Cell::Null)
                                && step.omit & (1 << i) != 0
                        })
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>();
                    let mut row = full_row(slot, &step.values);
                    for &i in &missing {
                        row[i] = current[i].clone();
                    }
                    state.insert(slot, row.clone());
                    let old = if case.full_identity {
                        Some(full_old())
                    } else {
                        step.with_old_key.then(|| OldTableRow::Key(key_only(slot)))
                    };
                    let updated = if missing.is_empty() {
                        UpdatedTableRow::Full(TableRow::new(row))
                    } else {
                        let present = row
                            .into_iter()
                            .enumerate()
                            .filter(|(i, _)| !missing.contains(i))
                            .map(|(_, cell)| cell)
                            .collect();
                        UpdatedTableRow::Partial(PartialTableRow::new(
                            columns.len(),
                            TableRow::new(present),
                            missing,
                        ))
                    };
                    events.push((HistoryEvent::Update { row: updated, old }, position));
                }
                2 => {
                    state.remove(&slot);
                    events.push((HistoryEvent::Delete { old: delete_old() }, position));
                }
                _ => {
                    // Delete and re-insert the key, as a delete-then-insert
                    // upsert does.
                    events.push((HistoryEvent::Delete { old: delete_old() }, position));
                    let row = full_row(slot, &step.values);
                    state.insert(slot, row.clone());
                    let same_transaction = Some((false, 0));
                    events
                        .push((HistoryEvent::Insert { row: TableRow::new(row) }, same_transaction));
                }
            }
        }
        events
    }

    /// Value a Snowflake column holds for `record`: an omitted VARIANT field
    /// is SQL NULL while an explicit JSON null stays JSON null; for every
    /// other type a JSON null loads as SQL NULL.
    fn loaded_value(column: &ColumnSchema, record: &Map<String, Value>) -> Option<Value> {
        let value = record.get(&column.name).cloned();
        if is_json_type(&column.typ) { value } else { value.filter(|value| !value.is_null()) }
    }

    /// The value Snowflake holds for `cell` written in full.
    fn expected_value(cell: &Cell) -> Option<Value> {
        match cell {
            Cell::Null => None,
            cell => Some(serde_json::to_value(CellSerializer(cell)).unwrap()),
        }
    }

    /// One row of the materialization query result.
    #[derive(Debug, PartialEq)]
    struct MaterializedRow {
        /// Column values in replicated order.
        values: Vec<Option<Value>>,
        /// `_cdc_anchored`.
        anchored: bool,
        /// `_cdc_unresolved_columns`.
        unresolved: Vec<String>,
    }

    /// Model of [`materialization_query`] over raw CDC records, mirroring its
    /// SQL: per key, compute the latest insert, delete and complete record;
    /// keep insert and update records after the latest delete and at or after
    /// the latest insert and complete record; and resolve each non-key column
    /// from the highest-sequence kept record that supplied it. Sequences
    /// compare as UTF-8 bytes, like `collate(seq, 'utf8')`.
    fn model_materialize(
        columns: &[ColumnSchema],
        records: &[Map<String, Value>],
    ) -> BTreeMap<String, MaterializedRow> {
        let key_of = |record: &Map<String, Value>| {
            let key = columns
                .iter()
                .filter(|c| c.primary_key())
                .map(|c| loaded_value(c, record))
                .collect::<Vec<_>>();
            serde_json::to_string(&key).unwrap()
        };
        let mut groups: BTreeMap<String, Vec<&Map<String, Value>>> = BTreeMap::new();
        for record in records {
            groups.entry(key_of(record)).or_default().push(record);
        }
        let op =
            |record: &Map<String, Value>| record[CDC_OPERATION_COLUMN].as_str().unwrap().to_owned();
        let seq =
            |record: &Map<String, Value>| record[CDC_SEQUENCE_COLUMN].as_str().unwrap().to_owned();
        let missing = |record: &Map<String, Value>| match record.get(CDC_MISSING_COLUMNS_COLUMN) {
            Some(Value::Array(attnums)) => attnums.iter().map(|a| a.as_i64().unwrap()).collect(),
            _ => Vec::new(),
        };

        let mut result = BTreeMap::new();
        for (key, group) in groups {
            let latest = |pick: &dyn Fn(&Map<String, Value>) -> bool| {
                group.iter().filter(|r| pick(r)).map(|r| seq(r)).max()
            };
            let latest_insert = latest(&|r| op(r) == "insert");
            let latest_delete = latest(&|r| op(r) == "delete");
            let latest_complete =
                latest(&|r| matches!(op(r).as_str(), "insert" | "update") && missing(r).is_empty());
            let eligible = group
                .iter()
                .filter(|r| {
                    let s = seq(r);
                    matches!(op(r).as_str(), "insert" | "update")
                        && latest_delete.as_ref().is_none_or(|d| s > *d)
                        && latest_insert.as_ref().is_none_or(|i| s >= *i)
                        && latest_complete.as_ref().is_none_or(|c| s >= *c)
                })
                .collect::<Vec<_>>();
            if eligible.is_empty() {
                continue;
            }
            let mut values = Vec::new();
            let mut unresolved = Vec::new();
            for column in columns {
                if column.primary_key() {
                    values.push(loaded_value(column, eligible[0]));
                    continue;
                }
                let attnum = i64::from(column.ordinal_position);
                let supplier = eligible
                    .iter()
                    .filter(|r| !missing(r).contains(&attnum))
                    .max_by_key(|r| seq(r));
                match supplier {
                    Some(record) => values.push(loaded_value(column, record)),
                    None => {
                        values.push(None);
                        unresolved.push(column.name.clone());
                    }
                }
            }
            let anchored = latest_insert
                .as_ref()
                .is_some_and(|i| latest_delete.as_ref().is_none_or(|d| i > d));
            result.insert(key, MaterializedRow { values, anchored, unresolved });
        }
        result
    }

    /// Rebuilding current state from the encoded CDC records, with the
    /// materialization query's semantics, gives the state of applying the
    /// source events in order: deleted keys are absent, every value comes from
    /// the right event (including SQL NULL versus JSON null and values left
    /// out of partial updates), and no column is unresolved.
    #[test]
    fn materialized_history_matches_sequential_apply() {
        run_property("snowflake history materialization", &history_case(), |case| {
            let schema = history_schema(case);
            prop_assert!(materialization_query("db", "s", "t", &schema).is_ok());
            let columns = schema
                .destination_column_schemas(SNOWFLAKE_COLUMN_NAME_MAPPING)
                .collect::<Vec<_>>();

            let mut state = BTreeMap::new();
            let events = history_events(case, &schema, &mut state);
            // Streaming starts past the zero offset of the initial copy and
            // crosses hex-digit boundaries as the LSN grows.
            let (mut lsn, mut ordinal) = (0xf0u64, 0u64);
            let mut records = Vec::new();
            for (event, position) in events {
                let offset = match position {
                    None => OffsetToken::zero(),
                    Some((new_transaction, lsn_gap)) => {
                        if new_transaction {
                            lsn += lsn_gap;
                            ordinal = 0;
                        } else {
                            ordinal += 1;
                        }
                        OffsetToken::new(PgLsn::from(lsn), ordinal)
                    }
                };
                let (operation, row, missing) = match event {
                    HistoryEvent::Insert { row, .. } => (CdcOperation::Insert, row, Vec::new()),
                    HistoryEvent::Update { row, old } => {
                        let (row, missing) = normalize_update_row(&schema, row, old).unwrap();
                        (CdcOperation::Update, row, missing)
                    }
                    HistoryEvent::Delete { old } => {
                        let (row, missing) = normalize_delete_row(&schema, Some(old)).unwrap();
                        (CdcOperation::Delete, row, missing)
                    }
                };
                let mut line = Vec::new();
                let cdc = CdcMeta::new(operation, offset.as_ref()).with_missing_columns(&missing);
                serialize_row(&mut line, &columns, &row, cdc).unwrap();
                records.push(serde_json::from_slice::<Map<String, Value>>(&line).unwrap());
            }

            let actual = model_materialize(&columns, &records);
            let expected = state
                .values()
                .map(|row| {
                    let key = columns
                        .iter()
                        .zip(row)
                        .filter(|(c, _)| c.primary_key())
                        .map(|(_, cell)| expected_value(cell))
                        .collect::<Vec<_>>();
                    let materialized = MaterializedRow {
                        values: row.iter().map(expected_value).collect(),
                        anchored: true,
                        unresolved: Vec::new(),
                    };
                    (serde_json::to_string(&key).unwrap(), materialized)
                })
                .collect::<BTreeMap<_, _>>();
            prop_assert_eq!(actual, expected);
            Ok(())
        });
    }
}
