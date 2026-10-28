use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime, Timelike};
use duckdb::{
    arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
            Int16Array, Int32Array, Int64Array, StringArray, Time64MicrosecondArray,
            TimestampMicrosecondArray, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit},
        record_batch::RecordBatch,
    },
    types::{TimeUnit, Value},
};
use etl::{
    data::{
        ArrayCell, Cell, Date, PgTime, TableRow, Timestamp, format_date, format_timestamp,
        format_timestamptz,
    },
    error::{ErrorKind, EtlResult, Retryability},
    etl_error,
    schema::{ColumnSchema, ReplicatedTableSchema, Type, is_array_type},
};
use pg_escape::quote_literal;

use crate::{
    ducklake::DUCKLAKE_COLUMN_NAME_MAPPING,
    retry::{ClassifiedError, classified_error},
};

/// Arrow errors come from encoding rows and fail the same way for the same
/// rows.
impl ClassifiedError for duckdb::arrow::error::ArrowError {
    fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}

/// Prepared row payload reused across retry attempts.
pub(super) enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    ArrowRecordBatch(RecordBatch),
    SqlLiterals(Vec<String>),
}

/// Converts table-copy rows into a retryable payload for DuckDB writes.
pub(super) fn prepare_copy_rows(
    replicated_table_schema: &ReplicatedTableSchema,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedRows> {
    let Some(column_kinds) = arrow_column_kinds(replicated_table_schema) else {
        return Ok(prepare_rows(table_rows));
    };

    if !copy_rows_match_arrow_kinds(&column_kinds, &table_rows) {
        return Ok(prepare_rows(table_rows));
    }

    Ok(PreparedRows::ArrowRecordBatch(copy_rows_to_arrow_record_batch(
        replicated_table_schema,
        column_kinds,
        table_rows,
    )?))
}

/// Converts table rows into a retryable payload for DuckDB writes.
pub(super) fn prepare_rows(table_rows: Vec<TableRow>) -> PreparedRows {
    if table_rows.iter().any(|row| row.values().iter().any(cell_requires_sql_literals)) {
        return PreparedRows::SqlLiterals(
            table_rows.into_iter().map(table_row_to_sql_literal).collect(),
        );
    }

    PreparedRows::Appender(
        table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect(),
    )
}

/// Arrow column shape used for COPY staging.
#[derive(Clone, Copy)]
enum ArrowColumnKind {
    Bool,
    I16,
    I32,
    I64,
    U64,
    F32,
    F64,
    Utf8,
    Date32,
    Time64Microsecond,
    TimestampMicrosecond,
    TimestampTzMicrosecond,
    Binary,
}

impl ArrowColumnKind {
    /// Returns the Arrow type represented by this column kind.
    fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::I16 => DataType::Int16,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::U64 => DataType::UInt64,
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::Date32 => DataType::Date32,
            Self::Time64Microsecond => DataType::Time64(ArrowTimeUnit::Microsecond),
            Self::TimestampMicrosecond => DataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            Self::TimestampTzMicrosecond => {
                DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("+00:00".into()))
            }
            Self::Binary => DataType::Binary,
        }
    }
}

/// Mutable Arrow column data accumulated from copied rows.
enum ArrowColumnValues {
    Bool(Vec<Option<bool>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    U64(Vec<Option<u64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Date32(Vec<Option<i32>>),
    Time64Microsecond(Vec<Option<i64>>),
    TimestampMicrosecond(Vec<Option<i64>>),
    TimestampTzMicrosecond(Vec<Option<i64>>),
    Binary(Vec<Option<Vec<u8>>>),
}

impl ArrowColumnValues {
    /// Creates an empty column value accumulator for one Arrow kind.
    fn with_capacity(kind: ArrowColumnKind, capacity: usize) -> Self {
        match kind {
            ArrowColumnKind::Bool => Self::Bool(Vec::with_capacity(capacity)),
            ArrowColumnKind::I16 => Self::I16(Vec::with_capacity(capacity)),
            ArrowColumnKind::I32 => Self::I32(Vec::with_capacity(capacity)),
            ArrowColumnKind::I64 => Self::I64(Vec::with_capacity(capacity)),
            ArrowColumnKind::U64 => Self::U64(Vec::with_capacity(capacity)),
            ArrowColumnKind::F32 => Self::F32(Vec::with_capacity(capacity)),
            ArrowColumnKind::F64 => Self::F64(Vec::with_capacity(capacity)),
            ArrowColumnKind::Utf8 => Self::Utf8(Vec::with_capacity(capacity)),
            ArrowColumnKind::Date32 => Self::Date32(Vec::with_capacity(capacity)),
            ArrowColumnKind::Time64Microsecond => {
                Self::Time64Microsecond(Vec::with_capacity(capacity))
            }
            ArrowColumnKind::TimestampMicrosecond => {
                Self::TimestampMicrosecond(Vec::with_capacity(capacity))
            }
            ArrowColumnKind::TimestampTzMicrosecond => {
                Self::TimestampTzMicrosecond(Vec::with_capacity(capacity))
            }
            ArrowColumnKind::Binary => Self::Binary(Vec::with_capacity(capacity)),
        }
    }

    /// Pushes one cell into the accumulator.
    fn push_cell(&mut self, column_name: &str, cell: Cell) -> EtlResult<()> {
        match (self, cell) {
            (Self::Bool(values), Cell::Null) => values.push(None),
            (Self::Bool(values), Cell::Bool(value)) => values.push(Some(value)),
            (Self::I16(values), Cell::Null) => values.push(None),
            (Self::I16(values), Cell::I16(value)) => values.push(Some(value)),
            (Self::I32(values), Cell::Null) => values.push(None),
            (Self::I32(values), Cell::I32(value)) => values.push(Some(value)),
            (Self::I64(values), Cell::Null) => values.push(None),
            (Self::I64(values), Cell::I64(value)) => values.push(Some(value)),
            (Self::U64(values), Cell::Null) => values.push(None),
            (Self::U64(values), Cell::U32(value)) => values.push(Some(u64::from(value))),
            (Self::F32(values), Cell::Null) => values.push(None),
            (Self::F32(values), Cell::F32(value)) => values.push(Some(value)),
            (Self::F64(values), Cell::Null) => values.push(None),
            (Self::F64(values), Cell::F64(value)) => values.push(Some(value)),
            (Self::Utf8(values), Cell::Null) => values.push(None),
            (Self::Utf8(values), Cell::String(value)) => values.push(Some(value)),
            (Self::Utf8(values), Cell::Numeric(value)) => values.push(Some(value.to_string())),
            (Self::Utf8(values), Cell::TimeTz(value)) => values.push(Some(value.to_string())),
            (Self::Utf8(values), Cell::Uuid(value)) => values.push(Some(value.to_string())),
            (Self::Utf8(values), Cell::Json(value)) => values.push(Some(value.to_string())),
            (Self::Date32(values), Cell::Null) => values.push(None),
            (Self::Date32(values), Cell::Date(Date::Value(value))) => {
                values.push(Some(date_days_since_epoch(value, column_name)?));
            }
            (Self::Time64Microsecond(values), Cell::Null) => values.push(None),
            (Self::Time64Microsecond(values), Cell::Time(PgTime::Value(value))) => {
                values.push(Some(time_micros_since_midnight(value)));
            }
            (Self::TimestampMicrosecond(values), Cell::Null) => values.push(None),
            (Self::TimestampMicrosecond(values), Cell::Timestamp(Timestamp::Value(value))) => {
                values.push(Some(value.and_utc().timestamp_micros()));
            }
            (Self::TimestampTzMicrosecond(values), Cell::Null) => values.push(None),
            (Self::TimestampTzMicrosecond(values), Cell::TimestampTz(Timestamp::Value(value))) => {
                values.push(Some(value.timestamp_micros()));
            }
            (Self::Binary(values), Cell::Null) => values.push(None),
            (Self::Binary(values), Cell::Bytes(value)) => values.push(Some(value)),
            _ => {
                return Err(etl_error!(
                    ErrorKind::ConversionError,
                    "DuckLake Arrow copy conversion failed",
                    format!("Column '{column_name}' has a value incompatible with its Arrow kind")
                ));
            }
        }
        Ok(())
    }

    /// Converts the accumulated data into an Arrow array.
    fn into_array(self) -> ArrayRef {
        match self {
            Self::Bool(values) => Arc::new(BooleanArray::from(values)),
            Self::I16(values) => Arc::new(Int16Array::from(values)),
            Self::I32(values) => Arc::new(Int32Array::from(values)),
            Self::I64(values) => Arc::new(Int64Array::from(values)),
            Self::U64(values) => Arc::new(UInt64Array::from(values)),
            Self::F32(values) => Arc::new(Float32Array::from(values)),
            Self::F64(values) => Arc::new(Float64Array::from(values)),
            Self::Utf8(values) => Arc::new(StringArray::from(values)),
            Self::Date32(values) => Arc::new(Date32Array::from(values)),
            Self::Time64Microsecond(values) => Arc::new(Time64MicrosecondArray::from(values)),
            Self::TimestampMicrosecond(values) => Arc::new(TimestampMicrosecondArray::from(values)),
            Self::TimestampTzMicrosecond(values) => {
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone_utc())
            }
            Self::Binary(values) => {
                let values: Vec<Option<&[u8]>> = values.iter().map(Option::as_deref).collect();
                Arc::new(BinaryArray::from_opt_vec(values))
            }
        }
    }
}

/// Returns Arrow column kinds for schemas that can use Arrow COPY staging.
fn arrow_column_kinds(
    replicated_table_schema: &ReplicatedTableSchema,
) -> Option<Vec<ArrowColumnKind>> {
    replicated_table_schema.column_schemas().map(arrow_column_kind).collect()
}

/// Returns the Arrow column kind for one replicated column.
fn arrow_column_kind(column_schema: &ColumnSchema) -> Option<ArrowColumnKind> {
    if is_array_type(&column_schema.typ) {
        return None;
    }

    match &column_schema.typ {
        &Type::BOOL => Some(ArrowColumnKind::Bool),
        &Type::INT2 => Some(ArrowColumnKind::I16),
        &Type::INT4 => Some(ArrowColumnKind::I32),
        &Type::INT8 => Some(ArrowColumnKind::I64),
        &Type::OID => Some(ArrowColumnKind::U64),
        &Type::FLOAT4 => Some(ArrowColumnKind::F32),
        &Type::FLOAT8 => Some(ArrowColumnKind::F64),
        &Type::DATE => Some(ArrowColumnKind::Date32),
        &Type::TIME => Some(ArrowColumnKind::Time64Microsecond),
        &Type::TIMESTAMP => Some(ArrowColumnKind::TimestampMicrosecond),
        &Type::TIMESTAMPTZ => Some(ArrowColumnKind::TimestampTzMicrosecond),
        &Type::BYTEA => Some(ArrowColumnKind::Binary),
        &Type::UUID | &Type::JSON | &Type::JSONB => None,
        _ => Some(ArrowColumnKind::Utf8),
    }
}

/// Returns whether all copied rows can be represented by the Arrow kinds.
fn copy_rows_match_arrow_kinds(column_kinds: &[ArrowColumnKind], table_rows: &[TableRow]) -> bool {
    table_rows.iter().all(|row| {
        row.values().len() == column_kinds.len()
            && row
                .values()
                .iter()
                .zip(column_kinds.iter())
                .all(|(cell, kind)| cell_matches_arrow_kind(cell, *kind))
    })
}

/// Returns whether one cell can be pushed into an Arrow column kind.
fn cell_matches_arrow_kind(cell: &Cell, kind: ArrowColumnKind) -> bool {
    if matches!(cell, Cell::Null) {
        return true;
    }

    matches!(
        (kind, cell),
        (ArrowColumnKind::Bool, Cell::Bool(_))
            | (ArrowColumnKind::I16, Cell::I16(_))
            | (ArrowColumnKind::I32, Cell::I32(_))
            | (ArrowColumnKind::I64, Cell::I64(_))
            | (ArrowColumnKind::U64, Cell::U32(_))
            | (ArrowColumnKind::F32, Cell::F32(_))
            | (ArrowColumnKind::F64, Cell::F64(_))
            | (
                ArrowColumnKind::Utf8,
                Cell::String(_)
                    | Cell::Numeric(_)
                    | Cell::TimeTz(_)
                    | Cell::Uuid(_)
                    | Cell::Json(_)
            )
            | (ArrowColumnKind::Date32, Cell::Date(Date::Value(_)))
            | (ArrowColumnKind::Time64Microsecond, Cell::Time(PgTime::Value(_)))
            | (ArrowColumnKind::TimestampMicrosecond, Cell::Timestamp(Timestamp::Value(_)))
            | (ArrowColumnKind::TimestampTzMicrosecond, Cell::TimestampTz(Timestamp::Value(_)))
            | (ArrowColumnKind::Binary, Cell::Bytes(_))
    )
}

/// Converts rows into a DuckDB-compatible Arrow record batch.
fn copy_rows_to_arrow_record_batch(
    replicated_table_schema: &ReplicatedTableSchema,
    column_kinds: Vec<ArrowColumnKind>,
    table_rows: Vec<TableRow>,
) -> EtlResult<RecordBatch> {
    let row_count = table_rows.len();
    let column_schemas = replicated_table_schema.column_schemas().collect::<Vec<_>>();
    let fields = column_schemas
        .iter()
        .zip(column_kinds.iter())
        .map(|(column_schema, kind)| {
            Field::new(
                DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column_schema.name),
                kind.data_type(),
                column_schema.nullable,
            )
        })
        .collect::<Vec<_>>();
    let mut column_values = column_kinds
        .iter()
        .copied()
        .map(|kind| ArrowColumnValues::with_capacity(kind, row_count))
        .collect::<Vec<_>>();

    for row in table_rows {
        for ((column_value, field), cell) in
            column_values.iter_mut().zip(fields.iter()).zip(row.into_values())
        {
            column_value.push_cell(field.name(), cell)?;
        }
    }

    let arrays = column_values.into_iter().map(ArrowColumnValues::into_array).collect::<Vec<_>>();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        classified_error(ErrorKind::ConversionError, "DuckLake Arrow copy conversion failed", error)
    })
}

/// Converts a date to DuckDB's Arrow date representation.
fn date_days_since_epoch(value: NaiveDate, column_name: &str) -> EtlResult<i32> {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date should be valid");
    i32::try_from(value.signed_duration_since(epoch_date).num_days()).map_err(|_| {
        etl_error!(
            ErrorKind::ConversionError,
            "DuckLake Arrow copy conversion failed",
            format!("Column '{column_name}' contains a date outside DuckDB's supported range")
        )
    })
}

/// Converts a time to DuckDB's microseconds-since-midnight representation.
fn time_micros_since_midnight(value: NaiveTime) -> i64 {
    i64::from(value.num_seconds_from_midnight()) * 1_000_000 + i64::from(value.nanosecond() / 1_000)
}

/// Serializes a borrowed row into a SQL `VALUES (...)` tuple.
pub(super) fn table_row_to_sql_literal_ref(row: &TableRow) -> String {
    format!("({})", row.values().iter().map(cell_to_sql_literal_ref).collect::<Vec<_>>().join(", "))
}

/// Serializes a borrowed cell into a DuckDB SQL literal expression.
pub(super) fn cell_to_sql_literal_ref(cell: &Cell) -> String {
    cell_to_sql_literal(cell_to_owned(cell))
}

/// Returns whether a cell must bypass the DuckDB appender path.
fn cell_requires_sql_literals(cell: &Cell) -> bool {
    matches!(
        cell,
        Cell::Array(_)
            | Cell::Date(Date::PosInfinity | Date::NegInfinity)
            | Cell::Time(PgTime::EndOfDay)
            | Cell::Timestamp(Timestamp::PosInfinity | Timestamp::NegInfinity)
            | Cell::TimestampTz(Timestamp::PosInfinity | Timestamp::NegInfinity)
    )
}

/// Serializes a row into a SQL `VALUES (...)` tuple.
fn table_row_to_sql_literal(row: TableRow) -> String {
    table_row_to_sql_literal_ref(&row)
}

/// Converts a [`Cell`] into a DuckDB SQL literal expression.
fn cell_to_sql_literal(cell: Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Bool(b) => {
            if b {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        Cell::String(s) => quote_literal(&s),
        Cell::I16(i) => i.to_string(),
        Cell::I32(i) => i.to_string(),
        Cell::U32(u) => u.to_string(),
        Cell::I64(i) => i.to_string(),
        Cell::F32(f) => float_literal(f64::from(f), false),
        Cell::F64(f) => float_literal(f, true),
        Cell::Numeric(n) => quote_literal(&n.to_string()),
        Cell::Date(d) => format!("DATE '{}'", format_date(&d)),
        Cell::Time(t) => format!("TIME '{t}'"),
        Cell::TimeTz(t) => quote_literal(&t.to_string()),
        Cell::Timestamp(dt) => {
            format!("TIMESTAMP '{}'", format_timestamp(&dt))
        }
        Cell::TimestampTz(dt) => {
            format!("TIMESTAMPTZ '{}'", format_timestamptz(&dt))
        }
        Cell::Uuid(u) => format!("CAST({} AS UUID)", quote_literal(&u.to_string())),
        Cell::Json(j) => format!("CAST({} AS JSON)", quote_literal(&j.to_string())),
        Cell::Bytes(b) => format!("from_hex('{}')", encode_hex(&b)),
        Cell::Array(arr) => array_cell_to_sql_literal(arr),
    }
}

/// Clones a [`Cell`] from a borrowed row reference.
fn cell_to_owned(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::Bool(*value),
        Cell::String(value) => Cell::String(value.clone()),
        Cell::I16(value) => Cell::I16(*value),
        Cell::I32(value) => Cell::I32(*value),
        Cell::U32(value) => Cell::U32(*value),
        Cell::I64(value) => Cell::I64(*value),
        Cell::F32(value) => Cell::F32(*value),
        Cell::F64(value) => Cell::F64(*value),
        Cell::Numeric(value) => Cell::Numeric(value.clone()),
        Cell::Date(value) => Cell::Date(*value),
        Cell::Time(value) => Cell::Time(*value),
        Cell::TimeTz(value) => Cell::TimeTz(*value),
        Cell::Timestamp(value) => Cell::Timestamp(*value),
        Cell::TimestampTz(value) => Cell::TimestampTz(*value),
        Cell::Uuid(value) => Cell::Uuid(*value),
        Cell::Json(value) => Cell::Json(value.clone()),
        Cell::Bytes(value) => Cell::Bytes(value.clone()),
        Cell::Array(value) => Cell::Array(array_cell_to_owned(value)),
    }
}

/// Clones an [`ArrayCell`] from a borrowed row reference.
fn array_cell_to_owned(cell: &ArrayCell) -> ArrayCell {
    match cell {
        ArrayCell::Bool(values) => ArrayCell::Bool(values.clone()),
        ArrayCell::String(values) => ArrayCell::String(values.clone()),
        ArrayCell::I16(values) => ArrayCell::I16(values.clone()),
        ArrayCell::I32(values) => ArrayCell::I32(values.clone()),
        ArrayCell::U32(values) => ArrayCell::U32(values.clone()),
        ArrayCell::I64(values) => ArrayCell::I64(values.clone()),
        ArrayCell::F32(values) => ArrayCell::F32(values.clone()),
        ArrayCell::F64(values) => ArrayCell::F64(values.clone()),
        ArrayCell::Numeric(values) => ArrayCell::Numeric(values.clone()),
        ArrayCell::Date(values) => ArrayCell::Date(values.clone()),
        ArrayCell::Time(values) => ArrayCell::Time(values.clone()),
        ArrayCell::TimeTz(values) => ArrayCell::TimeTz(values.clone()),
        ArrayCell::Timestamp(values) => ArrayCell::Timestamp(values.clone()),
        ArrayCell::TimestampTz(values) => ArrayCell::TimestampTz(values.clone()),
        ArrayCell::Uuid(values) => ArrayCell::Uuid(values.clone()),
        ArrayCell::Json(values) => ArrayCell::Json(values.clone()),
        ArrayCell::Bytes(values) => ArrayCell::Bytes(values.clone()),
    }
}

/// Converts an [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal(arr: ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| if value { "TRUE" } else { "FALSE" }.to_owned(),
                )
            })
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value)))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(|| "NULL".to_owned(), |value| float_literal(f64::from(value), false))
            })
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value, true)))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value.to_string())))
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("DATE '{}'", format_date(&value)),
                )
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| format!("TIME '{value}'")))
            .collect(),
        ArrayCell::TimeTz(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value.to_string())))
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("TIMESTAMP '{}'", format_timestamp(&value)),
                )
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("TIMESTAMPTZ '{}'", format_timestamptz(&value)),
                )
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS JSON)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("from_hex('{}')", encode_hex(&value)),
                )
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Returns a DuckDB SQL literal for a floating-point value.
///
/// DuckDB types a bare numeric literal by its text: a plain decimal becomes
/// DECIMAL, which rounds again on the way to the float column, `-0` becomes
/// integer zero, and a list literal takes a DECIMAL type from its elements
/// that later elements may not fit. Casting the shortest round-trip text
/// parses it directly as the target type, so the stored value keeps its bits.
/// `FLOAT` values arrive widened to `f64`, which represents them exactly.
fn float_literal(value: f64, is_double: bool) -> String {
    let sql_type = if is_double { "DOUBLE" } else { "FLOAT" };
    if value.is_nan() {
        format!("CAST('NaN' AS {sql_type})")
    } else if value.is_infinite() {
        let sign = if value.is_sign_negative() { "-" } else { "" };
        format!("CAST('{sign}Infinity' AS {sql_type})")
    } else {
        format!("CAST('{value:e}' AS {sql_type})")
    }
}

/// Encodes bytes as uppercase hexadecimal for DuckDB's `from_hex`.
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with parameterized
/// INSERT statements.
fn cell_to_value(cell: Cell) -> Value {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::Boolean(b),
        Cell::String(s) => Value::Text(s),
        Cell::I16(i) => Value::SmallInt(i),
        Cell::I32(i) => Value::Int(i),
        Cell::U32(u) => Value::UInt(u),
        Cell::I64(i) => Value::BigInt(i),
        Cell::F32(f) => Value::Float(f),
        Cell::F64(f) => Value::Double(f),
        // NUMERIC stored as VARCHAR to avoid precision loss.
        Cell::Numeric(n) => Value::Text(n.to_string()),
        Cell::Date(Date::Value(d)) => {
            Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32)
        }
        Cell::Time(PgTime::Value(t)) => {
            Value::Time64(TimeUnit::Microsecond, time_micros_since_midnight(t))
        }
        Cell::TimeTz(t) => Value::Text(t.to_string()),
        Cell::Timestamp(Timestamp::Value(dt)) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(Timestamp::Value(dt)) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros())
        }
        Cell::Date(value) => Value::Text(format_date(&value).to_string()),
        Cell::Time(value) => Value::Text(value.to_string()),
        Cell::Timestamp(value) => Value::Text(format_timestamp(&value).to_string()),
        Cell::TimestampTz(value) => Value::Text(format_timestamptz(&value).to_string()),
        // UUID stored as text; DuckDB casts VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        // JSON serialised as text.
        Cell::Json(j) => Value::Text(j.to_string()),
        Cell::Bytes(b) => Value::Blob(b),
        Cell::Array(arr) => array_cell_to_value(arr),
    }
}

/// Converts an [`ArrayCell`] (with nullable elements) to a `Value::List`.
fn array_cell_to_value(arr: ArrayCell) -> Value {
    let values = match arr {
        ArrayCell::Bool(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::Boolean)).collect()
        }
        ArrayCell::String(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Text)).collect(),
        ArrayCell::I16(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::SmallInt)).collect()
        }
        ArrayCell::I32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Int)).collect(),
        ArrayCell::U32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::UInt)).collect(),
        ArrayCell::I64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::BigInt)).collect(),
        ArrayCell::F32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Float)).collect(),
        ArrayCell::F64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Double)).collect(),
        ArrayCell::Numeric(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |n| Value::Text(n.to_string()))).collect()
        }
        ArrayCell::Date(v) => {
            v.into_iter().map(|value| cell_to_value(value.map_or(Cell::Null, Cell::Date))).collect()
        }
        ArrayCell::Time(v) => {
            v.into_iter().map(|value| cell_to_value(value.map_or(Cell::Null, Cell::Time))).collect()
        }
        ArrayCell::TimeTz(v) => v
            .into_iter()
            .map(|value| cell_to_value(value.map_or(Cell::Null, Cell::TimeTz)))
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|value| cell_to_value(value.map_or(Cell::Null, Cell::Timestamp)))
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|value| cell_to_value(value.map_or(Cell::Null, Cell::TimestampTz)))
            .collect(),
        ArrayCell::Uuid(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |u| Value::Text(u.to_string()))).collect()
        }
        ArrayCell::Json(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |j| Value::Text(j.to_string()))).collect()
        }
        ArrayCell::Bytes(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Blob)).collect(),
    };
    Value::List(values)
}

#[cfg(test)]
pub(super) mod tests {
    use etl::{
        data::{PgTime, Timestamp},
        schema::{
            ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type as PgType,
        },
    };

    use super::*;

    fn replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            columns,
        )))
    }

    #[test]
    fn cell_to_value_primitives() {
        assert_eq!(cell_to_value(Cell::Null), Value::Null);
        assert_eq!(cell_to_value(Cell::Bool(true)), Value::Boolean(true));
        assert_eq!(
            cell_to_value(Cell::String("hello".to_owned())),
            Value::Text("hello".to_owned())
        );
        assert_eq!(cell_to_value(Cell::I32(42)), Value::Int(42));
        assert_eq!(cell_to_value(Cell::I64(-1)), Value::BigInt(-1));
        assert_eq!(cell_to_value(Cell::F64(3.46)), Value::Double(3.46));
    }

    #[test]
    fn cell_to_value_preserves_time_microseconds_for_scalars_and_arrays() {
        let time = NaiveTime::from_hms_micro_opt(12, 34, 56, 789_012).unwrap();
        let micros = 45_296_789_012;
        let value = Value::Time64(TimeUnit::Microsecond, micros);

        assert_eq!(cell_to_value(Cell::Time(PgTime::Value(time))), value);
        assert_eq!(
            array_cell_to_value(ArrayCell::Time(vec![Some(PgTime::Value(time)), None])),
            Value::List(vec![value, Value::Null])
        );
    }

    #[test]
    fn array_cell_to_sql_literal_preserves_nulls() {
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            "[1, NULL, 3]"
        );
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::Json(vec![
                Some(serde_json::json!({"a": 1})),
                None,
            ])),
            "[CAST('{\"a\":1}' AS JSON), NULL]"
        );
    }

    #[test]
    fn prepare_rows_uses_sql_literals_for_arrays() {
        let prepared = prepare_rows(vec![TableRow::new(vec![
            Cell::I32(1),
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
        ])]);

        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(rows, vec!["(1, [1, NULL, 3])"]);
            }
            PreparedRows::Appender(_) => panic!("expected sql literal fallback"),
            PreparedRows::ArrowRecordBatch(_) => panic!("expected sql literal fallback"),
        }
    }

    #[test]
    fn prepare_copy_rows_uses_arrow_for_supported_columns() {
        let schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ColumnSchema::new("created_at".to_owned(), PgType::TIMESTAMP, -1, 3, true),
        ]);
        let rows = vec![
            TableRow::new(vec![
                Cell::I32(1),
                Cell::String("alice".to_owned()),
                Cell::Timestamp(Timestamp::Value(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                        .unwrap()
                        .and_hms_opt(3, 4, 5)
                        .unwrap(),
                )),
            ]),
            TableRow::new(vec![Cell::I32(2), Cell::Null, Cell::Null]),
        ];

        let prepared = prepare_copy_rows(&schema, rows).unwrap();

        match prepared {
            PreparedRows::ArrowRecordBatch(record_batch) => {
                assert_eq!(record_batch.num_rows(), 2);
                assert_eq!(record_batch.num_columns(), 3);
                assert_eq!(record_batch.schema().field(0).data_type(), &DataType::Int32);
                assert_eq!(record_batch.schema().field(1).data_type(), &DataType::Utf8);
                assert_eq!(
                    record_batch.schema().field(2).data_type(),
                    &DataType::Timestamp(ArrowTimeUnit::Microsecond, None)
                );
            }
            PreparedRows::Appender(_) | PreparedRows::SqlLiterals(_) => {
                panic!("expected Arrow record batch")
            }
        }
    }

    #[test]
    fn prepare_copy_rows_falls_back_for_array_columns() {
        let schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
            ColumnSchema::new("tags".to_owned(), PgType::INT4_ARRAY, -1, 2, true),
        ]);
        let rows = vec![TableRow::new(vec![
            Cell::I32(1),
            Cell::Array(ArrayCell::I32(vec![Some(1), None])),
        ])];

        let prepared = prepare_copy_rows(&schema, rows).unwrap();

        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(rows, vec!["(1, [1, NULL])"]);
            }
            PreparedRows::Appender(_) | PreparedRows::ArrowRecordBatch(_) => {
                panic!("expected sql literal fallback")
            }
        }
    }

    #[test]
    fn prepare_copy_rows_falls_back_for_cast_sensitive_columns() {
        let schema = replicated_schema(vec![ColumnSchema::new(
            "payload".to_owned(),
            PgType::JSON,
            -1,
            1,
            false,
        )]);
        let rows = vec![TableRow::new(vec![Cell::Json(serde_json::json!({"id": 1}))])];

        let prepared = prepare_copy_rows(&schema, rows).unwrap();

        match prepared {
            PreparedRows::Appender(rows) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedRows::SqlLiterals(_) | PreparedRows::ArrowRecordBatch(_) => {
                panic!("expected row appender fallback")
            }
        }
    }
    #[test]
    fn special_temporal_values_use_sql_literals_for_copy_and_cdc() {
        let schema = replicated_schema(vec![
            ColumnSchema::new("d".to_owned(), PgType::DATE, -1, 1, true),
            ColumnSchema::new("t".to_owned(), PgType::TIME, -1, 2, true),
            ColumnSchema::new("ts".to_owned(), PgType::TIMESTAMP, -1, 3, true),
            ColumnSchema::new("tsz".to_owned(), PgType::TIMESTAMPTZ, -1, 4, true),
        ]);
        let row = TableRow::new(vec![
            Cell::Date(Date::NegInfinity),
            Cell::Time(PgTime::EndOfDay),
            Cell::Timestamp(Timestamp::PosInfinity),
            Cell::TimestampTz(Timestamp::NegInfinity),
        ]);
        for prepared in
            [prepare_copy_rows(&schema, vec![row.clone()]).unwrap(), prepare_rows(vec![row])]
        {
            let PreparedRows::SqlLiterals(rows) = prepared else {
                panic!("expected SQL literals for special temporal values");
            };
            let conn = duckdb::Connection::open_in_memory().unwrap();
            let valid: bool = conn
                .query_row(
                    &format!(
                        "select d = date '-infinity' and t = time '24:00:00' and t <> time \
                         '00:00:00' and ts = timestamp 'infinity' and tsz = timestamptz \
                         '-infinity' from (values {}) as v(d, t, ts, tsz)",
                        rows.join(",")
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(valid);
        }
    }

    #[test]
    fn temporal_sql_literals_preserve_calendar_years() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for year in [-43, 0, 12023] {
            let date = NaiveDate::from_ymd_opt(year, 2, 1).unwrap();
            let timestamp = date.and_hms_opt(11, 12, 13).unwrap();
            let row = TableRow::new(vec![
                Cell::Date(Date::Value(date)),
                Cell::Timestamp(Timestamp::Value(timestamp)),
            ]);
            let literal = table_row_to_sql_literal(row);
            let valid: bool = conn
                .query_row(
                    &format!(
                        "select extract(year from d) = {year} and extract(year from ts) = {year} \
                         and ts::time = time '11:12:13' from (values {literal}) as v(d, ts)"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(valid);
        }
    }

    /// Bit-exact float literal and three-encoder agreement properties.
    ///
    /// DuckLake writes the same Postgres value through three encoders: SQL
    /// literals (partial-update `SET`s, array and special-temporal insert
    /// groups), the row appender (plain CDC insert groups), and Arrow (initial
    /// copy). A value that one encoder changes silently diverges between rows
    /// written by COPY and by CDC, so each path must store the original value.
    pub(in crate::ducklake) mod encoder_properties {
        use std::str::FromStr;

        use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
        use etl::{
            data::{PgNumeric, PgTimeTz},
            test_utils::property::{
                any_f32, any_f64, f32_matches, f64_matches, pg_text, pg_time, run_property,
            },
        };
        use proptest::{
            collection::{btree_map, vec},
            option,
            prelude::*,
            strategy::Union,
        };
        use uuid::Uuid;

        use super::*;
        use crate::ducklake::{DuckLakeTableName, schema::build_create_table_sql_ducklake};

        /// Microseconds per day, used to express `24:00:00` as DuckDB stores
        /// it.
        const MICROS_PER_DAY: i64 = 86_400_000_000;

        /// Returns the Postgres `numeric(p, s)` type modifier.
        fn numeric_typmod(precision: i32, scale: i32) -> i32 {
            ((precision << 16) | (scale & 0x7ff)) + 4
        }

        /// Parses one SQL float literal back through DuckDB as the target
        /// type.
        fn parse_float_literal(conn: &duckdb::Connection, literal: &str, sql_type: &str) -> Value {
            conn.query_row(&format!("select cast({literal} as {sql_type})"), [], |row| row.get(0))
                .unwrap()
        }

        /// Checks that DuckDB parses `float_literal(value, true)` back to the
        /// same `double` bit pattern.
        fn check_f64_literal(conn: &duckdb::Connection, value: f64) -> Result<(), TestCaseError> {
            let literal = float_literal(value, true);
            let Value::Double(parsed) = parse_float_literal(conn, &literal, "double") else {
                return Err(TestCaseError::fail("expected a double"));
            };
            prop_assert!(
                f64_matches(value, parsed),
                "literal {literal} for {value:e} ({:#018x}) parsed as {parsed:e} ({:#018x})",
                value.to_bits(),
                parsed.to_bits()
            );
            Ok(())
        }

        /// Checks that DuckDB parses `float_literal(value, false)` back to the
        /// same `float` bit pattern.
        fn check_f32_literal(conn: &duckdb::Connection, value: f32) -> Result<(), TestCaseError> {
            let literal = float_literal(f64::from(value), false);
            let Value::Float(parsed) = parse_float_literal(conn, &literal, "float") else {
                return Err(TestCaseError::fail("expected a float"));
            };
            prop_assert!(
                f32_matches(value, parsed),
                "literal {literal} for {value:e} ({:#010x}) parsed as {parsed:e} ({:#010x})",
                value.to_bits(),
                parsed.to_bits()
            );
            Ok(())
        }

        /// Checks one double literal on a fresh connection.
        fn assert_f64_literal_bit_exact(value: f64) {
            check_f64_literal(&duckdb::Connection::open_in_memory().unwrap(), value).unwrap();
        }

        /// Checks one float literal on a fresh connection.
        fn assert_f32_literal_bit_exact(value: f32) {
            check_f32_literal(&duckdb::Connection::open_in_memory().unwrap(), value).unwrap();
        }

        #[test]
        fn float8_sql_literals_parse_bit_exact() {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            run_property("ducklake_float8_sql_literal_bit_exact", &any_f64(), |&value| {
                check_f64_literal(&conn, value)
            });
        }

        #[test]
        fn float4_sql_literals_parse_bit_exact() {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            run_property("ducklake_float4_sql_literal_bit_exact", &any_f32(), |&value| {
                check_f32_literal(&conn, value)
            });
        }

        #[test]
        fn float8_sql_literal_keeps_decimal_fraction_exact() {
            // As a plain decimal, DuckDB reads 1e-30 as DECIMAL and rounds
            // again when casting to DOUBLE, landing on 9.999999999999999e-31.
            assert_f64_literal_bit_exact(1e-30);
        }

        #[test]
        fn float4_sql_literal_keeps_decimal_fraction_exact() {
            // As a plain decimal, the widened f64 text rounds through DECIMAL
            // to 0x21915b53.
            assert_f32_literal_bit_exact(f32::from_bits(0x2191_5b52));
        }

        #[test]
        fn float8_sql_literal_keeps_magnitude_of_huge_values() {
            // Plain notation spells 9.223372036854776e273 with 274 integer
            // digits, wider than any DuckDB integer or DECIMAL type.
            assert_f64_literal_bit_exact(f64::from_bits(0x78d1_0cb1_32c2_ff63));
        }

        #[test]
        fn float8_sql_literal_keeps_negative_zero() {
            assert_f64_literal_bit_exact(-0.0);
        }

        #[test]
        fn float4_sql_literal_keeps_negative_zero() {
            assert_f32_literal_bit_exact(-0.0);
        }

        /// One generated destination column with the cells written to it.
        #[derive(Clone, Debug)]
        struct EncoderCase {
            typ: PgType,
            modifier: i32,
            cells: Vec<Cell>,
        }

        /// Encoder path that writes rows into the destination table.
        #[derive(Clone, Copy, Debug)]
        enum EncoderPath {
            SqlLiteral,
            Appender,
            Copy,
        }

        impl EncoderPath {
            /// Returns the destination table name used for this path.
            fn table_name(self) -> DuckLakeTableName {
                let table = match self {
                    Self::SqlLiteral => "sql_literal",
                    Self::Appender => "appender",
                    Self::Copy => "copy",
                };
                DuckLakeTableName::new("public", table)
            }
        }

        /// Builds cases for one column type from a non-null cell strategy,
        /// mixing in NULLs.
        fn case_for(
            typ: PgType,
            modifier: i32,
            cell: impl Strategy<Value = Cell> + 'static,
        ) -> BoxedStrategy<EncoderCase> {
            vec(prop_oneof![1 => Just(Cell::Null), 6 => cell], 1..6)
                .prop_map(move |cells| EncoderCase { typ: typ.clone(), modifier, cells })
                .boxed()
        }

        /// Builds cases for one array column type with NULL elements.
        fn array_case<T: std::fmt::Debug + Clone + 'static>(
            typ: PgType,
            element: impl Strategy<Value = T> + 'static,
            wrap: fn(Vec<Option<T>>) -> ArrayCell,
        ) -> BoxedStrategy<EncoderCase> {
            case_for(
                typ,
                -1,
                vec(option::weighted(0.8, element), 0..5).prop_map(move |v| Cell::Array(wrap(v))),
            )
        }

        /// Text with SQL-significant characters, so E-string escaping is
        /// exercised often instead of only by chance.
        pub(in crate::ducklake) fn sql_text() -> impl Strategy<Value = String> {
            prop_oneof![pg_text(), "[a\\\\'\"\n ]{0,8}"]
        }

        /// Calendar dates inside the Postgres and chrono ranges, biased toward
        /// BC years, year zero, and years past 9999.
        fn date_value() -> impl Strategy<Value = NaiveDate> {
            use chrono::Datelike;

            let days = |year: i32| NaiveDate::from_ymd_opt(year, 1, 1).unwrap().num_days_from_ce();
            // Postgres dates start at 4713-11-24 BC; chrono stops at year
            // 262142.
            let min = NaiveDate::from_ymd_opt(-4712, 11, 24).unwrap().num_days_from_ce();
            let max = NaiveDate::MAX.num_days_from_ce();
            prop_oneof![
                min..=max,
                (days(0) - 800)..(days(0) + 800),
                (days(10_000) - 800)..(days(10_000) + 800),
                (days(1970) - 800)..(days(1970) + 800),
            ]
            .prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())
        }

        /// Dates including both infinities.
        fn pg_date() -> impl Strategy<Value = Date<NaiveDate>> {
            prop_oneof![
                8 => date_value().prop_map(Date::Value),
                1 => Just(Date::PosInfinity),
                1 => Just(Date::NegInfinity),
            ]
        }

        /// Times of day including Postgres `24:00:00`.
        fn pg_time_or_end_of_day() -> impl Strategy<Value = PgTime> {
            prop_oneof![9 => pg_time().prop_map(PgTime::Value), 1 => Just(PgTime::EndOfDay)]
        }

        /// Timestamps including both infinities.
        fn pg_timestamp() -> impl Strategy<Value = Timestamp<NaiveDateTime>> {
            prop_oneof![
                8 => (date_value(), pg_time())
                    .prop_map(|(date, time)| Timestamp::Value(date.and_time(time))),
                1 => Just(Timestamp::PosInfinity),
                1 => Just(Timestamp::NegInfinity),
            ]
        }

        /// UTC timestamps including both infinities.
        fn pg_timestamptz() -> impl Strategy<Value = Timestamp<DateTime<Utc>>> {
            pg_timestamp().prop_map(|timestamp| match timestamp {
                Timestamp::Value(value) => Timestamp::Value(value.and_utc()),
                Timestamp::PosInfinity => Timestamp::PosInfinity,
                Timestamp::NegInfinity => Timestamp::NegInfinity,
            })
        }

        /// `timetz` values with any offset Postgres accepts.
        fn pg_timetz() -> impl Strategy<Value = PgTimeTz> {
            (pg_time_or_end_of_day(), -57_599i32..=57_599).prop_map(|(time, offset)| {
                PgTimeTz::new(time, FixedOffset::east_opt(offset).unwrap())
            })
        }

        /// JSON numbers: integers, finite doubles, and digits beyond 64 bits.
        fn json_number() -> impl Strategy<Value = serde_json::Number> {
            prop_oneof![
                any::<i64>().prop_map(serde_json::Number::from),
                any_f64().prop_filter_map("json numbers are finite", serde_json::Number::from_f64),
                "-?[1-9][0-9]{18,40}(\\.[0-9]{1,20})?"
                    .prop_map(|text| serde_json::Number::from_str(&text).unwrap()),
            ]
        }

        /// Nested JSON documents with escaped strings and wide numbers.
        pub(in crate::ducklake) fn json_value() -> impl Strategy<Value = serde_json::Value> {
            let leaf = prop_oneof![
                Just(serde_json::Value::Null),
                any::<bool>().prop_map(serde_json::Value::Bool),
                json_number().prop_map(serde_json::Value::Number),
                sql_text().prop_map(serde_json::Value::String),
            ];
            leaf.prop_recursive(3, 16, 4, |inner| {
                prop_oneof![
                    vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
                    btree_map(sql_text(), inner, 0..4)
                        .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
                ]
            })
        }

        /// Renders `digits` as a decimal integer without leading zeros.
        fn digits_to_integer_text(digits: &[u8]) -> String {
            let text: String = digits
                .iter()
                .skip_while(|digit| **digit == 0)
                .map(|digit| char::from(b'0' + digit))
                .collect();
            if text.is_empty() { "0".to_owned() } else { text }
        }

        /// `numeric(p, s)` columns that map to DuckDB `decimal(p, s)`, with
        /// values rendered at the column scale as Postgres sends them.
        fn decimal_case() -> BoxedStrategy<EncoderCase> {
            (1i32..=38)
                .prop_flat_map(|precision| (Just(precision), 0..=precision))
                .prop_flat_map(|(precision, scale)| {
                    let integer_digits = (precision - scale) as usize;
                    let value = (
                        any::<bool>(),
                        (0..=integer_digits).prop_flat_map(|len| vec(0u8..10, len)),
                        vec(0u8..10, scale as usize),
                    )
                        .prop_map(|(negative, integer, fraction)| {
                            let integer = digits_to_integer_text(&integer);
                            let fraction: String =
                                fraction.iter().map(|digit| char::from(b'0' + digit)).collect();
                            let is_zero = integer == "0" && fraction.bytes().all(|b| b == b'0');
                            let sign = if negative && !is_zero { "-" } else { "" };
                            let text = if fraction.is_empty() {
                                format!("{sign}{integer}")
                            } else {
                                format!("{sign}{integer}.{fraction}")
                            };
                            Cell::Numeric(PgNumeric::from_str(&text).unwrap())
                        });
                    case_for(PgType::NUMERIC, numeric_typmod(precision, scale), value)
                })
                .boxed()
        }

        /// Numeric values for columns that DuckLake stores as `varchar`.
        fn varchar_numeric() -> impl Strategy<Value = PgNumeric> {
            prop_oneof![
                Just("NaN".to_owned()),
                Just("Infinity".to_owned()),
                Just("-Infinity".to_owned()),
                "-?[0-9]{1,40}(\\.[0-9]{1,40})?",
            ]
            .prop_map(|text| PgNumeric::from_str(&text).unwrap())
        }

        /// Every Postgres type in the DuckLake type map with codec-shaped
        /// cells.
        fn encoder_case() -> impl Strategy<Value = EncoderCase> {
            let uuid = || any::<u128>().prop_map(Uuid::from_u128);
            let bytes = || vec(any::<u8>(), 0..12);
            Union::new(vec![
                case_for(PgType::BOOL, -1, any::<bool>().prop_map(Cell::Bool)),
                case_for(PgType::INT2, -1, any::<i16>().prop_map(Cell::I16)),
                case_for(PgType::INT4, -1, any::<i32>().prop_map(Cell::I32)),
                case_for(PgType::INT8, -1, any::<i64>().prop_map(Cell::I64)),
                case_for(PgType::OID, -1, any::<u32>().prop_map(Cell::U32)),
                case_for(PgType::FLOAT4, -1, any_f32().prop_map(Cell::F32)),
                case_for(PgType::FLOAT8, -1, any_f64().prop_map(Cell::F64)),
                decimal_case(),
                prop_oneof![Just(-1), Just(numeric_typmod(50, 10)), Just(numeric_typmod(10, -2))]
                    .prop_flat_map(|modifier| {
                        case_for(
                            PgType::NUMERIC,
                            modifier,
                            varchar_numeric().prop_map(Cell::Numeric),
                        )
                    })
                    .boxed(),
                case_for(PgType::TEXT, -1, sql_text().prop_map(Cell::String)),
                case_for(PgType::DATE, -1, pg_date().prop_map(Cell::Date)),
                case_for(PgType::TIME, -1, pg_time_or_end_of_day().prop_map(Cell::Time)),
                case_for(PgType::TIMETZ, -1, pg_timetz().prop_map(Cell::TimeTz)),
                case_for(PgType::TIMESTAMP, -1, pg_timestamp().prop_map(Cell::Timestamp)),
                case_for(PgType::TIMESTAMPTZ, -1, pg_timestamptz().prop_map(Cell::TimestampTz)),
                case_for(PgType::UUID, -1, uuid().prop_map(Cell::Uuid)),
                case_for(PgType::JSON, -1, json_value().prop_map(Cell::Json)),
                case_for(PgType::JSONB, -1, json_value().prop_map(Cell::Json)),
                case_for(PgType::BYTEA, -1, bytes().prop_map(Cell::Bytes)),
                array_case(PgType::BOOL_ARRAY, any::<bool>(), ArrayCell::Bool),
                array_case(PgType::INT2_ARRAY, any::<i16>(), ArrayCell::I16),
                array_case(PgType::INT4_ARRAY, any::<i32>(), ArrayCell::I32),
                array_case(PgType::INT8_ARRAY, any::<i64>(), ArrayCell::I64),
                array_case(PgType::OID_ARRAY, any::<u32>(), ArrayCell::U32),
                array_case(PgType::FLOAT4_ARRAY, any_f32(), ArrayCell::F32),
                array_case(PgType::FLOAT8_ARRAY, any_f64(), ArrayCell::F64),
                array_case(PgType::NUMERIC_ARRAY, varchar_numeric(), ArrayCell::Numeric),
                array_case(PgType::TEXT_ARRAY, sql_text(), ArrayCell::String),
                array_case(PgType::DATE_ARRAY, pg_date(), ArrayCell::Date),
                array_case(PgType::TIME_ARRAY, pg_time_or_end_of_day(), ArrayCell::Time),
                array_case(PgType::TIMETZ_ARRAY, pg_timetz(), ArrayCell::TimeTz),
                array_case(PgType::TIMESTAMP_ARRAY, pg_timestamp(), ArrayCell::Timestamp),
                array_case(PgType::TIMESTAMPTZ_ARRAY, pg_timestamptz(), ArrayCell::TimestampTz),
                array_case(PgType::UUID_ARRAY, uuid(), ArrayCell::Uuid),
                array_case(PgType::JSON_ARRAY, json_value(), ArrayCell::Json),
                array_case(PgType::JSONB_ARRAY, json_value(), ArrayCell::Json),
                array_case(PgType::BYTEA_ARRAY, bytes(), ArrayCell::Bytes),
            ])
        }

        /// Returns the readback expression that yields a comparable value for
        /// `column`.
        ///
        /// UUID and JSON columns are read as text because the Rust client has
        /// no native value for them; decimals are read as text so precisions
        /// above the client's 28-digit decimal type stay exact. DuckDB renders
        /// `decimal(p, p)` values without the leading zero (`.50`), so that
        /// zero is restored to match Postgres text; codec numeric text always
        /// has it, so `varchar` numerics are unaffected.
        pub(in crate::ducklake) fn readback_expression(typ: &PgType, column: &str) -> String {
            let column = crate::ducklake::sql::quote_identifier(column);
            match *typ {
                PgType::NUMERIC => {
                    format!("regexp_replace(cast({column} as varchar), '^(-?)\\.', '\\10.')")
                }
                PgType::UUID | PgType::JSON | PgType::JSONB => {
                    format!("cast({column} as varchar)")
                }
                PgType::UUID_ARRAY | PgType::JSON_ARRAY | PgType::JSONB_ARRAY => {
                    format!("cast({column} as varchar[])")
                }
                _ => column,
            }
        }

        /// Returns DuckDB's day number for a date, using its infinity
        /// sentinels.
        fn expected_date_days(date: &Date<NaiveDate>) -> i32 {
            match date {
                Date::Value(date) => date
                    .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32,
                Date::PosInfinity => i32::MAX,
                Date::NegInfinity => -i32::MAX,
            }
        }

        /// Returns DuckDB's microsecond value for a timestamp, using its
        /// infinity sentinels.
        fn expected_timestamp_micros(micros: Timestamp<i64>) -> i64 {
            match micros {
                Timestamp::Value(micros) => micros,
                Timestamp::PosInfinity => i64::MAX,
                Timestamp::NegInfinity => -i64::MAX,
            }
        }

        /// Returns the value DuckDB must hand back for a source cell, derived
        /// from the cell's Postgres meaning rather than from any encoder.
        pub(in crate::ducklake) fn expected_value(cell: &Cell) -> Value {
            let micros = |unit_micros: i64| Value::Timestamp(TimeUnit::Microsecond, unit_micros);
            match cell {
                Cell::Null => Value::Null,
                Cell::Bool(value) => Value::Boolean(*value),
                Cell::String(value) => Value::Text(value.clone()),
                Cell::I16(value) => Value::SmallInt(*value),
                Cell::I32(value) => Value::Int(*value),
                Cell::U32(value) => Value::UBigInt(u64::from(*value)),
                Cell::I64(value) => Value::BigInt(*value),
                Cell::F32(value) => Value::Float(*value),
                Cell::F64(value) => Value::Double(*value),
                Cell::Numeric(value) => Value::Text(value.to_string()),
                Cell::Date(value) => Value::Date32(expected_date_days(value)),
                Cell::Time(PgTime::Value(value)) => Value::Time64(
                    TimeUnit::Microsecond,
                    (*value - NaiveTime::MIN).num_microseconds().unwrap(),
                ),
                Cell::Time(PgTime::EndOfDay) => {
                    Value::Time64(TimeUnit::Microsecond, MICROS_PER_DAY)
                }
                Cell::TimeTz(value) => Value::Text(value.to_string()),
                Cell::Timestamp(value) => micros(expected_timestamp_micros(match value {
                    Timestamp::Value(value) => Timestamp::Value(value.and_utc().timestamp_micros()),
                    Timestamp::PosInfinity => Timestamp::PosInfinity,
                    Timestamp::NegInfinity => Timestamp::NegInfinity,
                })),
                Cell::TimestampTz(value) => micros(expected_timestamp_micros(match value {
                    Timestamp::Value(value) => Timestamp::Value(value.timestamp_micros()),
                    Timestamp::PosInfinity => Timestamp::PosInfinity,
                    Timestamp::NegInfinity => Timestamp::NegInfinity,
                })),
                Cell::Uuid(value) => Value::Text(value.to_string()),
                Cell::Json(value) => Value::Text(value.to_string()),
                Cell::Bytes(value) => Value::Blob(value.clone()),
                Cell::Array(array) => Value::List(
                    array_elements(array).iter().map(expected_value).collect::<Vec<_>>(),
                ),
            }
        }

        /// Splits an array cell into one scalar cell per element.
        fn array_elements(array: &ArrayCell) -> Vec<Cell> {
            /// Maps optional elements to cells, turning `None` into NULL.
            fn cells<T: Clone>(values: &[Option<T>], wrap: fn(T) -> Cell) -> Vec<Cell> {
                values.iter().map(|value| value.clone().map_or(Cell::Null, wrap)).collect()
            }

            match array {
                ArrayCell::Bool(values) => cells(values, Cell::Bool),
                ArrayCell::String(values) => cells(values, Cell::String),
                ArrayCell::I16(values) => cells(values, Cell::I16),
                ArrayCell::I32(values) => cells(values, Cell::I32),
                ArrayCell::U32(values) => cells(values, Cell::U32),
                ArrayCell::I64(values) => cells(values, Cell::I64),
                ArrayCell::F32(values) => cells(values, Cell::F32),
                ArrayCell::F64(values) => cells(values, Cell::F64),
                ArrayCell::Numeric(values) => cells(values, Cell::Numeric),
                ArrayCell::Date(values) => cells(values, Cell::Date),
                ArrayCell::Time(values) => cells(values, Cell::Time),
                ArrayCell::TimeTz(values) => cells(values, Cell::TimeTz),
                ArrayCell::Timestamp(values) => cells(values, Cell::Timestamp),
                ArrayCell::TimestampTz(values) => cells(values, Cell::TimestampTz),
                ArrayCell::Uuid(values) => cells(values, Cell::Uuid),
                ArrayCell::Json(values) => cells(values, Cell::Json),
                ArrayCell::Bytes(values) => cells(values, Cell::Bytes),
            }
        }

        /// Compares a read-back value with the expected one: floats bit for
        /// bit, JSON as parsed documents, and lists element by element.
        pub(in crate::ducklake) fn values_match(
            expected: &Value,
            actual: &Value,
            is_json: bool,
        ) -> bool {
            match (expected, actual) {
                (Value::Float(expected), Value::Float(actual)) => f32_matches(*expected, *actual),
                (Value::Double(expected), Value::Double(actual)) => f64_matches(*expected, *actual),
                (Value::List(expected), Value::List(actual)) => {
                    expected.len() == actual.len()
                        && expected
                            .iter()
                            .zip(actual)
                            .all(|(expected, actual)| values_match(expected, actual, is_json))
                }
                (Value::Text(expected), Value::Text(actual)) if is_json => {
                    serde_json::from_str::<serde_json::Value>(expected).unwrap()
                        == serde_json::from_str::<serde_json::Value>(actual).unwrap()
                }
                _ => expected == actual,
            }
        }

        /// Returns the replicated schema for an `id` key plus the case column.
        fn case_schema(case: &EncoderCase) -> ReplicatedTableSchema {
            replicated_schema(vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("v".to_owned(), case.typ.clone(), case.modifier, 2, true),
            ])
        }

        /// Loads prepared rows through a temp staging table and inserts them
        /// into the destination, mirroring the production staging path.
        fn load_prepared_rows(
            conn: &duckdb::Connection,
            table_name: &DuckLakeTableName,
            prepared_rows: &PreparedRows,
        ) -> Result<(), duckdb::Error> {
            let target = crate::ducklake::sql::qualified_lake_table_name(table_name);
            conn.execute_batch(&format!(
                "create or replace temp table stage as select id, v from {target} limit 0;"
            ))?;
            match prepared_rows {
                PreparedRows::Appender(rows) => {
                    let mut appender = conn.appender("stage")?;
                    for values in rows {
                        appender.append_row(duckdb::appender_params_from_iter(values))?;
                    }
                    appender.flush()?;
                }
                PreparedRows::ArrowRecordBatch(record_batch) => {
                    let mut appender = conn.appender("stage")?;
                    appender.append_record_batch(record_batch.clone())?;
                    appender.flush()?;
                }
                PreparedRows::SqlLiterals(rows) if rows.is_empty() => {}
                PreparedRows::SqlLiterals(rows) => {
                    conn.execute_batch(&format!("insert into stage values {};", rows.join(", ")))?;
                }
            }
            conn.execute_batch(&format!(
                "insert into {target} select id, v from stage; drop table stage;"
            ))
        }

        /// Writes `rows` through one encoder path and returns the read-back
        /// `(id, value)` pairs ordered by id.
        fn write_and_read(
            conn: &duckdb::Connection,
            case: &EncoderCase,
            schema: &ReplicatedTableSchema,
            path: EncoderPath,
            rows: Vec<TableRow>,
        ) -> Result<Vec<(i32, Value)>, TestCaseError> {
            let table_name = path.table_name();
            let column_schemas = schema.column_schemas().cloned().collect::<Vec<_>>();
            conn.execute_batch(&build_create_table_sql_ducklake(&table_name, &column_schemas))
                .unwrap();
            let prepared_rows = match path {
                EncoderPath::SqlLiteral => PreparedRows::SqlLiterals(
                    rows.into_iter().map(table_row_to_sql_literal).collect(),
                ),
                EncoderPath::Appender => PreparedRows::Appender(
                    rows.into_iter()
                        .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
                        .collect(),
                ),
                EncoderPath::Copy => prepare_copy_rows(schema, rows).unwrap(),
            };
            load_prepared_rows(conn, &table_name, &prepared_rows).map_err(|error| {
                TestCaseError::fail(format!("{path:?} write of {:?} failed: {error}", case.typ))
            })?;
            let mut statement = conn
                .prepare(&format!(
                    "select id, {} from {} order by id",
                    readback_expression(&case.typ, "v"),
                    crate::ducklake::sql::qualified_lake_table_name(&table_name)
                ))
                .unwrap();
            Ok(statement
                .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, Value>(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap())
        }

        /// Checks that every encoder path stores each cell unchanged.
        fn check_encoders_agree(case: &EncoderCase) -> Result<(), TestCaseError> {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch("attach ':memory:' as lake;").unwrap();
            let schema = case_schema(case);
            let is_json = matches!(
                case.typ,
                PgType::JSON | PgType::JSONB | PgType::JSON_ARRAY | PgType::JSONB_ARRAY
            );
            let rows = case
                .cells
                .iter()
                .enumerate()
                .map(|(id, cell)| (id as i32, cell))
                .collect::<Vec<_>>();

            for path in [EncoderPath::SqlLiteral, EncoderPath::Appender, EncoderPath::Copy] {
                // Production never appends cells that need SQL literals, so the
                // appender path only sees cells `prepare_rows` would append.
                let path_rows = rows
                    .iter()
                    .filter(|(_, cell)| {
                        !matches!(path, EncoderPath::Appender) || !cell_requires_sql_literals(cell)
                    })
                    .collect::<Vec<_>>();
                let table_rows = path_rows
                    .iter()
                    .map(|(id, cell)| TableRow::new(vec![Cell::I32(*id), cell_to_owned(cell)]))
                    .collect();
                let stored = write_and_read(&conn, case, &schema, path, table_rows)?;

                prop_assert_eq!(
                    stored.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                    path_rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                    "{:?} stored a different row set",
                    path
                );
                for ((id, actual), (_, cell)) in stored.iter().zip(&path_rows) {
                    let expected = expected_value(cell);
                    prop_assert!(
                        values_match(&expected, actual, is_json),
                        "{path:?} row {id} of {:?}: wrote {cell:?}, expected {expected:?}, read \
                         {actual:?}",
                        case.typ
                    );
                }
            }
            Ok(())
        }

        #[test]
        fn sql_literal_appender_and_copy_encoders_store_the_same_value() {
            run_property("ducklake_encoders_agree", &encoder_case(), check_encoders_agree);
        }

        #[test]
        fn sql_literal_encoder_stores_floats_bit_exact() {
            let float_case = Union::new(vec![
                case_for(PgType::FLOAT4, -1, any_f32().prop_map(Cell::F32)),
                case_for(PgType::FLOAT8, -1, any_f64().prop_map(Cell::F64)),
                array_case(PgType::FLOAT4_ARRAY, any_f32(), ArrayCell::F32),
                array_case(PgType::FLOAT8_ARRAY, any_f64(), ArrayCell::F64),
            ]);
            run_property("ducklake_float_sql_literal_encoder", &float_case, check_encoders_agree);
        }

        #[test]
        fn float4_array_sql_literal_accepts_tiny_and_large_elements() {
            // As plain decimals, the tiny element types the list as a DECIMAL
            // with too few integer digits for 100000, so the insert fails.
            let case = EncoderCase {
                typ: PgType::FLOAT4_ARRAY,
                modifier: -1,
                cells: vec![Cell::Array(ArrayCell::F32(vec![
                    Some(1.234_567_8e-20),
                    Some(100_000.0),
                ]))],
            };
            check_encoders_agree(&case).unwrap();
        }
    }
}
