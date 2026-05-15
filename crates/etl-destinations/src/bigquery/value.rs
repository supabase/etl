use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{
    ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM,
};

/// BigQuery-compatible cell ready for Storage Write API encoding.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum BigQueryCell {
    /// SQL `NULL`.
    Null,
    /// BigQuery `BOOL`.
    Bool(bool),
    /// String-backed BigQuery values.
    String(String),
    /// BigQuery `INT64` carried by an `int32` proto field.
    Int32(i32),
    /// BigQuery `INT64` carried by an `int64` proto field.
    Int64(i64),
    /// BigQuery `FLOAT64` carried by a `float` proto field.
    Float32(f32),
    /// BigQuery `FLOAT64` carried by a `double` proto field.
    Float64(f64),
    /// BigQuery `BYTES`.
    Bytes(Vec<u8>),
    /// BigQuery repeated field.
    Array(BigQueryArrayCell),
}

/// BigQuery-compatible repeated field values.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum BigQueryArrayCell {
    /// Repeated `BOOL`.
    Bool(Vec<bool>),
    /// Repeated string-backed values.
    String(Vec<String>),
    /// Repeated `INT64` carried by `int32` proto values.
    Int32(Vec<i32>),
    /// Repeated `INT64` carried by `int64` proto values.
    Int64(Vec<i64>),
    /// Repeated `FLOAT64` carried by `float` proto values.
    Float32(Vec<f32>),
    /// Repeated `FLOAT64` carried by `double` proto values.
    Float64(Vec<f64>),
    /// Repeated `BYTES`.
    Bytes(Vec<Vec<u8>>),
}

impl BigQueryCell {
    /// Converts an ETL cell that is already BigQuery-compatible into a
    /// BigQuery-specific cell.
    pub(super) fn from_native_cell(cell: Cell) -> Self {
        match cell {
            Cell::Null => Self::Null,
            Cell::Bool(value) => Self::Bool(value),
            Cell::String(value) => Self::String(value),
            Cell::I16(value) => Self::Int32(i32::from(value)),
            Cell::I32(value) => Self::Int32(value),
            Cell::I64(value) => Self::Int64(value),
            Cell::F32(value) => Self::Float32(value),
            Cell::F64(value) => Self::Float64(value),
            Cell::Numeric(value) => Self::String(value.to_string()),
            Cell::Date(value) => Self::String(format_date(value)),
            Cell::Time(value) => Self::String(format_time(value)),
            Cell::Timestamp(value) => Self::String(format_timestamp(value)),
            Cell::TimestampTz(value) => Self::String(format_timestamptz(value)),
            Cell::Uuid(value) => Self::String(value.to_string()),
            Cell::U32(value) => Self::Int64(i64::from(value)),
            Cell::Bytes(value) => Self::Bytes(value),
            Cell::Array(value) => Self::Array(BigQueryArrayCell::from_native_array(value)),
        }
    }

    /// Returns a BigQuery scalar string cell.
    pub(super) fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    /// Clears the cell contents.
    pub(super) fn clear(&mut self) {
        match self {
            Self::Null
            | Self::Bool(_)
            | Self::Int32(_)
            | Self::Int64(_)
            | Self::Float32(_)
            | Self::Float64(_) => {}
            Self::String(value) => value.clear(),
            Self::Bytes(value) => value.clear(),
            Self::Array(value) => value.clear(),
        }
    }
}

impl BigQueryArrayCell {
    /// Converts an ETL array that is already BigQuery-compatible into a
    /// BigQuery-specific repeated field.
    pub(super) fn from_native_array(array: ArrayCell) -> Self {
        match array {
            ArrayCell::Bool(values) => Self::Bool(required_values(values)),
            ArrayCell::String(values) => Self::String(required_values(values)),
            ArrayCell::I16(values) => {
                Self::Int32(required_values(values).into_iter().map(i32::from).collect())
            }
            ArrayCell::I32(values) => Self::Int32(required_values(values)),
            ArrayCell::U32(values) => {
                Self::Int64(required_values(values).into_iter().map(i64::from).collect())
            }
            ArrayCell::I64(values) => Self::Int64(required_values(values)),
            ArrayCell::F32(values) => Self::Float32(required_values(values)),
            ArrayCell::F64(values) => Self::Float64(required_values(values)),
            ArrayCell::Numeric(values) => Self::String(
                required_values(values).into_iter().map(|value| value.to_string()).collect(),
            ),
            ArrayCell::Date(values) => {
                Self::String(required_values(values).into_iter().map(format_date).collect())
            }
            ArrayCell::Time(values) => {
                Self::String(required_values(values).into_iter().map(format_time).collect())
            }
            ArrayCell::Timestamp(values) => {
                Self::String(required_values(values).into_iter().map(format_timestamp).collect())
            }
            ArrayCell::TimestampTz(values) => {
                Self::String(required_values(values).into_iter().map(format_timestamptz).collect())
            }
            ArrayCell::Uuid(values) => Self::String(
                required_values(values).into_iter().map(|value| value.to_string()).collect(),
            ),
            ArrayCell::Bytes(values) => Self::Bytes(required_values(values)),
        }
    }

    /// Clears the repeated field contents.
    fn clear(&mut self) {
        match self {
            Self::Bool(values) => values.clear(),
            Self::String(values) => values.clear(),
            Self::Int32(values) => values.clear(),
            Self::Int64(values) => values.clear(),
            Self::Float32(values) => values.clear(),
            Self::Float64(values) => values.clear(),
            Self::Bytes(values) => values.clear(),
        }
    }
}

/// Returns array values after compatibility validation has removed `NULL`s.
fn required_values<T>(values: Vec<Option<T>>) -> Vec<T> {
    values
        .into_iter()
        .map(|value| {
            debug_assert!(value.is_some(), "BigQuery compatibility rejects null array elements");
            value.expect("BigQuery compatibility rejects null array elements")
        })
        .collect()
}

/// Formats a BigQuery date string.
fn format_date(value: NaiveDate) -> String {
    value.format(DATE_FORMAT).to_string()
}

/// Formats a BigQuery time string.
fn format_time(value: NaiveTime) -> String {
    value.format(TIME_FORMAT).to_string()
}

/// Formats a BigQuery datetime string.
fn format_timestamp(value: NaiveDateTime) -> String {
    value.format(TIMESTAMP_FORMAT).to_string()
}

/// Formats a BigQuery timestamp string.
fn format_timestamptz(value: DateTime<Utc>) -> String {
    value.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string()
}
