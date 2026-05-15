use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    error::EtlError,
    materialization::TypedCell,
    types::{
        ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM,
        TableRow, Type,
    },
};

use crate::bigquery::materialization::BigQueryMaterializer;

/// BigQuery column type used for destination schema materialization.
///
/// Some variants carry a Storage Write API encoding even though BigQuery uses a
/// single SQL type name. For example, PostgreSQL `real` and `double precision`
/// both materialize as BigQuery `FLOAT64`, but the Write API can receive them
/// as protobuf `float` and `double` respectively. Keeping that encoding here
/// preserves the source width while [`BigQueryType::to_sql`] still emits the
/// BigQuery SQL type required for DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BigQueryType {
    /// BigQuery `BOOL`.
    Bool,
    /// BigQuery `STRING`.
    String,
    /// BigQuery `INT64` plus the protobuf integer width used for row writes.
    Int64(BigQueryIntEncoding),
    /// BigQuery `FLOAT64` plus the protobuf float width used for row writes.
    Float64(BigQueryFloatEncoding),
    /// BigQuery `BIGNUMERIC`.
    BigNumeric,
    /// BigQuery `DATE`.
    Date,
    /// BigQuery `TIME`.
    Time,
    /// BigQuery `DATETIME`.
    DateTime,
    /// BigQuery `TIMESTAMP`.
    Timestamp,
    /// BigQuery `JSON`.
    Json,
    /// BigQuery `BYTES`.
    Bytes,
    /// BigQuery repeated type.
    Array(BigQueryArrayType),
}

/// BigQuery repeated column element type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BigQueryArrayType {
    /// Repeated BigQuery `BOOL`.
    Bool,
    /// Repeated BigQuery `STRING`.
    String,
    /// Repeated BigQuery `INT64`.
    Int64(BigQueryIntEncoding),
    /// Repeated BigQuery `FLOAT64`.
    Float64(BigQueryFloatEncoding),
    /// Repeated BigQuery `BIGNUMERIC`.
    BigNumeric,
    /// Repeated BigQuery `DATE`.
    Date,
    /// Repeated BigQuery `TIME`.
    Time,
    /// Repeated BigQuery `DATETIME`.
    DateTime,
    /// Repeated BigQuery `TIMESTAMP`.
    Timestamp,
    /// Repeated BigQuery `JSON`.
    Json,
    /// Repeated BigQuery `BYTES`.
    Bytes,
}

/// Storage Write API integer encoding for a BigQuery `INT64`.
///
/// BigQuery stores the column as `INT64`, but the Write API accepts narrower
/// protobuf integer fields when the source type is narrower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BigQueryIntEncoding {
    /// Encode values with protobuf `int32`.
    Int32,
    /// Encode values with protobuf `int64`.
    Int64,
}

/// Storage Write API float encoding for a BigQuery `FLOAT64`.
///
/// BigQuery stores both PostgreSQL `real` and `double precision` as `FLOAT64`.
/// The Write API descriptor still uses protobuf `float` for PostgreSQL
/// `real`, and protobuf `double` for PostgreSQL `double precision`, matching
/// the source value's precision and the existing main-branch wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BigQueryFloatEncoding {
    /// Encode values with protobuf `float`.
    Float,
    /// Encode values with protobuf `double`.
    Double,
}

impl BigQueryType {
    /// Returns the native BigQuery type for a source type.
    pub(super) fn native_for_source_type(typ: &Type) -> Self {
        match *typ {
            Type::BOOL => Self::Bool,
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => Self::String,
            Type::INT2 | Type::INT4 => Self::Int64(BigQueryIntEncoding::Int32),
            Type::INT8 | Type::OID => Self::Int64(BigQueryIntEncoding::Int64),
            // BigQuery has only `FLOAT64`, but the Storage Write API can use
            // either protobuf `float` or `double` as the incoming field type.
            Type::FLOAT4 => Self::Float64(BigQueryFloatEncoding::Float),
            Type::FLOAT8 => Self::Float64(BigQueryFloatEncoding::Double),
            Type::NUMERIC => Self::BigNumeric,
            Type::MONEY => Self::String,
            Type::DATE => Self::Date,
            Type::TIME => Self::Time,
            Type::TIMESTAMP => Self::DateTime,
            Type::TIMESTAMPTZ => Self::Timestamp,
            Type::UUID => Self::String,
            Type::JSON | Type::JSONB => Self::Json,
            Type::BYTEA => Self::Bytes,
            Type::BOOL_ARRAY => Self::Array(BigQueryArrayType::Bool),
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => Self::Array(BigQueryArrayType::String),
            Type::INT2_ARRAY | Type::INT4_ARRAY => {
                Self::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int32))
            }
            Type::INT8_ARRAY | Type::OID_ARRAY => {
                Self::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int64))
            }
            Type::FLOAT4_ARRAY => {
                Self::Array(BigQueryArrayType::Float64(BigQueryFloatEncoding::Float))
            }
            Type::FLOAT8_ARRAY => {
                Self::Array(BigQueryArrayType::Float64(BigQueryFloatEncoding::Double))
            }
            Type::NUMERIC_ARRAY => Self::Array(BigQueryArrayType::BigNumeric),
            Type::MONEY_ARRAY => Self::Array(BigQueryArrayType::String),
            Type::DATE_ARRAY => Self::Array(BigQueryArrayType::Date),
            Type::TIME_ARRAY => Self::Array(BigQueryArrayType::Time),
            Type::TIMESTAMP_ARRAY => Self::Array(BigQueryArrayType::DateTime),
            Type::TIMESTAMPTZ_ARRAY => Self::Array(BigQueryArrayType::Timestamp),
            Type::UUID_ARRAY => Self::Array(BigQueryArrayType::String),
            Type::JSON_ARRAY | Type::JSONB_ARRAY => Self::Array(BigQueryArrayType::Json),
            Type::BYTEA_ARRAY => Self::Array(BigQueryArrayType::Bytes),
            _ => Self::String,
        }
    }

    /// Returns `true` when this is a repeated BigQuery type.
    pub(super) const fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    /// Returns this type as a BigQuery SQL fragment.
    pub(super) fn to_sql(&self) -> String {
        match self {
            Self::Bool => "bool".to_owned(),
            Self::String => "string".to_owned(),
            Self::Int64(_) => "int64".to_owned(),
            Self::Float64(_) => "float64".to_owned(),
            Self::BigNumeric => "bignumeric".to_owned(),
            Self::Date => "date".to_owned(),
            Self::Time => "time".to_owned(),
            Self::DateTime => "datetime".to_owned(),
            Self::Timestamp => "timestamp".to_owned(),
            Self::Json => "json".to_owned(),
            Self::Bytes => "bytes".to_owned(),
            Self::Array(element_type) => format!("array<{}>", element_type.to_sql()),
        }
    }
}

impl BigQueryArrayType {
    /// Returns this repeated element type as a BigQuery SQL fragment.
    fn to_sql(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::String => "string",
            Self::Int64(_) => "int64",
            Self::Float64(_) => "float64",
            Self::BigNumeric => "bignumeric",
            Self::Date => "date",
            Self::Time => "time",
            Self::DateTime => "datetime",
            Self::Timestamp => "timestamp",
            Self::Json => "json",
            Self::Bytes => "bytes",
        }
    }
}

impl fmt::Display for BigQueryType {
    /// Formats this type as a BigQuery SQL fragment.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_sql())
    }
}

/// Protocol buffer wrapper for a BigQuery table row.
///
/// Materialization happens before this wrapper is built, so encoding
/// can assume array elements are valid for BigQuery.
#[derive(Debug)]
pub(super) struct BigQueryTableRow(Vec<(u32, BigQueryCell)>);

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts an empty [`TableRow`] to a [`BigQueryTableRow`].
    ///
    /// Non-empty rows need source types and must use
    /// [`BigQueryTableRow::try_from_typed_tagged_cells`].
    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        if value.values().is_empty() {
            Ok(BigQueryTableRow(Vec::new()))
        } else {
            Err(etl::etl_error!(
                etl::error::ErrorKind::InvalidState,
                "BigQuery rows require source types for materialization"
            ))
        }
    }
}

impl BigQueryTableRow {
    /// Converts typed cells into a BigQuery row with a materialization policy.
    #[cfg(test)]
    pub(super) fn try_from_typed_cells(
        typed_cells: impl IntoIterator<Item = (Type, Cell)>,
        materializer: &BigQueryMaterializer,
    ) -> Result<Self, EtlError> {
        Self::try_from_typed_tagged_cells(
            typed_cells.into_iter().enumerate().map(|(index, (typ, cell))| (index + 1, typ, cell)),
            materializer,
        )
    }

    /// Converts typed tagged cells into a BigQuery row with a materialization
    /// policy.
    pub(super) fn try_from_typed_tagged_cells(
        tagged_cells: impl IntoIterator<Item = (usize, Type, Cell)>,
        materializer: &BigQueryMaterializer,
    ) -> Result<Self, EtlError> {
        let cells = materializer
            .materialize_cells(
                tagged_cells
                    .into_iter()
                    .map(|(index, typ, cell)| TypedCell::new(typ, cell, index as u32)),
            )?
            .into_iter()
            .map(|cell| {
                let (_typ, cell, index) = cell.into_components();
                (index, cell)
            })
            .collect();

        Ok(BigQueryTableRow(cells))
    }

    /// Returns the tagged cells.
    pub(super) fn cells(&self) -> &[(u32, BigQueryCell)] {
        &self.0
    }

    /// Returns the mutable tagged cells.
    pub(super) fn cells_mut(&mut self) -> &mut [(u32, BigQueryCell)] {
        &mut self.0
    }

    /// Returns the tagged cells for assertions in tests.
    #[cfg(test)]
    pub(super) fn debug_cells(&self) -> &[(u32, BigQueryCell)] {
        self.cells()
    }
}

/// BigQuery materialized cell ready for Storage Write API encoding.
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

/// BigQuery repeated field values ready for Storage Write API encoding.
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
    /// Converts an ETL cell that is already materialized for BigQuery into a
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
    /// Converts an ETL array that is already materialized for BigQuery into a
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

/// Returns array values after materialization validation has removed `NULL`s.
fn required_values<T>(values: Vec<Option<T>>) -> Vec<T> {
    values
        .into_iter()
        .map(|value| {
            debug_assert!(value.is_some(), "BigQuery materialization rejects null array elements");
            value.expect("BigQuery materialization rejects null array elements")
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
