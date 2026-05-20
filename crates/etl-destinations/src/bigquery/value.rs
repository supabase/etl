use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    materialization::TypedCell,
    types::{
        ArrayCell, Cell, DATE_FORMAT, PgDate, PgTime, PgTimestamp, PgTimestampTz, TIME_FORMAT,
        TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM, TableRow, Type, is_array_type,
    },
};
use prost::bytes;

use crate::bigquery::materialization::BigQueryMaterializer;

/// BigQuery column type used for destination schema materialization.
///
/// Some variants carry a Storage Write API encoding even though BigQuery uses a
/// single SQL type name. For example, PostgreSQL `real` and `double precision`
/// both materialize as BigQuery `FLOAT64`, but the Write API can receive them
/// as protobuf `float` and `double` respectively. Keeping that encoding here
/// preserves the source width while [`BigQueryType::to_sql`] still emits the
/// BigQuery SQL type required for DDL.
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
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
            _ if is_array_type(typ) => Self::Array(BigQueryArrayType::String),
            _ => Self::String,
        }
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
                    .map(|(index, typ, cell)| (index as u32, TypedCell::new(typ, cell))),
            )?
            .into_iter()
            .map(|(index, typed_cell)| {
                let (_typ, cell) = typed_cell.into_parts();
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

impl prost::Message for BigQueryTableRow {
    /// Encodes the table row into the provided buffer using Protocol Buffer
    /// format.
    ///
    /// Each cell is encoded with the field tag stored alongside it, using the
    /// appropriate prost encoding method for the cell's data type.
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        for (tag, cell) in self.cells() {
            cell_encode_prost(cell, *tag, buf);
        }
    }

    /// Merges a field from a Protocol Buffer message into this table row.
    ///
    /// Decoding is not used for BigQuery streaming inserts, so incoming fields
    /// are skipped instead of decoded into row cells.
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        prost::encoding::skip_field(wire_type, tag, buf, ctx)
    }

    /// Calculates the encoded length of the table row in bytes.
    ///
    /// Sums the encoded lengths of all cells with their respective tags to
    /// determine the total serialized size.
    fn encoded_len(&self) -> usize {
        let mut len = 0;
        for (tag, cell) in self.cells() {
            len += cell_encode_len_prost(cell, *tag);
        }

        len
    }

    /// Clears all cell values in the table row by calling clear on each cell.
    fn clear(&mut self) {
        for (_, cell) in self.cells_mut() {
            cell.clear();
        }
    }
}

/// BigQuery materialized cell ready for Storage Write API encoding.
#[derive(Debug, PartialEq)]
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
#[derive(Debug, PartialEq)]
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
    pub(super) fn try_from_native_cell(cell: Cell) -> EtlResult<Self> {
        match cell {
            Cell::Null => Ok(Self::Null),
            Cell::Bool(value) => Ok(Self::Bool(value)),
            Cell::String(value) => Ok(Self::String(value)),
            Cell::I16(value) => Ok(Self::Int32(i32::from(value))),
            Cell::I32(value) => Ok(Self::Int32(value)),
            Cell::I64(value) => Ok(Self::Int64(value)),
            Cell::F32(value) => Ok(Self::Float32(value)),
            Cell::F64(value) => Ok(Self::Float64(value)),
            Cell::Numeric(value) => Ok(Self::String(value.to_string())),
            Cell::Date(value) => Ok(Self::String(format_pg_date(value))),
            Cell::Time(value) => Ok(Self::String(format_pg_time(value))),
            Cell::Timestamp(value) => Ok(Self::String(format_pg_timestamp(value))),
            Cell::TimestampTz(value) => Ok(Self::String(format_pg_timestamptz(value))),
            Cell::Uuid(value) => Ok(Self::String(value.to_string())),
            Cell::U32(value) => Ok(Self::Int64(i64::from(value))),
            Cell::Bytes(value) => Ok(Self::Bytes(value)),
            Cell::Array(value) => BigQueryArrayCell::try_from_native_array(value).map(Self::Array),
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
    pub(super) fn try_from_native_array(array: ArrayCell) -> EtlResult<Self> {
        match array {
            ArrayCell::Bool(values) => Ok(Self::Bool(required_values(values)?)),
            ArrayCell::String(values) => Ok(Self::String(required_values(values)?)),
            ArrayCell::I16(values) => {
                Ok(Self::Int32(required_values(values)?.into_iter().map(i32::from).collect()))
            }
            ArrayCell::I32(values) => Ok(Self::Int32(required_values(values)?)),
            ArrayCell::U32(values) => {
                Ok(Self::Int64(required_values(values)?.into_iter().map(i64::from).collect()))
            }
            ArrayCell::I64(values) => Ok(Self::Int64(required_values(values)?)),
            ArrayCell::F32(values) => Ok(Self::Float32(required_values(values)?)),
            ArrayCell::F64(values) => Ok(Self::Float64(required_values(values)?)),
            ArrayCell::Numeric(values) => Ok(Self::String(
                required_values(values)?.into_iter().map(|value| value.to_string()).collect(),
            )),
            ArrayCell::Date(values) => {
                Ok(Self::String(required_values(values)?.into_iter().map(format_pg_date).collect()))
            }
            ArrayCell::Time(values) => {
                Ok(Self::String(required_values(values)?.into_iter().map(format_pg_time).collect()))
            }
            ArrayCell::Timestamp(values) => Ok(Self::String(
                required_values(values)?.into_iter().map(format_pg_timestamp).collect(),
            )),
            ArrayCell::TimestampTz(values) => Ok(Self::String(
                required_values(values)?.into_iter().map(format_pg_timestamptz).collect(),
            )),
            ArrayCell::Uuid(values) => Ok(Self::String(
                required_values(values)?.into_iter().map(|value| value.to_string()).collect(),
            )),
            ArrayCell::Bytes(values) => Ok(Self::Bytes(required_values(values)?)),
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

/// Encodes a single [`BigQueryCell`] into Protocol Buffer format.
fn cell_encode_prost(cell: &BigQueryCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match cell {
        BigQueryCell::Null => {}
        BigQueryCell::Bool(value) => prost::encoding::bool::encode(tag, value, buf),
        BigQueryCell::String(value) => prost::encoding::string::encode(tag, value, buf),
        BigQueryCell::Int32(value) => prost::encoding::int32::encode(tag, value, buf),
        BigQueryCell::Int64(value) => prost::encoding::int64::encode(tag, value, buf),
        BigQueryCell::Float32(value) => prost::encoding::float::encode(tag, value, buf),
        BigQueryCell::Float64(value) => prost::encoding::double::encode(tag, value, buf),
        BigQueryCell::Bytes(value) => prost::encoding::bytes::encode(tag, value, buf),
        BigQueryCell::Array(value) => array_cell_encode_prost(value, tag, buf),
    }
}

/// Calculates the encoded length for a single [`BigQueryCell`].
fn cell_encode_len_prost(cell: &BigQueryCell, tag: u32) -> usize {
    match cell {
        BigQueryCell::Null => 0,
        BigQueryCell::Bool(value) => prost::encoding::bool::encoded_len(tag, value),
        BigQueryCell::String(value) => prost::encoding::string::encoded_len(tag, value),
        BigQueryCell::Int32(value) => prost::encoding::int32::encoded_len(tag, value),
        BigQueryCell::Int64(value) => prost::encoding::int64::encoded_len(tag, value),
        BigQueryCell::Float32(value) => prost::encoding::float::encoded_len(tag, value),
        BigQueryCell::Float64(value) => prost::encoding::double::encoded_len(tag, value),
        BigQueryCell::Bytes(value) => prost::encoding::bytes::encoded_len(tag, value),
        BigQueryCell::Array(value) => array_cell_encoded_len_prost(value, tag),
    }
}

/// Encodes a [`BigQueryArrayCell`] into Protocol Buffer format.
fn array_cell_encode_prost(array_cell: &BigQueryArrayCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match array_cell {
        BigQueryArrayCell::Bool(values) => prost::encoding::bool::encode_packed(tag, values, buf),
        BigQueryArrayCell::String(values) => {
            prost::encoding::string::encode_repeated(tag, values, buf);
        }
        BigQueryArrayCell::Int32(values) => {
            prost::encoding::int32::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Int64(values) => {
            prost::encoding::int64::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Float32(values) => {
            prost::encoding::float::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Float64(values) => {
            prost::encoding::double::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Bytes(values) => {
            prost::encoding::bytes::encode_repeated(tag, values, buf);
        }
    }
}

/// Calculates the encoded length for a [`BigQueryArrayCell`].
fn array_cell_encoded_len_prost(array_cell: &BigQueryArrayCell, tag: u32) -> usize {
    match array_cell {
        BigQueryArrayCell::Bool(values) => prost::encoding::bool::encoded_len_packed(tag, values),
        BigQueryArrayCell::String(values) => {
            prost::encoding::string::encoded_len_repeated(tag, values)
        }
        BigQueryArrayCell::Int32(values) => prost::encoding::int32::encoded_len_packed(tag, values),
        BigQueryArrayCell::Int64(values) => prost::encoding::int64::encoded_len_packed(tag, values),
        BigQueryArrayCell::Float32(values) => {
            prost::encoding::float::encoded_len_packed(tag, values)
        }
        BigQueryArrayCell::Float64(values) => {
            prost::encoding::double::encoded_len_packed(tag, values)
        }
        BigQueryArrayCell::Bytes(values) => {
            prost::encoding::bytes::encoded_len_repeated(tag, values)
        }
    }
}

/// Returns array values after materialization validation has removed `NULL`s.
fn required_values<T>(values: Vec<Option<T>>) -> EtlResult<Vec<T>> {
    let mut required = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => required.push(value),
            None => {
                return Err(etl_error!(
                    ErrorKind::NullValuesNotSupportedInArrayInDestination,
                    "Array contains NULL value",
                    format!("Element at index {index} is NULL, which is not supported in BigQuery")
                ));
            }
        }
    }

    Ok(required)
}

/// Formats a BigQuery date string.
fn format_date(value: NaiveDate) -> String {
    value.format(DATE_FORMAT).to_string()
}

/// Formats a PostgreSQL date for BigQuery string-backed encodings.
fn format_pg_date(value: PgDate) -> String {
    match value {
        PgDate::Finite(value) => format_date(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery time string.
fn format_time(value: NaiveTime) -> String {
    value.format(TIME_FORMAT).to_string()
}

/// Formats a PostgreSQL time for BigQuery string-backed encodings.
fn format_pg_time(value: PgTime) -> String {
    match value {
        PgTime::Finite(value) => format_time(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery datetime string.
fn format_timestamp(value: NaiveDateTime) -> String {
    value.format(TIMESTAMP_FORMAT).to_string()
}

/// Formats a PostgreSQL timestamp for BigQuery string-backed encodings.
fn format_pg_timestamp(value: PgTimestamp) -> String {
    match value {
        PgTimestamp::Finite(value) => format_timestamp(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery timestamp string.
fn format_timestamptz(value: DateTime<Utc>) -> String {
    value.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string()
}

/// Formats a PostgreSQL timestamptz for BigQuery string-backed encodings.
fn format_pg_timestamptz(value: PgTimestampTz) -> String {
    match value {
        PgTimestampTz::Finite(value) => format_timestamptz(value),
        value => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use etl::{
        error::{ErrorKind, EtlError},
        materialization::DestinationTypeCompatibility,
        types::{Cell, PgNumeric, Type},
    };

    use super::*;
    use crate::bigquery::materialization::BigQueryMaterialization;

    fn typed_row(
        cells: impl IntoIterator<Item = (Type, Cell)>,
        compatibility: DestinationTypeCompatibility,
    ) -> Result<BigQueryTableRow, EtlError> {
        let materializer = BigQueryMaterialization::materializer(compatibility);
        BigQueryTableRow::try_from_typed_cells(cells, &materializer)
    }

    #[test]
    fn bigquery_table_row_try_from_valid() {
        let result = typed_row(
            [
                (Type::INT4, Cell::I32(42)),
                (Type::TEXT, Cell::String("test".to_owned())),
                (Type::BOOL, Cell::Bool(true)),
                (Type::TEXT, Cell::Null),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_nan() {
        let result = typed_row(
            [(Type::INT4, Cell::I32(42)), (Type::NUMERIC, Cell::Numeric(PgNumeric::NaN))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_infinity() {
        let result = typed_row(
            [
                (Type::TEXT, Cell::String("valid".to_owned())),
                (Type::NUMERIC, Cell::Numeric(PgNumeric::PositiveInfinity)),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_number_outside_float_domain() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [(1, Type::JSON, Cell::String(r#"{"value":1e309}"#.to_owned()))],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::strict()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_integer_precision_loss() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [(1, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned()))],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::strict()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_preserve_accepts_string_materialized_values() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [
                (1, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
                (
                    2,
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
            ],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::preserve()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_date_outside_bigquery_domain() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();

        let result = typed_row(
            [(Type::DATE, Cell::Date(invalid_date.into()))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_nulls() {
        let array_with_nulls = ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let result = typed_row(
            [(Type::INT4_ARRAY, Cell::Array(array_with_nulls))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0 failed materialization"));
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_numeric_rounding_risk() {
        let array_with_rounding_risk = ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let result = typed_row(
            [(Type::NUMERIC_ARRAY, Cell::Array(array_with_rounding_risk))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn bigquery_table_row_try_from_valid_array() {
        let valid_array = ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let result = typed_row(
            [
                (Type::TEXT, Cell::String("prefix".to_owned())),
                (Type::INT4_ARRAY, Cell::Array(valid_array)),
                (Type::TEXT, Cell::String("suffix".to_owned())),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_multiple_errors_first_wins() {
        let result = typed_row(
            [
                (
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
                (
                    Type::NUMERIC_ARRAY,
                    Cell::Array(ArrayCell::Numeric(vec![Some(
                        PgNumeric::from_str("0.000000000000000000000000000000000000002").unwrap(),
                    )])),
                ),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn bigquery_table_row_try_from_valid_temporal_values() {
        let valid_date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let valid_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let valid_datetime = NaiveDateTime::new(valid_date, valid_time);

        let result = typed_row(
            [
                (Type::DATE, Cell::Date(valid_date.into())),
                (Type::TIME, Cell::Time(valid_time.into())),
                (Type::TIMESTAMP, Cell::Timestamp(valid_datetime.into())),
            ],
            DestinationTypeCompatibility::default(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_numeric_rounding_risk_fails() {
        let over_scale_numeric =
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();

        let result = typed_row(
            [(Type::NUMERIC, Cell::Numeric(over_scale_numeric))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn bigquery_table_row_try_from_uses_coerce_materialization_when_configured() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [
                (
                    1,
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
                (2, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
                (3, Type::FLOAT8, Cell::F64(-0.0)),
            ],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::coerce()),
        );

        assert!(result.is_ok());
    }
}
