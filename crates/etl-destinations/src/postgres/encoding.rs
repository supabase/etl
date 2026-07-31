//! Cell encoding for Postgres destination writes.

use std::error::Error;

use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    data::{ArrayCell, Cell, PgNumeric},
    error::{ErrorKind, EtlResult},
    etl_error,
};
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use uuid::Uuid;

/// A Postgres-bound value converted from an ETL [`Cell`].
#[derive(Debug, Clone)]
pub(crate) enum PostgresValue {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    /// Source `timetz` rendered as text; destination DDL maps `timetz` to
    /// `text`.
    TimeTz(String),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    BoolArray(Vec<Option<bool>>),
    StringArray(Vec<Option<String>>),
    I16Array(Vec<Option<i16>>),
    I32Array(Vec<Option<i32>>),
    U32Array(Vec<Option<u32>>),
    I64Array(Vec<Option<i64>>),
    F32Array(Vec<Option<f32>>),
    F64Array(Vec<Option<f64>>),
    NumericArray(Vec<Option<PgNumeric>>),
    DateArray(Vec<Option<NaiveDate>>),
    TimeArray(Vec<Option<NaiveTime>>),
    TimeTzArray(Vec<Option<String>>),
    TimestampArray(Vec<Option<NaiveDateTime>>),
    TimestampTzArray(Vec<Option<DateTime<Utc>>>),
    UuidArray(Vec<Option<Uuid>>),
    JsonArray(Vec<Option<serde_json::Value>>),
    BytesArray(Vec<Option<Vec<u8>>>),
}

impl PostgresValue {
    /// Converts an ETL cell into a bindable Postgres value.
    pub(crate) fn from_cell(cell: Cell) -> EtlResult<Self> {
        Ok(match cell {
            Cell::Null => Self::Null,
            Cell::Bool(value) => Self::Bool(value),
            Cell::String(value) => Self::String(value),
            Cell::I16(value) => Self::I16(value),
            Cell::I32(value) => Self::I32(value),
            Cell::U32(value) => Self::U32(value),
            Cell::I64(value) => Self::I64(value),
            Cell::F32(value) => Self::F32(value),
            Cell::F64(value) => Self::F64(value),
            Cell::Numeric(value) => Self::Numeric(value),
            Cell::Date(value) => Self::Date(value),
            Cell::Time(value) => Self::Time(value),
            Cell::TimeTz(value) => Self::TimeTz(value.to_string()),
            Cell::Timestamp(value) => Self::Timestamp(value),
            Cell::TimestampTz(value) => Self::TimestampTz(value),
            Cell::Uuid(value) => Self::Uuid(value),
            Cell::Json(value) => Self::Json(value),
            Cell::Bytes(value) => Self::Bytes(value),
            Cell::Array(array) => Self::from_array_cell(array)?,
        })
    }

    fn from_array_cell(array: ArrayCell) -> EtlResult<Self> {
        Ok(match array {
            ArrayCell::Bool(values) => Self::BoolArray(values),
            ArrayCell::String(values) => Self::StringArray(values),
            ArrayCell::I16(values) => Self::I16Array(values),
            ArrayCell::I32(values) => Self::I32Array(values),
            ArrayCell::U32(values) => Self::U32Array(values),
            ArrayCell::I64(values) => Self::I64Array(values),
            ArrayCell::F32(values) => Self::F32Array(values),
            ArrayCell::F64(values) => Self::F64Array(values),
            ArrayCell::Numeric(values) => Self::NumericArray(values),
            ArrayCell::Date(values) => Self::DateArray(values),
            ArrayCell::Time(values) => Self::TimeArray(values),
            ArrayCell::TimeTz(values) => Self::TimeTzArray(
                values.into_iter().map(|value| value.map(|inner| inner.to_string())).collect(),
            ),
            ArrayCell::Timestamp(values) => Self::TimestampArray(values),
            ArrayCell::TimestampTz(values) => Self::TimestampTzArray(values),
            ArrayCell::Uuid(values) => Self::UuidArray(values),
            ArrayCell::Json(values) => Self::JsonArray(values),
            ArrayCell::Bytes(values) => Self::BytesArray(values),
        })
    }
}

impl ToSql for PostgresValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Bool(value) => value.to_sql(ty, out),
            Self::String(value) => value.to_sql(ty, out),
            Self::I16(value) => value.to_sql(ty, out),
            Self::I32(value) => value.to_sql(ty, out),
            Self::U32(value) => value.to_sql(ty, out),
            Self::I64(value) => value.to_sql(ty, out),
            Self::F32(value) => value.to_sql(ty, out),
            Self::F64(value) => value.to_sql(ty, out),
            Self::Numeric(value) => value.to_sql(ty, out),
            Self::Date(value) => value.to_sql(ty, out),
            Self::Time(value) => value.to_sql(ty, out),
            Self::TimeTz(value) => value.to_sql(ty, out),
            Self::Timestamp(value) => value.to_sql(ty, out),
            Self::TimestampTz(value) => value.to_sql(ty, out),
            Self::Uuid(value) => value.to_sql(ty, out),
            Self::Json(value) => value.to_sql(ty, out),
            Self::Bytes(value) => value.to_sql(ty, out),
            Self::BoolArray(values) => values.to_sql(ty, out),
            Self::StringArray(values) => values.to_sql(ty, out),
            Self::I16Array(values) => values.to_sql(ty, out),
            Self::I32Array(values) => values.to_sql(ty, out),
            Self::U32Array(values) => values.to_sql(ty, out),
            Self::I64Array(values) => values.to_sql(ty, out),
            Self::F32Array(values) => values.to_sql(ty, out),
            Self::F64Array(values) => values.to_sql(ty, out),
            Self::NumericArray(values) => values.to_sql(ty, out),
            Self::DateArray(values) => values.to_sql(ty, out),
            Self::TimeArray(values) => values.to_sql(ty, out),
            Self::TimeTzArray(values) => values.to_sql(ty, out),
            Self::TimestampArray(values) => values.to_sql(ty, out),
            Self::TimestampTzArray(values) => values.to_sql(ty, out),
            Self::UuidArray(values) => values.to_sql(ty, out),
            Self::JsonArray(values) => values.to_sql(ty, out),
            Self::BytesArray(values) => values.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        // Destination DDL maps source `timetz` / `timetz[]` columns to `text` /
        // `text[]`, so reject native timetz bind targets.
        !matches!(*ty, Type::TIMETZ | Type::TIMETZ_ARRAY)
    }

    to_sql_checked!();
}

/// Converts a row of cells into Postgres bind values.
pub(crate) fn cells_to_postgres_values(cells: Vec<Cell>) -> EtlResult<Vec<PostgresValue>> {
    cells.into_iter().map(PostgresValue::from_cell).collect()
}

/// Converts cells into trait-object references for `tokio-postgres` binds.
pub(crate) fn values_as_tosql_params(values: &[PostgresValue]) -> Vec<&(dyn ToSql + Sync)> {
    values.iter().map(|value| value as &(dyn ToSql + Sync)).collect()
}

/// Maps a destination client/query failure into an [`etl::error::EtlError`].
pub(crate) fn map_postgres_error(
    error: tokio_postgres::Error,
    description: &'static str,
) -> etl::error::EtlError {
    etl_error!(ErrorKind::DestinationQueryFailed, description, source: error)
}
