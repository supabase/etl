use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use numeric::PgNumeric;
use std::fmt::Debug;
use uuid::Uuid;

use crate::bail;
use crate::error::ErrorKind;
use crate::error::EtlError;

pub mod bool;
pub mod event;
pub mod hex;
pub mod numeric;
pub mod table_row;
pub mod text;

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
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
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Array(ArrayCell),
}

impl Cell {
    pub fn clear(&mut self) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => *b = false,
            Cell::String(s) => s.clear(),
            Cell::I16(i) => *i = 0,
            Cell::I32(i) => *i = 0,
            Cell::I64(i) => *i = 0,
            Cell::F32(i) => *i = 0.,
            Cell::F64(i) => *i = 0.,
            Cell::Numeric(n) => *n = PgNumeric::default(),
            Cell::Date(t) => *t = NaiveDate::default(),
            Cell::Time(t) => *t = NaiveTime::default(),
            Cell::TimeStamp(t) => *t = NaiveDateTime::default(),
            Cell::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
            Cell::Uuid(u) => *u = Uuid::default(),
            Cell::Json(j) => *j = serde_json::Value::default(),
            Cell::U32(u) => *u = 0,
            Cell::Bytes(b) => b.clear(),
            Cell::Array(vec) => {
                vec.clear();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCell {
    Null,
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    U32(Vec<Option<u32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<PgNumeric>>),
    Date(Vec<Option<NaiveDate>>),
    Time(Vec<Option<NaiveTime>>),
    TimeStamp(Vec<Option<NaiveDateTime>>),
    TimeStampTz(Vec<Option<DateTime<Utc>>>),
    Uuid(Vec<Option<Uuid>>),
    Json(Vec<Option<serde_json::Value>>),
    Bytes(Vec<Option<Vec<u8>>>),
}

impl ArrayCell {
    fn clear(&mut self) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(vec) => vec.clear(),
            ArrayCell::String(vec) => vec.clear(),
            ArrayCell::I16(vec) => vec.clear(),
            ArrayCell::I32(vec) => vec.clear(),
            ArrayCell::U32(vec) => vec.clear(),
            ArrayCell::I64(vec) => vec.clear(),
            ArrayCell::F32(vec) => vec.clear(),
            ArrayCell::F64(vec) => vec.clear(),
            ArrayCell::Numeric(vec) => vec.clear(),
            ArrayCell::Date(vec) => vec.clear(),
            ArrayCell::Time(vec) => vec.clear(),
            ArrayCell::TimeStamp(vec) => vec.clear(),
            ArrayCell::TimeStampTz(vec) => vec.clear(),
            ArrayCell::Uuid(vec) => vec.clear(),
            ArrayCell::Json(vec) => vec.clear(),
            ArrayCell::Bytes(vec) => vec.clear(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CellNonOptional {
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
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Array(ArrayCellNonOptional),
}

impl TryFrom<Cell> for CellNonOptional {
    type Error = EtlError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(CellNonOptional::Null),
            Cell::Bool(val) => Ok(CellNonOptional::Bool(val)),
            Cell::String(val) => Ok(CellNonOptional::String(val)),
            Cell::I16(val) => Ok(CellNonOptional::I16(val)),
            Cell::I32(val) => Ok(CellNonOptional::I32(val)),
            Cell::U32(val) => Ok(CellNonOptional::U32(val)),
            Cell::I64(val) => Ok(CellNonOptional::I64(val)),
            Cell::F32(val) => Ok(CellNonOptional::F32(val)),
            Cell::F64(val) => Ok(CellNonOptional::F64(val)),
            Cell::Numeric(val) => Ok(CellNonOptional::Numeric(val)),
            Cell::Date(val) => Ok(CellNonOptional::Date(val)),
            Cell::Time(val) => Ok(CellNonOptional::Time(val)),
            Cell::TimeStamp(val) => Ok(CellNonOptional::TimeStamp(val)),
            Cell::TimeStampTz(val) => Ok(CellNonOptional::TimeStampTz(val)),
            Cell::Uuid(val) => Ok(CellNonOptional::Uuid(val)),
            Cell::Json(val) => Ok(CellNonOptional::Json(val)),
            Cell::Bytes(val) => Ok(CellNonOptional::Bytes(val)),
            Cell::Array(array_cell) => {
                let array_cell_non_optional = ArrayCellNonOptional::try_from(array_cell)?;
                Ok(CellNonOptional::Array(array_cell_non_optional))
            }
        }
    }
}

impl CellNonOptional {
    pub fn clear(&mut self) {
        match self {
            CellNonOptional::Null => {}
            CellNonOptional::Bool(b) => *b = false,
            CellNonOptional::String(s) => s.clear(),
            CellNonOptional::I16(i) => *i = 0,
            CellNonOptional::I32(i) => *i = 0,
            CellNonOptional::I64(i) => *i = 0,
            CellNonOptional::F32(i) => *i = 0.,
            CellNonOptional::F64(i) => *i = 0.,
            CellNonOptional::Numeric(n) => *n = PgNumeric::default(),
            CellNonOptional::Date(t) => *t = NaiveDate::default(),
            CellNonOptional::Time(t) => *t = NaiveTime::default(),
            CellNonOptional::TimeStamp(t) => *t = NaiveDateTime::default(),
            CellNonOptional::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
            CellNonOptional::Uuid(u) => *u = Uuid::default(),
            CellNonOptional::Json(j) => *j = serde_json::Value::default(),
            CellNonOptional::U32(u) => *u = 0,
            CellNonOptional::Bytes(b) => b.clear(),
            CellNonOptional::Array(vec) => {
                vec.clear();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCellNonOptional {
    Null,
    Bool(Vec<bool>),
    String(Vec<String>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Numeric(Vec<PgNumeric>),
    Date(Vec<NaiveDate>),
    Time(Vec<NaiveTime>),
    TimeStamp(Vec<NaiveDateTime>),
    TimeStampTz(Vec<DateTime<Utc>>),
    Uuid(Vec<Uuid>),
    Json(Vec<serde_json::Value>),
    Bytes(Vec<Vec<u8>>),
}

macro_rules! convert_array_variant {
    ($variant:ident, $vec:expr) => {
        if $vec.iter().any(|v| v.is_none()) {
            bail!(
                ErrorKind::NullValuesNotSupportedInArray,
                "NULL values in arrays are not supported",
                format!(
                    "Remove the NULL values from the array {:?} and try again",
                    $vec
                )
            )
        } else {
            Ok(ArrayCellNonOptional::$variant(
                $vec.into_iter().flatten().collect(),
            ))
        }
    };
}

impl TryFrom<ArrayCell> for ArrayCellNonOptional {
    type Error = EtlError;

    fn try_from(array_cell: ArrayCell) -> Result<Self, Self::Error> {
        match array_cell {
            ArrayCell::Null => Ok(ArrayCellNonOptional::Null),
            ArrayCell::Bool(vec) => convert_array_variant!(Bool, vec),
            ArrayCell::String(vec) => convert_array_variant!(String, vec),
            ArrayCell::I16(vec) => convert_array_variant!(I16, vec),
            ArrayCell::I32(vec) => convert_array_variant!(I32, vec),
            ArrayCell::U32(vec) => convert_array_variant!(U32, vec),
            ArrayCell::I64(vec) => convert_array_variant!(I64, vec),
            ArrayCell::F32(vec) => convert_array_variant!(F32, vec),
            ArrayCell::F64(vec) => convert_array_variant!(F64, vec),
            ArrayCell::Numeric(vec) => convert_array_variant!(Numeric, vec),
            ArrayCell::Date(vec) => convert_array_variant!(Date, vec),
            ArrayCell::Time(vec) => convert_array_variant!(Time, vec),
            ArrayCell::TimeStamp(vec) => convert_array_variant!(TimeStamp, vec),
            ArrayCell::TimeStampTz(vec) => convert_array_variant!(TimeStampTz, vec),
            ArrayCell::Uuid(vec) => convert_array_variant!(Uuid, vec),
            ArrayCell::Json(vec) => convert_array_variant!(Json, vec),
            ArrayCell::Bytes(vec) => convert_array_variant!(Bytes, vec),
        }
    }
}

impl ArrayCellNonOptional {
    fn clear(&mut self) {
        match self {
            ArrayCellNonOptional::Null => {}
            ArrayCellNonOptional::Bool(vec) => vec.clear(),
            ArrayCellNonOptional::String(vec) => vec.clear(),
            ArrayCellNonOptional::I16(vec) => vec.clear(),
            ArrayCellNonOptional::I32(vec) => vec.clear(),
            ArrayCellNonOptional::U32(vec) => vec.clear(),
            ArrayCellNonOptional::I64(vec) => vec.clear(),
            ArrayCellNonOptional::F32(vec) => vec.clear(),
            ArrayCellNonOptional::F64(vec) => vec.clear(),
            ArrayCellNonOptional::Numeric(vec) => vec.clear(),
            ArrayCellNonOptional::Date(vec) => vec.clear(),
            ArrayCellNonOptional::Time(vec) => vec.clear(),
            ArrayCellNonOptional::TimeStamp(vec) => vec.clear(),
            ArrayCellNonOptional::TimeStampTz(vec) => vec.clear(),
            ArrayCellNonOptional::Uuid(vec) => vec.clear(),
            ArrayCellNonOptional::Json(vec) => vec.clear(),
            ArrayCellNonOptional::Bytes(vec) => vec.clear(),
        }
    }
}
