use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use numeric::PgNumeric;
use std::fmt::Debug;
use tokio_postgres::types::Type;
use uuid::Uuid;

pub mod bool;
pub mod event;
pub mod hex;
pub mod numeric;
pub mod table_row;
pub mod text;

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null(Type),
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
    #[cfg(feature = "bigquery")]
    pub fn encode_prost(&self, tag: u32, buf: &mut impl bytes::BufMut) {
        use crate::conversions::text::TextFormatConverter;

        match self {
            Cell::Null(typ) => {
                TextFormatConverter::default_value(typ).encode_prost(tag, buf);
            }
            Cell::Bool(b) => {
                prost::encoding::bool::encode(tag, b, buf);
            }
            Cell::String(s) => {
                prost::encoding::string::encode(tag, s, buf);
            }
            Cell::I16(i) => {
                let val = *i as i32;
                prost::encoding::int32::encode(tag, &val, buf);
            }
            Cell::I32(i) => {
                prost::encoding::int32::encode(tag, i, buf);
            }
            Cell::I64(i) => {
                prost::encoding::int64::encode(tag, i, buf);
            }
            Cell::F32(i) => {
                prost::encoding::float::encode(tag, i, buf);
            }
            Cell::F64(i) => {
                prost::encoding::double::encode(tag, i, buf);
            }
            Cell::Numeric(n) => {
                let s = n.to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::U32(i) => {
                prost::encoding::uint32::encode(tag, i, buf);
            }
            Cell::Bytes(b) => {
                prost::encoding::bytes::encode(tag, b, buf);
            }
            Cell::Array(a) => {
                a.clone().encode_prost(tag, buf);
            }
        }
    }

    #[cfg(feature = "bigquery")]
    pub fn encoded_len_prost(&self, tag: u32) -> usize {
        use crate::conversions::text::TextFormatConverter;

        match self {
            Cell::Null(typ) => TextFormatConverter::default_value(typ).encoded_len_prost(tag),
            Cell::Bool(b) => prost::encoding::bool::encoded_len(tag, b),
            Cell::String(s) => prost::encoding::string::encoded_len(tag, s),
            Cell::I16(i) => {
                let val = *i as i32;
                prost::encoding::int32::encoded_len(tag, &val)
            }
            Cell::I32(i) => prost::encoding::int32::encoded_len(tag, i),
            Cell::I64(i) => prost::encoding::int64::encoded_len(tag, i),
            Cell::F32(i) => prost::encoding::float::encoded_len(tag, i),
            Cell::F64(i) => prost::encoding::double::encoded_len(tag, i),
            Cell::Numeric(n) => {
                let s = n.to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::U32(i) => prost::encoding::uint32::encoded_len(tag, i),
            Cell::Bytes(b) => prost::encoding::bytes::encoded_len(tag, b),
            Cell::Array(array_cell) => array_cell.clone().encoded_len_prost(tag),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Cell::Null(_) => {}
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
    #[cfg(feature = "bigquery")]
    pub fn encode_prost(self, tag: u32, buf: &mut impl bytes::BufMut) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                prost::encoding::bool::encode_packed(tag, &vec, buf);
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                prost::encoding::uint32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                prost::encoding::int64::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                prost::encoding::float::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                prost::encoding::double::encode_packed(tag, &vec, buf);
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                prost::encoding::bytes::encode_repeated(tag, &vec, buf);
            }
        }
    }

    #[cfg(feature = "bigquery")]
    pub fn encoded_len_prost(self, tag: u32) -> usize {
        match self {
            ArrayCell::Null => 0,
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                prost::encoding::bool::encoded_len_packed(tag, &vec)
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                prost::encoding::uint32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                prost::encoding::int64::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                prost::encoding::float::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                prost::encoding::double::encoded_len_packed(tag, &vec)
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                prost::encoding::bytes::encoded_len_repeated(tag, &vec)
            }
        }
    }

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
