use crate::error::EtlError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use core::str;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::bail;
use crate::conversions::{bool::parse_bool, hex};
use crate::error::{ErrorKind, EtlResult};

use super::{ArrayCell, Cell, numeric::PgNumeric};

pub struct TextFormatConverter;

impl TextFormatConverter {
    pub fn default_value(typ: &Type) -> EtlResult<Cell> {
        const DEFAULT_DATE: NaiveDate = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        const DEFAULT_TIMESTAMP: NaiveDateTime = NaiveDateTime::new(DEFAULT_DATE, NaiveTime::MIN);
        const DEFAULT_TIMESTAMPTZ: DateTime<Utc> =
            DateTime::<Utc>::from_naive_utc_and_offset(DEFAULT_TIMESTAMP, Utc);

        match *typ {
            Type::BOOL => Ok(Cell::Bool(bool::default())),
            Type::BOOL_ARRAY => Ok(Cell::Array(ArrayCell::Bool(Vec::default()))),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Ok(Cell::String(String::default()))
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => Ok(Cell::Array(ArrayCell::String(Vec::default()))),
            Type::INT2 => Ok(Cell::I16(i16::default())),
            Type::INT2_ARRAY => Ok(Cell::Array(ArrayCell::I16(Vec::default()))),
            Type::INT4 => Ok(Cell::I32(i32::default())),
            Type::INT4_ARRAY => Ok(Cell::Array(ArrayCell::I32(Vec::default()))),
            Type::INT8 => Ok(Cell::I64(i64::default())),
            Type::INT8_ARRAY => Ok(Cell::Array(ArrayCell::I64(Vec::default()))),
            Type::FLOAT4 => Ok(Cell::F32(f32::default())),
            Type::FLOAT4_ARRAY => Ok(Cell::Array(ArrayCell::F32(Vec::default()))),
            Type::FLOAT8 => Ok(Cell::F64(f64::default())),
            Type::FLOAT8_ARRAY => Ok(Cell::Array(ArrayCell::F64(Vec::default()))),
            Type::NUMERIC => Ok(Cell::Numeric(PgNumeric::default())),
            Type::NUMERIC_ARRAY => Ok(Cell::Array(ArrayCell::Numeric(Vec::default()))),
            Type::BYTEA => Ok(Cell::Bytes(Vec::default())),
            Type::BYTEA_ARRAY => Ok(Cell::Array(ArrayCell::Bytes(Vec::default()))),
            Type::DATE => Ok(Cell::Date(DEFAULT_DATE)),
            Type::DATE_ARRAY => Ok(Cell::Array(ArrayCell::Date(Vec::default()))),
            Type::TIME => Ok(Cell::Time(NaiveTime::MIN)),
            Type::TIME_ARRAY => Ok(Cell::Array(ArrayCell::Time(Vec::default()))),
            Type::TIMESTAMP => Ok(Cell::TimeStamp(DEFAULT_TIMESTAMP)),
            Type::TIMESTAMP_ARRAY => Ok(Cell::Array(ArrayCell::TimeStamp(Vec::default()))),
            Type::TIMESTAMPTZ => Ok(Cell::TimeStampTz(DEFAULT_TIMESTAMPTZ)),
            Type::TIMESTAMPTZ_ARRAY => Ok(Cell::Array(ArrayCell::TimeStampTz(Vec::default()))),
            Type::UUID => Ok(Cell::Uuid(Uuid::default())),
            Type::UUID_ARRAY => Ok(Cell::Array(ArrayCell::Uuid(Vec::default()))),
            Type::JSON | Type::JSONB => Ok(Cell::Json(serde_json::Value::default())),
            Type::JSON_ARRAY | Type::JSONB_ARRAY => {
                Ok(Cell::Array(ArrayCell::Json(Vec::default())))
            }
            Type::OID => Ok(Cell::U32(u32::default())),
            Type::OID_ARRAY => Ok(Cell::Array(ArrayCell::U32(Vec::default()))),
            #[cfg(feature = "unknown-types-to-bytes")]
            _ => Ok(Cell::String(String::default())),
            #[cfg(not(feature = "unknown-types-to-bytes"))]
            _ => {
                bail!(
                    ErrorKind::ConversionError,
                    "Unsupported type",
                    format!(
                        "The type {} is not supported, enable 'unknown-types-to-bytes' if you want to treat it as 'string'",
                        typ.name()
                    )
                )
            }
        }
    }

    pub fn try_from_str(typ: &Type, str: &str) -> EtlResult<Cell> {
        match *typ {
            Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
            Type::BOOL_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(parse_bool(str)?)),
                ArrayCell::Bool,
            ),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Ok(Cell::String(str.to_string()))
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.to_string())),
                ArrayCell::String,
            ),
            Type::INT2 => Ok(Cell::I16(str.parse()?)),
            Type::INT2_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I16)
            }
            Type::INT4 => Ok(Cell::I32(str.parse()?)),
            Type::INT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I32)
            }
            Type::INT8 => Ok(Cell::I64(str.parse()?)),
            Type::INT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I64)
            }
            Type::FLOAT4 => Ok(Cell::F32(str.parse()?)),
            Type::FLOAT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F32)
            }
            Type::FLOAT8 => Ok(Cell::F64(str.parse()?)),
            Type::FLOAT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F64)
            }
            Type::NUMERIC => Ok(Cell::Numeric(str.parse()?)),
            Type::NUMERIC_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::Numeric,
            ),
            Type::BYTEA => Ok(Cell::Bytes(hex::from_bytea_hex(str)?)),
            Type::BYTEA_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(hex::from_bytea_hex(str)?)),
                ArrayCell::Bytes,
            ),
            Type::DATE => {
                let val = NaiveDate::parse_from_str(str, "%Y-%m-%d")?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveDate::parse_from_str(str, "%Y-%m-%d")?)),
                ArrayCell::Date,
            ),
            Type::TIME => {
                let val = NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?)),
                ArrayCell::Time,
            ),
            Type::TIMESTAMP => {
                let val = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| {
                    Ok(Some(NaiveDateTime::parse_from_str(
                        str,
                        "%Y-%m-%d %H:%M:%S%.f",
                    )?))
                },
                ArrayCell::TimeStamp,
            ),
            Type::TIMESTAMPTZ => {
                let val =
                    match DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%#z") {
                        Ok(val) => val,
                        Err(_) => {
                            DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%:z")?
                        }
                    };
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => {
                match TextFormatConverter::parse_array(
                    str,
                    |str| {
                        Ok(Some(
                            DateTime::<FixedOffset>::parse_from_str(
                                str,
                                "%Y-%m-%d %H:%M:%S%.f%#z",
                            )?
                            .into(),
                        ))
                    },
                    ArrayCell::TimeStampTz,
                ) {
                    Ok(val) => Ok(val),
                    Err(_) => TextFormatConverter::parse_array(
                        str,
                        |str| {
                            Ok(Some(
                                DateTime::<FixedOffset>::parse_from_str(
                                    str,
                                    "%Y-%m-%d %H:%M:%S%.f%:z",
                                )?
                                .into(),
                            ))
                        },
                        ArrayCell::TimeStampTz,
                    ),
                }
            }
            Type::UUID => {
                let val = Uuid::parse_str(str)?;
                Ok(Cell::Uuid(val))
            }
            Type::UUID_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(Uuid::parse_str(str)?)),
                ArrayCell::Uuid,
            ),
            Type::JSON | Type::JSONB => {
                let val = serde_json::from_str(str)?;
                Ok(Cell::Json(val))
            }
            Type::JSON_ARRAY | Type::JSONB_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(serde_json::from_str(str)?)),
                ArrayCell::Json,
            ),
            Type::OID => {
                let val: u32 = str.parse()?;
                Ok(Cell::U32(val))
            }
            Type::OID_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::U32)
            }
            #[cfg(feature = "unknown-types-to-bytes")]
            _ => Ok(Cell::String(str.to_string())),
            #[cfg(not(feature = "unknown-types-to-bytes"))]
            _ => {
                bail!(
                    ErrorKind::ConversionError,
                    "Unsupported type",
                    format!(
                        "The type {} is not supported, enable 'unknown-types-to-bytes' if you want to treat it as 'string'",
                        typ.name()
                    )
                )
            }
        }
    }

    fn parse_array<P, M, T>(str: &str, mut parse: P, m: M) -> EtlResult<Cell>
    where
        P: FnMut(&str) -> EtlResult<Option<T>>,
        M: FnOnce(Vec<Option<T>>) -> ArrayCell,
    {
        if str.len() < 2 {
            bail!(ErrorKind::ConversionError, "The array input is too short");
        }

        if !str.starts_with('{') || !str.ends_with('}') {
            bail!(
                ErrorKind::ConversionError,
                "The array input is missing braces"
            );
        }

        let mut res = vec![];
        let str = &str[1..(str.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = str.chars();
        let mut done = str.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if !in_quotes {
                                val_quoted = true;
                            }
                            in_quotes = !in_quotes;
                        }
                        '\\' => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }

            let val = if !val_quoted && val_str.to_lowercase() == "null" {
                None
            } else {
                parse(&val_str)?
            };

            res.push(val);
            val_str.clear();
            val_quoted = false;
        }

        Ok(Cell::Array(m(res)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_array_quoted_null_as_string() {
        let cell =
            TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{\"a\",\"null\"}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), Some("null".to_string())]);
            }
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_text_array_unquoted_null_is_parsed_correctly() {
        let cell = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{a,NULL}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), None]);
            }
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_numeric_array_with_parsing_error() {
        // This should return an error because "invalid" cannot be parsed as a number
        let result = TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{1,invalid,3}");
        assert!(result.is_err());
        // The error should be a parsing error, not related to NULL handling
        let error = result.unwrap_err();
        assert!(!error.to_string().contains("NULL"));
    }
}
