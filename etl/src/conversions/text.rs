use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use core::str;
use etl_postgres::time::{
    DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM, TIMESTAMPTZ_FORMAT_HHMM,
};
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::bail;
use crate::conversions::{bool::parse_bool, hex};
use crate::error::{ErrorKind, EtlError, EtlResult};

use super::{ArrayCell, Cell, numeric::PgNumeric};

/// Utilities for converting PostgreSQL text-format data to typed [`Cell`] values.
///
/// This helper struct provides methods for parsing PostgreSQL's text representation
/// of various data types into strongly-typed [`Cell`] variants. It handles PostgreSQL's
/// specific text formatting conventions and provides fallback behavior for unknown types.
pub struct TextFormatConverter;

impl TextFormatConverter {
    /// Creates a default [`Cell`] value for the given PostgreSQL type.
    ///
    /// This helper method provides sensible default values for PostgreSQL types,
    /// primarily used during cell initialization and error recovery scenarios.
    /// The defaults are chosen to be the zero/empty value for each type where possible.
    ///
    /// For complex types like arrays, empty vectors are returned. For temporal types,
    /// minimal valid timestamps are used (year 1, month 1, day 1).
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

    /// Converts a PostgreSQL text-format string to a typed [`Cell`] value.
    ///
    /// This method parses PostgreSQL's text representation of various data types
    /// into strongly-typed [`Cell`] variants. It handles all major PostgreSQL types
    /// including arrays, and provides comprehensive error handling for malformed input.
    ///
    /// For array types, it delegates to [`parse_array`] which handles PostgreSQL's
    /// array literal syntax with proper escaping and null value support.
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
                let val = NaiveDate::parse_from_str(str, DATE_FORMAT)?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveDate::parse_from_str(str, DATE_FORMAT)?)),
                ArrayCell::Date,
            ),
            Type::TIME => {
                let val = NaiveTime::parse_from_str(str, TIME_FORMAT)?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveTime::parse_from_str(str, TIME_FORMAT)?)),
                ArrayCell::Time,
            ),
            Type::TIMESTAMP => {
                let val = NaiveDateTime::parse_from_str(str, TIMESTAMP_FORMAT)?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveDateTime::parse_from_str(str, TIMESTAMP_FORMAT)?)),
                ArrayCell::TimeStamp,
            ),
            Type::TIMESTAMPTZ => {
                let val =
                    match DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HHMM) {
                        Ok(val) => val,
                        Err(_) => {
                            DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HH_MM)?
                        }
                    };
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => {
                match TextFormatConverter::parse_array(
                    str,
                    |str| {
                        Ok(Some(
                            DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HHMM)?
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
                                    TIMESTAMPTZ_FORMAT_HH_MM,
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

    /// Parses PostgreSQL array literal syntax into a typed [`ArrayCell`].
    ///
    /// This function handles PostgreSQL's array format with curly braces, comma
    /// separation, and proper quoting. It supports null values (unquoted "null"),
    /// escaped characters within quoted strings, and delegates element parsing
    /// to the provided closure.
    ///
    /// The parser correctly handles quote escaping, comma separation within quotes,
    /// and distinguishes between null values and the string "null".
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
    use chrono::{Datelike, Timelike};

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

    #[test]
    fn try_from_str_bool() {
        let cell = TextFormatConverter::try_from_str(&Type::BOOL, "t").unwrap();
        assert_eq!(cell, Cell::Bool(true));

        let cell = TextFormatConverter::try_from_str(&Type::BOOL, "f").unwrap();
        assert_eq!(cell, Cell::Bool(false));

        assert!(TextFormatConverter::try_from_str(&Type::BOOL, "invalid").is_err());
    }

    #[test]
    fn try_from_str_integers() {
        let cell = TextFormatConverter::try_from_str(&Type::INT2, "123").unwrap();
        assert_eq!(cell, Cell::I16(123));

        let cell = TextFormatConverter::try_from_str(&Type::INT4, "-456").unwrap();
        assert_eq!(cell, Cell::I32(-456));

        let cell = TextFormatConverter::try_from_str(&Type::INT8, "9223372036854775807").unwrap();
        assert_eq!(cell, Cell::I64(9223372036854775807));

        let cell = TextFormatConverter::try_from_str(&Type::OID, "12345").unwrap();
        assert_eq!(cell, Cell::U32(12345));
    }

    #[test]
    fn try_from_str_integer_overflow() {
        assert!(TextFormatConverter::try_from_str(&Type::INT2, "99999").is_err());
        assert!(TextFormatConverter::try_from_str(&Type::INT4, "9999999999").is_err());
        assert!(TextFormatConverter::try_from_str(&Type::OID, "-1").is_err());
    }

    #[test]
    fn try_from_str_floats() {
        let cell = TextFormatConverter::try_from_str(&Type::FLOAT4, "3.15").unwrap();
        assert_eq!(cell, Cell::F32(3.15));

        let cell = TextFormatConverter::try_from_str(&Type::FLOAT8, "-2.818").unwrap();
        assert_eq!(cell, Cell::F64(-2.818));

        let cell = TextFormatConverter::try_from_str(&Type::FLOAT4, "inf").unwrap();
        assert_eq!(cell, Cell::F32(f32::INFINITY));

        let cell = TextFormatConverter::try_from_str(&Type::FLOAT8, "NaN").unwrap();
        assert!(matches!(cell, Cell::F64(val) if val.is_nan()));
    }

    #[test]
    fn try_from_str_string_types() {
        let test_string = "Hello, World!";

        let cell = TextFormatConverter::try_from_str(&Type::TEXT, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_string()));

        let cell = TextFormatConverter::try_from_str(&Type::VARCHAR, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_string()));

        let cell = TextFormatConverter::try_from_str(&Type::CHAR, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_string()));
    }

    #[test]
    fn try_from_str_numeric() {
        let cell = TextFormatConverter::try_from_str(&Type::NUMERIC, "123.45").unwrap();
        if let Cell::Numeric(num) = cell {
            assert_eq!(num.to_string(), "123.45");
        } else {
            panic!("Expected Numeric cell");
        }

        let cell = TextFormatConverter::try_from_str(&Type::NUMERIC, "NaN").unwrap();
        assert_eq!(cell, Cell::Numeric(PgNumeric::NaN));
    }

    #[test]
    fn try_from_str_bytea() {
        let cell = TextFormatConverter::try_from_str(&Type::BYTEA, "\\x48656c6c6f").unwrap();
        assert_eq!(cell, Cell::Bytes(b"Hello".to_vec()));

        assert!(TextFormatConverter::try_from_str(&Type::BYTEA, "invalid").is_err());
    }

    #[test]
    fn try_from_str_dates() {
        let cell = TextFormatConverter::try_from_str(&Type::DATE, "2023-12-25").unwrap();
        if let Cell::Date(date) = cell {
            assert_eq!(date.year(), 2023);
            assert_eq!(date.month(), 12);
            assert_eq!(date.day(), 25);
        } else {
            panic!("Expected Date cell");
        }

        assert!(TextFormatConverter::try_from_str(&Type::DATE, "invalid-date").is_err());
    }

    #[test]
    fn try_from_str_time() {
        let cell = TextFormatConverter::try_from_str(&Type::TIME, "14:30:45.123").unwrap();
        if let Cell::Time(time) = cell {
            assert_eq!(time.hour(), 14);
            assert_eq!(time.minute(), 30);
            assert_eq!(time.second(), 45);
        } else {
            panic!("Expected Time cell");
        }

        assert!(TextFormatConverter::try_from_str(&Type::TIME, "invalid-time").is_err());
    }

    #[test]
    fn try_from_str_timestamp() {
        let cell =
            TextFormatConverter::try_from_str(&Type::TIMESTAMP, "2023-12-25 14:30:45.123").unwrap();
        if let Cell::TimeStamp(ts) = cell {
            assert_eq!(ts.date().year(), 2023);
            assert_eq!(ts.time().hour(), 14);
        } else {
            panic!("Expected TimeStamp cell");
        }
    }

    #[test]
    fn try_from_str_timestamptz() {
        let cell =
            TextFormatConverter::try_from_str(&Type::TIMESTAMPTZ, "2023-12-25 14:30:45.123+00:00")
                .unwrap();
        if let Cell::TimeStampTz(ts) = cell {
            assert_eq!(ts.year(), 2023);
        } else {
            panic!("Expected TimeStampTz cell");
        }

        // Test fallback format
        let cell =
            TextFormatConverter::try_from_str(&Type::TIMESTAMPTZ, "2023-12-25 14:30:45.123+00")
                .unwrap();
        assert!(matches!(cell, Cell::TimeStampTz(_)));
    }

    #[test]
    fn try_from_str_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let cell = TextFormatConverter::try_from_str(&Type::UUID, uuid_str).unwrap();
        if let Cell::Uuid(uuid) = cell {
            assert_eq!(uuid.to_string(), uuid_str);
        } else {
            panic!("Expected Uuid cell");
        }

        assert!(TextFormatConverter::try_from_str(&Type::UUID, "invalid-uuid").is_err());
    }

    #[test]
    fn try_from_str_json() {
        let json_str = r#"{"key": "value", "number": 42}"#;
        let cell = TextFormatConverter::try_from_str(&Type::JSON, json_str).unwrap();
        if let Cell::Json(json) = cell {
            assert_eq!(json["key"], "value");
            assert_eq!(json["number"], 42);
        } else {
            panic!("Expected Json cell");
        }

        let cell = TextFormatConverter::try_from_str(&Type::JSONB, json_str).unwrap();
        assert!(matches!(cell, Cell::Json(_)));

        assert!(TextFormatConverter::try_from_str(&Type::JSON, "invalid json").is_err());
    }

    #[test]
    fn parse_array_basic() {
        let cell = TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{1,2,3}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert_eq!(v, vec![Some(1), Some(2), Some(3)]);
            }
            _ => panic!("Expected INT4 array"),
        }
    }

    #[test]
    fn parse_array_with_nulls() {
        let cell = TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{1,NULL,3}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert_eq!(v, vec![Some(1), None, Some(3)]);
            }
            _ => panic!("Expected INT4 array"),
        }
    }

    #[test]
    fn parse_array_quoted_strings() {
        let cell = TextFormatConverter::try_from_str(
            &Type::TEXT_ARRAY,
            r#"{"hello","world with spaces","with\"quotes"}"#,
        )
        .unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![
                        Some("hello".to_string()),
                        Some("world with spaces".to_string()),
                        Some("with\"quotes".to_string())
                    ]
                );
            }
            _ => panic!("Expected TEXT array"),
        }
    }

    #[test]
    fn parse_array_empty() {
        let cell = TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert!(v.is_empty());
            }
            _ => panic!("Expected empty INT4 array"),
        }
    }

    #[test]
    fn parse_array_single_element() {
        let cell = TextFormatConverter::try_from_str(&Type::BOOL_ARRAY, "{t}").unwrap();
        match cell {
            Cell::Array(ArrayCell::Bool(v)) => {
                assert_eq!(v, vec![Some(true)]);
            }
            _ => panic!("Expected BOOL array"),
        }
    }

    #[test]
    fn parse_array_invalid_format() {
        // Missing opening brace
        assert!(TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "1,2,3}").is_err());

        // Missing closing brace
        assert!(TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{1,2,3").is_err());

        // Too short
        assert!(TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "{").is_err());
        assert!(TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "}").is_err());
        assert!(TextFormatConverter::try_from_str(&Type::INT4_ARRAY, "").is_err());
    }

    #[test]
    fn parse_array_escape_sequences() {
        // The array parser doesn't process escape sequences in the same way as the table row parser
        // It expects literal characters in the array string
        let cell = TextFormatConverter::try_from_str(
            &Type::TEXT_ARRAY,
            r#"{"line1\\nline2","tab\\there"}"#,
        )
        .unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                // These should be literal strings since array parser doesn't decode escapes like table parser
                assert_eq!(
                    v,
                    vec![
                        Some("line1\\nline2".to_string()),
                        Some("tab\\there".to_string())
                    ]
                );
            }
            _ => panic!("Expected TEXT array with escape sequences"),
        }
    }

    #[test]
    fn parse_timestamptz_array_fallback() {
        // Test the fallback parsing for timestamptz arrays
        let cell = TextFormatConverter::try_from_str(
            &Type::TIMESTAMPTZ_ARRAY,
            "{\"2023-01-01 12:00:00.000+00\"}",
        )
        .unwrap();
        match cell {
            Cell::Array(ArrayCell::TimeStampTz(v)) => {
                assert_eq!(v.len(), 1);
                assert!(v[0].is_some());
            }
            _ => panic!("Expected TIMESTAMPTZ array"),
        }
    }

    #[cfg(feature = "unknown-types-to-bytes")]
    #[test]
    fn unknown_types_to_string() {
        use tokio_postgres::types::Type;
        // Create a custom type that's not normally supported
        let custom_type = Type::new(
            "custom".to_string(),
            99999,
            tokio_postgres::types::Kind::Simple,
            "public".to_string(),
        );

        let cell = TextFormatConverter::default_value(&custom_type).unwrap();
        assert_eq!(cell, Cell::String(String::new()));

        let cell = TextFormatConverter::try_from_str(&custom_type, "test").unwrap();
        assert_eq!(cell, Cell::String("test".to_string()));
    }
}
