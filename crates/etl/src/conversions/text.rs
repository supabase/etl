use core::str;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use etl_postgres::types::{
    DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM, TIMESTAMPTZ_FORMAT_HHMM,
    is_array_type,
};
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::{
    bail,
    conversions::{bool::parse_bool, hex},
    error::{ErrorKind, EtlResult},
    types::{ArrayCell, Cell},
};

/// Converts a Postgres text-format string to a typed [`Cell`] value.
///
/// This method parses Postgres's text representation of various data types
/// into strongly-typed [`Cell`] variants. It handles all major Postgres types
/// including arrays, and provides comprehensive error handling for malformed
/// input.
///
/// For array types, it delegates to [`parse_cell_from_postgres_text_array`]
/// which handles Postgres's array literal syntax with proper escaping and null
/// value support.
pub(crate) fn parse_cell_from_postgres_text(typ: &Type, str: &str) -> EtlResult<Cell> {
    match *typ {
        Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
        Type::BOOL_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(parse_bool(str)?)),
            ArrayCell::Bool,
        ),
        Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT | Type::MONEY => {
            Ok(Cell::String(str.to_owned()))
        }
        Type::CHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::NAME_ARRAY
        | Type::TEXT_ARRAY
        | Type::MONEY_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(str.to_owned())),
            ArrayCell::String,
        ),
        Type::INT2 => Ok(Cell::I16(str.parse()?)),
        Type::INT2_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I16)
        }
        Type::INT4 => Ok(Cell::I32(str.parse()?)),
        Type::INT4_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I32)
        }
        Type::INT8 => Ok(Cell::I64(str.parse()?)),
        Type::INT8_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I64)
        }
        Type::FLOAT4 => Ok(Cell::F32(str.parse()?)),
        Type::FLOAT4_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F32)
        }
        Type::FLOAT8 => Ok(Cell::F64(str.parse()?)),
        Type::FLOAT8_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F64)
        }
        Type::NUMERIC => Ok(Cell::Numeric(str.parse()?)),
        Type::NUMERIC_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(str.parse()?)),
            ArrayCell::Numeric,
        ),
        Type::BYTEA => Ok(Cell::Bytes(hex::parse_bytea_hex_string(str)?)),
        Type::BYTEA_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(hex::parse_bytea_hex_string(str)?)),
            ArrayCell::Bytes,
        ),
        Type::DATE => {
            let value =
                parse_temporal_or_raw(str, |value| NaiveDate::parse_from_str(value, DATE_FORMAT))?;
            Ok(value.map_or_else(|| Cell::String(str.to_owned()), Cell::Date))
        }
        Type::DATE_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(NaiveDate::parse_from_str(str, DATE_FORMAT)?)),
            ArrayCell::Date,
        )
        .or_else(|_| parse_text_array(str)),
        Type::TIME => {
            let value =
                parse_temporal_or_raw(str, |value| NaiveTime::parse_from_str(value, TIME_FORMAT))?;
            Ok(value.map_or_else(|| Cell::String(str.to_owned()), Cell::Time))
        }
        Type::TIME_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(NaiveTime::parse_from_str(str, TIME_FORMAT)?)),
            ArrayCell::Time,
        )
        .or_else(|_| parse_text_array(str)),
        Type::TIMESTAMP => {
            let value = parse_temporal_or_raw(str, |value| {
                NaiveDateTime::parse_from_str(value, TIMESTAMP_FORMAT)
            })?;
            Ok(value.map_or_else(|| Cell::String(str.to_owned()), Cell::Timestamp))
        }
        Type::TIMESTAMP_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(NaiveDateTime::parse_from_str(str, TIMESTAMP_FORMAT)?)),
            ArrayCell::Timestamp,
        )
        .or_else(|_| parse_text_array(str)),
        Type::TIMESTAMPTZ => {
            // PostgreSQL can render UTC offsets either as `+00` or `+00:00`,
            // so we accept both text formats here.
            let val = match DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HHMM) {
                Ok(val) => val,
                Err(_) => {
                    match DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HH_MM) {
                        Ok(val) => val,
                        Err(error) if may_be_postgres_temporal_outside_rust_domain(str) => {
                            return Ok(Cell::String(str.to_owned()));
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            };
            Ok(Cell::TimestampTz(val.into()))
        }
        Type::TIMESTAMPTZ_ARRAY => {
            match parse_cell_from_postgres_text_array(
                str,
                |str| {
                    Ok(Some(
                        DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HHMM)?
                            .into(),
                    ))
                },
                ArrayCell::TimestampTz,
            ) {
                Ok(val) => Ok(val),
                Err(_) => parse_cell_from_postgres_text_array(
                    str,
                    |str| {
                        Ok(Some(
                            DateTime::<FixedOffset>::parse_from_str(str, TIMESTAMPTZ_FORMAT_HH_MM)?
                                .into(),
                        ))
                    },
                    ArrayCell::TimestampTz,
                )
                .or_else(|_| parse_text_array(str)),
            }
        }
        Type::UUID => {
            let val = Uuid::parse_str(str)?;
            Ok(Cell::Uuid(val))
        }
        Type::UUID_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(Uuid::parse_str(str)?)),
            ArrayCell::Uuid,
        ),
        Type::JSON | Type::JSONB => {
            let _: serde_json::Value = serde_json::from_str(str)?;
            Ok(Cell::String(str.to_owned()))
        }
        Type::JSON_ARRAY | Type::JSONB_ARRAY => parse_cell_from_postgres_text_array(
            str,
            |str| {
                let _: serde_json::Value = serde_json::from_str(str)?;
                Ok(Some(str.to_owned()))
            },
            ArrayCell::String,
        ),
        Type::OID => {
            let val: u32 = str.parse()?;
            Ok(Cell::U32(val))
        }
        Type::OID_ARRAY => {
            parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::U32)
        }
        _ if is_array_type(typ) => parse_cell_from_postgres_text_array(
            str,
            |str| Ok(Some(str.to_owned())),
            ArrayCell::String,
        ),
        _ => Ok(Cell::String(str.to_owned())),
    }
}

/// Parses Postgres array literal syntax into a typed [`ArrayCell`].
///
/// This function handles Postgres's array format with curly braces, comma
/// separation, and proper quoting. It supports null values (unquoted "null"),
/// escaped characters within quoted strings, and delegates element parsing
/// to the provided closure.
///
/// The parser correctly handles quote escaping, comma separation within quotes,
/// and distinguishes between null values and the string "null".
fn parse_cell_from_postgres_text_array<P, M, T>(str: &str, mut parse: P, m: M) -> EtlResult<Cell>
where
    P: FnMut(&str) -> EtlResult<Option<T>>,
    M: FnOnce(Vec<Option<T>>) -> ArrayCell,
{
    if str.len() < 2 {
        bail!(ErrorKind::ConversionError, "Array input too short");
    }

    if !str.starts_with('{') || !str.ends_with('}') {
        bail!(ErrorKind::ConversionError, "Array input missing braces");
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

        // PostgreSQL treats unquoted `NULL` as a null array element, while
        // quoted `"NULL"` is just the literal string. Keep that distinction.
        let val = if !val_quoted && val_str.eq_ignore_ascii_case("null") {
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

/// Parses a text array preserving element text.
fn parse_text_array(str: &str) -> EtlResult<Cell> {
    parse_cell_from_postgres_text_array(str, |str| Ok(Some(str.to_owned())), ArrayCell::String)
}

/// Parses a temporal value or preserves valid PostgreSQL temporal text that
/// does not fit Rust's temporal domain.
fn parse_temporal_or_raw<T, E>(
    value: &str,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<Option<T>, E> {
    match parse(value) {
        Ok(value) => Ok(Some(value)),
        Err(error) if may_be_postgres_temporal_outside_rust_domain(value) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Returns whether a temporal text value can be valid PostgreSQL while not
/// fitting Rust's temporal parser.
fn may_be_postgres_temporal_outside_rust_domain(value: &str) -> bool {
    matches!(value, "infinity" | "-infinity")
        || value.starts_with("24:")
        || value.ends_with(" BC")
        || value
            .split_once('-')
            .and_then(|(year, _)| year.parse::<i32>().ok())
            .is_some_and(|year| !(1..=9999).contains(&year))
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};

    use super::*;
    use crate::types::PgNumeric;

    #[test]
    fn parse_text_array_quoted_null_as_string() {
        let cell = parse_cell_from_postgres_text(&Type::TEXT_ARRAY, "{\"a\",\"null\"}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_owned()), Some("null".to_owned())]);
            }
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_text_array_unquoted_null_is_parsed_correctly() {
        let cell = parse_cell_from_postgres_text(&Type::TEXT_ARRAY, "{a,NULL}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_owned()), None]);
            }
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_numeric_array_with_parsing_error() {
        // This should return an error because "invalid" cannot be parsed as a number
        let result = parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{1,invalid,3}");
        assert!(result.is_err());
        // The error should be a parsing error, not related to NULL handling
        let error = result.unwrap_err();
        assert!(!error.to_string().contains("NULL"));
    }

    #[test]
    fn try_from_str_bool() {
        let cell = parse_cell_from_postgres_text(&Type::BOOL, "t").unwrap();
        assert_eq!(cell, Cell::Bool(true));

        let cell = parse_cell_from_postgres_text(&Type::BOOL, "f").unwrap();
        assert_eq!(cell, Cell::Bool(false));

        assert!(parse_cell_from_postgres_text(&Type::BOOL, "invalid").is_err());
    }

    #[test]
    fn try_from_str_integers() {
        let cell = parse_cell_from_postgres_text(&Type::INT2, "123").unwrap();
        assert_eq!(cell, Cell::I16(123));

        let cell = parse_cell_from_postgres_text(&Type::INT4, "-456").unwrap();
        assert_eq!(cell, Cell::I32(-456));

        let cell = parse_cell_from_postgres_text(&Type::INT8, "9223372036854775807").unwrap();
        assert_eq!(cell, Cell::I64(9223372036854775807));

        let cell = parse_cell_from_postgres_text(&Type::OID, "12345").unwrap();
        assert_eq!(cell, Cell::U32(12345));
    }

    #[test]
    fn try_from_str_integer_boundaries() {
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT2, "-32768").unwrap(),
            Cell::I16(i16::MIN)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT2, "32767").unwrap(),
            Cell::I16(i16::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT4, "-2147483648").unwrap(),
            Cell::I32(i32::MIN)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT4, "2147483647").unwrap(),
            Cell::I32(i32::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT8, "-9223372036854775808").unwrap(),
            Cell::I64(i64::MIN)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT8, "9223372036854775807").unwrap(),
            Cell::I64(i64::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::OID, "4294967295").unwrap(),
            Cell::U32(u32::MAX)
        );
    }

    #[test]
    fn try_from_str_integer_overflow() {
        assert!(parse_cell_from_postgres_text(&Type::INT2, "99999").is_err());
        assert!(parse_cell_from_postgres_text(&Type::INT4, "9999999999").is_err());
        assert!(parse_cell_from_postgres_text(&Type::INT8, "9223372036854775808").is_err());
        assert!(parse_cell_from_postgres_text(&Type::INT8, "-9223372036854775809").is_err());
        assert!(parse_cell_from_postgres_text(&Type::OID, "-1").is_err());
        assert!(parse_cell_from_postgres_text(&Type::OID, "4294967296").is_err());
    }

    #[test]
    fn try_from_str_integer_array_boundaries() {
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT2_ARRAY, "{-32768,32767,NULL}").unwrap(),
            Cell::Array(ArrayCell::I16(vec![Some(i16::MIN), Some(i16::MAX), None]))
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{-2147483648,2147483647,NULL}")
                .unwrap(),
            Cell::Array(ArrayCell::I32(vec![Some(i32::MIN), Some(i32::MAX), None]))
        );
        assert_eq!(
            parse_cell_from_postgres_text(
                &Type::INT8_ARRAY,
                "{-9223372036854775808,9223372036854775807,NULL}",
            )
            .unwrap(),
            Cell::Array(ArrayCell::I64(vec![Some(i64::MIN), Some(i64::MAX), None]))
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::OID_ARRAY, "{0,4294967295,NULL}").unwrap(),
            Cell::Array(ArrayCell::U32(vec![Some(0), Some(u32::MAX), None]))
        );
    }

    #[test]
    fn try_from_str_floats() {
        let cell = parse_cell_from_postgres_text(&Type::FLOAT4, "3.15").unwrap();
        assert_eq!(cell, Cell::F32(3.15));

        let cell = parse_cell_from_postgres_text(&Type::FLOAT8, "-2.818").unwrap();
        assert_eq!(cell, Cell::F64(-2.818));

        let cell = parse_cell_from_postgres_text(&Type::FLOAT4, "inf").unwrap();
        assert_eq!(cell, Cell::F32(f32::INFINITY));

        let cell = parse_cell_from_postgres_text(&Type::FLOAT8, "NaN").unwrap();
        assert!(matches!(cell, Cell::F64(val) if val.is_nan()));
    }

    #[test]
    fn try_from_str_float_boundaries() {
        assert_eq!(
            parse_cell_from_postgres_text(&Type::FLOAT4, "3.4028235e38").unwrap(),
            Cell::F32(f32::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::FLOAT4, "-3.4028235e38").unwrap(),
            Cell::F32(-f32::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::FLOAT8, "1.7976931348623157e308").unwrap(),
            Cell::F64(f64::MAX)
        );
        assert_eq!(
            parse_cell_from_postgres_text(&Type::FLOAT8, "-1.7976931348623157e308").unwrap(),
            Cell::F64(-f64::MAX)
        );
    }

    #[test]
    fn try_from_str_float_array_boundaries() {
        let cell = parse_cell_from_postgres_text(
            &Type::FLOAT4_ARRAY,
            "{-3.4028235e38,3.4028235e38,NaN,Infinity,-Infinity,NULL}",
        )
        .unwrap();
        if let Cell::Array(ArrayCell::F32(values)) = cell {
            assert_eq!(values[0], Some(-f32::MAX));
            assert_eq!(values[1], Some(f32::MAX));
            assert!(values[2].is_some_and(f32::is_nan));
            assert_eq!(values[3], Some(f32::INFINITY));
            assert_eq!(values[4], Some(f32::NEG_INFINITY));
            assert_eq!(values[5], None);
        } else {
            panic!("Expected FLOAT4 array");
        }

        let cell = parse_cell_from_postgres_text(
            &Type::FLOAT8_ARRAY,
            "{-1.7976931348623157e308,1.7976931348623157e308,NaN,Infinity,-Infinity,NULL}",
        )
        .unwrap();
        if let Cell::Array(ArrayCell::F64(values)) = cell {
            assert_eq!(values[0], Some(-f64::MAX));
            assert_eq!(values[1], Some(f64::MAX));
            assert!(values[2].is_some_and(f64::is_nan));
            assert_eq!(values[3], Some(f64::INFINITY));
            assert_eq!(values[4], Some(f64::NEG_INFINITY));
            assert_eq!(values[5], None);
        } else {
            panic!("Expected FLOAT8 array");
        }
    }

    #[test]
    fn try_from_str_string_types() {
        let test_string = "Hello, World!";

        let cell = parse_cell_from_postgres_text(&Type::TEXT, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_owned()));

        let cell = parse_cell_from_postgres_text(&Type::VARCHAR, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_owned()));

        let cell = parse_cell_from_postgres_text(&Type::CHAR, test_string).unwrap();
        assert_eq!(cell, Cell::String(test_string.to_owned()));

        let cell = parse_cell_from_postgres_text(&Type::MONEY, "$1,234.56").unwrap();
        assert_eq!(cell, Cell::String("$1,234.56".to_owned()));
    }

    #[test]
    fn try_from_str_numeric() {
        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "123.45").unwrap();
        if let Cell::Numeric(num) = cell {
            assert_eq!(num.to_string(), "123.45");
        } else {
            panic!("Expected Numeric cell");
        }

        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "NaN").unwrap();
        assert_eq!(cell, Cell::Numeric(PgNumeric::NaN));

        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "Infinity").unwrap();
        assert_eq!(cell, Cell::Numeric(PgNumeric::PositiveInfinity));

        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "-Infinity").unwrap();
        assert_eq!(cell, Cell::Numeric(PgNumeric::NegativeInfinity));
    }

    #[test]
    fn try_from_str_numeric_postgres_range_boundaries() {
        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "1e131071").unwrap();
        if let Cell::Numeric(PgNumeric::Value { weight, scale, digits, .. }) = cell {
            assert_eq!(weight, i16::MAX);
            assert_eq!(scale, 0);
            assert_eq!(digits, vec![1000]);
        } else {
            panic!("Expected Numeric cell");
        }

        let cell = parse_cell_from_postgres_text(&Type::NUMERIC, "1e-16383").unwrap();
        if let Cell::Numeric(PgNumeric::Value { weight, scale, digits, .. }) = cell {
            assert_eq!(weight, -4096);
            assert_eq!(scale, 16_383);
            assert_eq!(digits, vec![10]);
        } else {
            panic!("Expected Numeric cell");
        }

        assert!(parse_cell_from_postgres_text(&Type::NUMERIC, "1e131072").is_err());
        assert!(parse_cell_from_postgres_text(&Type::NUMERIC, "1e-16384").is_err());
    }

    #[test]
    fn try_from_str_numeric_array_special_values() {
        let cell =
            parse_cell_from_postgres_text(&Type::NUMERIC_ARRAY, "{-Infinity,NaN,NULL,123.45}")
                .unwrap();
        assert_eq!(
            cell,
            Cell::Array(ArrayCell::Numeric(vec![
                Some(PgNumeric::NegativeInfinity),
                Some(PgNumeric::NaN),
                None,
                Some("123.45".parse().unwrap()),
            ]))
        );
    }

    #[test]
    fn try_from_str_bytea() {
        let cell = parse_cell_from_postgres_text(&Type::BYTEA, "\\x48656c6c6f").unwrap();
        assert_eq!(cell, Cell::Bytes(b"Hello".to_vec()));

        assert!(parse_cell_from_postgres_text(&Type::BYTEA, "invalid").is_err());
    }

    #[test]
    fn try_from_str_dates() {
        let cell = parse_cell_from_postgres_text(&Type::DATE, "2023-12-25").unwrap();
        if let Cell::Date(date) = cell {
            assert_eq!(date.year(), 2023);
            assert_eq!(date.month(), 12);
            assert_eq!(date.day(), 25);
        } else {
            panic!("Expected Date cell");
        }

        assert!(parse_cell_from_postgres_text(&Type::DATE, "invalid-date").is_err());
    }

    #[test]
    fn try_from_str_time() {
        let cell = parse_cell_from_postgres_text(&Type::TIME, "14:30:45.123").unwrap();
        if let Cell::Time(time) = cell {
            assert_eq!(time.hour(), 14);
            assert_eq!(time.minute(), 30);
            assert_eq!(time.second(), 45);
        } else {
            panic!("Expected Time cell");
        }

        assert!(parse_cell_from_postgres_text(&Type::TIME, "invalid-time").is_err());
    }

    #[test]
    fn try_from_str_timestamp() {
        let cell =
            parse_cell_from_postgres_text(&Type::TIMESTAMP, "2023-12-25 14:30:45.123").unwrap();
        if let Cell::Timestamp(ts) = cell {
            assert_eq!(ts.date().year(), 2023);
            assert_eq!(ts.time().hour(), 14);
        } else {
            panic!("Expected TimeStamp cell");
        }
    }

    #[test]
    fn try_from_str_timestamptz() {
        let cell =
            parse_cell_from_postgres_text(&Type::TIMESTAMPTZ, "2023-12-25 14:30:45.123+00:00")
                .unwrap();
        if let Cell::TimestampTz(ts) = cell {
            assert_eq!(ts.year(), 2023);
        } else {
            panic!("Expected TimeStampTz cell");
        }

        // Test fallback format
        let cell = parse_cell_from_postgres_text(&Type::TIMESTAMPTZ, "2023-12-25 14:30:45.123+00")
            .unwrap();
        assert!(matches!(cell, Cell::TimestampTz(_)));
    }

    #[test]
    fn try_from_str_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let cell = parse_cell_from_postgres_text(&Type::UUID, uuid_str).unwrap();
        if let Cell::Uuid(uuid) = cell {
            assert_eq!(uuid.to_string(), uuid_str);
        } else {
            panic!("Expected Uuid cell");
        }

        assert!(parse_cell_from_postgres_text(&Type::UUID, "invalid-uuid").is_err());
    }

    #[test]
    fn try_from_str_json() {
        let json_str = r#"{"key": "value", "number": 42}"#;
        let cell = parse_cell_from_postgres_text(&Type::JSON, json_str).unwrap();
        if let Cell::String(json) = cell {
            assert_eq!(json, json_str);
        } else {
            panic!("Expected string cell");
        }

        let cell = parse_cell_from_postgres_text(&Type::JSONB, json_str).unwrap();
        assert_eq!(cell, Cell::String(json_str.to_owned()));

        assert!(parse_cell_from_postgres_text(&Type::JSON, "invalid json").is_err());
    }

    #[test]
    fn try_from_str_json_accepts_wide_number_literals() {
        let json_str = r#"{"value":1e309}"#;
        let cell = parse_cell_from_postgres_text(&Type::JSON, json_str).unwrap();
        if let Cell::String(json) = cell {
            assert_eq!(json, json_str);
        } else {
            panic!("Expected string cell");
        }

        let cell = parse_cell_from_postgres_text(&Type::JSONB, json_str).unwrap();
        assert_eq!(cell, Cell::String(json_str.to_owned()));
    }

    #[test]
    fn parse_array_basic() {
        let cell = parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{1,2,3}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert_eq!(v, vec![Some(1), Some(2), Some(3)]);
            }
            _ => panic!("Expected INT4 array"),
        }
    }

    #[test]
    fn parse_array_with_nulls() {
        let cell = parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{1,NULL,3}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert_eq!(v, vec![Some(1), None, Some(3)]);
            }
            _ => panic!("Expected INT4 array"),
        }
    }

    #[test]
    fn parse_array_quoted_strings() {
        let cell = parse_cell_from_postgres_text(
            &Type::TEXT_ARRAY,
            r#"{"hello","world with spaces","with\"quotes"}"#,
        )
        .unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![
                        Some("hello".to_owned()),
                        Some("world with spaces".to_owned()),
                        Some("with\"quotes".to_owned())
                    ]
                );
            }
            _ => panic!("Expected TEXT array"),
        }
    }

    #[test]
    fn parse_money_array_preserves_text_values() {
        let cell =
            parse_cell_from_postgres_text(&Type::MONEY_ARRAY, r#"{"$1,234.56",NULL,"-$0.01"}"#)
                .unwrap();
        assert_eq!(
            cell,
            Cell::Array(ArrayCell::String(vec![
                Some("$1,234.56".to_owned()),
                None,
                Some("-$0.01".to_owned()),
            ]))
        );
    }

    #[test]
    fn unsupported_builtin_arrays_preserve_array_shape_as_strings() {
        let cell =
            parse_cell_from_postgres_text(&Type::INTERVAL_ARRAY, r#"{"1 day",NULL,"2 hours"}"#)
                .unwrap();
        assert_eq!(
            cell,
            Cell::Array(ArrayCell::String(vec![
                Some("1 day".to_owned()),
                None,
                Some("2 hours".to_owned()),
            ]))
        );

        let cell =
            parse_cell_from_postgres_text(&Type::INET_ARRAY, r#"{127.0.0.1,NULL,192.168.0.1}"#)
                .unwrap();
        assert_eq!(
            cell,
            Cell::Array(ArrayCell::String(vec![
                Some("127.0.0.1".to_owned()),
                None,
                Some("192.168.0.1".to_owned()),
            ]))
        );
    }

    #[test]
    fn parse_array_empty() {
        let cell = parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{}").unwrap();
        match cell {
            Cell::Array(ArrayCell::I32(v)) => {
                assert!(v.is_empty());
            }
            _ => panic!("Expected empty INT4 array"),
        }
    }

    #[test]
    fn parse_array_single_element() {
        let cell = parse_cell_from_postgres_text(&Type::BOOL_ARRAY, "{t}").unwrap();
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
        assert!(parse_cell_from_postgres_text(&Type::INT4_ARRAY, "1,2,3}").is_err());

        // Missing closing brace
        assert!(parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{1,2,3").is_err());

        // Too short
        assert!(parse_cell_from_postgres_text(&Type::INT4_ARRAY, "{").is_err());
        assert!(parse_cell_from_postgres_text(&Type::INT4_ARRAY, "}").is_err());
        assert!(parse_cell_from_postgres_text(&Type::INT4_ARRAY, "").is_err());
    }

    #[test]
    fn parse_array_escape_sequences() {
        // The array parser doesn't process escape sequences in the same way as the
        // table row parser It expects literal characters in the array string
        let cell =
            parse_cell_from_postgres_text(&Type::TEXT_ARRAY, r#"{"line1\\nline2","tab\\there"}"#)
                .unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                // These should be literal strings since array parser doesn't decode escapes
                // like table parser
                assert_eq!(
                    v,
                    vec![Some("line1\\nline2".to_owned()), Some("tab\\there".to_owned())]
                );
            }
            _ => panic!("Expected TEXT array with escape sequences"),
        }
    }

    #[test]
    fn parse_timestamptz_array_fallback() {
        // Test the fallback parsing for timestamptz arrays
        let cell = parse_cell_from_postgres_text(
            &Type::TIMESTAMPTZ_ARRAY,
            "{\"2023-01-01 12:00:00.000+00\"}",
        )
        .unwrap();
        match cell {
            Cell::Array(ArrayCell::TimestampTz(v)) => {
                assert_eq!(v.len(), 1);
                assert!(v[0].is_some());
            }
            _ => panic!("Expected TIMESTAMPTZ array"),
        }
    }

    #[test]
    fn unknown_types_to_string() {
        use tokio_postgres::types::Type;
        // Create a custom type that's not normally supported
        let custom_type = Type::new(
            "custom".to_owned(),
            99999,
            tokio_postgres::types::Kind::Simple,
            "public".to_owned(),
        );

        let cell = parse_cell_from_postgres_text(&custom_type, "test").unwrap();
        assert_eq!(cell, Cell::String("test".to_owned()));
    }
}
