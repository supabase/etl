use std::sync::LazyLock;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use etl::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{
        ArrayCellNonOptional, CellNonOptional, DATE_FORMAT, PgNumeric, TIME_FORMAT,
        TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM,
    },
};

/// BigQuery BIGNUMERIC precision description for validation errors.
///
/// Source: <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types>.
const BIGQUERY_BIGNUMERIC_PRECISION_DESCRIPTION: &str =
    "approximately 76.8 digits, where the 77th digit is partial";

/// BigQuery BIGNUMERIC maximum decimal places.
///
/// Source: <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types>.
const BIGQUERY_BIGNUMERIC_MAX_SCALE: usize = 38;

/// BigQuery BIGNUMERIC maximum supported positive value.
///
/// This is the exact decimal expansion of the documented scientific-notation
/// maximum.
const BIGQUERY_BIGNUMERIC_MAX: &str =
    "578960446186580977117854925043439539266.34992332820282019728792003956564819967";

/// BigQuery BIGNUMERIC maximum supported absolute negative value.
///
/// This is the exact decimal expansion of the documented scientific-notation
/// minimum's absolute value.
const BIGQUERY_BIGNUMERIC_MIN_ABS: &str =
    "578960446186580977117854925043439539266.34992332820282019728792003956564819968";

/// BigQuery JSON maximum nesting depth.
///
/// Source: <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#json_type>.
const BIGQUERY_JSON_MAX_NESTING_DEPTH: usize = 500;

/// BigQuery DATE minimum value: 0001-01-01.
const BIGQUERY_DATE_MIN: (i32, u32, u32) = (1, 1, 1);

/// BigQuery DATE maximum value: 9999-12-31.
const BIGQUERY_DATE_MAX: (i32, u32, u32) = (9999, 12, 31);

/// BigQuery TIME minimum value: 00:00:00.
const BIGQUERY_TIME_MIN: (u32, u32, u32) = (0, 0, 0);

/// BigQuery TIME maximum value: 23:59:59.999999.
const BIGQUERY_TIME_MAX: (u32, u32, u32, u32) = (23, 59, 59, 999999);

/// Static minimum BigQuery date value (0001-01-01).
static BIGQUERY_MIN_DATE: LazyLock<NaiveDate> = LazyLock::new(|| {
    NaiveDate::from_ymd_opt(BIGQUERY_DATE_MIN.0, BIGQUERY_DATE_MIN.1, BIGQUERY_DATE_MIN.2)
        .expect("BigQuery minimum date should be valid")
});

/// Static maximum BigQuery date value (9999-12-31).
static BIGQUERY_MAX_DATE: LazyLock<NaiveDate> = LazyLock::new(|| {
    NaiveDate::from_ymd_opt(BIGQUERY_DATE_MAX.0, BIGQUERY_DATE_MAX.1, BIGQUERY_DATE_MAX.2)
        .expect("BigQuery maximum date should be valid")
});

/// Static minimum BigQuery time value (00:00:00).
static BIGQUERY_MIN_TIME: LazyLock<NaiveTime> = LazyLock::new(|| {
    NaiveTime::from_hms_opt(BIGQUERY_TIME_MIN.0, BIGQUERY_TIME_MIN.1, BIGQUERY_TIME_MIN.2)
        .expect("BigQuery minimum time should be valid")
});

/// Static maximum BigQuery time value (23:59:59.999999).
static BIGQUERY_MAX_TIME: LazyLock<NaiveTime> = LazyLock::new(|| {
    NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    )
    .expect("BigQuery maximum time should be valid")
});

/// Static minimum BigQuery datetime value (0001-01-01 00:00:00).
static BIGQUERY_MIN_DATETIME: LazyLock<NaiveDateTime> =
    LazyLock::new(|| NaiveDateTime::new(*BIGQUERY_MIN_DATE, *BIGQUERY_MIN_TIME));

/// Static maximum BigQuery datetime value (9999-12-31 23:59:59.999999).
static BIGQUERY_MAX_DATETIME: LazyLock<NaiveDateTime> =
    LazyLock::new(|| NaiveDateTime::new(*BIGQUERY_MAX_DATE, *BIGQUERY_MAX_TIME));

/// Static minimum BigQuery timestamp value (0001-01-01 00:00:00 UTC).
static BIGQUERY_MIN_TIMESTAMP: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.from_utc_datetime(&BIGQUERY_MIN_DATETIME));

/// Static maximum BigQuery timestamp value (9999-12-31 23:59:59.999999 UTC).
static BIGQUERY_MAX_TIMESTAMP: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.from_utc_datetime(&BIGQUERY_MAX_DATETIME));

/// Validates that a [`PgNumeric`] value is within BigQuery's BIGNUMERIC
/// supported range.
///
/// Returns an error if the value is outside BigQuery's supported range instead
/// of clamping. BigQuery BIGNUMERIC supports up to ~77 digits of precision with
/// 38 digits after the decimal point.
fn validate_numeric_for_bigquery(numeric: &PgNumeric) -> EtlResult<()> {
    match numeric {
        PgNumeric::NaN => {
            bail!(
                ErrorKind::UnsupportedValueInDestination,
                "BigQuery NUMERIC/BIGNUMERIC does not support NaN values",
                "NaN cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::PositiveInfinity => {
            bail!(
                ErrorKind::UnsupportedValueInDestination,
                "BigQuery NUMERIC/BIGNUMERIC does not support infinity values",
                "Infinity cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::NegativeInfinity => {
            bail!(
                ErrorKind::UnsupportedValueInDestination,
                "BigQuery NUMERIC/BIGNUMERIC does not support infinity values",
                "Infinity cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::Value { .. } => {
            if !is_numeric_within_bigquery_bignumeric_limits(numeric) {
                bail!(
                    ErrorKind::UnsupportedValueInDestination,
                    "Numeric value exceeds BigQuery BIGNUMERIC limits",
                    format!(
                        "A numeric value exceeds BigQuery's BIGNUMERIC limits (max {}, {} decimal \
                         places)",
                        BIGQUERY_BIGNUMERIC_PRECISION_DESCRIPTION, BIGQUERY_BIGNUMERIC_MAX_SCALE
                    )
                );
            }

            Ok(())
        }
    }
}

/// Validates that a JSON value is within BigQuery's JSON domain.
fn validate_json_for_bigquery(json: &serde_json::Value) -> EtlResult<()> {
    validate_json_for_bigquery_with_depth(json, 1)
}

/// Validates a JSON value while tracking nesting depth.
fn validate_json_for_bigquery_with_depth(json: &serde_json::Value, depth: usize) -> EtlResult<()> {
    if depth > BIGQUERY_JSON_MAX_NESTING_DEPTH {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "JSON value exceeds BigQuery nesting limit",
            format!(
                "A JSON value exceeds BigQuery's maximum nesting depth of {} levels",
                BIGQUERY_JSON_MAX_NESTING_DEPTH
            )
        );
    }

    match json {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            Ok(())
        }
        serde_json::Value::Number(number) => validate_json_number_for_bigquery(number),
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_json_for_bigquery_with_depth(value, depth + 1).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "JSON array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for (index, value) in values.values().enumerate() {
                validate_json_for_bigquery_with_depth(value, depth + 1).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "JSON object field validation failed",
                        format!("Object field at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
    }
}

/// Validates that a JSON number is within BigQuery's JSON numeric domain.
fn validate_json_number_for_bigquery(number: &serde_json::Number) -> EtlResult<()> {
    let number = number.to_string();
    let valid = if is_json_integer_literal(&number) {
        if number.starts_with('-') {
            number.parse::<i64>().is_ok()
        } else {
            number.parse::<u64>().is_ok()
        }
    } else {
        number.parse::<f64>().is_ok_and(f64::is_finite)
    };

    if valid {
        Ok(())
    } else {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "JSON number exceeds BigQuery JSON limits",
            "A JSON number is outside BigQuery's JSON numeric domain"
        );
    }
}

/// Returns whether a JSON number literal is an integer literal.
fn is_json_integer_literal(number: &str) -> bool {
    !number.contains(['.', 'e', 'E'])
}

/// Validates that a [`NaiveDate`] is within BigQuery's supported range.
///
/// Returns an error if the date is outside BigQuery's supported range instead
/// of clamping. BigQuery DATE supports values from 0001-01-01 to 9999-12-31.
fn validate_date_for_bigquery(date: &NaiveDate) -> EtlResult<()> {
    if *date < *BIGQUERY_MIN_DATE {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Date value is before BigQuery's minimum supported date",
            format!(
                "A date value is before BigQuery's minimum supported date '{}'. BigQuery DATE \
                 supports values from 0001-01-01 to 9999-12-31",
                BIGQUERY_MIN_DATE.format(DATE_FORMAT)
            )
        );
    }

    if *date > *BIGQUERY_MAX_DATE {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Date value is after BigQuery's maximum supported date",
            format!(
                "A date value is after BigQuery's maximum supported date '{}'. BigQuery DATE \
                 supports values from 0001-01-01 to 9999-12-31",
                BIGQUERY_MAX_DATE.format(DATE_FORMAT)
            )
        );
    }

    Ok(())
}

/// Validates that a [`NaiveTime`] is within BigQuery's supported range.
///
/// Returns an error if the time is outside BigQuery's supported range instead
/// of clamping. BigQuery TIME supports values from 00:00:00 to 23:59:59.999999.
fn validate_time_for_bigquery(time: &NaiveTime) -> EtlResult<()> {
    if *time < *BIGQUERY_MIN_TIME {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Time value is before BigQuery's minimum supported time",
            format!(
                "A time value is before BigQuery's minimum supported time '{}'. BigQuery TIME \
                 supports values from 00:00:00 to 23:59:59.999999",
                BIGQUERY_MIN_TIME.format(TIME_FORMAT)
            )
        );
    }

    if *time > *BIGQUERY_MAX_TIME {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Time value is after BigQuery's maximum supported time",
            format!(
                "A time value is after BigQuery's maximum supported time '{}'. BigQuery TIME \
                 supports values from 00:00:00 to 23:59:59.999999",
                BIGQUERY_MAX_TIME.format(TIME_FORMAT)
            )
        );
    }

    Ok(())
}

/// Validates that a [`NaiveDateTime`] is within BigQuery's supported range.
///
/// Returns an error if the datetime is outside BigQuery's supported range
/// instead of clamping. BigQuery DATETIME supports values from 0001-01-01
/// 00:00:00 to 9999-12-31 23:59:59.999999.
fn validate_datetime_for_bigquery(datetime: &NaiveDateTime) -> EtlResult<()> {
    if *datetime < *BIGQUERY_MIN_DATETIME {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "DateTime value is before BigQuery's minimum supported datetime",
            format!(
                "A datetime value is before BigQuery's minimum supported datetime '{}'. BigQuery \
                 DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999",
                BIGQUERY_MIN_DATETIME.format(TIMESTAMP_FORMAT)
            )
        );
    }

    if *datetime > *BIGQUERY_MAX_DATETIME {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "DateTime value is after BigQuery's maximum supported datetime",
            format!(
                "A datetime value is after BigQuery's maximum supported datetime '{}'. BigQuery \
                 DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999",
                BIGQUERY_MAX_DATETIME.format(TIMESTAMP_FORMAT)
            )
        );
    }

    Ok(())
}

/// Validates that a [`DateTime<Utc>`] is within BigQuery's supported range.
///
/// Returns an error if the timestamp is outside BigQuery's supported range
/// instead of clamping. BigQuery TIMESTAMP supports values from 0001-01-01
/// 00:00:00 UTC to 9999-12-31 23:59:59.999999 UTC.
fn validate_timestamptz_for_bigquery(timestamptz: &DateTime<Utc>) -> EtlResult<()> {
    if *timestamptz < *BIGQUERY_MIN_TIMESTAMP {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Timestamp value is before BigQuery's minimum supported timestamp",
            format!(
                "A timestamp value is before BigQuery's minimum supported timestamp '{}'. \
                 BigQuery TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 \
                 23:59:59.999999 UTC",
                BIGQUERY_MIN_TIMESTAMP.format(TIMESTAMPTZ_FORMAT_HH_MM)
            )
        );
    }

    if *timestamptz > *BIGQUERY_MAX_TIMESTAMP {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Timestamp value is after BigQuery's maximum supported timestamp",
            format!(
                "A timestamp value is after BigQuery's maximum supported timestamp '{}'. BigQuery \
                 TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 \
                 23:59:59.999999 UTC",
                BIGQUERY_MAX_TIMESTAMP.format(TIMESTAMPTZ_FORMAT_HH_MM)
            )
        );
    }

    Ok(())
}

/// Validates that a [`CellNonOptional`] value is within BigQuery's supported
/// ranges.
///
/// Returns an error if any value is outside BigQuery's supported range for its
/// type. This function checks all temporal types and numeric types for BigQuery
/// compatibility.
pub(super) fn validate_cell_for_bigquery(cell: &CellNonOptional) -> EtlResult<()> {
    match cell {
        CellNonOptional::Null => Ok(()),
        CellNonOptional::Bool(_) => Ok(()),
        CellNonOptional::String(_) => Ok(()),
        CellNonOptional::I16(_) => Ok(()),
        CellNonOptional::I32(_) => Ok(()),
        CellNonOptional::U32(_) => Ok(()),
        CellNonOptional::I64(_) => Ok(()),
        CellNonOptional::F32(_) => Ok(()),
        CellNonOptional::F64(_) => Ok(()),
        CellNonOptional::Numeric(numeric) => validate_numeric_for_bigquery(numeric),
        CellNonOptional::Date(date) => validate_date_for_bigquery(date),
        CellNonOptional::Time(time) => validate_time_for_bigquery(time),
        CellNonOptional::Timestamp(datetime) => validate_datetime_for_bigquery(datetime),
        CellNonOptional::TimestampTz(timestamptz) => validate_timestamptz_for_bigquery(timestamptz),
        CellNonOptional::Uuid(_) => Ok(()),
        CellNonOptional::Json(json) => validate_json_for_bigquery(json),
        CellNonOptional::Bytes(_) => Ok(()),
        CellNonOptional::Array(array) => validate_array_cell_for_bigquery(array),
    }
}

/// Validates that an [`ArrayCellNonOptional`] contains values within BigQuery's
/// supported ranges.
///
/// Returns an error if any array element is outside BigQuery's supported range
/// for its type.
fn validate_array_cell_for_bigquery(array_cell: &ArrayCellNonOptional) -> EtlResult<()> {
    match array_cell {
        ArrayCellNonOptional::Bool(_) => Ok(()),
        ArrayCellNonOptional::String(_) => Ok(()),
        ArrayCellNonOptional::I16(_) => Ok(()),
        ArrayCellNonOptional::I32(_) => Ok(()),
        ArrayCellNonOptional::U32(_) => Ok(()),
        ArrayCellNonOptional::I64(_) => Ok(()),
        ArrayCellNonOptional::F32(_) => Ok(()),
        ArrayCellNonOptional::F64(_) => Ok(()),
        ArrayCellNonOptional::Numeric(numerics) => {
            for (index, numeric) in numerics.iter().enumerate() {
                validate_numeric_for_bigquery(numeric).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Date(dates) => {
            for (index, date) in dates.iter().enumerate() {
                validate_date_for_bigquery(date).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Time(times) => {
            for (index, time) in times.iter().enumerate() {
                validate_time_for_bigquery(time).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Timestamp(datetimes) => {
            for (index, datetime) in datetimes.iter().enumerate() {
                validate_datetime_for_bigquery(datetime).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::TimestampTz(timestamptzs) => {
            for (index, timestamptz) in timestamptzs.iter().enumerate() {
                validate_timestamptz_for_bigquery(timestamptz).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Uuid(_) => Ok(()),
        ArrayCellNonOptional::Json(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_json_for_bigquery(value).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Bytes(_) => Ok(()),
    }
}

/// Checks if a [`PgNumeric`] as string is within BigQuery's BIGNUMERIC limits.
///
/// BIGNUMERIC supports up to ~77 digits of precision with up to 38 decimal
/// places.
///
/// The rationale for checking only for BIGNUMERIC is that this is the type
/// conversion that we perform in `postgres_to_bigquery_type`.
fn is_numeric_within_bigquery_bignumeric_limits(pg_numeric: &PgNumeric) -> bool {
    let numeric_str = pg_numeric.to_string();
    let Some(numeric) = BigQueryDecimalParts::parse(&numeric_str) else {
        return false;
    };

    if numeric.fractional.len() > BIGQUERY_BIGNUMERIC_MAX_SCALE {
        return false;
    }

    let max =
        if numeric.is_negative { BIGQUERY_BIGNUMERIC_MIN_ABS } else { BIGQUERY_BIGNUMERIC_MAX };

    numeric.absolute_value_is_at_most(max)
}

/// Parsed decimal parts used for BigQuery BIGNUMERIC range checks.
struct BigQueryDecimalParts<'a> {
    /// Whether the decimal is negative.
    is_negative: bool,
    /// Decimal digits before the decimal point.
    integer: &'a str,
    /// Decimal digits after the decimal point.
    fractional: &'a str,
}

impl<'a> BigQueryDecimalParts<'a> {
    /// Parses a canonical decimal string into sign, integer, and fractional
    /// parts.
    fn parse(value: &'a str) -> Option<Self> {
        let (is_negative, value) =
            value.strip_prefix('-').map_or((false, value), |value| (true, value));
        let (integer, fractional) = value.split_once('.').unwrap_or((value, ""));

        if integer.chars().all(|ch| ch.is_ascii_digit())
            && fractional.chars().all(|ch| ch.is_ascii_digit())
        {
            Some(Self { is_negative, integer, fractional })
        } else {
            None
        }
    }

    /// Returns whether the absolute value is less than or equal to a bound.
    fn absolute_value_is_at_most(&self, bound: &'a str) -> bool {
        let bound = Self::parse(bound).expect("BigQuery BIGNUMERIC bound should be a decimal");

        let integer = normalize_integer_digits(self.integer);
        let bound_integer = normalize_integer_digits(bound.integer);

        match integer.len().cmp(&bound_integer.len()) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }

        match integer.cmp(bound_integer) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }

        let mut fractional = self.fractional.to_owned();
        let mut bound_fractional = bound.fractional.to_owned();
        fractional.push_str(&"0".repeat(BIGQUERY_BIGNUMERIC_MAX_SCALE - fractional.len()));
        bound_fractional
            .push_str(&"0".repeat(BIGQUERY_BIGNUMERIC_MAX_SCALE - bound_fractional.len()));

        fractional <= bound_fractional
    }
}

/// Normalizes integer digits for decimal magnitude comparisons.
fn normalize_integer_digits(integer: &str) -> &str {
    let normalized = integer.trim_start_matches('0');
    if normalized.is_empty() { "0" } else { normalized }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn validate_numeric_within_bounds() {
        let numeric = PgNumeric::from_str("123.456").unwrap();
        assert!(validate_numeric_for_bigquery(&numeric).is_ok());
    }

    #[test]
    fn validate_numeric_accepts_bigquery_bignumeric_boundaries() {
        for value in [
            "0",
            "0.00000000000000000000000000000000000001",
            BIGQUERY_BIGNUMERIC_MAX,
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
        ] {
            let numeric = PgNumeric::from_str(value).unwrap();
            assert!(validate_numeric_for_bigquery(&numeric).is_ok(), "{value}");
        }
    }

    #[test]
    fn validate_numeric_rejects_values_outside_bigquery_bignumeric_boundaries() {
        for value in [
            "0.000000000000000000000000000000000000001",
            "578960446186580977117854925043439539266.34992332820282019728792003956564819968",
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819969",
            "999999999999999999999999999999999999999",
        ] {
            let numeric = PgNumeric::from_str(value).unwrap();
            let result = validate_numeric_for_bigquery(&numeric);
            assert!(result.is_err(), "{value}");
            assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
        }
    }

    #[test]
    fn validate_numeric_nan_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::NaN);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("NaN cannot be stored in BigQuery"));
    }

    #[test]
    fn validate_numeric_positive_infinity_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::PositiveInfinity);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Infinity cannot be stored in BigQuery"));
    }

    #[test]
    fn validate_numeric_negative_infinity_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::NegativeInfinity);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Infinity cannot be stored in BigQuery"));
    }

    #[test]
    fn validate_date_within_bounds() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert!(validate_date_for_bigquery(&date).is_ok());
    }

    #[test]
    fn validate_temporal_values_accept_bigquery_boundaries() {
        let min_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        let max_date = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();
        let min_time = NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap();
        let max_time = NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap();
        let min_datetime = NaiveDateTime::new(min_date, min_time);
        let max_datetime = NaiveDateTime::new(max_date, max_time);
        let min_timestamp = Utc.from_utc_datetime(&min_datetime);
        let max_timestamp = Utc.from_utc_datetime(&max_datetime);

        assert!(validate_date_for_bigquery(&min_date).is_ok());
        assert!(validate_date_for_bigquery(&max_date).is_ok());
        assert!(validate_time_for_bigquery(&min_time).is_ok());
        assert!(validate_time_for_bigquery(&max_time).is_ok());
        assert!(validate_datetime_for_bigquery(&min_datetime).is_ok());
        assert!(validate_datetime_for_bigquery(&max_datetime).is_ok());
        assert!(validate_timestamptz_for_bigquery(&min_timestamp).is_ok());
        assert!(validate_timestamptz_for_bigquery(&max_timestamp).is_ok());
    }

    #[test]
    fn validate_date_before_min_fails() {
        let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();
        let result = validate_date_for_bigquery(&date);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("before BigQuery's minimum"));
    }

    #[test]
    fn validate_date_after_max_fails() {
        let date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
        let result = validate_date_for_bigquery(&date);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("after BigQuery's maximum"));
    }

    #[test]
    fn validate_timestamp_after_max_fails() {
        let datetime =
            NaiveDate::from_ymd_opt(10000, 1, 1).unwrap().and_hms_micro_opt(0, 0, 0, 0).unwrap();
        let timestamptz = Utc.from_utc_datetime(&datetime);

        assert!(validate_datetime_for_bigquery(&datetime).is_err());
        assert!(validate_timestamptz_for_bigquery(&timestamptz).is_err());
    }

    #[test]
    fn validate_json_accepts_bigquery_numeric_domain_boundaries() {
        for value in [
            r#"{"value":-9223372036854775808}"#,
            r#"{"value":18446744073709551615}"#,
            r#"{"value":1.7976931348623157e308}"#,
        ] {
            let json = serde_json::from_str(value).unwrap();
            assert!(validate_json_for_bigquery(&json).is_ok(), "{value}");
        }
    }

    #[test]
    fn validate_json_rejects_numbers_outside_bigquery_numeric_domain() {
        for value in [
            r#"{"value":-9223372036854775809}"#,
            r#"{"value":18446744073709551616}"#,
            r#"{"value":1e309}"#,
        ] {
            let json = serde_json::from_str(value).unwrap();
            let result = validate_json_for_bigquery(&json);
            assert!(result.is_err(), "{value}");
            assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
        }
    }

    #[test]
    fn validate_json_array_reports_invalid_element_index() {
        let json = serde_json::from_str(r#"[1, 1e309]"#).unwrap();
        let result = validate_json_for_bigquery(&json);
        assert!(result.is_err());
        assert!(result.unwrap_err().detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn validate_cell_for_bigquery_valid_types() {
        assert!(validate_cell_for_bigquery(&CellNonOptional::Null).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::Bool(true)).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::String("test".to_owned())).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::I32(42)).is_ok());
        assert!(
            validate_cell_for_bigquery(&CellNonOptional::Json(serde_json::json!({
                "value": 18446744073709551615u64
            })))
            .is_ok()
        );
    }

    #[test]
    fn validate_cell_for_bigquery_accepts_postgres_numeric_family_boundaries() {
        for cell in [
            CellNonOptional::I16(i16::MIN),
            CellNonOptional::I16(i16::MAX),
            CellNonOptional::I32(i32::MIN),
            CellNonOptional::I32(i32::MAX),
            CellNonOptional::U32(0),
            CellNonOptional::U32(u32::MAX),
            CellNonOptional::I64(i64::MIN),
            CellNonOptional::I64(i64::MAX),
            CellNonOptional::F32(-f32::MAX),
            CellNonOptional::F32(f32::MAX),
            CellNonOptional::F32(f32::NAN),
            CellNonOptional::F32(f32::INFINITY),
            CellNonOptional::F32(f32::NEG_INFINITY),
            CellNonOptional::F64(-f64::MAX),
            CellNonOptional::F64(f64::MAX),
            CellNonOptional::F64(f64::NAN),
            CellNonOptional::F64(f64::INFINITY),
            CellNonOptional::F64(f64::NEG_INFINITY),
            CellNonOptional::Numeric(PgNumeric::from_str(BIGQUERY_BIGNUMERIC_MAX).unwrap()),
            CellNonOptional::Numeric(
                PgNumeric::from_str(
                    "-578960446186580977117854925043439539266.\
                     34992332820282019728792003956564819968",
                )
                .unwrap(),
            ),
        ] {
            assert!(validate_cell_for_bigquery(&cell).is_ok(), "{cell:?}");
        }
    }

    #[test]
    fn validate_array_cell_for_bigquery_accepts_postgres_numeric_family_boundaries() {
        for array in [
            ArrayCellNonOptional::I16(vec![i16::MIN, i16::MAX]),
            ArrayCellNonOptional::I32(vec![i32::MIN, i32::MAX]),
            ArrayCellNonOptional::U32(vec![0, u32::MAX]),
            ArrayCellNonOptional::I64(vec![i64::MIN, i64::MAX]),
            ArrayCellNonOptional::F32(vec![
                -f32::MAX,
                f32::MAX,
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ]),
            ArrayCellNonOptional::F64(vec![
                -f64::MAX,
                f64::MAX,
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]),
            ArrayCellNonOptional::Numeric(vec![
                PgNumeric::from_str(BIGQUERY_BIGNUMERIC_MAX).unwrap(),
                PgNumeric::from_str(
                    "-578960446186580977117854925043439539266.\
                     34992332820282019728792003956564819968",
                )
                .unwrap(),
            ]),
        ] {
            assert!(validate_array_cell_for_bigquery(&array).is_ok(), "{array:?}");
        }
    }

    #[test]
    fn validate_cell_for_bigquery_invalid_numeric() {
        let cell = CellNonOptional::Numeric(PgNumeric::NaN);
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
    }

    #[test]
    fn validate_cell_for_bigquery_invalid_json_number() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let cell = CellNonOptional::Json(json);
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
    }

    #[test]
    fn validate_array_cell_with_invalid_numeric() {
        let array_cell = ArrayCellNonOptional::Numeric(vec![
            PgNumeric::from_str("123.456").unwrap(),
            PgNumeric::NaN,
            PgNumeric::from_str("789.012").unwrap(),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }
}
