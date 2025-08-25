use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::types::{ArrayCellNonOptional, CellNonOptional, PgNumeric};
use etl::{bail, etl_error};

/// BigQuery BIGNUMERIC maximum practical digits for validation
const BIGQUERY_BIGNUMERIC_MAX_PRACTICAL_DIGITS: usize = 76;

/// BigQuery BIGNUMERIC maximum decimal places
const BIGQUERY_BIGNUMERIC_MAX_SCALE: usize = 38;

/// BigQuery DATE minimum value: 0001-01-01
const BIGQUERY_DATE_MIN: (i32, u32, u32) = (1, 1, 1);

/// BigQuery DATE maximum value: 9999-12-31
const BIGQUERY_DATE_MAX: (i32, u32, u32) = (9999, 12, 31);

/// BigQuery TIME minimum value: 00:00:00
const BIGQUERY_TIME_MIN: (u32, u32, u32) = (0, 0, 0);

/// BigQuery TIME maximum value: 23:59:59.999999
const BIGQUERY_TIME_MAX: (u32, u32, u32, u32) = (23, 59, 59, 999999);

/// Validates that a [`PgNumeric`] value is within BigQuery's BIGNUMERIC supported range.
///
/// Returns an error if the value is outside BigQuery's supported range instead of clamping.
/// BigQuery BIGNUMERIC supports up to ~77 digits of precision with 38 digits after the decimal point.
pub fn validate_numeric_for_bigquery(numeric: &PgNumeric) -> EtlResult<()> {
    match numeric {
        PgNumeric::NaN => {
            bail!(
                ErrorKind::UnsupportedValue,
                "BigQuery NUMERIC/BIGNUMERIC does not support NaN values",
                "The numeric value NaN cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::PositiveInfinity => {
            bail!(
                ErrorKind::UnsupportedValue,
                "BigQuery NUMERIC/BIGNUMERIC does not support infinity values",
                "The numeric value +Infinity cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::NegativeInfinity => {
            bail!(
                ErrorKind::UnsupportedValue,
                "BigQuery NUMERIC/BIGNUMERIC does not support infinity values",
                "The numeric value -Infinity cannot be stored in BigQuery. Please provide a finite numeric value"
            );
        }
        PgNumeric::Value { .. } => {
            let numeric_str = numeric.to_string();

            if !is_numeric_within_bigquery_bignumeric_limits(&numeric_str) {
                bail!(
                    ErrorKind::UnsupportedValue,
                    "Numeric value exceeds BigQuery BIGNUMERIC limits",
                    format!(
                        "The numeric value '{}' exceeds BigQuery's BIGNUMERIC limits (max {} digits, {} decimal places)",
                        numeric_str,
                        BIGQUERY_BIGNUMERIC_MAX_PRACTICAL_DIGITS,
                        BIGQUERY_BIGNUMERIC_MAX_SCALE
                    )
                );
            }

            Ok(())
        }
    }
}

/// Validates that a [`NaiveDate`] is within BigQuery's supported range.
///
/// Returns an error if the date is outside BigQuery's supported range instead of clamping.
/// BigQuery DATE supports values from 0001-01-01 to 9999-12-31.
pub fn validate_date_for_bigquery(date: &NaiveDate) -> EtlResult<()> {
    let min_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery date bounds"
        )
    })?;

    let max_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery date bounds"
        )
    })?;

    if *date < min_date {
        bail!(
            ErrorKind::UnsupportedValue,
            "Date value is before BigQuery's minimum supported date",
            format!(
                "The date '{}' is before BigQuery's minimum supported date '{}'. BigQuery DATE supports values from 0001-01-01 to 9999-12-31",
                date.format("%Y-%m-%d"),
                min_date.format("%Y-%m-%d")
            )
        );
    }

    if *date > max_date {
        bail!(
            ErrorKind::UnsupportedValue,
            "Date value is after BigQuery's maximum supported date",
            format!(
                "The date '{}' is after BigQuery's maximum supported date '{}'. BigQuery DATE supports values from 0001-01-01 to 9999-12-31",
                date.format("%Y-%m-%d"),
                max_date.format("%Y-%m-%d")
            )
        );
    }

    Ok(())
}

/// Validates that a [`NaiveTime`] is within BigQuery's supported range.
///
/// Returns an error if the time is outside BigQuery's supported range instead of clamping.
/// BigQuery TIME supports values from 00:00:00 to 23:59:59.999999.
pub fn validate_time_for_bigquery(time: &NaiveTime) -> EtlResult<()> {
    let min_time = NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery time bounds"
        )
    })?;

    let max_time = NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery time bounds"
        )
    })?;

    if *time < min_time {
        bail!(
            ErrorKind::UnsupportedValue,
            "Time value is before BigQuery's minimum supported time",
            format!(
                "The time '{}' is before BigQuery's minimum supported time '{}'. BigQuery TIME supports values from 00:00:00 to 23:59:59.999999",
                time.format("%H:%M:%S"),
                min_time.format("%H:%M:%S")
            )
        );
    }

    if *time > max_time {
        bail!(
            ErrorKind::UnsupportedValue,
            "Time value is after BigQuery's maximum supported time",
            format!(
                "The time '{}' is after BigQuery's maximum supported time '{}'. BigQuery TIME supports values from 00:00:00 to 23:59:59.999999",
                time.format("%H:%M:%S%.6f"),
                max_time.format("%H:%M:%S%.6f")
            )
        );
    }

    Ok(())
}

/// Validates that a [`NaiveDateTime`] is within BigQuery's supported range.
///
/// Returns an error if the datetime is outside BigQuery's supported range instead of clamping.
/// BigQuery DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999.
pub fn validate_datetime_for_bigquery(datetime: &NaiveDateTime) -> EtlResult<()> {
    let min_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery date bounds"
        )
    })?;

    let min_time = NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery time bounds"
        )
    })?;

    let max_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery date bounds"
        )
    })?;

    let max_time = NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery time bounds"
        )
    })?;

    let min_datetime = NaiveDateTime::new(min_date, min_time);
    let max_datetime = NaiveDateTime::new(max_date, max_time);

    if *datetime < min_datetime {
        bail!(
            ErrorKind::UnsupportedValue,
            "DateTime value is before BigQuery's minimum supported datetime",
            format!(
                "The datetime '{}' is before BigQuery's minimum supported datetime '{}'. BigQuery DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999",
                datetime.format("%Y-%m-%d %H:%M:%S"),
                min_datetime.format("%Y-%m-%d %H:%M:%S")
            )
        );
    }

    if *datetime > max_datetime {
        bail!(
            ErrorKind::UnsupportedValue,
            "DateTime value is after BigQuery's maximum supported datetime",
            format!(
                "The datetime '{}' is after BigQuery's maximum supported datetime '{}'. BigQuery DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999",
                datetime.format("%Y-%m-%d %H:%M:%S%.6f"),
                max_datetime.format("%Y-%m-%d %H:%M:%S%.6f")
            )
        );
    }

    Ok(())
}

/// Validates that a [`DateTime<Utc>`] is within BigQuery's supported range.
///
/// Returns an error if the timestamp is outside BigQuery's supported range instead of clamping.
/// BigQuery TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 23:59:59.999999 UTC.
pub fn validate_timestamptz_for_bigquery(timestamptz: &DateTime<Utc>) -> EtlResult<()> {
    let min_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery date bounds"
        )
    })?;

    let min_time = NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid minimum BigQuery time bounds"
        )
    })?;

    let max_date = NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery date bounds"
        )
    })?;

    let max_time = NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    )
    .ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValue,
            "Invalid maximum BigQuery time bounds"
        )
    })?;

    let min_timestamp = Utc.from_utc_datetime(&NaiveDateTime::new(min_date, min_time));
    let max_timestamp = Utc.from_utc_datetime(&NaiveDateTime::new(max_date, max_time));

    if *timestamptz < min_timestamp {
        bail!(
            ErrorKind::UnsupportedValue,
            "Timestamp value is before BigQuery's minimum supported timestamp",
            format!(
                "The timestamp '{}' is before BigQuery's minimum supported timestamp '{}'. BigQuery TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 23:59:59.999999 UTC",
                timestamptz.format("%Y-%m-%d %H:%M:%S%z"),
                min_timestamp.format("%Y-%m-%d %H:%M:%S%z")
            )
        );
    }

    if *timestamptz > max_timestamp {
        bail!(
            ErrorKind::UnsupportedValue,
            "Timestamp value is after BigQuery's maximum supported timestamp",
            format!(
                "The timestamp '{}' is after BigQuery's maximum supported timestamp '{}'. BigQuery TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 23:59:59.999999 UTC",
                timestamptz.format("%Y-%m-%d %H:%M:%S%.6f%z"),
                max_timestamp.format("%Y-%m-%d %H:%M:%S%.6f%z")
            )
        );
    }

    Ok(())
}

/// Validates that a [`CellNonOptional`] value is within BigQuery's supported ranges.
///
/// Returns an error if any value is outside BigQuery's supported range for its type.
/// This function checks all temporal types and numeric types for BigQuery compatibility.
pub fn validate_cell_for_bigquery(cell: &CellNonOptional) -> EtlResult<()> {
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
        CellNonOptional::TimeStamp(datetime) => validate_datetime_for_bigquery(datetime),
        CellNonOptional::TimeStampTz(timestamptz) => validate_timestamptz_for_bigquery(timestamptz),
        CellNonOptional::Uuid(_) => Ok(()),
        CellNonOptional::Json(_) => Ok(()),
        CellNonOptional::Bytes(_) => Ok(()),
        CellNonOptional::Array(array) => validate_array_cell_for_bigquery(array),
    }
}

/// Validates that an [`ArrayCellNonOptional`] contains values within BigQuery's supported ranges.
///
/// Returns an error if any array element is outside BigQuery's supported range for its type.
pub fn validate_array_cell_for_bigquery(array_cell: &ArrayCellNonOptional) -> EtlResult<()> {
    match array_cell {
        ArrayCellNonOptional::Null => Ok(()),
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
                    if let Some(detail) = err.detail() {
                        EtlError::from((
                            ErrorKind::UnsupportedValue,
                            "Array element validation failed",
                            format!("Element at index {}: {}", index, detail),
                        ))
                    } else {
                        err
                    }
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Date(dates) => {
            for (index, date) in dates.iter().enumerate() {
                validate_date_for_bigquery(date).map_err(|err| {
                    if let Some(detail) = err.detail() {
                        EtlError::from((
                            ErrorKind::UnsupportedValue,
                            "Array element validation failed",
                            format!("Element at index {}: {}", index, detail),
                        ))
                    } else {
                        err
                    }
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Time(times) => {
            for (index, time) in times.iter().enumerate() {
                validate_time_for_bigquery(time).map_err(|err| {
                    if let Some(detail) = err.detail() {
                        EtlError::from((
                            ErrorKind::UnsupportedValue,
                            "Array element validation failed",
                            format!("Element at index {}: {}", index, detail),
                        ))
                    } else {
                        err
                    }
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::TimeStamp(datetimes) => {
            for (index, datetime) in datetimes.iter().enumerate() {
                validate_datetime_for_bigquery(datetime).map_err(|err| {
                    if let Some(detail) = err.detail() {
                        EtlError::from((
                            ErrorKind::UnsupportedValue,
                            "Array element validation failed",
                            format!("Element at index {}: {}", index, detail),
                        ))
                    } else {
                        err
                    }
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::TimeStampTz(timestamptzs) => {
            for (index, timestamptz) in timestamptzs.iter().enumerate() {
                validate_timestamptz_for_bigquery(timestamptz).map_err(|err| {
                    if let Some(detail) = err.detail() {
                        EtlError::from((
                            ErrorKind::UnsupportedValue,
                            "Array element validation failed",
                            format!("Element at index {}: {}", index, detail),
                        ))
                    } else {
                        err
                    }
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Uuid(_) => Ok(()),
        ArrayCellNonOptional::Json(_) => Ok(()),
        ArrayCellNonOptional::Bytes(_) => Ok(()),
    }
}

/// Checks if a numeric string is within BigQuery's BIGNUMERIC limits.
///
/// BIGNUMERIC supports up to ~77 digits of precision with up to 38 decimal places.
fn is_numeric_within_bigquery_bignumeric_limits(numeric_str: &str) -> bool {
    // Count actual digits (excluding sign, decimal point)
    let digit_count: usize = numeric_str.chars().filter(|c| c.is_ascii_digit()).count();

    // BigQuery BIGNUMERIC supports up to ~77 digits of total precision
    if digit_count > BIGQUERY_BIGNUMERIC_MAX_PRACTICAL_DIGITS {
        return false;
    }

    // Check decimal places if there's a decimal point
    if let Some(decimal_pos) = numeric_str.find('.') {
        let decimal_part = &numeric_str[decimal_pos + 1..];
        let decimal_digits = decimal_part.chars().filter(|c| c.is_ascii_digit()).count();

        // BigQuery BIGNUMERIC supports up to 38 decimal places
        if decimal_digits > BIGQUERY_BIGNUMERIC_MAX_SCALE {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_validate_numeric_within_bounds() {
        let numeric = PgNumeric::from_str("123.456").unwrap();
        assert!(validate_numeric_for_bigquery(&numeric).is_ok());
    }

    #[test]
    fn test_validate_numeric_nan_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::NaN);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(
            err.detail()
                .unwrap()
                .contains("NaN cannot be stored in BigQuery")
        );
    }

    #[test]
    fn test_validate_numeric_positive_infinity_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::PositiveInfinity);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(
            err.detail()
                .unwrap()
                .contains("Infinity cannot be stored in BigQuery")
        );
    }

    #[test]
    fn test_validate_numeric_negative_infinity_fails() {
        let result = validate_numeric_for_bigquery(&PgNumeric::NegativeInfinity);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(
            err.detail()
                .unwrap()
                .contains("Infinity cannot be stored in BigQuery")
        );
    }

    #[test]
    fn test_validate_date_within_bounds() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert!(validate_date_for_bigquery(&date).is_ok());
    }

    #[test]
    fn test_validate_date_before_min_fails() {
        let date = NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap();
        let result = validate_date_for_bigquery(&date);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(err.detail().unwrap().contains("before BigQuery's minimum"));
    }

    #[test]
    fn test_validate_date_after_max_fails() {
        let date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
        let result = validate_date_for_bigquery(&date);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(err.detail().unwrap().contains("after BigQuery's maximum"));
    }

    #[test]
    fn test_validate_cell_for_bigquery_valid_types() {
        assert!(validate_cell_for_bigquery(&CellNonOptional::Null).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::Bool(true)).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::String("test".to_string())).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::I32(42)).is_ok());
    }

    #[test]
    fn test_validate_cell_for_bigquery_invalid_numeric() {
        let cell = CellNonOptional::Numeric(PgNumeric::NaN);
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValue);
    }

    #[test]
    fn test_validate_array_cell_with_invalid_numeric() {
        let array_cell = ArrayCellNonOptional::Numeric(vec![
            PgNumeric::from_str("123.456").unwrap(),
            PgNumeric::NaN,
            PgNumeric::from_str("789.012").unwrap(),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValue);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }
}
