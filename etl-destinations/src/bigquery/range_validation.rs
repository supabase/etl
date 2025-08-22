use etl::error::EtlResult;
use etl::types::PgNumeric;
use etl_postgres::time::{DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM};
use tracing::warn;

// Import chrono types from where they're re-exported by dependencies
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

/// BigQuery BIGNUMERIC minimum value (simplified representation)
/// BIGNUMERIC supports up to ~77 digits with 38 decimal places, which is much larger than most practical needs.
/// For performance, we use a very generous but practical limit check instead of the theoretical maximum.
const BIGQUERY_BIGNUMERIC_MAX_PRACTICAL_DIGITS: usize = 76;

/// BigQuery BIGNUMERIC maximum decimal places
const BIGQUERY_BIGNUMERIC_MAX_SCALE: usize = 38;

/// BigQuery BIGNUMERIC minimum value (clamping extreme)
/// This represents a practical minimum value for clamping purposes
const BIGQUERY_BIGNUMERIC_MIN_STR: &str =
    "-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38";

/// BigQuery BIGNUMERIC maximum value (clamping extreme)  
/// This represents a practical maximum value for clamping purposes
const BIGQUERY_BIGNUMERIC_MAX_STR: &str =
    "5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38";

/// BigQuery DATE minimum value: 0001-01-01
/// This represents the earliest date that BigQuery can store.
const BIGQUERY_DATE_MIN: (i32, u32, u32) = (1, 1, 1);

/// BigQuery DATE maximum value: 9999-12-31
/// This represents the latest date that BigQuery can store.
const BIGQUERY_DATE_MAX: (i32, u32, u32) = (9999, 12, 31);

/// BigQuery TIME minimum value: 00:00:00
/// This represents the earliest time that BigQuery can store.
const BIGQUERY_TIME_MIN: (u32, u32, u32) = (0, 0, 0);

/// BigQuery TIME maximum value: 23:59:59.999999
/// This represents the latest time that BigQuery can store.
const BIGQUERY_TIME_MAX: (u32, u32, u32, u32) = (23, 59, 59, 999999);

/// Validates and clamps a [`PgNumeric`] value to BigQuery's BIGNUMERIC supported range.
///
/// BigQuery BIGNUMERIC supports up to ~77 digits of precision with 38 digits after the decimal point.
/// Values outside this range are clamped to the min/max bounds using saturating arithmetic.
/// Special values (NaN, Infinity, -Infinity) throw InvalidData errors.
pub fn clamp_numeric_for_bigquery(numeric: &PgNumeric) -> String {
    match numeric {
        PgNumeric::NaN => {
            // BigQuery NUMERIC/BIGNUMERIC do not support NaN. Saturate to 0 to avoid load errors.
            warn!("Clamping NUMERIC NaN to 0 for BigQuery");
            "0".to_string()
        }
        PgNumeric::PositiveInfinity => {
            // Saturate to maximum BIGNUMERIC bound
            warn!(
                "clamping NUMERIC +Infinity to BigQuery BIGNUMERIC maximum: {}",
                BIGQUERY_BIGNUMERIC_MAX_STR
            );
            BIGQUERY_BIGNUMERIC_MAX_STR.to_string()
        }
        PgNumeric::NegativeInfinity => {
            // Saturate to minimum BIGNUMERIC bound
            warn!(
                "clamping NUMERIC -Infinity to BigQuery BIGNUMERIC minimum: {}",
                BIGQUERY_BIGNUMERIC_MIN_STR
            );
            BIGQUERY_BIGNUMERIC_MIN_STR.to_string()
        }
        PgNumeric::Value { .. } => {
            let numeric_str = numeric.to_string();

            // Check if the numeric string representation would fit in BigQuery's BIGNUMERIC limits
            if is_numeric_within_bigquery_bignumeric_limits(&numeric_str) {
                numeric_str
            } else {
                // Determine if we should clamp to min or max
                let clamped_value = if numeric_str.starts_with('-') {
                    warn!(
                        "clamping numeric value {} to BigQuery BIGNUMERIC minimum: {}",
                        numeric_str, BIGQUERY_BIGNUMERIC_MIN_STR
                    );
                    BIGQUERY_BIGNUMERIC_MIN_STR.to_string()
                } else {
                    warn!(
                        "clamping numeric value {} to BigQuery BIGNUMERIC maximum: {}",
                        numeric_str, BIGQUERY_BIGNUMERIC_MAX_STR
                    );
                    BIGQUERY_BIGNUMERIC_MAX_STR.to_string()
                };
                clamped_value
            }
        }
    }
}

/// Checks if a numeric string is within BigQuery's BIGNUMERIC limits.
///
/// BIGNUMERIC supports up to ~77 digits of precision with up to 38 decimal places.
/// This is extremely generous and should handle virtually all real-world numeric values.
fn is_numeric_within_bigquery_bignumeric_limits(numeric_str: &str) -> bool {
    // Length-based safety check - if the string is extremely long, it might exceed limits
    if numeric_str.len() > 100 {
        return false;
    }

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

    // BIGNUMERIC limits are so generous that most values will pass
    true
}

/// Validates and clamps a [`NaiveDate`] to BigQuery's supported range.
///
/// BigQuery DATE supports values from 0001-01-01 to 9999-12-31.
/// Dates outside this range are clamped to the min/max bounds.
pub fn clamp_date_for_bigquery(date: &NaiveDate) -> String {
    let min_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    ) {
        Some(d) => d,
        None => return date.format(DATE_FORMAT).to_string(),
    };
    let max_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    ) {
        Some(d) => d,
        None => return date.format(DATE_FORMAT).to_string(),
    };

    let clamped_date = if *date < min_date {
        warn!(
            "clamping date {} to BigQuery minimum: {}",
            date.format(DATE_FORMAT),
            min_date.format(DATE_FORMAT)
        );
        min_date
    } else if *date > max_date {
        warn!(
            "clamping date {} to BigQuery maximum: {}",
            date.format(DATE_FORMAT),
            max_date.format(DATE_FORMAT)
        );
        max_date
    } else {
        *date
    };

    clamped_date.format(DATE_FORMAT).to_string()
}

/// Validates and clamps a [`NaiveTime`] to BigQuery's supported range.
///
/// BigQuery TIME supports values from 00:00:00 to 23:59:59.999999.
/// Times are typically within this range, but this provides safety.
pub fn clamp_time_for_bigquery(time: &NaiveTime) -> String {
    let min_time = match NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    ) {
        Some(t) => t,
        None => return time.format(TIME_FORMAT).to_string(),
    };
    let max_time = match NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    ) {
        Some(t) => t,
        None => return time.format(TIME_FORMAT).to_string(),
    };

    let clamped_time = if *time < min_time {
        warn!(
            "clamping time {} to BigQuery minimum: {}",
            time.format(TIME_FORMAT),
            min_time.format(TIME_FORMAT)
        );
        min_time
    } else if *time > max_time {
        warn!(
            "clamping time {} to BigQuery maximum: {}",
            time.format(TIME_FORMAT),
            max_time.format(TIME_FORMAT)
        );
        max_time
    } else {
        *time
    };

    clamped_time.format(TIME_FORMAT).to_string()
}

/// Validates and clamps a [`NaiveDateTime`] to BigQuery's supported range.
///
/// BigQuery DATETIME supports values from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999.
/// DateTimes outside this range are clamped to the min/max bounds.
pub fn clamp_datetime_for_bigquery(datetime: &NaiveDateTime) -> String {
    let min_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    ) {
        Some(d) => d,
        None => return datetime.format(TIMESTAMP_FORMAT).to_string(),
    };
    let min_time = match NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    ) {
        Some(t) => t,
        None => return datetime.format(TIMESTAMP_FORMAT).to_string(),
    };
    let max_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    ) {
        Some(d) => d,
        None => return datetime.format(TIMESTAMP_FORMAT).to_string(),
    };
    let max_time = match NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    ) {
        Some(t) => t,
        None => return datetime.format(TIMESTAMP_FORMAT).to_string(),
    };
    let min_datetime = NaiveDateTime::new(min_date, min_time);
    let max_datetime = NaiveDateTime::new(max_date, max_time);

    let clamped_datetime = if *datetime < min_datetime {
        warn!(
            "clamping datetime {} to BigQuery minimum: {}",
            datetime.format(TIMESTAMP_FORMAT),
            min_datetime.format(TIMESTAMP_FORMAT)
        );
        min_datetime
    } else if *datetime > max_datetime {
        warn!(
            "clamping datetime {} to BigQuery maximum: {}",
            datetime.format(TIMESTAMP_FORMAT),
            max_datetime.format(TIMESTAMP_FORMAT)
        );
        max_datetime
    } else {
        *datetime
    };

    clamped_datetime.format(TIMESTAMP_FORMAT).to_string()
}

/// Validates and clamps a [`DateTime<Utc>`] to BigQuery's supported range.
///
/// BigQuery TIMESTAMP supports values from 0001-01-01 00:00:00 UTC to 9999-12-31 23:59:59.999999 UTC.
/// Timestamps outside this range are clamped to the min/max bounds.
pub fn clamp_timestamptz_for_bigquery(timestamptz: &DateTime<Utc>) -> String {
    let min_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MIN.0,
        BIGQUERY_DATE_MIN.1,
        BIGQUERY_DATE_MIN.2,
    ) {
        Some(d) => d,
        None => return timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string(),
    };
    let min_time = match NaiveTime::from_hms_opt(
        BIGQUERY_TIME_MIN.0,
        BIGQUERY_TIME_MIN.1,
        BIGQUERY_TIME_MIN.2,
    ) {
        Some(t) => t,
        None => return timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string(),
    };
    let max_date = match NaiveDate::from_ymd_opt(
        BIGQUERY_DATE_MAX.0,
        BIGQUERY_DATE_MAX.1,
        BIGQUERY_DATE_MAX.2,
    ) {
        Some(d) => d,
        None => return timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string(),
    };
    let max_time = match NaiveTime::from_hms_micro_opt(
        BIGQUERY_TIME_MAX.0,
        BIGQUERY_TIME_MAX.1,
        BIGQUERY_TIME_MAX.2,
        BIGQUERY_TIME_MAX.3,
    ) {
        Some(t) => t,
        None => return timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string(),
    };
    let min_timestamp = Utc.from_utc_datetime(&NaiveDateTime::new(min_date, min_time));
    let max_timestamp = Utc.from_utc_datetime(&NaiveDateTime::new(max_date, max_time));

    let clamped_timestamp = if *timestamptz < min_timestamp {
        warn!(
            "clamping timestamptz {} to BigQuery minimum: {}",
            timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM),
            min_timestamp.format(TIMESTAMPTZ_FORMAT_HH_MM)
        );
        min_timestamp
    } else if *timestamptz > max_timestamp {
        warn!(
            "clamping timestamptz {} to BigQuery maximum: {}",
            timestamptz.format(TIMESTAMPTZ_FORMAT_HH_MM),
            max_timestamp.format(TIMESTAMPTZ_FORMAT_HH_MM)
        );
        max_timestamp
    } else {
        *timestamptz
    };

    clamped_timestamp
        .format(TIMESTAMPTZ_FORMAT_HH_MM)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::PgNumeric;
    use std::str::FromStr;

    #[test]
    fn test_is_numeric_within_limits_simple() {
        assert!(is_numeric_within_bigquery_bignumeric_limits("123.45"));
        assert!(is_numeric_within_bigquery_bignumeric_limits("-123.45"));
        assert!(is_numeric_within_bigquery_bignumeric_limits("0"));
    }

    #[test]
    fn test_is_numeric_within_limits_edge_cases() {
        // Too many digits (>76)
        let very_long_number = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        assert!(!is_numeric_within_bigquery_bignumeric_limits(very_long_number));

        // Too many fractional digits (>38)
        let excessive_decimals = "123.123456789012345678901234567890123456789012345678901234567890";
        assert!(!is_numeric_within_bigquery_bignumeric_limits(excessive_decimals));

        // Reasonable large within limits (74 total digits)
        let reasonable_large = "1234567890123456789012345678901234567.1234567890123456789012345678901234567";
        assert!(is_numeric_within_bigquery_bignumeric_limits(reasonable_large));
    }

    #[test]
    fn test_clamp_numeric_within_bounds() {
        let numeric = PgNumeric::from_str("123.456").unwrap();
        let result = clamp_numeric_for_bigquery(&numeric);
        assert_eq!(result, "123.456");
    }

    #[test]
    fn test_clamp_numeric_nan() {
        let result = clamp_numeric_for_bigquery(&PgNumeric::NaN);
        assert_eq!(result, "0");
    }

    #[test]
    fn test_clamp_numeric_positive_infinity() {
        let result = clamp_numeric_for_bigquery(&PgNumeric::PositiveInfinity);
        assert!(result.starts_with("5.7896044618"));
    }

    #[test]
    fn test_clamp_numeric_negative_infinity() {
        let result = clamp_numeric_for_bigquery(&PgNumeric::NegativeInfinity);
        assert!(result.starts_with("-5.7896044618"));
    }

    #[test]
    fn test_numeric_exceeds_limits_positive_and_negative() {
        // Excessive magnitude clamps to max/min
        let too_big = PgNumeric::from_str("1e1000").unwrap();
        let too_small = PgNumeric::from_str("-1e1000").unwrap();
        let maxed = clamp_numeric_for_bigquery(&too_big);
        let mined = clamp_numeric_for_bigquery(&too_small);
        assert!(maxed.starts_with("5.7896044618"));
        assert!(mined.starts_with("-5.7896044618"));
    }

    #[test]
    fn test_numeric_too_many_fractional_digits() {
        // 39 fractional digits -> clamp to extreme
        let val = PgNumeric::from_str("1.123456789012345678901234567890123456789").unwrap();
        let clamped = clamp_numeric_for_bigquery(&val);
        assert!(clamped.starts_with("5.7896044618"));
    }

    #[test]
    fn test_clamp_date_within_bounds() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let result = clamp_date_for_bigquery(&date);
        assert_eq!(result, "2024-01-15");
    }

    #[test]
    fn test_clamp_date_before_min() {
        let date = NaiveDate::from_ymd_opt(0001, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap(); // Before year 1
        let result = clamp_date_for_bigquery(&date);
        assert_eq!(result, "0001-01-01");
    }

    #[test]
    fn test_clamp_date_after_max() {
        let date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
        let result = clamp_date_for_bigquery(&date);
        assert_eq!(result, "9999-12-31");
    }

    #[test]
    fn test_clamp_time_within_bounds() {
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let result = clamp_time_for_bigquery(&time);
        assert_eq!(result, "12:30:45");
    }

    #[test]
    fn test_clamp_datetime_within_bounds() {
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        let result = clamp_datetime_for_bigquery(&datetime);
        assert_eq!(result, "2024-06-15 10:30:00");
    }

    #[test]
    fn test_clamp_timestamptz_within_bounds() {
        let timestamptz = Utc.from_utc_datetime(&NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        ));
        let result = clamp_timestamptz_for_bigquery(&timestamptz);
        assert!(result.starts_with("2024-06-15 10:30:00"));
        assert!(result.contains("+00:00")); // UTC timezone
    }

    #[test]
    fn test_clamp_datetime_before_min() {
        let before_min_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();
        let dt = NaiveDateTime::new(before_min_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let result = clamp_datetime_for_bigquery(&dt);
        assert_eq!(result, "0001-01-01 00:00:00");
    }

    #[test]
    fn test_clamp_datetime_after_max() {
        let after_max_date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
        let dt = NaiveDateTime::new(after_max_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let result = clamp_datetime_for_bigquery(&dt);
        assert_eq!(result, "9999-12-31 23:59:59.999999");
    }

    #[test]
    fn test_clamp_timestamptz_before_min() {
        let before_min_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();
        let ts = Utc.from_utc_datetime(&NaiveDateTime::new(
            before_min_date,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ));
        let result = clamp_timestamptz_for_bigquery(&ts);
        assert_eq!(result, "0001-01-01 00:00:00+00:00");
    }

    #[test]
    fn test_clamp_timestamptz_after_max() {
        let after_max_date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
        let ts = Utc.from_utc_datetime(&NaiveDateTime::new(
            after_max_date,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ));
        let result = clamp_timestamptz_for_bigquery(&ts);
        assert_eq!(result, "9999-12-31 23:59:59.999999+00:00");
    }   
}
