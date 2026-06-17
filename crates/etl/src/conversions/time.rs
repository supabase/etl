//! Date and time conversion helpers for Postgres text output.

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
use etl_postgres::types::{ParseTimeError, PgTimeTz, TIMESTAMP_FORMAT, parse_postgres_utc_offset};

/// Minimum byte index after `YYYY-MM-DD` where a UTC offset sign can appear.
const MIN_TIMESTAMP_OFFSET_INDEX: usize = 10;

/// Parses a Postgres `time with time zone` text value.
pub(crate) fn parse_postgres_timetz(value: &str) -> Result<PgTimeTz, ParseTimeError> {
    value.parse()
}

/// Parses a Postgres `timestamp with time zone` text value.
pub(crate) fn parse_postgres_timestamptz(
    value: &str,
) -> Result<DateTime<FixedOffset>, ParseTimeError> {
    let (timestamp, offset) = split_timestamp_offset(value).ok_or(ParseTimeError::InvalidSyntax)?;
    let timestamp = NaiveDateTime::parse_from_str(timestamp.trim_end(), TIMESTAMP_FORMAT)?;
    let offset = parse_postgres_utc_offset(offset).ok_or(ParseTimeError::InvalidSyntax)?;

    offset.from_local_datetime(&timestamp).single().ok_or(ParseTimeError::InvalidSyntax)
}

/// Splits a timestamp string into timestamp and numeric UTC offset parts.
fn split_timestamp_offset(value: &str) -> Option<(&str, &str)> {
    value.char_indices().rev().find_map(|(idx, ch)| {
        (idx > MIN_TIMESTAMP_OFFSET_INDEX && matches!(ch, '+' | '-')).then(|| value.split_at(idx))
    })
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveTime, Timelike};

    use super::*;

    #[test]
    fn timetz_parses_via_shared_type() {
        let value = parse_postgres_timetz("12:30:00.123456+02:30").unwrap();

        assert_eq!(value.time(), NaiveTime::from_hms_micro_opt(12, 30, 0, 123_456).unwrap());
        assert_eq!(value.offset().local_minus_utc(), 9_000);
    }

    #[test]
    fn timetz_rejects_invalid_values() {
        assert!(parse_postgres_timetz("12:30:00").is_err());
        assert!(parse_postgres_timetz("24:00:00+00").is_err());
        assert!(parse_postgres_timetz("12:30:00+16").is_err());
    }

    #[test]
    fn timestamptz_parses_supported_offset_forms() {
        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+02").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 7_200);

        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+0230").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 9_000);

        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+023015").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 9_015);

        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+02:30").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 9_000);

        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+02:30:15").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 9_015);
    }

    #[test]
    fn timestamptz_preserves_local_time_before_utc_normalization() {
        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00.123456-07:30").unwrap();

        assert_eq!(datetime.time().hour(), 12);
        assert_eq!(datetime.time().minute(), 30);
        assert_eq!(datetime.time().second(), 0);
        assert_eq!(datetime.time().nanosecond(), 123_456_000);
        assert_eq!(datetime.offset().local_minus_utc(), -27_000);
    }

    #[test]
    fn timestamptz_parses_postgres_offset_boundary() {
        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00+15:59:59").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), 57_599);

        let datetime = parse_postgres_timestamptz("2026-01-01 12:30:00-15:59:59").unwrap();
        assert_eq!(datetime.offset().local_minus_utc(), -57_599);
    }

    #[test]
    fn timestamptz_rejects_invalid_values() {
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+16").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+16:00").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+15:60").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+15:59:60").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+1").is_err());
        assert!(parse_postgres_timestamptz("2026-01-01 12:30:00+01:02:03:04").is_err());
    }

    #[test]
    fn timestamptz_distinguishes_chrono_and_offset_errors() {
        assert!(matches!(
            parse_postgres_timestamptz("2026-99-01 12:30:00+00").unwrap_err(),
            ParseTimeError::Chrono(_)
        ));
        assert_eq!(
            parse_postgres_timestamptz("2026-01-01 12:30:00+16").unwrap_err(),
            ParseTimeError::InvalidSyntax
        );
    }
}
