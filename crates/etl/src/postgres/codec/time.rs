//! Date and time conversion helpers for Postgres text output.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use etl_postgres::time::{
    DATE_FORMAT, ParseTimeError, PgTimeTz, TIME_FORMAT, TIMESTAMP_FORMAT, parse_postgres_utc_offset,
};

/// Minimum byte index after `YYYY-MM-DD` where a UTC offset sign can appear.
const MIN_TIMESTAMP_OFFSET_INDEX: usize = 10;

/// Parses a Postgres `time with time zone` text value.
pub(crate) fn parse_postgres_timetz(value: &str) -> Result<PgTimeTz, ParseTimeError> {
    value.parse()
}

/// Parses a Postgres `date` text value.
///
/// Postgres emits ISO dates as `YYYY-MM-DD` on replication connections. That
/// shape is decoded with a fixed-layout byte parser; anything else falls back
/// to chrono's format machinery so accepted and rejected inputs stay identical.
pub(crate) fn parse_postgres_date(value: &str) -> Result<NaiveDate, chrono::ParseError> {
    if let Some(date) = parse_iso_date_fast(value.as_bytes()) {
        return Ok(date);
    }

    NaiveDate::parse_from_str(value, DATE_FORMAT)
}

/// Parses a Postgres `time` text value.
///
/// Postgres emits ISO times as `HH:MM:SS[.ffffff]` on replication connections.
/// That shape is decoded with a fixed-layout byte parser; anything else falls
/// back to chrono's format machinery so accepted and rejected inputs stay
/// identical.
pub(crate) fn parse_postgres_time(value: &str) -> Result<NaiveTime, chrono::ParseError> {
    if let Some(time) = parse_iso_time_fast(value.as_bytes()) {
        return Ok(time);
    }

    NaiveTime::parse_from_str(value, TIME_FORMAT)
}

/// Parses a Postgres `timestamp` text value.
///
/// Postgres emits ISO timestamps as `YYYY-MM-DD HH:MM:SS[.ffffff]` on
/// replication connections. That shape is decoded with a fixed-layout byte
/// parser; anything else falls back to chrono's format machinery so accepted
/// and rejected inputs stay identical.
pub(crate) fn parse_postgres_timestamp(value: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    if let Some(timestamp) = parse_iso_timestamp_fast(value.as_bytes()) {
        return Ok(timestamp);
    }

    NaiveDateTime::parse_from_str(value, TIMESTAMP_FORMAT)
}

/// Parses a Postgres `timestamp with time zone` text value.
///
/// Replication connections set the Postgres session timezone to UTC. This
/// parser accepts numeric offsets, but UTC session output keeps
/// `timestamptz` values deterministic before they are normalized into
/// [`chrono::Utc`].
pub(crate) fn parse_postgres_timestamptz(
    value: &str,
) -> Result<DateTime<FixedOffset>, ParseTimeError> {
    let (timestamp, offset) = split_timestamp_offset(value).ok_or(ParseTimeError::InvalidSyntax)?;
    let timestamp = parse_postgres_timestamp(timestamp.trim_end())?;
    let offset = parse_postgres_utc_offset(offset).ok_or(ParseTimeError::InvalidSyntax)?;

    offset.from_local_datetime(&timestamp).single().ok_or(ParseTimeError::InvalidSyntax)
}

/// Parses two ASCII decimal digits into a number.
#[inline]
fn parse_two_digits(bytes: &[u8]) -> Option<u32> {
    let high = bytes[0].wrapping_sub(b'0');
    let low = bytes[1].wrapping_sub(b'0');
    if high > 9 || low > 9 {
        return None;
    }

    Some(u32::from(high) * 10 + u32::from(low))
}

/// Parses a strict `YYYY-MM-DD` byte layout into a [`NaiveDate`].
///
/// Returns [`None`] for any deviation, including out-of-range components, so
/// the caller can defer to chrono for the exact accept/reject decision.
fn parse_iso_date_fast(bytes: &[u8]) -> Option<NaiveDate> {
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }

    let year = parse_two_digits(&bytes[0..2])? * 100 + parse_two_digits(&bytes[2..4])?;
    let month = parse_two_digits(&bytes[5..7])?;
    let day = parse_two_digits(&bytes[8..10])?;

    NaiveDate::from_ymd_opt(year as i32, month, day)
}

/// Parses a strict `HH:MM:SS[.f{1,9}]` byte layout into a [`NaiveTime`].
///
/// Returns [`None`] for any deviation, including out-of-range components and
/// leap seconds, so the caller can defer to chrono for the exact accept/reject
/// decision.
fn parse_iso_time_fast(bytes: &[u8]) -> Option<NaiveTime> {
    if bytes.len() < 8 || bytes[2] != b':' || bytes[5] != b':' {
        return None;
    }

    let hour = parse_two_digits(&bytes[0..2])?;
    let minute = parse_two_digits(&bytes[3..5])?;
    let second = parse_two_digits(&bytes[6..8])?;

    let nanos = if bytes.len() == 8 {
        0
    } else {
        if bytes[8] != b'.' {
            return None;
        }

        let fraction = &bytes[9..];
        if fraction.is_empty() || fraction.len() > 9 {
            return None;
        }

        let mut nanos: u32 = 0;
        for &byte in fraction {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            nanos = nanos * 10 + u32::from(digit);
        }

        nanos * 10u32.pow(9 - fraction.len() as u32)
    };

    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
}

/// Parses a strict `YYYY-MM-DD HH:MM:SS[.f{1,9}]` byte layout into a
/// [`NaiveDateTime`].
fn parse_iso_timestamp_fast(bytes: &[u8]) -> Option<NaiveDateTime> {
    if bytes.len() < 19 || bytes[10] != b' ' {
        return None;
    }

    let date = parse_iso_date_fast(&bytes[..10])?;
    let time = parse_iso_time_fast(&bytes[11..])?;

    Some(NaiveDateTime::new(date, time))
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
    fn date_fast_path_matches_chrono_for_iso_dates() {
        for value in ["2023-12-25", "0001-01-01", "9999-12-31", "2024-02-29"] {
            assert_eq!(
                parse_postgres_date(value).unwrap(),
                NaiveDate::parse_from_str(value, DATE_FORMAT).unwrap(),
                "value: {value}"
            );
        }
    }

    #[test]
    fn date_falls_back_for_non_iso_shapes() {
        // Single-digit components are chrono-only shapes.
        assert_eq!(
            parse_postgres_date("2023-1-01").unwrap(),
            NaiveDate::parse_from_str("2023-1-01", DATE_FORMAT).unwrap()
        );

        // Chrono's `%Y` requires a `+` prefix for years above 4 digits, so the
        // fallback rejects this exactly like the previous direct parse did.
        assert!(parse_postgres_date("12023-01-01").is_err());
        assert!(parse_postgres_date("2023-13-01").is_err());
        assert!(parse_postgres_date("2023-02-30").is_err());
        assert!(parse_postgres_date("2023-12-25 BC").is_err());
        assert!(parse_postgres_date("2023-1é-01").is_err());
        assert!(parse_postgres_date("not-a-date").is_err());
        assert!(parse_postgres_date("").is_err());
    }

    #[test]
    fn time_fast_path_matches_chrono_for_iso_times() {
        for value in
            ["00:00:00", "23:59:59", "14:30:45.1", "14:30:45.123", "14:30:45.123456", "12:00:00.5"]
        {
            assert_eq!(
                parse_postgres_time(value).unwrap(),
                NaiveTime::parse_from_str(value, TIME_FORMAT).unwrap(),
                "value: {value}"
            );
        }
    }

    #[test]
    fn time_falls_back_for_non_iso_shapes() {
        // Leap seconds only parse through chrono.
        assert_eq!(
            parse_postgres_time("23:59:60").unwrap(),
            NaiveTime::parse_from_str("23:59:60", TIME_FORMAT).unwrap()
        );

        // Chrono's `%.f` accepts more than nine fractional digits, so the
        // fallback keeps accepting them exactly like the previous direct
        // parse did.
        assert_eq!(
            parse_postgres_time("12:30:45.1234567890").unwrap(),
            NaiveTime::parse_from_str("12:30:45.1234567890", TIME_FORMAT).unwrap()
        );

        assert!(parse_postgres_time("24:00:00").is_err());
        assert!(parse_postgres_time("12:61:00").is_err());
        assert!(parse_postgres_time("12:30:45.").is_err());
        assert!(parse_postgres_time("12:30:45extra").is_err());
        assert!(parse_postgres_time("12:30:4é").is_err());
        assert!(parse_postgres_time("invalid").is_err());
    }

    #[test]
    fn timestamp_fast_path_matches_chrono_for_iso_timestamps() {
        for value in [
            "2023-12-25 14:30:45",
            "2023-12-25 14:30:45.123",
            "2023-12-25 14:30:45.123456",
            "1970-01-01 00:00:00",
        ] {
            assert_eq!(
                parse_postgres_timestamp(value).unwrap(),
                NaiveDateTime::parse_from_str(value, TIMESTAMP_FORMAT).unwrap(),
                "value: {value}"
            );
        }
    }

    #[test]
    fn timestamp_falls_back_for_non_iso_shapes() {
        assert_eq!(
            parse_postgres_timestamp("2023-12-25 23:59:60").unwrap(),
            NaiveDateTime::parse_from_str("2023-12-25 23:59:60", TIMESTAMP_FORMAT).unwrap()
        );
        assert_eq!(
            parse_postgres_timestamp("2023-12-25 12:30:45.1234567890").unwrap(),
            NaiveDateTime::parse_from_str("2023-12-25 12:30:45.1234567890", TIMESTAMP_FORMAT)
                .unwrap()
        );

        assert!(parse_postgres_timestamp("2023-12-25T14:30:45").is_err());
        assert!(parse_postgres_timestamp("2023-12-25 14:30").is_err());
        assert!(parse_postgres_timestamp("2023-12-25 14:30:45 tail").is_err());
        assert!(parse_postgres_timestamp("2023-12-25 12:30:4é").is_err());
        assert!(parse_postgres_timestamp("").is_err());
    }

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
