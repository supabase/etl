use std::{
    fmt,
    str::FromStr,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{FixedOffset, NaiveTime};

/// Postgres date format string for parsing dates in YYYY-MM-DD format.
pub const DATE_FORMAT: &str = "%Y-%m-%d";

/// Postgres time format string for parsing times with optional fractional
/// seconds.
pub const TIME_FORMAT: &str = "%H:%M:%S%.f";

/// Postgres timetz format string with time zone offset in +HHMM format.
pub const TIMETZ_FORMAT_HHMM: &str = "%H:%M:%S%.f%#z";

/// Postgres timetz format string with time zone offset in +HH:MM format.
pub const TIMETZ_FORMAT_HH_MM: &str = "%H:%M:%S%.f%:z";

/// Postgres timestamp format string for parsing timestamps with optional
/// fractional seconds.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Postgres timestamptz format string with time zone offset in +HHMM format.
pub const TIMESTAMPTZ_FORMAT_HHMM: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

/// Postgres timestamptz format string with time zone offset in +HH:MM format.
pub const TIMESTAMPTZ_FORMAT_HH_MM: &str = "%Y-%m-%d %H:%M:%S%.f%:z";

/// Maximum numeric UTC offset accepted by Postgres, exclusive.
const POSTGRES_UTC_OFFSET_LIMIT_SECONDS: i32 = 16 * 60 * 60;

/// Number of seconds between Unix epoch (1970-01-01) and Postgres epoch
/// (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Postgres epoch (2000-01-01 00:00:00 UTC) for timestamp calculations.
pub static POSTGRES_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS));

/// A Postgres `time with time zone` value.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PgTimeTz {
    /// The time of day without a date.
    time: NaiveTime,
    /// The fixed UTC offset associated with the time.
    offset: FixedOffset,
}

impl PgTimeTz {
    /// Creates a new [`PgTimeTz`] from a time of day and fixed UTC offset.
    pub fn new(time: NaiveTime, offset: FixedOffset) -> Self {
        Self { time, offset }
    }

    /// Returns the time of day.
    pub fn time(&self) -> NaiveTime {
        self.time
    }

    /// Returns the fixed UTC offset.
    pub fn offset(&self) -> FixedOffset {
        self.offset
    }
}

impl Default for PgTimeTz {
    fn default() -> Self {
        Self {
            time: NaiveTime::default(),
            offset: FixedOffset::east_opt(0).expect("zero UTC offset should be valid"),
        }
    }
}

impl FromStr for PgTimeTz {
    type Err = chrono::ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_postgres_timetz(value)
    }
}

impl fmt::Display for PgTimeTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.time.format(TIME_FORMAT))?;
        write_utc_offset(f, self.offset)
    }
}

/// Parses a Postgres timetz text value with supported UTC offset forms.
fn parse_postgres_timetz(value: &str) -> Result<PgTimeTz, chrono::ParseError> {
    let (time, offset) = split_timetz_offset(value).ok_or_else(invalid_time_error)?;
    let time = NaiveTime::parse_from_str(time.trim_end(), TIME_FORMAT)?;
    let offset = parse_postgres_utc_offset(offset).ok_or_else(invalid_time_error)?;

    Ok(PgTimeTz::new(time, offset))
}

/// Splits a timetz string into time and numeric UTC offset parts.
fn split_timetz_offset(value: &str) -> Option<(&str, &str)> {
    split_utc_offset(value, 0)
}

/// Splits a string at the last UTC offset sign after `minimum_sign_index`.
fn split_utc_offset(value: &str, minimum_sign_index: usize) -> Option<(&str, &str)> {
    value.char_indices().rev().find_map(|(idx, ch)| {
        (idx > minimum_sign_index && matches!(ch, '+' | '-')).then(|| value.split_at(idx))
    })
}

/// Parses a signed UTC offset in +HH, +HHMM, +HHMMSS, +HH:MM, or +HH:MM:SS
/// form.
pub fn parse_postgres_utc_offset(value: &str) -> Option<FixedOffset> {
    let (sign, value) = match value.as_bytes().first()? {
        b'+' => (1, &value[1..]),
        b'-' => (-1, &value[1..]),
        _ => return None,
    };
    let (hours, minutes, seconds) = if value.contains(':') {
        parse_colon_utc_offset(value)?
    } else {
        parse_compact_utc_offset(value)?
    };

    if minutes >= 60 || seconds >= 60 {
        return None;
    }

    let total_seconds = (hours * 3600) + (minutes * 60) + seconds;
    if total_seconds >= POSTGRES_UTC_OFFSET_LIMIT_SECONDS {
        return None;
    }

    FixedOffset::east_opt(sign * total_seconds)
}

/// Parses an unsigned UTC offset in HH:MM or HH:MM:SS form.
fn parse_colon_utc_offset(value: &str) -> Option<(i32, i32, i32)> {
    let mut parts = value.split(':');
    let hours = parse_two_digits(parts.next()?)?;
    let minutes = parse_two_digits(parts.next()?)?;
    let seconds = match parts.next() {
        Some(seconds) => parse_two_digits(seconds)?,
        None => 0,
    };

    parts.next().is_none().then_some((hours, minutes, seconds))
}

/// Parses an unsigned UTC offset in HH, HHMM, or HHMMSS form.
fn parse_compact_utc_offset(value: &str) -> Option<(i32, i32, i32)> {
    match value.len() {
        2 => Some((parse_two_digits(value)?, 0, 0)),
        4 => Some((parse_two_digits(&value[..2])?, parse_two_digits(&value[2..])?, 0)),
        6 => Some((
            parse_two_digits(&value[..2])?,
            parse_two_digits(&value[2..4])?,
            parse_two_digits(&value[4..])?,
        )),
        _ => None,
    }
}

/// Parses exactly two ASCII digits.
fn parse_two_digits(value: &str) -> Option<i32> {
    let bytes = value.as_bytes();
    if bytes.len() != 2 || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }

    Some(((bytes[0] - b'0') as i32 * 10) + (bytes[1] - b'0') as i32)
}

/// Returns a stable chrono parse error for invalid time values.
fn invalid_time_error() -> chrono::ParseError {
    NaiveTime::parse_from_str("", TIME_FORMAT).expect_err("empty time should not parse")
}

/// Writes a fixed offset as `+HH:MM` or `+HH:MM:SS`.
fn write_utc_offset(f: &mut fmt::Formatter<'_>, offset: FixedOffset) -> fmt::Result {
    let seconds = offset.local_minus_utc();
    let sign = if seconds < 0 { '-' } else { '+' };
    let seconds = seconds.abs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let remaining_seconds = seconds % 60;

    if remaining_seconds == 0 {
        write!(f, "{sign}{hours:02}:{minutes:02}")
    } else {
        write!(f, "{sign}{hours:02}:{minutes:02}:{remaining_seconds:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timetz_parses_and_formats_offsets() {
        let value: PgTimeTz = "12:30:00.123+02".parse().unwrap();
        assert_eq!(value.to_string(), "12:30:00.123+02:00");

        let value: PgTimeTz = "12:30:00-07:30".parse().unwrap();
        assert_eq!(value.to_string(), "12:30:00-07:30");

        let value: PgTimeTz = "12:30:00+07:30:15".parse().unwrap();
        assert_eq!(value.to_string(), "12:30:00+07:30:15");
    }

    #[test]
    fn timetz_preserves_local_time_and_fixed_offset() {
        let value: PgTimeTz = "12:30:00.123456+02:30".parse().unwrap();

        assert_eq!(value.time(), NaiveTime::from_hms_micro_opt(12, 30, 0, 123_456).unwrap());
        assert_eq!(value.offset().local_minus_utc(), 9_000);
    }

    #[test]
    fn timetz_parses_postgres_offset_boundary() {
        let value: PgTimeTz = "00:00:00+15:59:59".parse().unwrap();
        assert_eq!(value.to_string(), "00:00:00+15:59:59");

        let value: PgTimeTz = "23:59:59.999999-15:59:59".parse().unwrap();
        assert_eq!(value.offset().local_minus_utc(), -57_599);
    }

    #[test]
    fn timetz_rejects_invalid_values() {
        assert!("12:30:00".parse::<PgTimeTz>().is_err());
        assert!("24:00:00+00".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+16".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+16:00".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+15:60".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+15:59:60".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+1".parse::<PgTimeTz>().is_err());
        assert!("12:30:00+01:02:03:04".parse::<PgTimeTz>().is_err());
    }

    #[test]
    fn utc_offset_parser_accepts_postgres_forms() {
        assert_eq!(parse_postgres_utc_offset("+02").unwrap().local_minus_utc(), 7_200);
        assert_eq!(parse_postgres_utc_offset("+0230").unwrap().local_minus_utc(), 9_000);
        assert_eq!(parse_postgres_utc_offset("+023015").unwrap().local_minus_utc(), 9_015);
        assert_eq!(parse_postgres_utc_offset("+02:30").unwrap().local_minus_utc(), 9_000);
        assert_eq!(parse_postgres_utc_offset("-02:30:15").unwrap().local_minus_utc(), -9_015);
        assert_eq!(parse_postgres_utc_offset("+15:59:59").unwrap().local_minus_utc(), 57_599);
    }

    #[test]
    fn utc_offset_parser_rejects_invalid_values() {
        assert!(parse_postgres_utc_offset("").is_none());
        assert!(parse_postgres_utc_offset("02").is_none());
        assert!(parse_postgres_utc_offset("+1").is_none());
        assert!(parse_postgres_utc_offset("+16").is_none());
        assert!(parse_postgres_utc_offset("+16:00").is_none());
        assert!(parse_postgres_utc_offset("+15:60").is_none());
        assert!(parse_postgres_utc_offset("+15:59:60").is_none());
        assert!(parse_postgres_utc_offset("+01:02:03:04").is_none());
    }
}
