use std::{
    fmt,
    str::FromStr,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, FixedOffset, NaiveDateTime, NaiveTime, TimeZone};

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

/// Minimum byte index after `YYYY-MM-DD` where a UTC offset sign can appear.
const MIN_TIMESTAMP_OFFSET_INDEX: usize = 10;

/// Postgres timestamptz format string with time zone offset in +HHMM format.
pub const TIMESTAMPTZ_FORMAT_HHMM: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

/// Postgres timestamptz format string with time zone offset in +HH:MM format.
pub const TIMESTAMPTZ_FORMAT_HH_MM: &str = "%Y-%m-%d %H:%M:%S%.f%:z";

/// A Postgres `time with time zone` value.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PgTimeTz {
    /// The time of day without a date.
    time: NaiveTime,
    /// The fixed UTC offset associated with the time.
    offset: FixedOffset,
}

/// Parses a Postgres timestamptz text value with supported UTC offset forms.
pub fn parse_postgres_timestamptz(
    value: &str,
) -> Result<DateTime<FixedOffset>, chrono::ParseError> {
    match DateTime::<FixedOffset>::parse_from_str(value, TIMESTAMPTZ_FORMAT_HHMM) {
        Ok(datetime) => Ok(datetime),
        Err(_) => match DateTime::<FixedOffset>::parse_from_str(value, TIMESTAMPTZ_FORMAT_HH_MM) {
            Ok(datetime) => Ok(datetime),
            Err(error) => parse_postgres_timestamptz_with_offset_seconds(value).ok_or(error),
        },
    }
}

/// Parses a timestamptz value with an offset that includes seconds.
fn parse_postgres_timestamptz_with_offset_seconds(value: &str) -> Option<DateTime<FixedOffset>> {
    let (timestamp, offset) = split_timestamp_offset(value)?;
    let timestamp = timestamp.trim_end();
    let timestamp = NaiveDateTime::parse_from_str(timestamp, TIMESTAMP_FORMAT).ok()?;
    let offset = parse_utc_offset(offset)?;

    offset.from_local_datetime(&timestamp).single()
}

/// Splits a timestamp string into timestamp and numeric UTC offset parts.
fn split_timestamp_offset(value: &str) -> Option<(&str, &str)> {
    value.char_indices().rev().find_map(|(idx, ch)| {
        (idx > MIN_TIMESTAMP_OFFSET_INDEX && matches!(ch, '+' | '-')).then(|| value.split_at(idx))
    })
}

/// Parses a signed UTC offset in +HH, +HHMM, +HHMMSS, +HH:MM, or +HH:MM:SS
/// form.
fn parse_utc_offset(value: &str) -> Option<FixedOffset> {
    let sign = match value.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let value = &value[1..];
    let (hours, minutes, seconds) = if value.contains(':') {
        let parts = value.split(':').collect::<Vec<_>>();
        if !(2..=3).contains(&parts.len()) {
            return None;
        }

        let hours = parts[0].parse::<i32>().ok()?;
        let minutes = parts[1].parse::<i32>().ok()?;
        let seconds = parts.get(2).map_or(Some(0), |value| value.parse::<i32>().ok())?;
        (hours, minutes, seconds)
    } else {
        match value.len() {
            2 => (value.parse::<i32>().ok()?, 0, 0),
            4 => (value[..2].parse::<i32>().ok()?, value[2..].parse::<i32>().ok()?, 0),
            6 => (
                value[..2].parse::<i32>().ok()?,
                value[2..4].parse::<i32>().ok()?,
                value[4..].parse::<i32>().ok()?,
            ),
            _ => return None,
        }
    };

    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) || !(0..=59).contains(&seconds) {
        return None;
    }

    let seconds = sign * ((hours * 3600) + (minutes * 60) + seconds);
    FixedOffset::east_opt(seconds)
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
        let timestamp = format!("1970-01-01 {value}");
        let datetime = parse_postgres_timestamptz(&timestamp)?;

        Ok(Self::new(datetime.time(), *datetime.offset()))
    }
}

impl fmt::Display for PgTimeTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.time.format(TIME_FORMAT), format_offset(self.offset))
    }
}

/// Formats a fixed offset as `+HH:MM` or `+HH:MM:SS`.
fn format_offset(offset: FixedOffset) -> String {
    let seconds = offset.local_minus_utc();
    let sign = if seconds < 0 { '-' } else { '+' };
    let seconds = seconds.abs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let remaining_seconds = seconds % 60;

    if remaining_seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{remaining_seconds:02}")
    }
}

/// Number of seconds between Unix epoch (1970-01-01) and Postgres epoch
/// (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Postgres epoch (2000-01-01 00:00:00 UTC) for timestamp calculations.
pub static POSTGRES_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS));

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
    fn timestamptz_parses_supported_offset_forms() {
        parse_postgres_timestamptz("2026-01-01 12:30:00+02").unwrap();
        parse_postgres_timestamptz("2026-01-01 12:30:00+02:30").unwrap();
        parse_postgres_timestamptz("2026-01-01 12:30:00+02:30:15").unwrap();
    }
}
