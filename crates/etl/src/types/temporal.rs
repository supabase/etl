use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl_postgres::types::{DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM};

/// Temporal value that falls outside Rust's parsed temporal domain.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PgTemporalOutOfRange {
    /// Original PostgreSQL text representation.
    text: String,
    /// Position relative to finite ranges supported by destinations.
    bound: PgTemporalBound,
}

impl PgTemporalOutOfRange {
    /// Creates an out-of-range temporal value.
    pub fn new(text: impl Into<String>, bound: PgTemporalBound) -> Self {
        Self { text: text.into(), bound }
    }

    /// Returns the original PostgreSQL text representation.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the relative bound represented by the value.
    pub fn bound(&self) -> PgTemporalBound {
        self.bound
    }
}

impl fmt::Display for PgTemporalOutOfRange {
    /// Formats the original PostgreSQL temporal text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

/// Relative position of an out-of-range temporal value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PgTemporalBound {
    /// Value is below the finite range supported by the parser.
    Lower,
    /// Value is above the finite range supported by the parser.
    Upper,
}

/// PostgreSQL `date` value.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PgDate {
    /// Finite date parsed into chrono.
    Finite(NaiveDate),
    /// PostgreSQL positive infinity.
    PosInfinity,
    /// PostgreSQL negative infinity.
    NegInfinity,
    /// PostgreSQL date that is valid source text but outside chrono's range.
    OutOfRange(PgTemporalOutOfRange),
}

impl PgDate {
    /// Returns the finite value when this date is finite.
    pub fn as_finite(&self) -> Option<NaiveDate> {
        match self {
            Self::Finite(value) => Some(*value),
            Self::PosInfinity | Self::NegInfinity | Self::OutOfRange(_) => None,
        }
    }

    /// Returns the relative bound for non-finite values.
    pub fn non_finite_bound(&self) -> Option<PgTemporalBound> {
        match self {
            Self::Finite(_) => None,
            Self::PosInfinity => Some(PgTemporalBound::Upper),
            Self::NegInfinity => Some(PgTemporalBound::Lower),
            Self::OutOfRange(value) => Some(value.bound()),
        }
    }
}

impl Default for PgDate {
    /// Returns the default finite date.
    fn default() -> Self {
        Self::Finite(NaiveDate::default())
    }
}

impl From<NaiveDate> for PgDate {
    /// Converts a finite chrono date into a PostgreSQL date.
    fn from(value: NaiveDate) -> Self {
        Self::Finite(value)
    }
}

impl fmt::Display for PgDate {
    /// Formats this date as PostgreSQL text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finite(value) => write!(f, "{}", value.format(DATE_FORMAT)),
            Self::PosInfinity => f.write_str("infinity"),
            Self::NegInfinity => f.write_str("-infinity"),
            Self::OutOfRange(value) => write!(f, "{value}"),
        }
    }
}

/// PostgreSQL `time without time zone` value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PgTime {
    /// Finite time parsed into chrono.
    Finite(NaiveTime),
    /// PostgreSQL's upper endpoint, which chrono cannot represent.
    TwentyFourHour,
}

impl PgTime {
    /// Returns the finite value when this time is finite.
    pub fn as_finite(&self) -> Option<NaiveTime> {
        match self {
            Self::Finite(value) => Some(*value),
            Self::TwentyFourHour => None,
        }
    }
}

impl Default for PgTime {
    /// Returns the default finite time.
    fn default() -> Self {
        Self::Finite(NaiveTime::default())
    }
}

impl From<NaiveTime> for PgTime {
    /// Converts a finite chrono time into a PostgreSQL time.
    fn from(value: NaiveTime) -> Self {
        Self::Finite(value)
    }
}

impl fmt::Display for PgTime {
    /// Formats this time as PostgreSQL text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finite(value) => write!(f, "{}", value.format(TIME_FORMAT)),
            Self::TwentyFourHour => f.write_str("24:00:00"),
        }
    }
}

/// PostgreSQL `timestamp without time zone` value.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PgTimestamp {
    /// Finite timestamp parsed into chrono.
    Finite(NaiveDateTime),
    /// PostgreSQL positive infinity.
    PosInfinity,
    /// PostgreSQL negative infinity.
    NegInfinity,
    /// PostgreSQL timestamp that is valid source text but outside chrono's
    /// range.
    OutOfRange(PgTemporalOutOfRange),
}

impl PgTimestamp {
    /// Returns the finite value when this timestamp is finite.
    pub fn as_finite(&self) -> Option<NaiveDateTime> {
        match self {
            Self::Finite(value) => Some(*value),
            Self::PosInfinity | Self::NegInfinity | Self::OutOfRange(_) => None,
        }
    }

    /// Returns the relative bound for non-finite values.
    pub fn non_finite_bound(&self) -> Option<PgTemporalBound> {
        match self {
            Self::Finite(_) => None,
            Self::PosInfinity => Some(PgTemporalBound::Upper),
            Self::NegInfinity => Some(PgTemporalBound::Lower),
            Self::OutOfRange(value) => Some(value.bound()),
        }
    }
}

impl Default for PgTimestamp {
    /// Returns the default finite timestamp.
    fn default() -> Self {
        Self::Finite(NaiveDateTime::default())
    }
}

impl From<NaiveDateTime> for PgTimestamp {
    /// Converts a finite chrono timestamp into a PostgreSQL timestamp.
    fn from(value: NaiveDateTime) -> Self {
        Self::Finite(value)
    }
}

impl fmt::Display for PgTimestamp {
    /// Formats this timestamp as PostgreSQL text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finite(value) => write!(f, "{}", value.format(TIMESTAMP_FORMAT)),
            Self::PosInfinity => f.write_str("infinity"),
            Self::NegInfinity => f.write_str("-infinity"),
            Self::OutOfRange(value) => write!(f, "{value}"),
        }
    }
}

/// PostgreSQL `timestamp with time zone` value normalized to UTC.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PgTimestampTz {
    /// Finite timestamp parsed into chrono.
    Finite(DateTime<Utc>),
    /// PostgreSQL positive infinity.
    PosInfinity,
    /// PostgreSQL negative infinity.
    NegInfinity,
    /// PostgreSQL timestamp that is valid source text but outside chrono's
    /// range.
    OutOfRange(PgTemporalOutOfRange),
}

impl PgTimestampTz {
    /// Returns the finite value when this timestamp is finite.
    pub fn as_finite(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Finite(value) => Some(*value),
            Self::PosInfinity | Self::NegInfinity | Self::OutOfRange(_) => None,
        }
    }

    /// Returns the relative bound for non-finite values.
    pub fn non_finite_bound(&self) -> Option<PgTemporalBound> {
        match self {
            Self::Finite(_) => None,
            Self::PosInfinity => Some(PgTemporalBound::Upper),
            Self::NegInfinity => Some(PgTemporalBound::Lower),
            Self::OutOfRange(value) => Some(value.bound()),
        }
    }
}

impl Default for PgTimestampTz {
    /// Returns the default finite timestamp.
    fn default() -> Self {
        Self::Finite(DateTime::<Utc>::default())
    }
}

impl From<DateTime<Utc>> for PgTimestampTz {
    /// Converts a finite chrono timestamp into a PostgreSQL timestamptz.
    fn from(value: DateTime<Utc>) -> Self {
        Self::Finite(value)
    }
}

impl fmt::Display for PgTimestampTz {
    /// Formats this timestamp as PostgreSQL text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finite(value) => write!(f, "{}", value.format(TIMESTAMPTZ_FORMAT_HH_MM)),
            Self::PosInfinity => f.write_str("infinity"),
            Self::NegInfinity => f.write_str("-infinity"),
            Self::OutOfRange(value) => write!(f, "{value}"),
        }
    }
}
