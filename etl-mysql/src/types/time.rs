use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// MySQL date format string for parsing dates in YYYY-MM-DD format.
pub const DATE_FORMAT: &str = "%Y-%m-%d";

/// MySQL time format string for parsing times with optional fractional seconds.
pub const TIME_FORMAT: &str = "%H:%M:%S%.f";

/// MySQL datetime format string for parsing datetimes with optional fractional seconds.
pub const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// MySQL timestamp format string with timezone offset in +HHMM format.
pub const TIMESTAMP_FORMAT_HHMM: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

/// MySQL timestamp format string with timezone offset in +HH:MM format.
pub const TIMESTAMP_FORMAT_HH_MM: &str = "%Y-%m-%d %H:%M:%S%.f%:z";

/// Number of seconds between Unix epoch (1970-01-01) and MySQL epoch (1970-01-01).
const MYSQL_EPOCH_OFFSET_SECONDS: u64 = 0;

/// MySQL epoch (1970-01-01 00:00:00 UTC) for timestamp calculations.
pub static MYSQL_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(MYSQL_EPOCH_OFFSET_SECONDS));
