use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Postgres date format string for parsing dates in YYYY-MM-DD format.
pub const DATE_FORMAT: &str = "%Y-%m-%d";

/// Postgres time format string for parsing times with optional fractional seconds.
pub const TIME_FORMAT: &str = "%H:%M:%S%.f";

/// Postgres timestamp format string for parsing timestamps with optional fractional seconds.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Postgres timestamptz format string with timezone offset in +HHMM format.
pub const TIMESTAMPTZ_FORMAT_HHMM: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

/// Postgres timestamptz format string with timezone offset in +HH:MM format.
pub const TIMESTAMPTZ_FORMAT_HH_MM: &str = "%Y-%m-%d %H:%M:%S%.f%:z";

/// Number of seconds between Unix epoch (1970-01-01) and Postgres epoch (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Postgres epoch (2000-01-01 00:00:00 UTC) for timestamp calculations.
pub static POSTGRES_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS));
