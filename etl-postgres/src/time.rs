use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Number of seconds between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// PostgreSQL epoch (2000-01-01 00:00:00 UTC) for timestamp calculations.
pub static POSTGRES_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS));
