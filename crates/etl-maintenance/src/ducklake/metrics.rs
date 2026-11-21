//! Metrics shared by DuckLake catalog metadata readers.

use std::{io, sync::Once};

use metrics::{Unit, counter, describe_counter};
use sqlx::Error;

/// Failed metadata attempts, including failures recovered by direct fallback.
pub(super) const ETL_DUCKLAKE_CATALOG_METADATA_READ_ERRORS_TOTAL: &str =
    "etl_ducklake_catalog_metadata_read_errors_total";
/// Catalog connection route used by a metadata attempt.
const ROUTE_LABEL: &str = "route";
/// Bounded failure category without provider-controlled text.
const REASON_LABEL: &str = "reason";
/// Registers the metadata metric description once per process.
static REGISTER_METRICS: Once = Once::new();

/// Counts a failed attempt on the pooler or direct connection.
pub(super) fn record_metadata_read_error(route: &'static str, error: &Error) {
    REGISTER_METRICS.call_once(|| {
        describe_counter!(
            ETL_DUCKLAKE_CATALOG_METADATA_READ_ERRORS_TOTAL,
            Unit::Count,
            "Failed DuckLake catalog metadata read attempts, labeled by route and reason. Query \
             cancellation includes server statement timeouts."
        );
    });

    let reason = match error {
        Error::Io(error) if error.kind() == io::ErrorKind::TimedOut => "client_timeout",
        Error::PoolTimedOut => "checkout_timeout",
        // PostgreSQL uses 57014 for statement timeouts and other cancellations.
        Error::Database(error) if error.code().as_deref() == Some("57014") => "query_canceled",
        _ => "other",
    };

    counter!(
        ETL_DUCKLAKE_CATALOG_METADATA_READ_ERRORS_TOTAL,
        ROUTE_LABEL => route,
        REASON_LABEL => reason,
    )
    .increment(1);
}
