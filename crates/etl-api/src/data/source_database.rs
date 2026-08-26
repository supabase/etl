use std::{io::ErrorKind, sync::LazyLock};

use etl_config::shared::{PgConnectionConfig, PgConnectionOptions};
use etl_postgres::source::connect_to_source_database;
use sqlx::{PgPool, error::DatabaseError};

/// Classification for source database errors exposed by API routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceDatabaseErrorKind {
    /// The source operation or transaction exceeded a configured timeout.
    TimedOut,
    /// The source is unavailable, a connection was lost, or the pool cannot
    /// provide a usable connection.
    Unavailable,
    /// The source database returned an error that is not a connection lifecycle
    /// failure.
    Failed,
}

/// Minimum number of connections for the source Postgres connection pool.
///
/// API source pools are request-scoped administrative pools, so they should not
/// keep customer database connections open after their work is done.
const MIN_POOL_CONNECTIONS: u32 = 0;
/// Maximum number of connections for the source Postgres connection pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Application name for ETL API source database connections.
const APP_NAME_API: &str = "supabase_etl_api";

/// Connection options for API source database queries.
///
/// Uses strict timeouts to keep API requests responsive under contention.
static API_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_API).build());

/// Connects to the source database with API route defaults.
///
/// Uses state management options with moderate timeouts suitable for
/// administrative queries like listing tables and reading publications. If
/// configured, the source connection uses `hostaddr` as the TCP target and
/// preserves `host` as the canonical database hostname in stored API state.
pub(crate) async fn connect(config: &PgConnectionConfig) -> Result<PgPool, sqlx::Error> {
    connect_to_source_database(
        config,
        MIN_POOL_CONNECTIONS,
        MAX_POOL_CONNECTIONS,
        Some(&API_OPTIONS),
    )
    .await
}

/// Classifies a source database error using stable SQLSTATE and IO categories.
pub(crate) fn classify_error(error: &sqlx::Error) -> SourceDatabaseErrorKind {
    match error {
        sqlx::Error::Io(error) => classify_io_error(error.kind()),
        sqlx::Error::Database(error) => classify_database_error(error.as_ref()),
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed => {
            SourceDatabaseErrorKind::Unavailable
        }
        _ => SourceDatabaseErrorKind::Failed,
    }
}

/// Classifies an IO error without conflating timeouts with connectivity loss.
fn classify_io_error(error_kind: ErrorKind) -> SourceDatabaseErrorKind {
    if error_kind == ErrorKind::TimedOut {
        SourceDatabaseErrorKind::TimedOut
    } else if io_error_is_unavailable(error_kind) {
        SourceDatabaseErrorKind::Unavailable
    } else {
        SourceDatabaseErrorKind::Failed
    }
}

/// Returns true when an IO error means the source connection cannot be used.
fn io_error_is_unavailable(error_kind: ErrorKind) -> bool {
    matches!(
        error_kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::NetworkDown
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
    )
}

/// Classifies a PostgreSQL error using its stable SQLSTATE code.
fn classify_database_error(error: &dyn DatabaseError) -> SourceDatabaseErrorKind {
    let Some(code) = error.code() else {
        return SourceDatabaseErrorKind::Failed;
    };

    if sqlstate_is_timeout(code.as_ref()) {
        SourceDatabaseErrorKind::TimedOut
    } else if sqlstate_is_unavailable(code.as_ref()) {
        SourceDatabaseErrorKind::Unavailable
    } else {
        SourceDatabaseErrorKind::Failed
    }
}

/// Returns whether a SQLSTATE represents a timeout in API source sessions.
fn sqlstate_is_timeout(code: &str) -> bool {
    // API source sessions configure statement, lock, and idle-in-transaction
    // timeouts and do not issue explicit query cancellation or NOWAIT queries.
    // PostgreSQL uses the broader query_canceled and lock_not_available codes
    // when the first two timeouts expire. Newer PostgreSQL versions also
    // define a transaction_timeout code.
    matches!(code, "57014" | "55P03" | "25P03" | "25P04")
}

/// Returns whether a SQLSTATE means the source connection cannot be used.
fn sqlstate_is_unavailable(code: &str) -> bool {
    // PostgreSQL recommends matching on SQLSTATE codes rather than localized
    // message text. Class 08 covers connection exceptions; 53300 covers server
    // connection exhaustion; 57P01-57P05 cover shutdown/crash/database-dropped
    // and idle-session lifecycle failures.
    code.starts_with("08")
        || matches!(code, "53300" | "57P01" | "57P02" | "57P03" | "57P04" | "57P05")
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, error::Error as StdError, fmt};

    use super::*;

    /// Test database error that lets tests exercise SQLSTATE classification.
    #[derive(Debug)]
    struct TestDatabaseError {
        /// SQLSTATE exposed by this test error.
        code: &'static str,
    }

    impl TestDatabaseError {
        /// Creates a new test database error with the provided SQLSTATE.
        fn new(code: &'static str) -> Self {
            Self { code }
        }
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test database error {}", self.code)
        }
    }

    impl StdError for TestDatabaseError {}

    impl DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn pool_lifecycle_errors_are_reported_as_service_unavailable() {
        for error in
            [sqlx::Error::PoolTimedOut, sqlx::Error::PoolClosed, sqlx::Error::WorkerCrashed]
        {
            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Unavailable);
        }
    }

    #[test]
    fn non_connectivity_sqlx_errors_are_reported_as_upstream_failures() {
        for error in
            [sqlx::Error::Protocol("bad source response".to_owned()), sqlx::Error::RowNotFound]
        {
            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Failed);
        }
    }

    #[test]
    fn non_connectivity_io_errors_are_reported_as_upstream_failures() {
        for error_kind in [ErrorKind::InvalidData, ErrorKind::PermissionDenied] {
            let error = sqlx::Error::Io(std::io::Error::new(error_kind, "test IO error"));

            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Failed);
        }
    }

    #[test]
    fn io_timeout_is_reported_as_gateway_timeout() {
        let error = sqlx::Error::Io(std::io::Error::new(ErrorKind::TimedOut, "timed out"));

        assert_eq!(classify_error(&error), SourceDatabaseErrorKind::TimedOut);
    }

    #[test]
    fn connection_io_errors_are_reported_as_service_unavailable() {
        for error_kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::HostUnreachable,
            ErrorKind::NetworkUnreachable,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::NetworkDown,
            ErrorKind::BrokenPipe,
            ErrorKind::UnexpectedEof,
        ] {
            let error = sqlx::Error::Io(std::io::Error::new(error_kind, "test IO error"));

            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Unavailable);
        }
    }

    #[test]
    fn connection_sqlstate_errors_are_reported_as_service_unavailable() {
        for code in [
            "08000", "08001", "08003", "08004", "08006", "08007", "08P01", "53300", "57P01",
            "57P02", "57P03", "57P04", "57P05",
        ] {
            let error = sqlx::Error::Database(Box::new(TestDatabaseError::new(code)));

            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Unavailable);
        }
    }

    #[test]
    fn server_side_timeouts_are_reported_as_timed_out() {
        for code in ["57014", "55P03", "25P03", "25P04"] {
            let error = sqlx::Error::Database(Box::new(TestDatabaseError::new(code)));

            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::TimedOut);
        }
    }

    #[test]
    fn non_connection_sqlstate_errors_are_reported_as_upstream_failures() {
        for code in [
            "25P02", "40001", "40P01", "42501", "53100", "53200", "53400", "55006", "57000",
            "58030",
        ] {
            let error = sqlx::Error::Database(Box::new(TestDatabaseError::new(code)));

            assert_eq!(classify_error(&error), SourceDatabaseErrorKind::Failed);
        }
    }
}
