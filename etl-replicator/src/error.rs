use etl::error::EtlError;
use std::backtrace::Backtrace;
use std::fmt;
use thiserror::Error;

/// Result type for replicator operations.
pub type ReplicatorResult<T> = Result<T, ReplicatorError>;

/// Captured backtrace wrapper to avoid thiserror's unstable feature detection.
pub struct CapturedBacktrace(Backtrace);

impl CapturedBacktrace {
    fn capture() -> Self {
        Self(Backtrace::capture())
    }
}

impl fmt::Debug for CapturedBacktrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for the replicator service.
///
/// Wraps [`EtlError`] for pipeline errors and provides variants for
/// infrastructure errors (config, migrations, etc.). All variants capture
/// backtraces at creation time for debugging.
#[derive(Error)]
pub enum ReplicatorError {
    /// Pipeline or ETL-related error.
    #[error(transparent)]
    Etl(#[from] EtlError),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(
        #[source] Box<dyn std::error::Error + Send + Sync>,
        CapturedBacktrace,
    ),

    /// Database migration error.
    #[error("migration error: {0}")]
    Migration(#[source] sqlx::Error, CapturedBacktrace),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error, CapturedBacktrace),
}

impl fmt::Debug for ReplicatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{self}")?;

        if let Some(bt) = self.backtrace() {
            writeln!(f, "\nBacktrace:\n{bt}")?;
        }

        Ok(())
    }
}

impl ReplicatorError {
    /// Returns the backtrace for this error.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            ReplicatorError::Etl(e) => e.backtrace(),
            ReplicatorError::Config(_, cb) => Some(&cb.0),
            ReplicatorError::Migration(_, cb) => Some(&cb.0),
            ReplicatorError::Io(_, cb) => Some(&cb.0),
        }
    }

    /// Returns the inner [`EtlError`] if this is an ETL error.
    pub fn as_etl_error(&self) -> Option<&EtlError> {
        match self {
            ReplicatorError::Etl(e) => Some(e),
            _ => None,
        }
    }

    /// Creates a configuration error.
    pub fn config<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        ReplicatorError::Config(Box::new(err), CapturedBacktrace::capture())
    }
}

impl From<sqlx::Error> for ReplicatorError {
    fn from(err: sqlx::Error) -> Self {
        ReplicatorError::Migration(err, CapturedBacktrace::capture())
    }
}

impl From<std::io::Error> for ReplicatorError {
    fn from(err: std::io::Error) -> Self {
        ReplicatorError::Io(err, CapturedBacktrace::capture())
    }
}
