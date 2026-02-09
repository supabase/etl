use etl::error::EtlError;
use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt;

/// Returns whether terminal output should include backtraces.
fn should_render_backtrace() -> bool {
    matches!(
        std::env::var("RUST_BACKTRACE").as_deref(),
        Ok("1") | Ok("full")
    )
}

/// Result type for replicator operations.
pub type ReplicatorResult<T> = Result<T, ReplicatorError>;

/// Captured backtrace wrapper to avoid thiserror's unstable feature detection.
pub struct CapturedBacktrace(Backtrace);

impl CapturedBacktrace {
    /// Captures a new backtrace for an error variant.
    fn capture() -> Self {
        Self(Backtrace::capture())
    }
}

impl fmt::Debug for CapturedBacktrace {
    /// Renders the wrapped backtrace for debugging output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for the replicator service.
///
/// Wraps [`EtlError`] for pipeline errors and provides variants for
/// infrastructure errors.
#[derive(Debug)]
pub enum ReplicatorError {
    /// Pipeline or ETL-related error.
    Etl(EtlError),
    /// Configuration error.
    Config(Box<dyn Error + Send + Sync>, CapturedBacktrace),
    /// Database migration error.
    Migration(sqlx::Error, CapturedBacktrace),
    /// I/O error.
    Io(std::io::Error, CapturedBacktrace),
}

impl ReplicatorError {
    /// Returns a short category label for this error.
    pub fn category(&self) -> &'static str {
        match self {
            ReplicatorError::Etl(_) => "replicator error",
            ReplicatorError::Config(_, _) => "configuration error",
            ReplicatorError::Migration(_, _) => "migration error",
            ReplicatorError::Io(_, _) => "i/o error",
        }
    }

    /// Returns the backtrace for this error.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            ReplicatorError::Etl(err) => err.backtrace(),
            ReplicatorError::Config(_, cb) => Some(&cb.0),
            ReplicatorError::Migration(_, cb) => Some(&cb.0),
            ReplicatorError::Io(_, cb) => Some(&cb.0),
        }
    }

    /// Creates a configuration error from any boxed source.
    pub fn config<E: Error + Send + Sync + 'static>(err: E) -> Self {
        ReplicatorError::Config(Box::new(err), CapturedBacktrace::capture())
    }

    /// Returns a user-oriented report for terminal output.
    pub fn render_report(&self) -> String {
        let mut out = String::new();
        out.push_str("replicator failed\n");
        out.push_str(&format!("category: {}\n", self.category()));
        out.push_str(&format!("error: {}\n", self));

        if !matches!(self, ReplicatorError::Etl(err) if err.errors().is_some()) {
            let mut source = Error::source(self);
            let mut idx = 1usize;
            while let Some(err) = source {
                out.push_str(&format!("cause {idx}: {err}\n"));
                source = err.source();
                idx += 1;
            }
        }

        if should_render_backtrace()
            && let Some(backtrace) = self.backtrace()
        {
            out.push_str("backtrace:\n");
            out.push_str(&backtrace.to_string());
            if !out.ends_with('\n') {
                out.push('\n');
            }
        }

        out
    }
}

impl fmt::Display for ReplicatorError {
    /// Renders a user-focused one-line description for terminal and log output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicatorError::Etl(err) => write!(f, "{err}"),
            ReplicatorError::Config(source, _) => write!(f, "configuration error: {source}"),
            ReplicatorError::Migration(source, _) => write!(f, "migration error: {source}"),
            ReplicatorError::Io(source, _) => write!(f, "i/o error: {source}"),
        }
    }
}

impl Error for ReplicatorError {
    /// Returns the direct cause for this error variant.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ReplicatorError::Etl(err) => err.source(),
            ReplicatorError::Config(source, _) => Some(source.as_ref()),
            ReplicatorError::Migration(source, _) => Some(source),
            ReplicatorError::Io(source, _) => Some(source),
        }
    }
}

impl From<sqlx::Error> for ReplicatorError {
    /// Converts a SQLx error into a migration error variant.
    fn from(err: sqlx::Error) -> Self {
        ReplicatorError::Migration(err, CapturedBacktrace::capture())
    }
}

impl From<std::io::Error> for ReplicatorError {
    /// Converts an I/O error into an I/O error variant.
    fn from(err: std::io::Error) -> Self {
        ReplicatorError::Io(err, CapturedBacktrace::capture())
    }
}

impl From<EtlError> for ReplicatorError {
    /// Converts an ETL error into a replicator ETL error variant.
    fn from(err: EtlError) -> Self {
        ReplicatorError::Etl(err)
    }
}
