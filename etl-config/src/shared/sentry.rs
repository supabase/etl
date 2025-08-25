use serde::{Deserialize, Serialize};

/// Sentry error tracking and monitoring configuration.
///
/// Contains the DSN and other settings required to initialize Sentry for
/// error tracking and performance monitoring in ETL applications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentryConfig {
    /// Sentry DSN (Data Source Name) for error reporting and monitoring.
    pub dsn: String,
}
