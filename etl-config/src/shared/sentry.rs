use secrecy::SecretString;
use serde::Deserialize;

/// Sentry error tracking and monitoring configuration.
///
/// Contains the DSN and other settings required to initialize Sentry for
/// error tracking and performance monitoring in ETL applications.
#[derive(Debug, Clone, Deserialize)]
pub struct SentryConfig {
    /// Sentry DSN (Data Source Name) for error reporting and monitoring.
    pub dsn: SecretString,
}
