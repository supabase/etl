use serde::{Deserialize, Serialize};

/// Configuration for Sentry error tracking and performance monitoring.
///
/// This struct holds the necessary configuration to initialize Sentry in the application.
/// It includes the DSN (Data Source Name) and optional service-specific tags to differentiate
/// between different services reporting to the same Sentry project.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SentryConfig {
    /// Sentry DSN (Data Source Name) for error reporting.
    pub dsn: String,
}
