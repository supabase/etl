//! Validation framework for ETL destinations and pipelines.
//!
//! Provides a trait-based validation framework for checking configuration
//! and runtime requirements before creating destinations or pipelines.
//!
//! # Validation Types
//!
//! The module supports three validation scenarios:
//!
//! - **Destination validation**: Validates destination connectivity (BigQuery dataset,
//!   Iceberg catalog). Use [`validate_destination`].
//! - **Pipeline validation**: Validates source database prerequisites (publication
//!   existence, replication slots). Use [`validate_pipeline`].
//! - **Combined validation**: Validates both destination and pipeline prerequisites
//!   together. Use [`validate_destination_pipeline`].

mod validators;

use std::fmt;

use async_trait::async_trait;
use etl_config::shared::PgConnectionConfig;
use sqlx::PgPool;

use crate::configs::destination::FullApiDestinationConfig;
use crate::configs::pipeline::FullApiPipelineConfig;
use crate::db::connect_to_source_database_with_defaults;

pub use validators::{DestinationValidator, PipelineValidator};

/// Shared context provided to validators during validation.
pub struct ValidationContext {
    /// Connection pool to the source PostgreSQL database.
    /// Required for pipeline validation, optional for destination validation.
    pub source_pool: Option<PgPool>,
}

impl ValidationContext {
    /// Creates an empty validation context (for destination-only validation).
    pub fn empty() -> Self {
        Self { source_pool: None }
    }

    /// Creates a validation context with the given source database pool.
    pub fn with_source(source_pool: PgPool) -> Self {
        Self {
            source_pool: Some(source_pool),
        }
    }
}

/// A validation failure with details about what failed.
#[derive(Debug, Clone)]
pub struct ValidationFailure {
    /// Name identifying what failed.
    pub name: String,
    /// Human-readable reason for the failure.
    pub reason: String,
}

impl ValidationFailure {
    /// Creates a new validation failure.
    pub fn new(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ValidationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.reason)
    }
}

/// Trait for implementing validation checks.
#[async_trait]
pub trait Validator: Send + Sync {
    /// Executes the validation check and returns a list of failures.
    /// An empty list means validation passed.
    async fn validate(&self, ctx: &ValidationContext) -> Vec<ValidationFailure>;
}

/// Validates destination configuration.
///
/// Returns a list of validation failures. Empty list means validation passed.
///
/// Checks that the destination is accessible and properly configured:
/// - **BigQuery**: Validates dataset exists and is accessible.
/// - **Iceberg**: Validates catalog connectivity.
/// - **Memory**: Always passes.
pub async fn validate_destination(
    destination_config: &FullApiDestinationConfig,
) -> Vec<ValidationFailure> {
    let ctx = ValidationContext::empty();
    let validator = DestinationValidator::new(destination_config.clone());
    validator.validate(&ctx).await
}

/// Validates pipeline configuration against the source database.
///
/// Returns a list of validation failures. Empty list means validation passed.
///
/// Checks pipeline prerequisites:
/// - Publication exists in the source database.
/// - Sufficient replication slots are available.
pub async fn validate_pipeline(
    pipeline_config: &FullApiPipelineConfig,
    source_config: &PgConnectionConfig,
) -> Vec<ValidationFailure> {
    let source_pool = match connect_to_source_database_with_defaults(source_config).await {
        Ok(pool) => pool,
        Err(e) => {
            return vec![ValidationFailure::new("Source Connection", e.to_string())];
        }
    };

    let ctx = ValidationContext::with_source(source_pool);
    let validator = PipelineValidator::new(pipeline_config.clone());
    validator.validate(&ctx).await
}

/// Validates both destination and pipeline configuration.
///
/// Returns a list of validation failures. Empty list means validation passed.
///
/// Runs all validators and collects all failures, returning them together
/// for comprehensive error reporting.
pub async fn validate_destination_pipeline(
    destination_config: &FullApiDestinationConfig,
    pipeline_config: &FullApiPipelineConfig,
    source_config: &PgConnectionConfig,
) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();

    // Validate destination (doesn't need source connection)
    failures.extend(validate_destination(destination_config).await);

    // Validate pipeline (needs source connection)
    failures.extend(validate_pipeline(pipeline_config, source_config).await);

    failures
}

/// Formats validation failures into a human-readable error message.
pub fn format_validation_failures(failures: &[ValidationFailure]) -> String {
    failures
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validation_result_display() {
        let result = ValidationFailure::new("test", "Something wrong");
        assert_eq!(format!("{}", result), "test: Something wrong");
    }

    #[tokio::test]
    async fn test_format_validation_failures() {
        let failures = vec![
            ValidationFailure::new("test1", "Error 1"),
            ValidationFailure::new("test2", "Error 2"),
        ];

        let message = format_validation_failures(&failures);
        assert_eq!(message, "test1: Error 1; test2: Error 2");
    }

    #[tokio::test]
    async fn test_format_empty_failures() {
        let failures: Vec<ValidationFailure> = vec![];
        let message = format_validation_failures(&failures);
        assert_eq!(message, "");
    }
}
