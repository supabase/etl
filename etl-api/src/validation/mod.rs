//! Pipeline validation for the ETL API.
//!
//! Provides a trait-based validation framework for checking pipeline configuration
//! and runtime requirements before starting replication. Validators receive a
//! [`ValidationContext`] containing shared resources like database connections.
//!
//! # Design Decision
//!
//! All validation logic is intentionally contained within the API crate rather than
//! being distributed across `etl` or `etl-destinations`. This design choice:
//!
//! - **Decouples validation from core ETL logic**: The `etl` crate focuses on
//!   replication mechanics without validation concerns.
//! - **Decouples validation from destinations**: The `etl-destinations` crate
//!   implements destination adapters without API-specific validation requirements.
//! - **Centralizes API-specific checks**: All prerequisites validation happens in
//!   one place, making it easier to maintain and extend.
//! - **Enables API-specific error handling**: Validation errors can be transformed
//!   into appropriate HTTP responses without leaking implementation details.

mod validators;

use std::fmt;

use async_trait::async_trait;
use etl_config::shared::PgConnectionConfig;
use sqlx::PgPool;
use thiserror::Error;

use crate::db::connect_to_source_database_with_defaults;

pub use validators::{
    BigQueryValidator, DestinationValidator, IcebergValidator, PublicationExistsValidator,
    ReplicationSlotsValidator, SourceConnectionValidator,
};

/// Shared context provided to all validators during validation.
///
/// Contains resources that validators may need to perform their checks,
/// such as database connection pools and configuration parameters.
pub struct ValidationContext {
    /// Connection pool to the source PostgreSQL database.
    pub source_pool: PgPool,
}

impl ValidationContext {
    /// Creates a new validation context with the given source database pool.
    pub fn new(source_pool: PgPool) -> Self {
        Self { source_pool }
    }
}

/// Result of a single validation check.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Validation passed successfully.
    Passed,
    /// Validation failed with details.
    Failed {
        /// Name identifying what failed.
        name: String,
        /// Human-readable reason for the failure.
        reason: String,
    },
}

impl ValidationResult {
    /// Returns true if validation passed.
    pub fn passed(&self) -> bool {
        matches!(self, ValidationResult::Passed)
    }
}

impl fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationResult::Passed => write!(f, "[PASSED]"),
            ValidationResult::Failed { name, reason } => write!(f, "[FAILED] {}: {}", name, reason),
        }
    }
}

/// Trait for implementing validation checks.
///
/// Validators perform async checks against destinations, sources, or configuration
/// to determine if the ETL pipeline can operate correctly. Each validator receives
/// a [`ValidationContext`] providing access to shared resources.
///
/// This trait is object-safe and can be used with `Box<dyn Validator>` for
/// heterogeneous collections of validators.
#[async_trait]
pub trait Validator: Send + Sync {
    /// Returns a human-readable name for this validator.
    fn name(&self) -> &str;

    /// Executes the validation check.
    ///
    /// Returns a [`ValidationResult`] indicating success or failure with an explanatory message.
    /// Should not panic; errors during validation should be captured and returned as failed results.
    async fn validate(&self, ctx: &ValidationContext) -> ValidationResult;
}

/// Runs multiple validators and collects their results.
///
/// Executes all provided validators sequentially and returns the aggregated results.
/// Does not short-circuit on failure; all validators run to completion.
pub async fn run_validations(
    validators: &[Box<dyn Validator>],
    ctx: &ValidationContext,
) -> Vec<ValidationResult> {
    let mut results = Vec::with_capacity(validators.len());

    for validator in validators {
        let result = validator.validate(ctx).await;
        results.push(result);
    }

    results
}

/// Checks if all validation results passed.
pub fn all_passed(results: &[ValidationResult]) -> bool {
    results.iter().all(|r| r.passed())
}

/// Returns only the failed validation results.
pub fn failed_validations(results: &[ValidationResult]) -> Vec<&ValidationResult> {
    results.iter().filter(|r| !r.passed()).collect()
}

/// Error returned when pipeline validation fails.
#[derive(Debug, Error)]
#[error("Pipeline validation failed: {0}")]
pub struct PipelineValidationError(pub String);

impl PipelineValidationError {
    /// Creates a new validation error from failed validation results.
    pub fn from_results(results: &[ValidationResult]) -> Self {
        let failed = failed_validations(results);
        let messages: Vec<String> = failed
            .iter()
            .filter_map(|r| match r {
                ValidationResult::Failed { name, reason } => Some(format!("{}: {}", name, reason)),
                ValidationResult::Passed => None,
            })
            .collect();
        Self(messages.join("; "))
    }
}

/// Validates pipeline prerequisites before creation.
///
/// Runs the following checks:
/// - Source database connectivity
/// - Publication exists
/// - Sufficient replication slots available for `max_table_sync_workers + 1`
pub async fn validate_pipeline_prerequisites(
    source_config: &PgConnectionConfig,
    publication_name: &str,
    max_table_sync_workers: u16,
) -> Result<(), PipelineValidationError> {
    let source_pool = connect_to_source_database_with_defaults(source_config)
        .await
        .map_err(|e| PipelineValidationError(format!("Failed to connect to source database: {e}")))?;

    let ctx = ValidationContext::new(source_pool);

    let validators: Vec<Box<dyn Validator>> = vec![
        Box::new(SourceConnectionValidator),
        Box::new(PublicationExistsValidator::new(publication_name.to_string())),
        Box::new(ReplicationSlotsValidator::new(max_table_sync_workers)),
    ];

    let results = run_validations(&validators, &ctx).await;

    if !all_passed(&results) {
        return Err(PipelineValidationError::from_results(&results));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysPassValidator;

    #[async_trait]
    impl Validator for AlwaysPassValidator {
        fn name(&self) -> &str {
            "always_pass"
        }

        async fn validate(&self, _ctx: &ValidationContext) -> ValidationResult {
            ValidationResult::Passed
        }
    }

    struct AlwaysFailValidator;

    #[async_trait]
    impl Validator for AlwaysFailValidator {
        fn name(&self) -> &str {
            "always_fail"
        }

        async fn validate(&self, _ctx: &ValidationContext) -> ValidationResult {
            ValidationResult::Failed {
                name: "always_fail".to_string(),
                reason: "Test failed".to_string(),
            }
        }
    }

    #[tokio::test]
    async fn test_validation_result_passed() {
        let result = ValidationResult::Passed;
        assert!(result.passed());
    }

    #[tokio::test]
    async fn test_validation_result_failed() {
        let result = ValidationResult::Failed {
            name: "test".to_string(),
            reason: "Something wrong".to_string(),
        };
        assert!(!result.passed());
    }

    #[tokio::test]
    async fn test_all_passed() {
        let results = vec![ValidationResult::Passed, ValidationResult::Passed];
        assert!(all_passed(&results));

        let results_with_failure = vec![
            ValidationResult::Passed,
            ValidationResult::Failed {
                name: "b".to_string(),
                reason: "fail".to_string(),
            },
        ];
        assert!(!all_passed(&results_with_failure));
    }

    #[tokio::test]
    async fn test_failed_validations() {
        let results = vec![
            ValidationResult::Passed,
            ValidationResult::Failed {
                name: "b".to_string(),
                reason: "fail".to_string(),
            },
            ValidationResult::Failed {
                name: "c".to_string(),
                reason: "also fail".to_string(),
            },
        ];

        let failed = failed_validations(&results);
        assert_eq!(failed.len(), 2);
    }

    #[tokio::test]
    async fn test_validation_result_display() {
        let success = ValidationResult::Passed;
        assert_eq!(format!("{}", success), "[PASSED]");

        let failure = ValidationResult::Failed {
            name: "test".to_string(),
            reason: "Something wrong".to_string(),
        };
        assert_eq!(format!("{}", failure), "[FAILED] test: Something wrong");
    }
}
