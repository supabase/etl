//! Validation framework for ETL sources, destinations, and pipelines.
//!
//! Provides a trait-based validation framework for checking configuration
//! and runtime requirements before creating sources, destinations, or pipelines.

mod validators;

use std::fmt;

use async_trait::async_trait;
use etl_config::Environment;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::config::ApiConfig;
use crate::configs::destination::FullApiDestinationConfig;
use crate::configs::pipeline::FullApiPipelineConfig;
use crate::configs::source::StoredSourceConfig;
use crate::db::connect_to_source_database_from_api;
use crate::k8s::{TrustedRootCertsCache, TrustedRootCertsError};
use crate::validation::validators::{DestinationValidator, PipelineValidator, SourceValidator};

/// Shared context provided to validators during validation.
pub struct ValidationContext {
    /// Runtime environment for environment-specific configuration.
    pub environment: Environment,
    /// Connection pool to the source PostgreSQL database.
    /// Required for source and pipeline validation, optional for destination validation.
    pub source_pool: Option<PgPool>,
    /// Trusted username used to validate the source role profile.
    pub trusted_username: Option<String>,
}

impl ValidationContext {
    /// Creates a new validation context builder.
    pub fn builder(environment: Environment) -> ValidationContextBuilder {
        ValidationContextBuilder {
            environment,
            source_pool: None,
            trusted_username: None,
        }
    }

    /// Builds a [`ValidationContext`] by connecting to a source database.
    pub async fn build_from_source(
        source_config: StoredSourceConfig,
        api_config: &ApiConfig,
        trusted_root_certs_cache: &TrustedRootCertsCache,
    ) -> Result<Self, ValidationError> {
        let tls_config = trusted_root_certs_cache
            .get_tls_config(api_config.source.tls_enabled)
            .await?;
        let source_pool =
            connect_to_source_database_from_api(&source_config.into_connection_config(tls_config))
                .await?;
        let environment = Environment::load()?;

        Ok(Self::builder(environment)
            .source_pool(source_pool)
            .trusted_username(api_config.source.trusted_username.clone())
            .build())
    }
}

/// Builder for constructing a [`ValidationContext`].
pub struct ValidationContextBuilder {
    environment: Environment,
    source_pool: Option<PgPool>,
    trusted_username: Option<String>,
}

impl ValidationContextBuilder {
    /// Sets the source database connection pool.
    pub fn source_pool(mut self, pool: PgPool) -> Self {
        self.source_pool = Some(pool);
        self
    }

    /// Sets the trusted username used for source role validation.
    pub fn trusted_username(mut self, trusted_username: Option<String>) -> Self {
        self.trusted_username = trusted_username;
        self
    }

    /// Builds the [`ValidationContext`].
    pub fn build(self) -> ValidationContext {
        ValidationContext {
            environment: self.environment,
            source_pool: self.source_pool,
            trusted_username: self.trusted_username,
        }
    }
}

/// Severity level of a validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    /// Critical failures that prevent the pipeline from running correctly.
    Critical,
    /// Warnings that don't prevent operation but indicate potential issues.
    Warning,
}

/// A validation failure with details about what failed.
#[derive(Debug, Clone)]
pub struct ValidationFailure {
    /// Name identifying what failed.
    pub name: String,
    /// Human-readable reason for the failure.
    pub reason: String,
    /// Severity of the failure.
    pub failure_type: FailureType,
}

impl ValidationFailure {
    /// Creates a new critical validation failure.
    pub fn critical(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reason: reason.into(),
            failure_type: FailureType::Critical,
        }
    }

    /// Creates a new warning validation failure.
    pub fn warning(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reason: reason.into(),
            failure_type: FailureType::Warning,
        }
    }
}

impl fmt::Display for ValidationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.reason)
    }
}

/// Errors that can occur during validation execution.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// Failed to execute a database query.
    #[error("database query failed: {0}")]
    Database(#[from] sqlx::Error),

    /// Failed to load trusted root certs for source connections.
    #[error(transparent)]
    TrustedRootCerts(#[from] TrustedRootCertsError),

    /// Failed to load the application environment.
    #[error("failed to load environment: {0}")]
    Environment(#[from] std::io::Error),

    /// Failed to connect to BigQuery.
    #[error("bigquery connection failed: {0}")]
    BigQuery(String),

    /// Failed to connect to Iceberg catalog.
    #[error("iceberg connection failed: {0}")]
    Iceberg(String),
}

/// Trait for implementing validation checks.
#[async_trait]
pub trait Validator: Send + Sync {
    /// Executes the validation check and returns a list of failures.
    /// An empty list means validation passed. Returns an error if validation
    /// could not be completed due to connection or configuration issues.
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError>;
}

/// Validates the connected source role profile.
///
/// Returns a list of validation failures. Empty list means validation passed.
/// Returns an error if validation could not be completed.
pub async fn validate_source(
    ctx: &ValidationContext,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    let validator = SourceValidator;
    validator.validate(ctx).await
}

/// Validates destination configuration.
///
/// Returns a list of validation failures. Empty list means validation passed.
/// Returns an error if validation could not be completed.
///
/// Checks that the destination is accessible and properly configured:
/// - **BigQuery**: Validates dataset exists and is accessible.
/// - **Iceberg**: Validates catalog connectivity.
pub async fn validate_destination(
    ctx: &ValidationContext,
    destination_config: &FullApiDestinationConfig,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    validate_destination_internal(ctx, destination_config, true).await
}

/// Validates pipeline configuration against the source database.
///
/// Returns a list of validation failures. Empty list means validation passed.
/// Returns an error if validation could not be completed.
///
/// Checks pipeline prerequisites:
/// - Publication exists in the source database.
/// - Sufficient replication slots are available.
pub async fn validate_pipeline(
    ctx: &ValidationContext,
    pipeline_config: &FullApiPipelineConfig,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    validate_pipeline_internal(ctx, pipeline_config, true).await
}

async fn validate_destination_internal(
    ctx: &ValidationContext,
    destination_config: &FullApiDestinationConfig,
    validate_source_profile: bool,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    let mut failures = Vec::new();

    if validate_source_profile {
        failures.extend(validate_source(ctx).await?);
    }

    let validator = DestinationValidator::new(destination_config.clone());
    failures.extend(validator.validate(ctx).await?);

    Ok(failures)
}

async fn validate_pipeline_internal(
    ctx: &ValidationContext,
    pipeline_config: &FullApiPipelineConfig,
    validate_source_profile: bool,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    let mut failures = Vec::new();

    if validate_source_profile {
        failures.extend(validate_source(ctx).await?);
    }

    let validator = PipelineValidator::new(pipeline_config.clone());
    failures.extend(validator.validate(ctx).await?);

    Ok(failures)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validation_failure_display() {
        let critical = ValidationFailure::critical("test_error", "Something wrong");
        assert_eq!(critical.to_string(), "test_error: Something wrong");
        assert_eq!(critical.failure_type, FailureType::Critical);

        let warning = ValidationFailure::warning("test_warning", "Something to note");
        assert_eq!(warning.to_string(), "test_warning: Something to note");
        assert_eq!(warning.failure_type, FailureType::Warning);
    }
}
