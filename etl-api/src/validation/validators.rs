//! Built-in validators for common ETL pipeline prerequisites.

use async_trait::async_trait;
use etl::destination::Destination;
use etl::store::both::memory::MemoryStore;
use etl_destinations::bigquery::BigQueryDestination;
use etl_destinations::iceberg::{DestinationNamespace, IcebergClient, IcebergDestination};
use secrecy::ExposeSecret;

use crate::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};

use super::{ValidationContext, ValidationResult, Validator};

// =============================================================================
// Source Validators
// =============================================================================

/// Validates that the source Postgres database is accessible.
///
/// Attempts to execute a simple query to verify connectivity,
/// authentication, and basic permissions.
pub struct SourceConnectionValidator;

#[async_trait]
impl Validator for SourceConnectionValidator {
    fn name(&self) -> &str {
        "source_connection"
    }

    async fn validate(&self, ctx: &ValidationContext) -> ValidationResult {
        match sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&ctx.source_pool)
            .await
        {
            Ok(_) => ValidationResult::Passed,
            Err(e) => ValidationResult::Failed {
                name: "source_connection".to_string(),
                reason: format!("Failed to connect to source PostgreSQL database: {e}"),
            },
        }
    }
}

/// Validates that the required publication exists in the source database.
///
/// Checks that the publication specified in the pipeline configuration
/// exists in the source Postgres database. The publication must exist before
/// replication can begin.
pub struct PublicationExistsValidator {
    publication_name: String,
}

impl PublicationExistsValidator {
    /// Creates a new publication exists validator.
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExistsValidator {
    fn name(&self) -> &str {
        "publication_exists"
    }

    async fn validate(&self, ctx: &ValidationContext) -> ValidationResult {
        let exists: Result<bool, _> = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
        )
        .bind(&self.publication_name)
        .fetch_one(&ctx.source_pool)
        .await;

        match exists {
            Ok(true) => ValidationResult::Passed,
            Ok(false) => ValidationResult::Failed {
                name: "publication_exists".to_string(),
                reason: format!(
                    "Publication '{}' does not exist. Create it with: \
                    CREATE PUBLICATION {} FOR TABLE ...",
                    self.publication_name, self.publication_name
                ),
            },
            Err(e) => ValidationResult::Failed {
                name: "publication_exists".to_string(),
                reason: format!("Failed to check publication: {e}"),
            },
        }
    }
}

/// Validates that there are enough free replication slots for the pipeline.
///
/// Checks the source Postgres database to ensure that the configured
/// `max_table_sync_workers + 1` does not exceed the number of available replication slots.
/// The pipeline requires:
/// - 1 slot for the apply worker (main replication stream)
/// - Up to `max_table_sync_workers` slots for parallel initial table synchronization
pub struct ReplicationSlotsValidator {
    max_table_sync_workers: u16,
}

impl ReplicationSlotsValidator {
    /// Creates a new replication slots validator.
    pub fn new(max_table_sync_workers: u16) -> Self {
        Self {
            max_table_sync_workers,
        }
    }
}

#[async_trait]
impl Validator for ReplicationSlotsValidator {
    fn name(&self) -> &str {
        "replication_slots"
    }

    async fn validate(&self, ctx: &ValidationContext) -> ValidationResult {
        let max_slots: Result<i32, _> =
            sqlx::query_scalar("SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'")
                .fetch_one(&ctx.source_pool)
                .await;

        let max_slots = match max_slots {
            Ok(v) => v,
            Err(e) => {
                return ValidationResult::Failed {
                    name: "replication_slots".to_string(),
                    reason: format!("Failed to query max_replication_slots: {e}"),
                };
            }
        };

        let used_slots: Result<i64, _> =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_replication_slots")
                .fetch_one(&ctx.source_pool)
                .await;

        let used_slots = match used_slots {
            Ok(v) => v,
            Err(e) => {
                return ValidationResult::Failed {
                    name: "replication_slots".to_string(),
                    reason: format!("Failed to query replication slots: {e}"),
                };
            }
        };

        let free_slots = max_slots as i64 - used_slots;
        let required_slots = self.max_table_sync_workers as i64 + 1;

        if required_slots <= free_slots {
            ValidationResult::Passed
        } else {
            ValidationResult::Failed {
                name: "replication_slots".to_string(),
                reason: format!(
                    "Insufficient replication slots: {free_slots} free but {required_slots} required \
                    (max_table_sync_workers={} + 1 apply worker). \
                    Current usage: {used_slots}/{max_slots} slots. \
                    Either increase max_replication_slots in PostgreSQL or reduce max_table_sync_workers.",
                    self.max_table_sync_workers
                ),
            }
        }
    }
}

// =============================================================================
// Destination Validators
// =============================================================================

/// Validates BigQuery destination by instantiating the destination and calling validate.
///
/// Creates a [`BigQueryDestination`] instance and calls its `validate()` method
/// to verify connectivity and dataset accessibility.
pub struct BigQueryValidator {
    project_id: String,
    dataset_id: String,
    service_account_key: String,
}

impl BigQueryValidator {
    /// Creates a new BigQuery validator.
    pub fn new(project_id: String, dataset_id: String, service_account_key: String) -> Self {
        Self {
            project_id,
            dataset_id,
            service_account_key,
        }
    }
}

#[async_trait]
impl Validator for BigQueryValidator {
    fn name(&self) -> &str {
        "bigquery_destination"
    }

    async fn validate(&self, _ctx: &ValidationContext) -> ValidationResult {
        // Create a MemoryStore for validation (not actually used by validate())
        let store = MemoryStore::new();

        let destination = match BigQueryDestination::new_with_key(
            self.project_id.clone(),
            self.dataset_id.clone(),
            &self.service_account_key,
            None, // max_staleness_mins
            1,    // max_concurrent_streams (not used for validation)
            store,
        )
        .await
        {
            Ok(dest) => dest,
            Err(e) => {
                return ValidationResult::Failed {
                    name: "bigquery_destination".to_string(),
                    reason: format!(
                        "Failed to create BigQuery destination: {}",
                        e.detail().unwrap_or(&e.to_string())
                    ),
                };
            }
        };

        match destination.validate().await {
            Ok(()) => ValidationResult::Passed,
            Err(e) => ValidationResult::Failed {
                name: "bigquery_destination".to_string(),
                reason: e.detail().unwrap_or(&e.to_string()).to_string(),
            },
        }
    }
}

/// Validates Iceberg destination by instantiating the destination and calling validate.
///
/// Creates an [`IcebergDestination`] instance and calls its `validate()` method
/// to verify catalog connectivity.
pub struct IcebergValidator {
    config: FullApiIcebergConfig,
}

impl IcebergValidator {
    /// Creates a new Iceberg validator.
    pub fn new(config: FullApiIcebergConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for IcebergValidator {
    fn name(&self) -> &str {
        "iceberg_destination"
    }

    async fn validate(&self, _ctx: &ValidationContext) -> ValidationResult {
        // Create the Iceberg client based on config type
        let (client, namespace) = match &self.config {
            FullApiIcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                namespace,
                ..
            } => {
                let client = IcebergClient::new_with_supabase_catalog(
                    project_ref,
                    "supabase.com",
                    catalog_token.expose_secret().to_string(),
                    warehouse_name.clone(),
                    s3_access_key_id.expose_secret().to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                    s3_region.clone(),
                )
                .await;
                (client, namespace.clone())
            }
            FullApiIcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
                namespace,
                ..
            } => {
                let props = std::collections::HashMap::from([
                    (
                        "s3.access-key-id".to_string(),
                        s3_access_key_id.expose_secret().to_string(),
                    ),
                    (
                        "s3.secret-access-key".to_string(),
                        s3_secret_access_key.expose_secret().to_string(),
                    ),
                    ("s3.endpoint".to_string(), s3_endpoint.clone()),
                    ("s3.region".to_string(), "auto".to_string()),
                ]);
                let client = IcebergClient::new_with_rest_catalog(
                    catalog_uri.clone(),
                    warehouse_name.clone(),
                    props,
                )
                .await;
                (client, namespace.clone())
            }
        };

        let client = match client {
            Ok(client) => client,
            Err(e) => {
                return ValidationResult::Failed {
                    name: "iceberg_destination".to_string(),
                    reason: format!("Failed to create Iceberg client: {e}"),
                };
            }
        };

        // Create a MemoryStore for validation (not actually used by validate())
        let store = MemoryStore::new();
        let destination_namespace = match namespace {
            Some(ns) => DestinationNamespace::Single(ns),
            None => DestinationNamespace::OnePerSchema,
        };
        let destination = IcebergDestination::new(client, destination_namespace, store);

        match destination.validate().await {
            Ok(()) => ValidationResult::Passed,
            Err(e) => ValidationResult::Failed {
                name: "iceberg_destination".to_string(),
                reason: e.detail().unwrap_or(&e.to_string()).to_string(),
            },
        }
    }
}

/// Validates destination connectivity based on the destination configuration.
///
/// Dispatches to the appropriate destination-specific validator:
/// - **BigQuery**: Uses [`BigQueryValidator`]
/// - **Iceberg**: Uses [`IcebergValidator`]
/// - **Memory**: Always passes (in-memory destination is always available)
pub struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    /// Creates a new destination validator with the given configuration.
    pub fn new(config: FullApiDestinationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for DestinationValidator {
    fn name(&self) -> &str {
        "destination"
    }

    async fn validate(&self, ctx: &ValidationContext) -> ValidationResult {
        match &self.config {
            FullApiDestinationConfig::Memory => ValidationResult::Passed,
            FullApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                ..
            } => {
                let validator = BigQueryValidator::new(
                    project_id.clone(),
                    dataset_id.clone(),
                    service_account_key.expose_secret().to_string(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Iceberg { config } => {
                let validator = IcebergValidator::new(config.clone());
                validator.validate(ctx).await
            }
        }
    }
}
