//! Built-in validators for ETL pipeline and destination prerequisites.

use std::collections::HashMap;

use async_trait::async_trait;
use etl_destinations::bigquery::BigQueryClient;
use etl_destinations::iceberg::{
    IcebergClient, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY,
};
use secrecy::ExposeSecret;

use crate::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};
use crate::configs::pipeline::FullApiPipelineConfig;

use super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates that the required publication exists in the source database.
pub struct PublicationExistsValidator {
    publication_name: String,
}

impl PublicationExistsValidator {
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExistsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for publication validation");

        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
                .bind(&self.publication_name)
                .fetch_one(source_pool)
                .await?;

        if exists {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::new(
                "Publication Not Found",
                format!(
                    "'{}' does not exist. Create with: CREATE PUBLICATION {} FOR TABLE ...",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that there are enough free replication slots for the pipeline.
pub struct ReplicationSlotsValidator {
    max_table_sync_workers: u16,
}

impl ReplicationSlotsValidator {
    pub fn new(max_table_sync_workers: u16) -> Self {
        Self {
            max_table_sync_workers,
        }
    }
}

#[async_trait]
impl Validator for ReplicationSlotsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for replication slots validation");

        let max_slots: i32 = sqlx::query_scalar(
            "select setting::int from pg_settings where name = 'max_replication_slots'",
        )
        .fetch_one(source_pool)
        .await?;

        let used_slots: i64 = sqlx::query_scalar("select count(*) from pg_replication_slots")
            .fetch_one(source_pool)
            .await?;

        let free_slots = max_slots as i64 - used_slots;
        // We need 1 slot for the apply worker plus at most `max_table_sync_workers` other slots
        // for table sync workers.
        let required_slots = self.max_table_sync_workers as i64 + 1;

        if required_slots <= free_slots {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::new(
                "Insufficient Replication Slots",
                format!(
                    "{free_slots} free, {required_slots} required ({used_slots}/{max_slots} in use). \
                    Try to increase max_replication_slots in the source database or delete unused slots",
                ),
            )])
        }
    }
}

/// Composite validator for pipeline prerequisites.
pub struct PipelineValidator {
    config: FullApiPipelineConfig,
}

impl PipelineValidator {
    pub fn new(config: FullApiPipelineConfig) -> Self {
        Self { config }
    }

    fn sub_validators(&self) -> Vec<Box<dyn Validator>> {
        let max_table_sync_workers = self.config.max_table_sync_workers.unwrap_or(4);

        vec![
            Box::new(PublicationExistsValidator::new(
                self.config.publication_name.clone(),
            )),
            Box::new(ReplicationSlotsValidator::new(max_table_sync_workers)),
        ]
    }
}

#[async_trait]
impl Validator for PipelineValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let mut failures = Vec::new();

        for validator in self.sub_validators() {
            failures.extend(validator.validate(ctx).await?);
        }

        Ok(failures)
    }
}

/// Validates BigQuery destination connectivity and dataset accessibility.
struct BigQueryValidator {
    project_id: String,
    dataset_id: String,
    service_account_key: String,
}

impl BigQueryValidator {
    fn new(project_id: String, dataset_id: String, service_account_key: String) -> Self {
        Self {
            project_id,
            dataset_id,
            service_account_key,
        }
    }
}

#[async_trait]
impl Validator for BigQueryValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let client =
            BigQueryClient::new_with_key(self.project_id.clone(), &self.service_account_key)
                .await
                .map_err(|err| ValidationError::BigQuery(err.to_string()))?;

        match client.dataset_exists(&self.dataset_id).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::new(
                "BigQuery Dataset Not Found",
                format!("'{}' in project '{}'", self.dataset_id, self.project_id),
            )]),
            Err(err) => Err(ValidationError::BigQuery(err.to_string())),
        }
    }
}

/// Validates Iceberg destination connectivity.
struct IcebergValidator {
    config: FullApiIcebergConfig,
}

impl IcebergValidator {
    fn new(config: FullApiIcebergConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for IcebergValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let client = match &self.config {
            FullApiIcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                ..
            } => {
                IcebergClient::new_with_supabase_catalog(
                    project_ref,
                    ctx.environment.get_supabase_domain(),
                    catalog_token.expose_secret().to_string(),
                    warehouse_name.clone(),
                    s3_access_key_id.expose_secret().to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                    s3_region.clone(),
                )
                .await
            }
            FullApiIcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
                ..
            } => {
                let mut props = HashMap::new();
                props.insert(
                    S3_ACCESS_KEY_ID.to_string(),
                    s3_access_key_id.expose_secret().to_string(),
                );
                props.insert(
                    S3_SECRET_ACCESS_KEY.to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                );
                props.insert(S3_ENDPOINT.to_string(), s3_endpoint.clone());

                IcebergClient::new_with_rest_catalog(
                    catalog_uri.clone(),
                    warehouse_name.clone(),
                    props,
                )
                .await
            }
        };

        let client = client.map_err(|err| ValidationError::Iceberg(err.to_string()))?;

        match client.validate_connectivity().await {
            Ok(()) => Ok(vec![]),
            Err(err) => Ok(vec![ValidationFailure::new(
                "Iceberg Catalog",
                err.to_string(),
            )]),
        }
    }
}

/// Composite validator for destination prerequisites.
pub struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    pub fn new(config: FullApiDestinationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for DestinationValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        match &self.config {
            FullApiDestinationConfig::Memory => Ok(vec![]),
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
