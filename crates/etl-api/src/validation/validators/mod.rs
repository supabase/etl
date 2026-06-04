mod bigquery;
mod clickhouse;
mod ducklake;
mod iceberg;
mod pipeline;
mod snowflake;
mod source;

use async_trait::async_trait;
use bigquery::BigQueryValidator;
use clickhouse::ClickHouseValidator;
use ducklake::DucklakeValidator;
use iceberg::IcebergValidator;
use pipeline::{
    GeneratedColumnsValidator, PrimaryKeysValidator, PublicationExcludesEtlTablesValidator,
    PublicationExistsValidator, PublicationHasTablesValidator, ReplicationPermissionsValidator,
    ReplicationSlotsValidator, WalLevelValidator,
};
use secrecy::ExposeSecret;
use snowflake::SnowflakeValidator;
pub(super) use source::SourceValidator;

use super::{ValidationContext, ValidationError, ValidationFailure, Validator};
use crate::configs::{destination::FullApiDestinationConfig, pipeline::FullApiPipelineConfig};

/// Composite validator for pipeline prerequisites.
#[derive(Debug)]
pub(super) struct PipelineValidator {
    config: FullApiPipelineConfig,
}

impl PipelineValidator {
    pub(super) fn new(config: FullApiPipelineConfig) -> Self {
        Self { config }
    }

    fn sub_validators(&self) -> Vec<Box<dyn Validator>> {
        let max_table_sync_workers = self.config.max_table_sync_workers.unwrap_or(4);
        let publication_name = self.config.publication_name.clone();

        vec![
            Box::new(WalLevelValidator),
            Box::new(ReplicationPermissionsValidator),
            Box::new(PublicationExistsValidator::new(publication_name.clone())),
            Box::new(PublicationHasTablesValidator::new(publication_name.clone())),
            Box::new(PublicationExcludesEtlTablesValidator::new(publication_name.clone())),
            Box::new(PrimaryKeysValidator::new(publication_name.clone())),
            Box::new(GeneratedColumnsValidator::new(publication_name)),
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

/// Composite validator for destination prerequisites.
#[derive(Debug)]
pub(super) struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    pub(super) fn new(config: FullApiDestinationConfig) -> Self {
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
            FullApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                ..
            } => {
                let validator = BigQueryValidator::new(
                    project_id.clone(),
                    dataset_id.clone(),
                    service_account_key.expose_secret().to_owned(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::ClickHouse { url, user, password, database, .. } => {
                let validator = ClickHouseValidator::new(
                    url.clone(),
                    user.clone(),
                    password.clone(),
                    database.clone(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Iceberg { config } => {
                let validator = IcebergValidator::new(config.clone());
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode: _,
            } => {
                let validator = DucklakeValidator::new(
                    catalog_url.expose_secret().to_owned(),
                    data_path.clone(),
                    pool_size.unwrap_or(
                        etl_config::shared::DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE,
                    ),
                    s3_access_key_id.as_ref().map(|value| value.expose_secret().to_owned()),
                    s3_secret_access_key.as_ref().map(|value| value.expose_secret().to_owned()),
                    s3_region.clone(),
                    s3_endpoint.clone(),
                    s3_url_style.clone(),
                    *s3_use_ssl,
                    metadata_schema.clone(),
                    maintenance_target_file_size.clone(),
                    expire_snapshots_older_than.clone(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => {
                let validator = SnowflakeValidator::new(
                    account_id.clone(),
                    user.clone(),
                    private_key.clone(),
                    private_key_passphrase.clone(),
                    database.clone(),
                    schema.clone(),
                    role.clone(),
                );
                validator.validate(ctx).await
            }
        }
    }
}
