#[cfg(feature = "bigquery")]
mod bigquery;
#[cfg(feature = "clickhouse")]
mod clickhouse;
mod destination;
#[cfg(feature = "ducklake")]
mod ducklake;
#[cfg(feature = "iceberg")]
mod iceberg;
mod pipeline;
mod primary_key;
mod replica_identity;
#[cfg(feature = "snowflake")]
mod snowflake;
mod source;

use async_trait::async_trait;
pub(super) use destination::DestinationValidator;
use pipeline::{
    GeneratedColumnsValidator, LogicalReplicationSettingsValidator,
    PublicationExcludesEtlTablesValidator, PublicationExistsValidator,
    PublicationHasTablesValidator,
};
pub(super) use source::SourceValidator;

use super::{ValidationContext, ValidationError, ValidationFailure, Validator};
use crate::configs::pipeline::FullApiPipelineConfig;

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
            Box::new(LogicalReplicationSettingsValidator::new(max_table_sync_workers)),
            Box::new(PublicationExistsValidator::new(publication_name.clone())),
            Box::new(PublicationHasTablesValidator::new(publication_name.clone())),
            Box::new(PublicationExcludesEtlTablesValidator::new(publication_name.clone())),
            Box::new(GeneratedColumnsValidator::new(publication_name)),
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
