//! Destination validation dispatch.

use async_trait::async_trait;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};
use crate::configs::destination::FullApiDestinationConfig;

/// Composite validator for destination prerequisites.
#[derive(Debug)]
pub(in crate::validation) struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    /// Creates a destination validator for the provided configuration.
    pub(in crate::validation) fn new(config: FullApiDestinationConfig) -> Self {
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
            FullApiDestinationConfig::BigQuery { .. } => {
                bigquery::validate(&self.config, ctx).await
            }
            FullApiDestinationConfig::ClickHouse { .. } => {
                clickhouse::validate(&self.config, ctx).await
            }
            FullApiDestinationConfig::Iceberg { .. } => iceberg::validate(&self.config, ctx).await,
            FullApiDestinationConfig::Ducklake { .. } => {
                ducklake::validate(&self.config, ctx).await
            }
            FullApiDestinationConfig::Snowflake { .. } => {
                snowflake::validate(&self.config, ctx).await
            }
        }
    }
}

/// Defines a disabled destination validation adapter module.
macro_rules! disabled_destination {
    ($module:ident, $feature:literal, $destination:literal) => {
        /// Disabled destination validation adapter.
        #[cfg(not(feature = $feature))]
        mod $module {
            use super::{
                FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure,
            };

            /// Returns a validation failure for disabled destination support.
            pub(super) fn validate(
                _config: &FullApiDestinationConfig,
                _ctx: &ValidationContext,
            ) -> std::future::Ready<Result<Vec<ValidationFailure>, ValidationError>> {
                std::future::ready(Ok(vec![ValidationFailure::critical(
                    format!("{} Backend Disabled", $destination),
                    format!("{} support is not compiled into this API binary.", $destination),
                )]))
            }
        }
    };
}

/// BigQuery validation adapter.
#[cfg(feature = "bigquery")]
mod bigquery {
    use secrecy::ExposeSecret;

    use super::{FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::bigquery::BigQueryValidator};

    /// Validates a BigQuery destination configuration.
    pub(super) async fn validate(
        config: &FullApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let FullApiDestinationConfig::BigQuery {
            project_id, dataset_id, service_account_key, ..
        } = config
        else {
            unreachable!("Destination config should match BigQuery.");
        };

        BigQueryValidator::new(
            project_id.clone(),
            dataset_id.clone(),
            service_account_key.expose_secret().to_owned(),
        )
        .validate(ctx)
        .await
    }
}

disabled_destination!(bigquery, "bigquery", "BigQuery");

/// ClickHouse validation adapter.
#[cfg(feature = "clickhouse")]
mod clickhouse {
    use super::{FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::clickhouse::ClickHouseValidator};

    /// Validates a ClickHouse destination configuration.
    pub(super) async fn validate(
        config: &FullApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let FullApiDestinationConfig::ClickHouse { url, user, password, database, .. } = config
        else {
            unreachable!("Destination config should match ClickHouse.");
        };

        ClickHouseValidator::new(url.clone(), user.clone(), password.clone(), database.clone())
            .validate(ctx)
            .await
    }
}

disabled_destination!(clickhouse, "clickhouse", "ClickHouse");

/// DuckLake validation adapter.
#[cfg(feature = "ducklake")]
mod ducklake {
    use secrecy::ExposeSecret;

    use super::{FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::ducklake::DucklakeValidator};

    /// Validates a DuckLake destination configuration.
    pub(super) async fn validate(
        config: &FullApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let FullApiDestinationConfig::Ducklake {
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
            duckdb_memory_cache_limit,
            maintenance_target_file_size,
            expire_snapshots_older_than,
            maintenance_mode: _,
        } = config
        else {
            unreachable!("Destination config should match DuckLake.");
        };

        DucklakeValidator::new(
            catalog_url.expose_secret().to_owned(),
            data_path.clone(),
            pool_size.unwrap_or(etl_config::shared::DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE),
            s3_access_key_id.as_ref().map(|value| value.expose_secret().to_owned()),
            s3_secret_access_key.as_ref().map(|value| value.expose_secret().to_owned()),
            s3_region.clone(),
            s3_endpoint.clone(),
            s3_url_style.clone(),
            *s3_use_ssl,
            metadata_schema.clone(),
            duckdb_memory_cache_limit.clone(),
            maintenance_target_file_size.clone(),
            expire_snapshots_older_than.clone(),
        )
        .validate(ctx)
        .await
    }
}

disabled_destination!(ducklake, "ducklake", "DuckLake");

/// Iceberg validation adapter.
#[cfg(feature = "iceberg")]
mod iceberg {
    use super::{FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::iceberg::IcebergValidator};

    /// Validates an Iceberg destination configuration.
    pub(super) async fn validate(
        config: &FullApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let FullApiDestinationConfig::Iceberg { config } = config else {
            unreachable!("Destination config should match Iceberg.");
        };

        IcebergValidator::new(config.clone()).validate(ctx).await
    }
}

disabled_destination!(iceberg, "iceberg", "Iceberg");

/// Snowflake validation adapter.
#[cfg(feature = "snowflake")]
mod snowflake {
    use super::{FullApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::snowflake::SnowflakeValidator};

    /// Validates a Snowflake destination configuration.
    pub(super) async fn validate(
        config: &FullApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let FullApiDestinationConfig::Snowflake {
            account_id,
            user,
            private_key,
            private_key_passphrase,
            database,
            schema,
            role,
        } = config
        else {
            unreachable!("Destination config should match Snowflake.");
        };

        SnowflakeValidator::new(
            account_id.clone(),
            user.clone(),
            private_key.clone(),
            private_key_passphrase.clone(),
            database.clone(),
            schema.clone(),
            role.clone(),
        )
        .validate(ctx)
        .await
    }
}

disabled_destination!(snowflake, "snowflake", "Snowflake");
