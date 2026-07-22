//! Destination validation dispatch.

use async_trait::async_trait;
use etl::schema::IdentityType;
use etl_config::shared::ClickHouseEngine;

use super::{
    super::{ValidationContext, ValidationError, ValidationFailure, Validator},
    primary_key::PrimaryKeyValidator,
    replica_identity::ReplicaIdentityValidator,
};
use crate::configs::destination::ApiDestinationConfig;

/// Composite validator for destination prerequisites.
#[derive(Debug)]
pub(crate) struct DestinationValidator {
    /// Destination configuration to validate.
    config: ApiDestinationConfig,
    /// Publication name of the pipeline that will be used for table checks.
    publication_name: Option<String>,
}

impl DestinationValidator {
    /// Creates a destination validator for the provided configuration.
    pub(crate) fn new(config: ApiDestinationConfig, publication_name: Option<String>) -> Self {
        Self { config, publication_name }
    }

    /// Builds the replica identity validator for the configured destination.
    fn replica_identity_validator(&self) -> Option<ReplicaIdentityValidator> {
        let publication_name = self.publication_name.clone()?;

        Some(match &self.config {
            ApiDestinationConfig::BigQuery { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "BigQuery",
                &[IdentityType::PrimaryKey, IdentityType::Full],
                &[IdentityType::PrimaryKey, IdentityType::Full],
            ),
            ApiDestinationConfig::ClickHouse { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "ClickHouse",
                &[IdentityType::PrimaryKey, IdentityType::Full],
                &[IdentityType::PrimaryKey, IdentityType::Full],
            ),
            ApiDestinationConfig::Iceberg { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "Iceberg",
                &[IdentityType::Full],
                &[IdentityType::Full],
            ),
            ApiDestinationConfig::Ducklake { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "DuckLake",
                &[IdentityType::PrimaryKey, IdentityType::AlternativeKey, IdentityType::Full],
                &[IdentityType::PrimaryKey, IdentityType::AlternativeKey, IdentityType::Full],
            ),
            ApiDestinationConfig::Snowflake { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "Snowflake",
                &[IdentityType::Full],
                &[IdentityType::PrimaryKey, IdentityType::AlternativeKey, IdentityType::Full],
            ),
            ApiDestinationConfig::Postgres { .. } => ReplicaIdentityValidator::new(
                publication_name,
                "Postgres",
                &[IdentityType::PrimaryKey, IdentityType::Full],
                &[IdentityType::PrimaryKey, IdentityType::Full],
            ),
        })
    }

    /// Builds the primary-key validator for destinations that require source
    /// primary keys.
    fn primary_key_validator(&self) -> Option<PrimaryKeyValidator> {
        let publication_name = self.publication_name.clone()?;

        match &self.config {
            ApiDestinationConfig::BigQuery { .. } => Some(PrimaryKeyValidator::new(
                publication_name,
                "BigQuery",
                "BigQuery uses the source primary key to match rows during initial loads, \
                 upserts, deletes, and updates that change primary-key values.",
                true,
            )),
            ApiDestinationConfig::ClickHouse {
                engine: ClickHouseEngine::ReplacingMergeTree,
                ..
            } => Some(PrimaryKeyValidator::new(
                publication_name,
                "ClickHouse ReplacingMergeTree",
                "ClickHouse ReplacingMergeTree uses the source primary key as the `ORDER BY` and \
                 deduplication key.",
                true,
            )),
            ApiDestinationConfig::ClickHouse { engine: ClickHouseEngine::MergeTree, .. } => {
                Some(PrimaryKeyValidator::new(
                    publication_name,
                    "ClickHouse MergeTree",
                    "ClickHouse uses replicated source primary-key columns to apply row-level \
                     updates and deletes when a source primary key exists.",
                    false,
                ))
            }
            _ => None,
        }
    }
}

#[async_trait]
impl Validator for DestinationValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let mut failures = match &self.config {
            ApiDestinationConfig::BigQuery { .. } => bigquery::validate(&self.config, ctx).await,
            ApiDestinationConfig::ClickHouse { .. } => {
                clickhouse::validate(&self.config, ctx).await
            }
            ApiDestinationConfig::Iceberg { .. } => iceberg::validate(&self.config, ctx).await,
            ApiDestinationConfig::Ducklake { .. } => ducklake::validate(&self.config, ctx).await,
            ApiDestinationConfig::Snowflake { .. } => snowflake::validate(&self.config, ctx).await,
            ApiDestinationConfig::Postgres { .. } => postgres::validate(&self.config, ctx).await,
        }?;

        if let Some(validator) = self.replica_identity_validator() {
            failures.extend(validator.validate(ctx).await?);
        }

        if let Some(validator) = self.primary_key_validator() {
            failures.extend(validator.validate(ctx).await?);
        }

        Ok(failures)
    }
}

/// Defines a disabled destination validation adapter module.
macro_rules! disabled_destination {
    ($module:ident, $feature:literal, $destination:literal) => {
        /// Disabled destination validation adapter.
        #[cfg(not(feature = $feature))]
        mod $module {
            use super::{
                ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure,
            };

            /// Returns a validation failure for disabled destination support.
            pub(super) fn validate(
                _config: &ApiDestinationConfig,
                _ctx: &ValidationContext,
            ) -> std::future::Ready<Result<Vec<ValidationFailure>, ValidationError>> {
                std::future::ready(Ok(vec![ValidationFailure::critical(
                    format!("{} Backend Disabled", $destination),
                    format!(
                        "This API server was built without {} destination support.\n\nChoose a \
                         destination enabled on this deployment, or ask the ETL API operator to \
                         deploy a build with {} support enabled.",
                        $destination, $destination
                    ),
                )]))
            }
        }
    };
}

/// BigQuery validation adapter.
#[cfg(feature = "bigquery")]
mod bigquery {
    use secrecy::ExposeSecret;

    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::bigquery::BigQueryValidator};

    /// Validates a BigQuery destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::BigQuery { project_id, dataset_id, service_account_key, .. } =
            config
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
    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::clickhouse::ClickHouseValidator};

    /// Validates a ClickHouse destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::ClickHouse { url, user, password, database, .. } = config else {
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

    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::ducklake::DucklakeValidator};

    /// Validates a DuckLake destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::Ducklake {
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
    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::iceberg::IcebergValidator};

    /// Validates an Iceberg destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::Iceberg { config } = config else {
            unreachable!("Destination config should match Iceberg.");
        };

        IcebergValidator::new(config.clone()).validate(ctx).await
    }
}

disabled_destination!(iceberg, "iceberg", "Iceberg");

/// Snowflake validation adapter.
#[cfg(feature = "snowflake")]
mod snowflake {
    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};
    use crate::validation::{Validator, validators::snowflake::SnowflakeValidator};

    /// Validates a Snowflake destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::Snowflake {
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

/// Postgres validation adapter.
#[cfg(feature = "postgres")]
mod postgres {
    use super::{ApiDestinationConfig, ValidationContext, ValidationError, ValidationFailure};

    /// Validates a Postgres destination configuration.
    pub(super) async fn validate(
        config: &ApiDestinationConfig,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let ApiDestinationConfig::Postgres { host, name, username, .. } = config else {
            unreachable!("Destination config should match Postgres.");
        };

        let mut failures = Vec::new();
        if host.trim().is_empty() {
            failures.push(ValidationFailure::critical(
                "Postgres Host Required",
                "Enter the Postgres host that ETL should connect to.",
            ));
        }
        if name.trim().is_empty() {
            failures.push(ValidationFailure::critical(
                "Postgres Database Required",
                "Choose the Postgres database where replicated tables should be written.",
            ));
        }
        if username.trim().is_empty() {
            failures.push(ValidationFailure::critical(
                "Postgres Username Required",
                "Enter the Postgres user that ETL should connect with.",
            ));
        }

        Ok(failures)
    }
}

disabled_destination!(postgres, "postgres", "Postgres");
