//! Destination startup dispatch.

use etl_config::shared::{DestinationKind, ReplicatorConfig};

use super::ReplicatorStore;
use crate::error::ReplicatorResult;

/// Starts the configured destination pipeline.
pub(super) async fn start(
    replicator_config: ReplicatorConfig,
    store: ReplicatorStore,
) -> ReplicatorResult<()> {
    #[cfg(not(feature = "any-destination"))]
    let _ = &store;

    match replicator_config.destination.kind() {
        DestinationKind::BigQuery => {
            #[cfg(feature = "bigquery")]
            {
                bigquery::start(replicator_config, store).await
            }

            #[cfg(not(feature = "bigquery"))]
            {
                Err(disabled_destination_error(DestinationKind::BigQuery))
            }
        }
        DestinationKind::ClickHouse => {
            #[cfg(feature = "clickhouse")]
            {
                clickhouse::start(replicator_config, store).await
            }

            #[cfg(not(feature = "clickhouse"))]
            {
                Err(disabled_destination_error(DestinationKind::ClickHouse))
            }
        }
        DestinationKind::Ducklake => {
            #[cfg(feature = "ducklake")]
            {
                ducklake::start(replicator_config, store).await
            }

            #[cfg(not(feature = "ducklake"))]
            {
                Err(disabled_destination_error(DestinationKind::Ducklake))
            }
        }
        DestinationKind::Iceberg => {
            #[cfg(feature = "iceberg")]
            {
                iceberg::start(replicator_config, store).await
            }

            #[cfg(not(feature = "iceberg"))]
            {
                Err(disabled_destination_error(DestinationKind::Iceberg))
            }
        }
        DestinationKind::Snowflake => {
            #[cfg(feature = "snowflake")]
            {
                snowflake::start(replicator_config, store).await
            }

            #[cfg(not(feature = "snowflake"))]
            {
                Err(disabled_destination_error(DestinationKind::Snowflake))
            }
        }
    }
}

#[cfg(any(
    not(feature = "bigquery"),
    not(feature = "clickhouse"),
    not(feature = "ducklake"),
    not(feature = "iceberg"),
    not(feature = "snowflake")
))]
fn disabled_destination_error(kind: DestinationKind) -> crate::error::ReplicatorError {
    crate::error::ReplicatorError::config(std::io::Error::other(format!(
        "Destination `{}` support is not compiled into this binary.",
        kind.as_str()
    )))
}

/// BigQuery destination startup.
#[cfg(feature = "bigquery")]
mod bigquery {
    use etl::pipeline::Pipeline;
    use etl_config::shared::{DestinationConfig, ReplicatorConfig};
    use etl_destinations::bigquery::BigQueryDestination;
    use secrecy::ExposeSecret;

    use super::super::{ReplicatorStore, pipeline};
    use crate::error::ReplicatorResult;

    /// Starts the BigQuery destination pipeline.
    pub(super) async fn start(
        replicator_config: ReplicatorConfig,
        store: ReplicatorStore,
    ) -> ReplicatorResult<()> {
        let pipeline_id = replicator_config.pipeline.id;

        let DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
            connection_pool_size,
            table_options,
        } = &replicator_config.destination
        else {
            unreachable!("Destination kind should match BigQuery config");
        };

        let destination = BigQueryDestination::new_with_key(
            project_id.clone(),
            dataset_id.clone(),
            service_account_key.expose_secret(),
            *max_staleness_mins,
            *connection_pool_size,
            pipeline_id,
            store.clone(),
        )
        .await?
        .with_table_options(table_options.clone());

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }
}

/// ClickHouse destination startup.
#[cfg(feature = "clickhouse")]
mod clickhouse {
    use etl::pipeline::Pipeline;
    use etl_config::shared::{DestinationConfig, ReplicatorConfig};
    use etl_destinations::clickhouse::{
        ClickHouseClientConfig, ClickHouseDestination, ClickHouseInserterConfig,
    };
    use secrecy::ExposeSecret;

    use super::super::{ReplicatorStore, pipeline};
    use crate::error::ReplicatorResult;

    /// Returns whether a ClickHouse configuration requires public HTTPS
    /// enforcement.
    fn requires_public_network_policy(is_managed: bool, scheme: &str) -> bool {
        is_managed || scheme == "https"
    }

    /// Starts the ClickHouse destination pipeline.
    pub(super) async fn start(
        replicator_config: ReplicatorConfig,
        store: ReplicatorStore,
    ) -> ReplicatorResult<()> {
        let DestinationConfig::ClickHouse { url, user, password, database, engine } =
            &replicator_config.destination
        else {
            unreachable!("Destination kind should match ClickHouse config");
        };

        let password = password.as_ref().map(|password| password.expose_secret().to_owned());
        let inserter_config = ClickHouseInserterConfig { engine: *engine, ..Default::default() };
        let client_config = ClickHouseClientConfig::default();

        // Managed configurations must use public HTTPS. Standalone HTTPS also
        // uses the guard, while trusted standalone HTTP remains available for
        // local development.
        let enforce_public_network_policy =
            requires_public_network_policy(replicator_config.supabase.is_some(), url.scheme());
        let destination = if enforce_public_network_policy {
            ClickHouseDestination::new_public(
                url.clone(),
                user,
                password,
                database,
                inserter_config,
                client_config,
                store.clone(),
            )
            .await?
        } else {
            ClickHouseDestination::new(
                url.clone(),
                user,
                password,
                database,
                inserter_config,
                client_config,
                store.clone(),
            )?
        };
        destination.validate_engine_support().await?;

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn selects_public_network_policy_for_managed_and_https_configs() {
            assert!(requires_public_network_policy(true, "http"));
            assert!(requires_public_network_policy(true, "https"));
            assert!(requires_public_network_policy(false, "https"));
            assert!(!requires_public_network_policy(false, "http"));
        }
    }
}

/// DuckLake destination startup.
#[cfg(feature = "ducklake")]
mod ducklake {
    use etl::pipeline::Pipeline;
    use etl_config::{
        default_ducklake_s3_url_style, default_ducklake_s3_use_ssl, parse_ducklake_s3_data_path,
        parse_ducklake_url,
        shared::{
            DestinationConfig, DuckLakeMaintenanceMode as ConfigDuckLakeMaintenanceMode,
            ReplicatorConfig,
        },
    };
    use etl_destinations::ducklake::{
        DuckLakeDestination, DuckLakeExternalMaintenanceConfig, DuckLakeMaintenanceMode,
        S3Config as DucklakeS3Config,
    };
    use secrecy::ExposeSecret;

    use super::super::{ReplicatorStore, pipeline};
    use crate::error::{ReplicatorError, ReplicatorResult};

    /// Starts the DuckLake destination pipeline.
    pub(super) async fn start(
        replicator_config: ReplicatorConfig,
        store: ReplicatorStore,
    ) -> ReplicatorResult<()> {
        let pipeline_id = replicator_config.pipeline.id;

        let DestinationConfig::Ducklake {
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
            parquet_row_group_size_bytes,
            parquet_row_group_size,
            expire_snapshots_older_than,
            maintenance_mode,
            copy_buffer,
            table_sorting,
        } = &replicator_config.destination
        else {
            unreachable!("Destination kind should match DuckLake config");
        };

        let s3_config = match (s3_access_key_id, s3_secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(DucklakeS3Config {
                access_key_id: access_key_id.expose_secret().to_owned(),
                secret_access_key: secret_access_key.expose_secret().to_owned(),
                region: s3_region.clone().unwrap_or_else(|| "us-east-1".to_owned()),
                endpoint: s3_endpoint.clone(),
                url_style: s3_url_style.clone().unwrap_or_else(|| {
                    default_ducklake_s3_url_style(s3_endpoint.as_deref()).to_owned()
                }),
                use_ssl: s3_use_ssl.unwrap_or_else(default_ducklake_s3_use_ssl),
            }),
            (None, None) => None,
            _ => {
                return Err(ReplicatorError::config(std::io::Error::other(
                    "DuckLake S3 credentials must include both access key id and secret access key",
                )));
            }
        };

        let maintenance_mode = match maintenance_mode {
            ConfigDuckLakeMaintenanceMode::Disabled => DuckLakeMaintenanceMode::Disabled,
            ConfigDuckLakeMaintenanceMode::Kubernetes => DuckLakeMaintenanceMode::Kubernetes,
            ConfigDuckLakeMaintenanceMode::Postgres => DuckLakeMaintenanceMode::Postgres,
        };
        let external_maintenance =
            DuckLakeExternalMaintenanceConfig { mode: maintenance_mode, pipeline_id };

        let destination = DuckLakeDestination::builder(
            parse_ducklake_url(catalog_url.expose_secret()).map_err(ReplicatorError::config)?,
            parse_ducklake_s3_data_path(data_path).map_err(ReplicatorError::config)?,
            *pool_size,
            store.clone(),
        )
        .s3(s3_config)
        .metadata_schema(metadata_schema.clone())
        .maintenance_target_file_size(maintenance_target_file_size.clone())
        .parquet_row_group_size_bytes(parquet_row_group_size_bytes.clone())
        .parquet_row_group_size(parquet_row_group_size.clone())
        .expire_snapshots_older_than(expire_snapshots_older_than.clone())
        .copy_buffer(*copy_buffer)
        .table_sorting(table_sorting.clone())
        .external_maintenance(external_maintenance)
        .build()
        .await?;

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }
}

/// Iceberg destination startup.
#[cfg(feature = "iceberg")]
mod iceberg {
    use std::collections::HashMap;

    use etl::{config::IcebergConfig, pipeline::Pipeline};
    use etl_config::{Environment, shared::ReplicatorConfig};
    use etl_destinations::iceberg::{
        DestinationNamespace, IcebergClient, IcebergDestination, S3_ACCESS_KEY_ID, S3_ENDPOINT,
        S3_SECRET_ACCESS_KEY,
    };
    use secrecy::ExposeSecret;

    use super::super::{ReplicatorStore, pipeline};
    use crate::error::{ReplicatorError, ReplicatorResult};

    /// Starts the Iceberg destination pipeline.
    pub(super) async fn start(
        replicator_config: ReplicatorConfig,
        store: ReplicatorStore,
    ) -> ReplicatorResult<()> {
        let client = match &replicator_config.destination {
            etl_config::shared::DestinationConfig::Iceberg {
                config:
                    IcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        catalog_token,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_region,
                        ..
                    },
            } => {
                let env = Environment::load().map_err(ReplicatorError::config)?;
                IcebergClient::new_with_supabase_catalog(
                    project_ref,
                    env.get_supabase_domain(),
                    catalog_token.expose_secret().to_owned(),
                    warehouse_name.clone(),
                    s3_access_key_id.expose_secret().to_owned(),
                    s3_secret_access_key.expose_secret().to_owned(),
                    s3_region.clone(),
                )
                .await
                .map_err(ReplicatorError::config)?
            }
            etl_config::shared::DestinationConfig::Iceberg {
                config:
                    IcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_endpoint,
                        ..
                    },
            } => IcebergClient::new_with_rest_catalog(
                catalog_uri.clone(),
                warehouse_name.clone(),
                create_props(
                    s3_access_key_id.expose_secret().to_owned(),
                    s3_secret_access_key.expose_secret().to_owned(),
                    s3_endpoint.clone(),
                ),
            )
            .await
            .map_err(ReplicatorError::config)?,
            _ => unreachable!("Destination kind should match Iceberg config"),
        };

        let etl_config::shared::DestinationConfig::Iceberg { config } =
            &replicator_config.destination
        else {
            unreachable!("Destination kind should match Iceberg config");
        };
        let namespace = match config {
            IcebergConfig::Supabase { namespace, .. } | IcebergConfig::Rest { namespace, .. } => {
                match namespace {
                    Some(ns) => DestinationNamespace::Single(ns.clone()),
                    None => DestinationNamespace::OnePerSchema,
                }
            }
        };
        let destination = IcebergDestination::new(client, namespace, store.clone());

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }

    /// Creates Iceberg REST catalog S3 properties.
    fn create_props(
        s3_access_key_id: String,
        s3_secret_access_key: String,
        s3_endpoint: String,
    ) -> HashMap<String, String> {
        let mut props: HashMap<String, String> = HashMap::new();

        props.insert(S3_ACCESS_KEY_ID.to_owned(), s3_access_key_id);
        props.insert(S3_SECRET_ACCESS_KEY.to_owned(), s3_secret_access_key);
        props.insert(S3_ENDPOINT.to_owned(), s3_endpoint);

        props
    }
}

/// Snowflake destination startup.
#[cfg(feature = "snowflake")]
mod snowflake {
    use etl::pipeline::Pipeline;
    use etl_config::shared::{DestinationConfig, ReplicatorConfig};
    use etl_destinations::snowflake as snowflake_destination;

    use super::super::{ReplicatorStore, pipeline};
    use crate::error::{ReplicatorError, ReplicatorResult};

    /// Starts the Snowflake destination pipeline.
    pub(super) async fn start(
        replicator_config: ReplicatorConfig,
        store: ReplicatorStore,
    ) -> ReplicatorResult<()> {
        let pipeline_id = replicator_config.pipeline.id;

        let DestinationConfig::Snowflake {
            account_id,
            user,
            private_key,
            private_key_passphrase,
            database,
            schema,
            role,
        } = &replicator_config.destination
        else {
            unreachable!("Destination kind should match Snowflake config");
        };

        let mut config = snowflake_destination::Config::new(account_id, user, database, schema)
            .map_err(ReplicatorError::config)?;
        if let Some(r) = role {
            config = config.with_role(r);
        }
        config = config.with_private_key(private_key.clone(), private_key_passphrase.clone());
        let auth = std::sync::Arc::new(
            snowflake_destination::AuthManager::new(config).map_err(ReplicatorError::config)?,
        );
        let client = snowflake_destination::Client::new(auth, pipeline_id);
        let destination = snowflake_destination::Destination::new(client, store.clone());

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }
}
