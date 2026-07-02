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
            gcs_staging_bucket,
            service_account_key,
            max_staleness_mins,
            connection_pool_size,
        } = &replicator_config.destination
        else {
            unreachable!("Destination kind should match BigQuery config");
        };

        let mut destination = BigQueryDestination::new_with_key(
            project_id.clone(),
            dataset_id.clone(),
            service_account_key.expose_secret(),
            *max_staleness_mins,
            *connection_pool_size,
            pipeline_id,
            store.clone(),
        )
        .await?
        .with_initial_copy_parallelism(replicator_config.pipeline.max_copy_connections_per_table);

        if let Some(gcs_staging_bucket) = gcs_staging_bucket {
            destination =
                destination.with_gcs_initial_copy_staging_bucket(gcs_staging_bucket.clone());
        }

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

        let destination = ClickHouseDestination::new(
            url.clone(),
            user,
            password.as_ref().map(|p| p.expose_secret().to_owned()),
            database,
            ClickHouseInserterConfig { engine: *engine, ..Default::default() },
            ClickHouseClientConfig::default(),
            store.clone(),
        )?;
        destination.validate_engine_support().await?;

        let pipeline = Pipeline::new(replicator_config.pipeline, store, destination);
        pipeline::start(pipeline).await
    }
}

/// DuckLake destination startup.
#[cfg(feature = "ducklake")]
mod ducklake {
    use etl::pipeline::Pipeline;
    use etl_config::{
        parse_ducklake_s3_data_path, parse_ducklake_url,
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
            expire_snapshots_older_than,
            maintenance_mode,
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
                url_style: s3_url_style.clone().unwrap_or_else(|| "path".to_owned()),
                use_ssl: s3_use_ssl.unwrap_or(false),
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

        let destination = DuckLakeDestination::new_with_external_maintenance(
            parse_ducklake_url(catalog_url.expose_secret()).map_err(ReplicatorError::config)?,
            parse_ducklake_s3_data_path(data_path).map_err(ReplicatorError::config)?,
            *pool_size,
            s3_config,
            metadata_schema.clone(),
            maintenance_target_file_size.clone(),
            expire_snapshots_older_than.clone(),
            external_maintenance,
            store.clone(),
        )
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
