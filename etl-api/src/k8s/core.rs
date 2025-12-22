use etl_config::Environment;
use etl_config::shared::{ReplicatorConfigWithoutSecrets, SupabaseConfigWithoutSecrets, TlsConfig};
use secrecy::ExposeSecret;

use crate::configs::destination::{StoredDestinationConfig, StoredIcebergConfig};
use crate::configs::log::LogLevel;
use crate::configs::pipeline::StoredPipelineConfig;
use crate::configs::source::StoredSourceConfig;
use crate::db::destinations::Destination;
use crate::db::images::Image;
use crate::db::pipelines::Pipeline;
use crate::db::replicators::Replicator;
use crate::db::sources::Source;
use crate::k8s::{DestinationType, K8sClient, ReplicatorConfigMapFile};
use crate::routes::pipelines::PipelineError;

/// Secret types required by different destination configurations.
///
/// This enum encapsulates the various credential combinations needed for different
/// replicator destinations, ensuring type-safe secret management.
#[derive(Debug)]
pub enum Secrets {
    /// No secrets required for the destination.
    None,
    /// Credentials for BigQuery destinations.
    BigQuery {
        /// PostgreSQL source database password.
        postgres_password: String,
        /// Google Cloud service account key JSON for BigQuery authentication.
        big_query_service_account_key: String,
    },
    /// Credentials for Iceberg destinations.
    Iceberg {
        /// PostgreSQL source database password.
        postgres_password: String,
        /// Authentication token for the Iceberg catalog.
        catalog_token: String,
        /// AWS S3 access key ID for object storage.
        s3_access_key_id: String,
        /// AWS S3 secret access key for object storage.
        s3_secret_access_key: String,
    },
}

/// Creates or updates all Kubernetes resources required for a pipeline.
///
/// This function orchestrates the creation or update of secrets, config maps, and stateful
/// sets needed to run a pipeline in Kubernetes. It extracts credentials from the
/// source and destination configurations, builds the replicator configuration, and applies
/// all resources to the cluster.
#[allow(clippy::too_many_arguments)]
pub async fn create_or_update_pipeline_resources_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    pipeline: Pipeline,
    replicator: Replicator,
    image: Image,
    source: Source,
    destination: Destination,
    supabase_api_url: Option<&str>,
    trusted_root_certs: &str,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    let secrets = build_secrets_from_configs(&source.config, &destination.config);

    let environment = Environment::load().map_err(|_| PipelineError::MissingEnvironment)?;

    let destination_type = (&destination.config).into();

    let supabase_config = SupabaseConfigWithoutSecrets {
        project_ref: tenant_id.to_owned(),
        api_url: supabase_api_url.map(|url| url.to_owned()),
    };

    let log_level = pipeline.config.log_level.clone().unwrap_or_default();

    let tls_config = TlsConfig {
        enabled: true,
        trusted_root_certs: trusted_root_certs.to_owned(),
    };
    let replicator_config = build_replicator_config_without_secrets(
        // We are safe to perform this conversion, since the i64 -> u64 conversion performs wrap
        // around, and we won't have two different values map to the same u64, since the domain size
        // is the same.
        pipeline.id as u64,
        source.config,
        destination.config,
        pipeline.config,
        supabase_config,
        tls_config,
    );

    create_or_update_dynamic_replicator_secrets(k8s_client, &prefix, secrets).await?;
    create_or_update_replicator_config(k8s_client, &prefix, replicator_config, environment).await?;
    create_or_update_replicator_stateful_set(
        k8s_client,
        &prefix,
        image.name,
        environment,
        destination_type,
        log_level,
    )
    .await?;

    Ok(())
}

/// Deletes all Kubernetes resources associated with a pipeline.
///
/// This function removes all secrets, config maps, and stateful sets created for a
/// replicator. It uses the tenant ID and replicator ID to identify and delete
/// the correct resources.
pub async fn delete_pipeline_resources_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    replicator: Replicator,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    delete_dynamic_replicator_secrets(k8s_client, &prefix).await?;
    delete_replicator_config(k8s_client, &prefix).await?;
    delete_replicator_stateful_set(k8s_client, &prefix).await?;

    Ok(())
}

/// Extracts and combines credentials from source and destination configurations.
///
/// This function determines which credentials are needed based on the destination type
/// and extracts them from the provided configurations, exposing secrets as plain strings
/// for Kubernetes secret creation.
fn build_secrets_from_configs(
    source_config: &StoredSourceConfig,
    destination_config: &StoredDestinationConfig,
) -> Secrets {
    let postgres_password = source_config
        .password
        .as_ref()
        .map(|p| p.expose_secret().to_owned())
        .unwrap_or_default();

    match destination_config {
        StoredDestinationConfig::Memory => Secrets::None,
        StoredDestinationConfig::BigQuery {
            service_account_key,
            ..
        } => Secrets::BigQuery {
            postgres_password,
            big_query_service_account_key: service_account_key.expose_secret().to_string(),
        },
        StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Rest { .. },
        } => Secrets::None,
        StoredDestinationConfig::Iceberg {
            config:
                StoredIcebergConfig::Supabase {
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    ..
                },
        } => Secrets::Iceberg {
            postgres_password,
            catalog_token: catalog_token.expose_secret().to_string(),
            s3_access_key_id: s3_access_key_id.expose_secret().to_string(),
            s3_secret_access_key: s3_secret_access_key.expose_secret().to_string(),
        },
    }
}

/// Builds a replicator configuration with credentials omitted.
///
/// This function constructs a [`ReplicatorConfigWithoutSecrets`] by combining pipeline,
/// source, and destination configurations. It uses the provided trusted root certificates
/// for TLS configuration. Secrets are managed separately through Kubernetes secret resources.
fn build_replicator_config_without_secrets(
    pipeline_id: u64,
    source_config: StoredSourceConfig,
    destination_config: StoredDestinationConfig,
    pipeline_config: StoredPipelineConfig,
    supabase_config: SupabaseConfigWithoutSecrets,
    tls_config: TlsConfig,
) -> ReplicatorConfigWithoutSecrets {
    let pg_connection_config = source_config.into_connection_config(tls_config);

    ReplicatorConfigWithoutSecrets {
        destination: destination_config.into_etl_config().into(),
        pipeline: pipeline_config
            .into_etl_config(pipeline_id, pg_connection_config)
            .into(),
        supabase: Some(supabase_config),
    }
}

/// Creates a consistent naming prefix for Kubernetes resources.
///
/// This function generates a prefix string by combining the tenant ID and replicator ID,
/// which is used to name all Kubernetes resources (secrets, config maps, stateful sets)
/// associated with a specific pipeline instance.
pub fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

/// Creates or updates Kubernetes secrets based on the destination type.
///
/// This function creates different sets of secrets depending on the [`Secrets`] variant,
/// handling PostgreSQL credentials and destination-specific authentication credentials.
/// For [`Secrets::None`], no secrets are created.
async fn create_or_update_dynamic_replicator_secrets(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    secrets: Secrets,
) -> Result<(), PipelineError> {
    match secrets {
        Secrets::None => {}
        Secrets::BigQuery {
            postgres_password,
            big_query_service_account_key,
        } => {
            k8s_client
                .create_or_update_postgres_secret(prefix, &postgres_password)
                .await?;
            k8s_client
                .create_or_update_bigquery_secret(prefix, &big_query_service_account_key)
                .await?;
        }
        Secrets::Iceberg {
            postgres_password,
            catalog_token,
            s3_access_key_id,
            s3_secret_access_key,
        } => {
            k8s_client
                .create_or_update_postgres_secret(prefix, &postgres_password)
                .await?;
            k8s_client
                .create_or_update_iceberg_secret(
                    prefix,
                    &catalog_token,
                    &s3_access_key_id,
                    &s3_secret_access_key,
                )
                .await?;
        }
    }

    Ok(())
}

/// Creates or updates the replicator configuration in a Kubernetes config map.
///
/// This function serializes the [`ReplicatorConfigWithoutSecrets`] to JSON and stores it
/// in a Kubernetes config map. The replicator pods mount this config map to access their
/// runtime configuration.
async fn create_or_update_replicator_config(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    config: ReplicatorConfigWithoutSecrets,
    environment: Environment,
) -> Result<(), PipelineError> {
    let env_config = serde_json::to_string(&config)?;

    let files = vec![
        ReplicatorConfigMapFile {
            filename: "base.json".to_string(),
            // For our setup, we don't need to add config params to the base config file; everything
            // is added directly in the environment-specific config file.
            content: "{}".to_owned(),
        },
        ReplicatorConfigMapFile {
            filename: format!("{environment}.json"),
            content: env_config,
        },
    ];

    k8s_client
        .create_or_update_replicator_config_map(prefix, files)
        .await?;

    Ok(())
}

/// Creates or updates the Kubernetes stateful set for the replicator.
///
/// This function creates a stateful set that runs the replicator container with the
/// specified image. The stateful set is configured with environment-specific settings
/// and destination-type-specific resource requirements.
async fn create_or_update_replicator_stateful_set(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    replicator_image: String,
    environment: Environment,
    destination_type: DestinationType,
    log_level: LogLevel,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_replicator_stateful_set(
            prefix,
            &replicator_image,
            environment,
            destination_type,
            log_level,
        )
        .await?;

    Ok(())
}

/// Deletes all Kubernetes secrets associated with a replicator.
///
/// This function deletes PostgreSQL, BigQuery, and Iceberg secrets for the given prefix.
/// It attempts to delete all secret types regardless of which were actually created,
/// safely handling cases where secrets don't exist. This approach prevents orphaned
/// secrets when a pipeline's destination type is changed.
async fn delete_dynamic_replicator_secrets(
    k8s_client: &dyn K8sClient,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;

    // Although it won't happen that there are both bq and iceberg secrets at the same time
    // we delete them both here because the state in the db might not be the same as that
    // running in the k8s cluster. E.g. if a pipeline is updated from bq to iceberg or vice-versa
    // then there's a risk of wrong secret type being attempted for deletion which might leave
    // the actual secret behind. So for simplicty we just delete both kinds of secrets. The
    // one which doesn't exist will be safely ignored.
    k8s_client.delete_bigquery_secret(prefix).await?;
    k8s_client.delete_iceberg_secret(prefix).await?;

    Ok(())
}

/// Deletes the Kubernetes config map containing replicator configuration.
async fn delete_replicator_config(
    k8s_client: &dyn K8sClient,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_replicator_config_map(prefix).await?;

    Ok(())
}

/// Deletes the Kubernetes stateful set running the replicator.
async fn delete_replicator_stateful_set(
    k8s_client: &dyn K8sClient,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_replicator_stateful_set(prefix).await?;

    Ok(())
}
