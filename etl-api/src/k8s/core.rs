use etl_config::Environment;
use etl_config::shared::{
    ReplicatorConfigWithoutSecrets, SupabaseConfigWithoutSecrets,
    TlsConfig,
};
use secrecy::ExposeSecret;

use crate::configs::destination::{StoredDestinationConfig, StoredIcebergConfig};
use crate::configs::pipeline::StoredPipelineConfig;
use crate::configs::source::StoredSourceConfig;
use crate::db::destinations::Destination;
use crate::db::images::Image;
use crate::db::pipelines::Pipeline;
use crate::db::replicators::Replicator;
use crate::db::sources::Source;
use crate::k8s::http::{TRUSTED_ROOT_CERT_CONFIG_MAP_NAME, TRUSTED_ROOT_CERT_KEY_NAME};
use crate::k8s::{DestinationType, K8sClient};
use crate::routes::pipelines::PipelineError;

#[derive(Debug)]
pub enum Secrets {
    None,
    BigQuery {
        postgres_password: String,
        big_query_service_account_key: String,
    },
    Iceberg {
        postgres_password: String,
        catalog_token: String,
        s3_access_key_id: String,
        s3_secret_access_key: String,
    },
}

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
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    let secrets = build_secrets_from_configs(&source.config, &destination.config);

    let environment = Environment::load().map_err(|_| PipelineError::MissingEnvironment)?;

    let destination_type = (&destination.config).into();

    let supabase_config = SupabaseConfigWithoutSecrets {
        project_ref: tenant_id.to_owned(),
        api_url: supabase_api_url.map(|url| url.to_owned()),
    };
    let replicator_config = build_replicator_config_without_secrets(
        k8s_client,
        // We are safe to perform this conversion, since the i64 -> u64 conversion performs wrap
        // around, and we won't have two different values map to the same u64, since the domain size
        // is the same.
        pipeline.id as u64,
        source.config,
        destination.config,
        pipeline.config,
        supabase_config,
    )
    .await?;

    create_or_update_dynamic_replicator_secrets(k8s_client, &prefix, secrets).await?;
    create_or_update_replicator_config(k8s_client, &prefix, replicator_config, environment)
        .await?;
    create_or_update_replicator_stateful_set(
        k8s_client,
        &prefix,
        image.name,
        environment,
        destination_type,
    )
    .await?;

    Ok(())
}

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

async fn build_replicator_config_without_secrets(
    k8s_client: &dyn K8sClient,
    pipeline_id: u64,
    source_config: StoredSourceConfig,
    destination_config: StoredDestinationConfig,
    pipeline_config: StoredPipelineConfig,
    supabase_config: SupabaseConfigWithoutSecrets,
) -> Result<ReplicatorConfigWithoutSecrets, PipelineError> {
    let trusted_root_certs = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .get(TRUSTED_ROOT_CERT_KEY_NAME)
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .clone();
    let pg_connection_config = source_config.into_connection_config_with_tls(TlsConfig {
        trusted_root_certs,
        enabled: true,
    });

    let config = ReplicatorConfigWithoutSecrets {
        destination: destination_config.into_etl_config().into(),
        pipeline: pipeline_config
            .into_etl_config(pipeline_id, pg_connection_config)
            .into(),
        supabase: Some(supabase_config),
    };

    Ok(config)
}

pub fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

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
                .create_or_update_bq_secret(prefix, &big_query_service_account_key)
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

async fn create_or_update_replicator_config(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    config: ReplicatorConfigWithoutSecrets,
    environment: Environment,
) -> Result<(), PipelineError> {
    // For now the base config is empty.
    let base_config = "";
    let env_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &env_config, environment)
        .await?;

    Ok(())
}

async fn create_or_update_replicator_stateful_set(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    replicator_image: String,
    environment: Environment,
    destination_type: DestinationType,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image, environment, destination_type)
        .await?;

    Ok(())
}

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
    k8s_client.delete_bq_secret(prefix).await?;
    k8s_client.delete_iceberg_secret(prefix).await?;

    Ok(())
}

async fn delete_replicator_config(
    k8s_client: &dyn K8sClient,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;

    Ok(())
}

async fn delete_replicator_stateful_set(
    k8s_client: &dyn K8sClient,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;

    Ok(())
}
