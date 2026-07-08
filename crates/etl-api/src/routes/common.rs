use crate::{
    config::ApiConfig,
    configs::{encryption::EncryptionKeyring, source::StoredSourceConfig},
    data::pipelines::read_pipeline_components,
    k8s::{
        K8sClient, SourceTlsConfig,
        core::{create_or_update_pipeline_resources_in_k8s, should_reconcile_replicator_resources},
    },
    routes::pipelines::PipelineError,
    validation::{self, ValidationContext, ValidationError, ValidationFailure},
};

/// Reconciles and restarts the running replicator for a pipeline.
///
/// Update endpoints that can change source, destination, pipeline, image, or
/// runtime resource configuration should call this after persisting the new API
/// state. The helper first materializes the latest Kubernetes Secrets,
/// ConfigMap, maintenance resources, and StatefulSet from that state. It then
/// relies on the StatefulSet materialization to change the pod template restart
/// annotation, which intentionally kicks off pod recreation.
///
/// This forced recreation is part of the contract. The replicator loads its
/// mounted config and secret-backed environment when the process starts, so a
/// running pod must be restarted after config materialization in order to pick
/// up those changes.
///
/// If the pipeline has no active Kubernetes resources, the call is a no-op.
pub(crate) async fn restart_pipeline_replicator_if_running(
    connection: &mut sqlx::PgConnection,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKeyring,
    k8s_client: &dyn K8sClient,
    source_tls_config: &SourceTlsConfig,
    api_config: &ApiConfig,
) -> Result<(), PipelineError> {
    let (pipeline, replicator, image, source, destination) =
        read_pipeline_components(connection, tenant_id, pipeline_id, encryption_key).await?;

    if !should_reconcile_replicator_resources(k8s_client, tenant_id, replicator.id).await? {
        return Ok(());
    }

    create_or_update_pipeline_resources_in_k8s(
        k8s_client,
        tenant_id,
        pipeline,
        replicator,
        image,
        source,
        destination,
        api_config.supabase_api_url.as_deref(),
        source_tls_config.get_tls_config(),
    )
    .await?;

    Ok(())
}

/// Validates a source config against the trusted source profile, when enabled.
pub async fn validate_source_config(
    source_config: StoredSourceConfig,
    api_config: &ApiConfig,
    source_tls_config: &SourceTlsConfig,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    if api_config.source.trusted_username.is_none() {
        return Ok(vec![]);
    }

    let ctx =
        ValidationContext::build_from_source(source_config, api_config, source_tls_config).await?;
    validation::validate_source(&ctx).await
}
