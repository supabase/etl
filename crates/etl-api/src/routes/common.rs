use crate::{
    config::ApiConfig,
    configs::{encryption::EncryptionKeyring, source::StoredSourceConfig},
    data::{pipelines::read_pipeline_components, source_database},
    k8s::{
        K8sClient, SourceTlsConfig,
        core::{
            create_k8s_object_prefix, create_or_update_pipeline_resources_in_k8s,
            should_reconcile_replicator_resources,
        },
    },
    routes::pipelines::PipelineError,
    validation::{self, ValidationContext, ValidationError, ValidationFailure},
};
use etl::store::TableState;
use etl_postgres::store::table_state;

#[derive(Clone, Copy)]
enum RestartVpaPolicy {
    Preserve,
    ResetBeforeTableCopy,
}

/// Reconciles and restarts the running replicator for a pipeline.
///
/// Update endpoints that can change source, destination, pipeline, image, or
/// runtime resource configuration should call this after persisting the new API
/// state. The helper materializes the latest Kubernetes resources and relies on
/// the StatefulSet materialization to change the pod template restart
/// annotation.
///
/// This forced recreation is part of the contract. The replicator loads its
/// mounted config and secret-backed environment when the process starts, so a
/// running pod must be restarted after config materialization in order to pick
/// up those changes.
///
/// If Kubernetes support is unavailable, or the pipeline has no active
/// Kubernetes resources, the call returns `false` without reconciling.
/// Otherwise, it returns `true` after the Kubernetes resources are reconciled.
pub(crate) async fn restart_pipeline_replicator_if_running(
    connection: &mut sqlx::PgConnection,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKeyring,
    k8s_client: &dyn K8sClient,
    source_tls_config: &SourceTlsConfig,
    api_config: &ApiConfig,
) -> Result<bool, PipelineError> {
    restart_pipeline_replicator_if_running_with_vpa_policy(
        connection,
        tenant_id,
        pipeline_id,
        encryption_key,
        k8s_client,
        source_tls_config,
        api_config,
        RestartVpaPolicy::Preserve,
    )
    .await
}

/// Restarts a running pipeline from the explicit restart endpoint.
///
/// Unlike reconciliation-triggered restarts, an explicit restart can predict
/// from durable source state whether it will repeat an initial table copy. In
/// that case, this resets the VPA before reconciliation so the new copy starts
/// from the configured initial allocation instead of a steady-state
/// recommendation. Source inspection is deliberately best-effort: an
/// unavailable or invalid source preserves the existing VPA recommendation.
pub(crate) async fn restart_pipeline_from_endpoint(
    connection: &mut sqlx::PgConnection,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKeyring,
    k8s_client: &dyn K8sClient,
    source_tls_config: &SourceTlsConfig,
    api_config: &ApiConfig,
) -> Result<bool, PipelineError> {
    restart_pipeline_replicator_if_running_with_vpa_policy(
        connection,
        tenant_id,
        pipeline_id,
        encryption_key,
        k8s_client,
        source_tls_config,
        api_config,
        RestartVpaPolicy::ResetBeforeTableCopy,
    )
    .await
}

async fn restart_pipeline_replicator_if_running_with_vpa_policy(
    connection: &mut sqlx::PgConnection,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKeyring,
    k8s_client: &dyn K8sClient,
    source_tls_config: &SourceTlsConfig,
    api_config: &ApiConfig,
    vpa_policy: RestartVpaPolicy,
) -> Result<bool, PipelineError> {
    let (pipeline, replicator, image, source, destination) =
        read_pipeline_components(connection, tenant_id, pipeline_id, encryption_key).await?;

    if !should_reconcile_replicator_resources(k8s_client, tenant_id, replicator.id).await? {
        return Ok(false);
    }

    if matches!(vpa_policy, RestartVpaPolicy::ResetBeforeTableCopy)
        && restart_will_repeat_table_copy(
            pipeline_id,
            source.id,
            &source.config,
            source_tls_config,
        )
        .await
    {
        let resource_prefix = create_k8s_object_prefix(tenant_id, replicator.id);
        k8s_client.delete_replicator_vertical_pod_autoscaler(&resource_prefix).await?;
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

    Ok(true)
}

async fn restart_will_repeat_table_copy(
    pipeline_id: i64,
    source_id: i64,
    source_config: &StoredSourceConfig,
    source_tls_config: &SourceTlsConfig,
) -> bool {
    let result = async {
        let connection_config =
            source_config.clone().into_connection_config(source_tls_config.get_tls_config());
        let source_pool = source_database::connect(&connection_config).await?;
        let state_rows = table_state::get_table_state_rows(&source_pool, pipeline_id).await?;

        for state_row in state_rows {
            let Some(metadata) = state_row.metadata else {
                return Err(PipelineError::MissingTableState);
            };
            let state: TableState = serde_json::from_value(metadata)
                .map_err(PipelineError::InvalidTableState)?;

            // SyncDone and Ready keep their existing destination data after a
            // restart. Earlier lifecycle states repeat the copy; errored
            // tables remain stopped and are not automatically recopied.
            if !state.as_type().has_completed_table_sync() && !state.is_errored() {
                return Ok(true);
            }
        }

        Ok(false)
    }
    .await;

    match result {
        Ok(will_repeat_copy) => will_repeat_copy,
        Err(error) => {
            tracing::warn!(
                pipeline_id,
                source_id,
                error = %error,
                "failed to determine whether pipeline restart will repeat table copy, preserving vertical pod autoscaler",
            );
            false
        }
    }
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
