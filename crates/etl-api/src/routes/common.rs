use tracing::{info, warn};

use crate::{
    config::ApiConfig,
    configs::{
        encryption::EncryptionKeyring, pipeline::StoredPipelineConfig, source::StoredSourceConfig,
    },
    data::{
        pipelines::{read_pipeline_components, read_pipeline_tables_to_sync},
        source_database,
    },
    k8s::{
        K8sClient, SourceTlsConfig,
        core::{
            create_k8s_object_prefix, create_or_update_pipeline_runtime_in_k8s,
            should_reconcile_pipeline_runtime,
        },
    },
    routes::pipelines::PipelineError,
    validation::{self, ValidationContext, ValidationError, ValidationFailure},
};

/// Checks whether published tables would perform initial sync, preserving the
/// VPA when inspection fails.
///
/// Uses the current API pipeline and source configuration supplied by the
/// caller, which must also be used to materialize the replacement. These
/// values may differ from the running Pod's configuration after an API update.
/// Inspection uses the source connection's statement and lock timeouts.
async fn restart_would_perform_table_sync(
    pipeline_id: i64,
    pipeline_config: &StoredPipelineConfig,
    source_id: i64,
    source_config: &StoredSourceConfig,
    source_tls_config: &SourceTlsConfig,
) -> bool {
    let inspection = async {
        let connection_config =
            source_config.clone().into_connection_config(source_tls_config.get_tls_config());
        let source_pool = source_database::connect(&connection_config).await?;
        read_pipeline_tables_to_sync(&source_pool, pipeline_id, &pipeline_config.publication_name)
            .await
    };

    match inspection.await {
        Ok(tables_to_sync) => {
            info!(
                pipeline_id,
                source_id,
                table_count = tables_to_sync.len(),
                "determined tables to sync on pipeline restart",
            );
            !tables_to_sync.is_empty()
        }
        Err(error) => {
            warn!(
                pipeline_id,
                source_id,
                error = %error,
                "failed to determine tables to sync on pipeline restart, preserving vertical pod autoscaler",
            );
            false
        }
    }
}

/// Reconciles and restarts the running replicator for a pipeline.
///
/// Update endpoints that can change source, destination, pipeline, image, or
/// runtime resource configuration should call this after writing the new API
/// state through the supplied connection, including within an uncommitted
/// transaction. The helper reads that state once and uses the same loaded
/// pipeline and source configuration for both sync preflight and Kubernetes
/// materialization. Updating the StatefulSet changes the pod template restart
/// annotation.
///
/// This forced recreation is part of the contract. The replicator loads its
/// mounted config and secret-backed environment when the process starts, so a
/// running pod must be restarted after config materialization in order to pick
/// up those changes.
///
/// Before reconciliation, checks current publication membership and durable
/// table state. If any table would perform initial sync, it deletes the VPA so
/// reconciliation restores its configured bounds and initial update mode.
/// This covers table sync even when copying existing rows is skipped. With
/// `Off`, the replacement Pod starts with the configured resources; this does
/// not guarantee that memory stays at that level throughout initial sync.
/// The recommender may retain usage history. Inspection failures and timeouts
/// preserve the VPA and do not block restart.
///
/// State or publication changes after inspection can race this decision.
/// Internal pipeline retries, container restarts, and Kubernetes-initiated Pod
/// replacements bypass it, including during initial sync. They do not reset
/// the VPA: the current Pod retains its resources, and a replacement may
/// receive an existing recommendation. Resource allocation outside this API
/// path is therefore governed by Kubernetes and the VPA's live policy.
///
/// If Kubernetes support is unavailable, or the pipeline has no active
/// Kubernetes resources, the call returns `false` without reconciling.
/// Otherwise, it returns `true` after the Kubernetes resources are reconciled.
pub(crate) async fn restart_replicator_if_running(
    connection: &mut sqlx::PgConnection,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKeyring,
    k8s_client: &dyn K8sClient,
    source_tls_config: &SourceTlsConfig,
    api_config: &ApiConfig,
) -> Result<bool, PipelineError> {
    let (pipeline, replicator, image, source, destination) =
        read_pipeline_components(connection, tenant_id, pipeline_id, encryption_key).await?;

    if !should_reconcile_pipeline_runtime(k8s_client, tenant_id, replicator.id).await? {
        return Ok(false);
    }

    if restart_would_perform_table_sync(
        pipeline_id,
        &pipeline.config,
        source.id,
        &source.config,
        source_tls_config,
    )
    .await
    {
        let resource_prefix = create_k8s_object_prefix(tenant_id, replicator.id);
        k8s_client.delete_replicator_vertical_pod_autoscaler(&resource_prefix).await?;
    }

    create_or_update_pipeline_runtime_in_k8s(
        k8s_client,
        tenant_id,
        pipeline,
        replicator,
        image,
        source,
        destination,
        api_config.supabase_api_url.as_deref(),
        api_config.replicator.destination_defaults.ducklake.copy_buffer,
        source_tls_config.get_tls_config(),
    )
    .await?;

    Ok(true)
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
