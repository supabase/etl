use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use etl_config::shared::{DestinationKind, PgConnectionConfigWithoutSecrets};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::{
    configs::{destination::ApiDestinationConfig, encryption::EncryptionKeyring},
    data::pipelines::{
        PipelinesDbError, read_pipeline_components, read_pipeline_ids_for_destination,
        read_pipeline_ids_for_destination_selector,
    },
    k8s::SourceTlsConfig,
    routes::{
        IntoInner, TenantIdError, error_response_with_internal_error, extract_tenant_id,
        pipelines::PipelineError,
    },
};

/// Criteria for resolving a runtime when the destination id is not known.
#[derive(Debug, Clone, Deserialize)]
pub struct ResolveRuntimeConfigRequest {
    /// Product kind of the destination to resolve.
    pub destination_kind: DestinationKind,
    /// Exact destination name to resolve.
    pub destination_name: String,
}

/// Runtime configuration, including credentials, resolved for one destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveRuntimeConfigResponse {
    /// Tenant owning the runtime.
    pub tenant_id: String,
    /// Destination selected by the request path.
    pub destination_id: i64,
    /// Human-readable destination name.
    pub destination_name: String,
    /// Pipeline using the destination.
    pub pipeline_id: i64,
    /// Replicator associated with the runtime.
    pub replicator_id: i64,
    /// Source connection configuration without its password.
    pub source: PgConnectionConfigWithoutSecrets,
    /// Destination configuration, including credentials required at runtime.
    pub destination: ApiDestinationConfig,
}

/// Errors resolving an internal destination runtime configuration.
#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    /// The tenant header was missing or invalid.
    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    /// No pipeline for the requested destination was found in the tenant.
    #[error("The destination with id {0} was not found or is not attached to a pipeline")]
    DestinationNotFound(i64),

    /// More than one pipeline uses the destination.
    #[error("The destination with id {0} is attached to multiple pipelines")]
    MultiplePipelines(i64),

    /// The tenant has no runtime matching the requested selector.
    #[error("The tenant has no destination matching the requested selector attached to a pipeline")]
    TenantRuntimeNotFound,

    /// More than one runtime matches the requested selector.
    #[error("The tenant has multiple destinations matching the requested selector")]
    MultipleTenantRuntimes,

    /// Reading pipeline identifiers failed.
    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    /// Reading the selected pipeline components failed.
    #[error(transparent)]
    Pipeline(#[from] PipelineError),

    /// Acquiring an API database connection failed.
    #[error("Error while acquiring an API database connection")]
    Database(#[source] sqlx::Error),
}

impl RuntimeConfigError {
    fn to_message(&self) -> String {
        match self {
            Self::TenantId(_)
            | Self::DestinationNotFound(_)
            | Self::MultiplePipelines(_)
            | Self::TenantRuntimeNotFound
            | Self::MultipleTenantRuntimes => self.to_string(),
            Self::PipelinesDb(_) | Self::Pipeline(_) | Self::Database(_) => {
                "Internal server error".to_owned()
            }
        }
    }
}

impl IntoResponse for RuntimeConfigError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::TenantId(_) => StatusCode::BAD_REQUEST,
            Self::DestinationNotFound(_) | Self::TenantRuntimeNotFound => StatusCode::NOT_FOUND,
            Self::MultiplePipelines(_) | Self::MultipleTenantRuntimes => StatusCode::CONFLICT,
            Self::PipelinesDb(_) | Self::Pipeline(_) | Self::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        error_response_with_internal_error(status, self.to_message(), &self)
    }
}

/// Resolves runtime metadata and credentials for one destination.
pub(crate) async fn resolve_runtime_config(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    destination_id: Path<i64>,
) -> Result<Response, RuntimeConfigError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let destination_id = destination_id.into_inner();
    let pipeline_ids = read_pipeline_ids_for_destination(&pool, tenant_id, destination_id).await?;
    let pipeline_id = match pipeline_ids.as_slice() {
        [] => return Err(RuntimeConfigError::DestinationNotFound(destination_id)),
        [pipeline_id] => *pipeline_id,
        _ => return Err(RuntimeConfigError::MultiplePipelines(destination_id)),
    };

    resolve_runtime_config_inner(
        tenant_id,
        destination_id,
        pipeline_id,
        &pool,
        &encryption_key,
        &source_tls_config,
    )
    .await
}

/// Resolves a tenant runtime using destination criteria supplied by the caller.
pub(crate) async fn resolve_tenant_runtime_config(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Json(request): Json<ResolveRuntimeConfigRequest>,
) -> Result<Response, RuntimeConfigError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let runtimes = read_pipeline_ids_for_destination_selector(
        &pool,
        tenant_id,
        &request.destination_name,
        request.destination_kind.as_str(),
    )
    .await?;
    let (destination_id, pipeline_id) = match runtimes.as_slice() {
        [] => return Err(RuntimeConfigError::TenantRuntimeNotFound),
        [(destination_id, pipeline_id)] => (*destination_id, *pipeline_id),
        _ => return Err(RuntimeConfigError::MultipleTenantRuntimes),
    };

    resolve_runtime_config_inner(
        tenant_id,
        destination_id,
        pipeline_id,
        &pool,
        &encryption_key,
        &source_tls_config,
    )
    .await
}

async fn resolve_runtime_config_inner(
    tenant_id: &str,
    destination_id: i64,
    pipeline_id: i64,
    pool: &PgPool,
    encryption_key: &EncryptionKeyring,
    source_tls_config: &SourceTlsConfig,
) -> Result<Response, RuntimeConfigError> {
    let mut connection = pool.acquire().await.map_err(RuntimeConfigError::Database)?;
    let (pipeline, replicator, _image, source, destination) =
        read_pipeline_components(&mut connection, tenant_id, pipeline_id, encryption_key).await?;

    let source = PgConnectionConfigWithoutSecrets::from(
        source.config.into_connection_config(source_tls_config.get_tls_config()),
    );
    let destination = ApiDestinationConfig::from(destination.config);

    let mut response = Json(ResolveRuntimeConfigResponse {
        tenant_id: tenant_id.to_owned(),
        destination_id,
        destination_name: pipeline.destination_name,
        pipeline_id: pipeline.id,
        replicator_id: replicator.id,
        source,
        destination,
    })
    .into_response();
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));

    Ok(response)
}
