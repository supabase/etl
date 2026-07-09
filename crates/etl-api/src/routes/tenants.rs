use std::{ops::DerefMut, sync::Arc};

use axum::{
    Extension, Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing::warn;
use utoipa::ToSchema;

use crate::{
    configs::encryption::EncryptionKeyring,
    data,
    data::{
        pipelines::{
            PipelinesDbError, delete_pipeline_replication_slots, delete_pipelines_source_state,
            read_all_pipelines_for_deletion,
        },
        source_database::{self, SourceDatabaseErrorKind},
        sources::SourcesDbError,
        tenants::TenantsDbError,
    },
    k8s::{
        K8sClient, SourceTlsConfig,
        core::{K8sCoreError, first_active_pipeline_id},
    },
    routes::{
        ErrorMessage, IntoInner, TenantIdError, error_response_with_internal_error, utils,
        validate_tenant_id,
    },
};

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("The tenant with id {0} was not found")]
    TenantNotFound(String),

    #[error(transparent)]
    TenantsDb(#[from] TenantsDbError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Source database error: {0}")]
    SourceDatabase(sqlx::Error),

    #[error("Source pipeline state operation failed: {0}")]
    SourcePipelineState(PipelinesDbError),

    #[error(transparent)]
    K8sCore(#[from] K8sCoreError),

    #[error("The pipeline with id {0} is active; stop it before deleting it")]
    ActivePipeline(i64),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),
}

impl TenantError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantError::TenantsDb(TenantsDbError::Database(_))
            | TenantError::SourcesDb(_)
            | TenantError::PipelinesDb(_)
            | TenantError::Database(_)
            | TenantError::K8sCore(_) => "Internal server error".to_owned(),
            TenantError::SourceDatabase(_) | TenantError::SourcePipelineState(_) => {
                utils::source_database_query_error_message().to_owned()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }

    fn allows_best_effort_source_cleanup_to_continue(&self) -> bool {
        match self {
            TenantError::SourceDatabase(error)
            | TenantError::SourcePipelineState(PipelinesDbError::Database(error)) => matches!(
                source_database::classify_error(error),
                SourceDatabaseErrorKind::TimedOut | SourceDatabaseErrorKind::Unavailable
            ),
            _ => false,
        }
    }
}

impl IntoResponse for TenantError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            TenantError::TenantsDb(TenantsDbError::Conflict(_))
            | TenantError::ActivePipeline(_) => StatusCode::CONFLICT,
            TenantError::TenantId(_) => StatusCode::BAD_REQUEST,
            TenantError::TenantsDb(_)
            | TenantError::SourcesDb(_)
            | TenantError::PipelinesDb(_)
            | TenantError::Database(_)
            | TenantError::K8sCore(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantError::SourceDatabase(error) => utils::source_database_error_status_code(error),
            TenantError::SourcePipelineState(error) => match error {
                PipelinesDbError::Database(error) => {
                    utils::source_database_error_status_code(error)
                }
                _ => StatusCode::BAD_GATEWAY,
            },
            TenantError::TenantNotFound(_) => StatusCode::NOT_FOUND,
        };

        error_response_with_internal_error(status_code, self.to_message(), &self)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    #[schema(example = "abczjjlmfsijwrlnwatw", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub id: String,
    #[schema(example = "My Tenant", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateOrUpdateTenantRequest {
    #[schema(example = "My Updated Tenant", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateOrUpdateTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    #[schema(example = "My Updated Tenant", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
    #[schema(example = "My Tenant")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadTenantsResponse {
    pub tenants: Vec<ReadTenantResponse>,
}

#[utoipa::path(
    post,
    path = "/tenants",
    summary = "Create a tenant",
    description = "Creates a new tenant with the provided ID and name.",
    request_body = CreateTenantRequest,
    responses(
        (status = 200, description = "Tenant created successfully", body = CreateTenantResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 409, description = "Tenant already exists", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn create_tenant(
    Extension(pool): Extension<PgPool>,
    tenant: Json<CreateTenantRequest>,
) -> Result<impl IntoResponse, TenantError> {
    let tenant = tenant.into_inner();
    validate_tenant_id(&tenant.id)?;

    tracing::Span::current().record("project", &tenant.id);

    let id = data::tenants::create_tenant(&pool, &tenant.id, &tenant.name).await?;

    let response = CreateTenantResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    put,
    path = "/tenants/{tenant_id}",
    summary = "Create or update a tenant",
    description = "Creates a tenant if it does not exist; otherwise updates its name.",
    request_body = CreateOrUpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant created or updated successfully", body = CreateOrUpdateTenantResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn create_or_update_tenant(
    Extension(pool): Extension<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<CreateOrUpdateTenantRequest>,
) -> Result<impl IntoResponse, TenantError> {
    let tenant_id = tenant_id.into_inner();
    let tenant = tenant.into_inner();
    validate_tenant_id(&tenant_id)?;

    tracing::Span::current().record("project", &tenant_id);

    let id = data::tenants::create_or_update_tenant(&pool, &tenant_id, &tenant.name).await?;
    let response = CreateOrUpdateTenantResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}",
    summary = "Retrieve a tenant",
    description = "Returns the tenant identified by the provided ID.",
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant retrieved successfully", body = ReadTenantResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn read_tenant(
    Extension(pool): Extension<PgPool>,
    tenant_id: Path<String>,
) -> Result<impl IntoResponse, TenantError> {
    let tenant_id = tenant_id.into_inner();
    validate_tenant_id(&tenant_id)?;

    tracing::Span::current().record("project", &tenant_id);

    let response = data::tenants::read_tenant(&pool, &tenant_id)
        .await?
        .map(|t| ReadTenantResponse { id: t.id, name: t.name })
        .ok_or(TenantError::TenantNotFound(tenant_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/tenants/{tenant_id}",
    summary = "Update a tenant",
    description = "Updates the tenant's display name.",
    request_body = UpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant updated successfully"),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn update_tenant(
    Extension(pool): Extension<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<UpdateTenantRequest>,
) -> Result<impl IntoResponse, TenantError> {
    let tenant = tenant.into_inner();
    let tenant_id = tenant_id.into_inner();
    validate_tenant_id(&tenant_id)?;

    tracing::Span::current().record("project", &tenant_id);

    data::tenants::update_tenant(&pool, &tenant_id, &tenant.name)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    delete,
    path = "/tenants/{tenant_id}",
    summary = "Delete a tenant",
    description = "Deletes the tenant identified by the provided ID.",
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant deleted successfully"),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 409, description = "Tenant has active pipelines or pipelines still defined", body = ErrorMessage),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn delete_tenant(
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(k8s_client): Extension<Option<Arc<dyn K8sClient>>>,
    tenant_id: Path<String>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
) -> Result<impl IntoResponse, TenantError> {
    let tenant_id = tenant_id.into_inner();
    validate_tenant_id(&tenant_id)?;

    tracing::Span::current().record("project", &tenant_id);

    let pipelines = read_all_pipelines_for_deletion(&pool, &tenant_id).await?;
    if let Some(pipeline_id) =
        first_active_pipeline_id(k8s_client.as_deref(), &tenant_id, &pipelines).await?
    {
        return Err(TenantError::ActivePipeline(pipeline_id));
    }

    let sources =
        data::sources::read_all_source_connections(&pool, &tenant_id, &encryption_key).await?;
    let tls_config = source_tls_config.get_tls_config();
    let mut pipelines_by_source = std::collections::BTreeMap::new();
    for pipeline in pipelines {
        pipelines_by_source.entry(pipeline.source_id).or_insert_with(Vec::new).push(pipeline);
    }

    // We intentionally visit every stored source connection. If multiple source
    // records happen to target the same physical database, the source-side
    // cleanup stays idempotent, so repeated passes are still safe without
    // deduplicating connection configs.
    for source in sources {
        // If the source database is already unreachable during tenant teardown, we
        // treat it as effectively deleted and keep removing the tenant's
        // API-side state.
        let source_pool = match source_database::connect(
            &source.config.into_connection_config(tls_config.clone()),
        )
        .await
        {
            Ok(source_pool) => source_pool,
            Err(error) => {
                warn!(
                    tenant_id = %tenant_id,
                    source_id = source.id,
                    error = %error,
                    "failed to connect to source database during tenant deletion, skipping source cleanup",
                );

                continue;
            }
        };

        let source_cleanup_result = async {
            let mut source_txn = source_pool.begin().await.map_err(TenantError::SourceDatabase)?;

            let source_pipelines = pipelines_by_source.remove(&source.id).unwrap_or_default();
            let deleted_pipeline_ids =
                delete_pipelines_source_state(source_txn.deref_mut(), &source_pipelines)
                    .await
                    .map_err(TenantError::SourcePipelineState)?;
            data::sources::uninstall_source_installation(source_txn.deref_mut())
                .await
                .map_err(TenantError::SourceDatabase)?;

            source_txn.commit().await.map_err(TenantError::SourceDatabase)?;

            Ok::<_, TenantError>(deleted_pipeline_ids)
        }
        .await;

        let deleted_pipeline_ids = match source_cleanup_result {
            Ok(deleted_pipeline_ids) => deleted_pipeline_ids,
            Err(error) if error.allows_best_effort_source_cleanup_to_continue() => {
                warn!(
                    tenant_id = %tenant_id,
                    source_id = source.id,
                    error = %error,
                    "source database became unavailable during tenant deletion, skipping source cleanup",
                );

                continue;
            }
            Err(error) => return Err(error),
        };
        for pipeline_id in deleted_pipeline_ids {
            let slot_cleanup_result = delete_pipeline_replication_slots(&source_pool, pipeline_id)
                .await
                .map_err(TenantError::SourcePipelineState);

            if let Err(error) = slot_cleanup_result {
                if error.allows_best_effort_source_cleanup_to_continue() {
                    warn!(
                        tenant_id = %tenant_id,
                        source_id = source.id,
                        pipeline_id,
                        error = %error,
                        "source database became unavailable during replication slot cleanup, skipping remaining slot cleanup",
                    );

                    break;
                }

                return Err(error);
            }
        }
    }

    // Deleting the tenant is enough for API-side cleanup because Postgres cascades
    // tenant-owned rows in the app schema; we only clean source databases
    // manually above.
    data::tenants::delete_tenant(&pool, &tenant_id)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/tenants",
    summary = "List tenants",
    description = "Returns all tenants.",
    responses(
        (status = 200, description = "Tenants listed successfully", body = ReadTenantsResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
pub(crate) async fn read_all_tenants(
    Extension(pool): Extension<PgPool>,
) -> Result<impl IntoResponse, TenantError> {
    let tenants: Vec<ReadTenantResponse> = data::tenants::read_all_tenants(&pool)
        .await?
        .drain(..)
        .map(|t| ReadTenantResponse { id: t.id, name: t.name })
        .collect();

    let response = ReadTenantsResponse { tenants };

    Ok(Json(response))
}
