use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    config::ApiConfig,
    configs::{
        encryption::EncryptionKey,
        source::{FullApiSourceConfig, StoredSourceConfig, StrippedApiSourceConfig},
    },
    data,
    data::{
        pipelines::{PipelinesDbError, read_pipelines_for_source_for_deletion},
        sources::{SourcesDbError, source_exists},
    },
    k8s::{
        K8sClient, TrustedRootCertsCache,
        core::{K8sCoreError, first_active_pipeline_id},
    },
    routes::{
        ErrorMessage, IntoInner, TenantIdError, common, error_response, extract_tenant_id, utils,
    },
    validation::{FailureType, ValidationError, ValidationFailure},
};

pub mod publications;
pub mod tables;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error(transparent)]
    Validation(#[from] ValidationError),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error(transparent)]
    K8sCore(#[from] K8sCoreError),

    #[error("The pipeline with id {0} is active; stop it before deleting it")]
    ActivePipeline(i64),

    #[error("The source with id {0} is still used by pipelines; delete those pipelines first")]
    SourceInUse(i64),
}

impl SourceError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            SourceError::SourcesDb(SourcesDbError::Database(_))
            | SourceError::PipelinesDb(PipelinesDbError::Database(_))
            | SourceError::K8sCore(_) => "Internal server error".to_owned(),
            SourceError::Validation(error) => utils::validation_error_message(error).to_owned(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl IntoResponse for SourceError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            SourceError::SourceNotFound(_) => StatusCode::NOT_FOUND,
            SourceError::TenantId(_) => StatusCode::BAD_REQUEST,
            SourceError::SourcesDb(_) | SourceError::PipelinesDb(_) | SourceError::K8sCore(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            SourceError::Validation(error) => utils::validation_error_status_code(error),
            SourceError::ValidationFailed(_) => StatusCode::UNPROCESSABLE_ENTITY,
            SourceError::ActivePipeline(_) | SourceError::SourceInUse(_) => StatusCode::CONFLICT,
        };

        error_response(status_code, self.to_message())
    }
}

async fn validate_source_config(
    source_config: StoredSourceConfig,
    api_config: &ApiConfig,
    trusted_root_certs_cache: &TrustedRootCertsCache,
) -> Result<(), SourceError> {
    let failures =
        common::validate_source_config(source_config, api_config, trusted_root_certs_cache).await?;

    if !failures.is_empty() {
        return Err(SourceError::ValidationFailed(utils::format_validation_failures(failures)));
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceRequest {
    #[schema(example = "My Postgres Source", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(required = true)]
    pub config: FullApiSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateSourceRequest {
    #[schema(example = "My Updated Postgres Source", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(required = true)]
    pub config: FullApiSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadSourceResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = "My Postgres Source")]
    pub name: String,
    pub config: StrippedApiSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadSourcesResponse {
    pub sources: Vec<ReadSourceResponse>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateSourceRequest {
    #[schema(required = true)]
    pub config: FullApiSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidationFailureResponse {
    #[schema(example = "source Role Mismatch")]
    pub name: String,
    #[schema(example = "Source role 'postgres' does not match trusted username 'etl_user'")]
    pub reason: String,
    #[schema(example = "critical")]
    pub failure_type: FailureType,
}

impl From<ValidationFailure> for ValidationFailureResponse {
    fn from(failure: ValidationFailure) -> Self {
        Self { name: failure.name, reason: failure.reason, failure_type: failure.failure_type }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateSourceResponse {
    pub validation_failures: Vec<ValidationFailureResponse>,
}

#[utoipa::path(
    post,
    path = "/sources",
    summary = "Create a source",
    description = "Creates a source for the specified tenant.",
    request_body = CreateSourceRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Source created successfully", body = CreateSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 422, description = "Source profile validation failed", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
pub(crate) async fn create_source(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    Extension(encryption_key): Extension<Arc<EncryptionKey>>,
    source: Json<CreateSourceRequest>,
) -> Result<impl IntoResponse, SourceError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let source = source.into_inner();

    validate_source_config(
        source.config.clone().into(),
        api_config.as_ref(),
        trusted_root_certs_cache.as_ref(),
    )
    .await?;

    let id = data::sources::create_source(
        &pool,
        tenant_id,
        &source.name,
        source.config,
        &encryption_key,
    )
    .await?;

    let response = CreateSourceResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/sources/validate",
    summary = "Validate source configuration",
    description = "Validates source access using the source validation checks configured for the API.",
    request_body = ValidateSourceRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Validation completed", body = ValidateSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Sources"
)]
pub(crate) async fn validate_source(
    headers: HeaderMap,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    request: Json<ValidateSourceRequest>,
) -> Result<impl IntoResponse, SourceError> {
    let _tenant_id = extract_tenant_id(&headers)?;
    let request = request.into_inner();

    let failures = common::validate_source_config(
        request.config.into(),
        api_config.as_ref(),
        trusted_root_certs_cache.as_ref(),
    )
    .await?;
    let response = ValidateSourceResponse {
        validation_failures: failures.into_iter().map(Into::into).collect(),
    };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/sources/{source_id}",
    summary = "Retrieve a source",
    description = "Returns a source by ID. Sensitive fields are omitted from the configuration.",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Source retrieved successfully", body = ReadSourceResponse),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
pub(crate) async fn read_source(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKey>>,
    source_id: Path<i64>,
) -> Result<impl IntoResponse, SourceError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let source_id = source_id.into_inner();

    let response = data::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| ReadSourceResponse {
            id: s.id,
            tenant_id: s.tenant_id,
            name: s.name,
            config: s.config.into(),
        })
        .ok_or(SourceError::SourceNotFound(source_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/sources/{source_id}",
    summary = "Update a source",
    description = "Updates a source's name and configuration.",
    request_body = UpdateSourceRequest,
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Source updated successfully"),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 422, description = "Source profile validation failed", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
pub(crate) async fn update_source(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    source_id: Path<i64>,
    Extension(encryption_key): Extension<Arc<EncryptionKey>>,
    source: Json<UpdateSourceRequest>,
) -> Result<impl IntoResponse, SourceError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let source_id = source_id.into_inner();
    let source = source.into_inner();

    validate_source_config(
        source.config.clone().into(),
        api_config.as_ref(),
        trusted_root_certs_cache.as_ref(),
    )
    .await?;

    data::sources::update_source(
        &pool,
        tenant_id,
        &source.name,
        source_id,
        source.config,
        &encryption_key,
    )
    .await?
    .ok_or(SourceError::SourceNotFound(source_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    delete,
    path = "/sources/{source_id}",
    summary = "Delete a source",
    description = "Deletes a source by ID for the given tenant.",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Source deleted successfully"),
        (status = 409, description = "Source has an active pipeline or is still used by pipelines", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
pub(crate) async fn delete_source(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(k8s_client): Extension<Arc<dyn K8sClient>>,
    source_id: Path<i64>,
) -> Result<impl IntoResponse, SourceError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let source_id = source_id.into_inner();

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(SourceError::SourceNotFound(source_id));
    }

    let pipelines = read_pipelines_for_source_for_deletion(&pool, tenant_id, source_id).await?;
    if let Some(pipeline_id) =
        first_active_pipeline_id(k8s_client.as_ref(), tenant_id, &pipelines).await?
    {
        return Err(SourceError::ActivePipeline(pipeline_id));
    }

    if !pipelines.is_empty() {
        return Err(SourceError::SourceInUse(source_id));
    }

    // We intentionally keep this endpoint simple: the checks above provide the
    // normal user-facing guard rails, but we do not try to serialize against
    // concurrent pipeline creation here. A pipeline can still appear between
    // the check and the final delete, in which case the database constraints
    // are the last line of defense.
    data::sources::delete_source(&pool, tenant_id, source_id)
        .await?
        .ok_or(SourceError::SourceNotFound(source_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/sources",
    summary = "List sources",
    description = "Returns all sources for the specified tenant.",
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Sources listed successfully", body = ReadSourcesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
pub(crate) async fn read_all_sources(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKey>>,
) -> Result<impl IntoResponse, SourceError> {
    let tenant_id = extract_tenant_id(&headers)?;

    let mut sources = vec![];
    for source in data::sources::read_all_sources(&pool, tenant_id, &encryption_key).await? {
        let source = ReadSourceResponse {
            id: source.id,
            tenant_id: source.tenant_id,
            name: source.name,
            config: source.config.into(),
        };
        sources.push(source);
    }

    let response = ReadSourcesResponse { sources };

    Ok(Json(response))
}
