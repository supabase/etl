use std::{ops::DerefMut, sync::Arc};

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use etl_config::Environment;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    config::ApiConfig,
    configs::{
        destination::{FullApiDestinationConfig, UpdateApiDestinationConfig},
        encryption::EncryptionKeyring,
        pipeline::FullApiPipelineConfig,
    },
    data,
    data::{
        destinations::{DestinationsDbError, destination_exists},
        pipelines::{PipelinesDbError, read_pipelines_for_destination_for_deletion},
        sources::SourcesDbError,
    },
    k8s::{
        K8sClient, TrustedRootCertsCache,
        core::{K8sCoreError, first_active_pipeline_id},
    },
    routes::{
        ErrorMessage, IntoInner, TenantIdError, error_response_with_internal_error,
        extract_tenant_id, utils,
    },
    validation,
    validation::{FailureType, ValidationContext, ValidationError, ValidationFailure},
};

#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    Validation(#[from] ValidationError),

    #[error("Failed to load environment: {0}")]
    Environment(#[from] std::io::Error),

    #[error(transparent)]
    K8sCore(#[from] K8sCoreError),

    #[error("The pipeline with id {0} is active; stop it before deleting it")]
    ActivePipeline(i64),

    #[error("The destination with id {0} is still used by pipelines; delete those pipelines first")]
    DestinationInUse(i64),

    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("Invalid destination validation request: {0}")]
    InvalidValidationRequest(String),
}

impl DestinationError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal details in error messages.
            DestinationError::DestinationsDb(DestinationsDbError::Database(_))
            | DestinationError::PipelinesDb(PipelinesDbError::Database(_))
            | DestinationError::SourcesDb(SourcesDbError::Database(_))
            | DestinationError::Environment(_)
            | DestinationError::K8sCore(_) => "Internal server error".to_owned(),
            DestinationError::Validation(error) => {
                utils::validation_error_message(error).to_owned()
            }
            // Every other message is ok, as they do not divulge sensitive information.
            err => err.to_string(),
        }
    }
}

impl IntoResponse for DestinationError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            DestinationError::DestinationsDb(DestinationsDbError::DestinationConfigUpdate(_)) => {
                StatusCode::BAD_REQUEST
            }
            DestinationError::DestinationsDb(_)
            | DestinationError::PipelinesDb(_)
            | DestinationError::SourcesDb(_)
            | DestinationError::Environment(_)
            | DestinationError::K8sCore(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DestinationError::Validation(error) => utils::validation_error_status_code(error),
            DestinationError::DestinationNotFound(_) | DestinationError::SourceNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            DestinationError::ActivePipeline(_) | DestinationError::DestinationInUse(_) => {
                StatusCode::CONFLICT
            }
            DestinationError::TenantId(_) | DestinationError::InvalidValidationRequest(_) => {
                StatusCode::BAD_REQUEST
            }
        };

        error_response_with_internal_error(status_code, self.to_message(), &self)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateDestinationRequest {
    #[schema(example = "My BigQuery Destination", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(required = true)]
    pub config: FullApiDestinationConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateDestinationResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateDestinationRequest {
    #[schema(example = "My Updated BigQuery Destination", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(required = true)]
    pub config: UpdateApiDestinationConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadDestinationResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = "My BigQuery Destination")]
    pub name: String,
    pub config: FullApiDestinationConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadDestinationsResponse {
    pub destinations: Vec<ReadDestinationResponse>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateDestinationRequest {
    /// Source identifier used for source-aware destination validation.
    #[schema(example = 1)]
    pub source_id: Option<i64>,
    /// Destination configuration to validate.
    #[schema(required = true)]
    pub config: FullApiDestinationConfig,
    /// Pipeline configuration used to cross-reference source publication
    /// details.
    pub pipeline_config: Option<FullApiPipelineConfig>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidationFailureResponse {
    #[schema(example = "BigQuery Dataset Not Found")]
    pub name: String,
    #[schema(example = "Dataset 'my_dataset' does not exist in project 'my_project'")]
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
pub struct ValidateDestinationResponse {
    /// Validation failures found for the destination configuration.
    pub validation_failures: Vec<ValidationFailureResponse>,
}

#[utoipa::path(
    post,
    path = "/destinations",
    summary = "Create a destination",
    description = "Creates a destination for the specified tenant.",
    request_body = CreateDestinationRequest,
    responses(
        (status = 200, description = "Destination created successfully", body = CreateDestinationResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    tag = "Destinations"
)]
pub(crate) async fn create_destination(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    destination: Json<CreateDestinationRequest>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let destination = destination.into_inner();

    let id = data::destinations::create_destination(
        &pool,
        tenant_id,
        &destination.name,
        destination.config,
        &encryption_key,
    )
    .await?;

    let response = CreateDestinationResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/destinations/{destination_id}",
    summary = "Retrieve a destination",
    description = "Returns a destination identified by its ID for the given tenant.",
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination retrieved successfully", body = ReadDestinationResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
pub(crate) async fn read_destination(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    destination_id: Path<i64>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let destination_id = destination_id.into_inner();

    let response =
        data::destinations::read_destination(&pool, tenant_id, destination_id, &encryption_key)
            .await?
            .map(|destination| ReadDestinationResponse {
                id: destination.id,
                tenant_id: destination.tenant_id,
                name: destination.name,
                config: destination.config.into(),
            })
            .ok_or(DestinationError::DestinationNotFound(destination_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/destinations/{destination_id}",
    summary = "Update a destination",
    description = "Updates the destination's name and configuration.",
    request_body = UpdateDestinationRequest,
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination updated successfully"),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
pub(crate) async fn update_destination(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    destination_id: Path<i64>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    destination: Json<UpdateDestinationRequest>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let destination_id = destination_id.into_inner();
    let destination = destination.into_inner();

    let mut conn = pool.acquire().await.map_err(DestinationsDbError::from)?;
    data::destinations::update_destination(
        conn.deref_mut(),
        tenant_id,
        &destination.name,
        destination_id,
        destination.config,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationError::DestinationNotFound(destination_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    delete,
    path = "/destinations/{destination_id}",
    summary = "Delete a destination",
    description = "Deletes a destination by ID for the given tenant.",
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination deleted successfully"),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 409, description = "Destination has an active pipeline or is still used by pipelines", body = ErrorMessage),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
pub(crate) async fn delete_destination(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    destination_id: Path<i64>,
    Extension(k8s_client): Extension<Arc<dyn K8sClient>>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let destination_id = destination_id.into_inner();

    if !destination_exists(&pool, tenant_id, destination_id).await? {
        return Err(DestinationError::DestinationNotFound(destination_id));
    }

    let pipelines =
        read_pipelines_for_destination_for_deletion(&pool, tenant_id, destination_id).await?;
    if let Some(pipeline_id) =
        first_active_pipeline_id(k8s_client.as_ref(), tenant_id, &pipelines).await?
    {
        return Err(DestinationError::ActivePipeline(pipeline_id));
    }

    if !pipelines.is_empty() {
        return Err(DestinationError::DestinationInUse(destination_id));
    }

    // We intentionally keep this endpoint simple: the checks above provide the
    // normal user-facing guard rails, but we do not try to serialize against
    // concurrent pipeline creation here. A pipeline can still appear between
    // the check and the final delete, in which case the database constraints
    // are the last line of defense.
    data::destinations::delete_destination(&pool, tenant_id, destination_id)
        .await?
        .ok_or(DestinationError::DestinationNotFound(destination_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/destinations",
    summary = "List destinations",
    description = "Returns all destinations for the specified tenant.",
    responses(
        (status = 200, description = "Destinations listed successfully", body = ReadDestinationsResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    tag = "Destinations"
)]
pub(crate) async fn read_all_destinations(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;

    let mut destinations = vec![];
    for destination in
        data::destinations::read_all_destinations(&pool, tenant_id, &encryption_key).await?
    {
        let destination = ReadDestinationResponse {
            id: destination.id,
            tenant_id: destination.tenant_id,
            name: destination.name,
            config: destination.config.into(),
        };
        destinations.push(destination);
    }

    let response = ReadDestinationsResponse { destinations };

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/destinations/validate",
    summary = "Validate destination configuration",
    description = "Validates that the destination is accessible and properly configured.",
    request_body = ValidateDestinationRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Validation completed", body = ValidateDestinationResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Destination dependency or your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Destination dependency or your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to destination dependency or your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
pub(crate) async fn validate_destination(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    request: Json<ValidateDestinationRequest>,
) -> Result<impl IntoResponse, DestinationError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let request = request.into_inner();
    if let Some(pipeline_config) = &request.pipeline_config {
        pipeline_config.validate().map_err(DestinationError::InvalidValidationRequest)?;
    }
    let pipeline_config = request.pipeline_config.as_ref();
    let publication_name =
        pipeline_config.map(|pipeline_config| pipeline_config.publication_name.as_str());

    let ctx = match (request.source_id, publication_name) {
        (Some(source_id), Some(_)) => {
            let source = data::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
                .await?
                .ok_or(DestinationError::SourceNotFound(source_id))?;

            ValidationContext::build_from_source(
                source.config,
                api_config.as_ref(),
                trusted_root_certs_cache.as_ref(),
            )
            .await?
        }
        (None, None) => {
            let environment = Environment::load()?;
            ValidationContext::builder(environment).build()
        }
        _ => {
            return Err(DestinationError::InvalidValidationRequest(
                "`source_id` and `pipeline_config` must be provided together.".to_owned(),
            ));
        }
    };

    let failures = validation::validate_destination(&ctx, &request.config, pipeline_config).await?;
    let response = ValidateDestinationResponse {
        validation_failures: failures.into_iter().map(Into::into).collect(),
    };

    Ok(Json(response))
}
