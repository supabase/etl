use std::ops::DerefMut;

use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, delete,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use super::{ErrorMessage, TenantIdError, extract_tenant_id};
use crate::{
    config::ApiConfig,
    configs::{
        destination::FullApiDestinationConfig, encryption::EncryptionKey,
        pipeline::FullApiPipelineConfig,
    },
    data,
    data::{
        connect_to_source_database_from_api,
        destinations::{DestinationsDbError, destination_exists},
        destinations_pipelines::DestinationPipelinesDbError,
        images::ImagesDbError,
        pipelines::{
            MAX_PIPELINES_PER_TENANT, PipelinesDbError, count_pipelines_for_tenant,
            delete_pipeline_api_and_source_state, delete_pipeline_api_state,
            delete_pipeline_replication_slots, read_pipeline, read_pipeline_for_deletion,
            read_pipelines_for_destination_for_deletion,
        },
        sources::SourcesDbError,
    },
    feature_flags::{FeatureFlagsClient, get_max_pipelines_per_tenant},
    k8s::{
        K8sClient, TrustedRootCertsCache, TrustedRootCertsError,
        core::{K8sCoreError, is_replicator_active},
    },
    validation::ValidationError,
};

#[derive(Debug, Error)]
enum DestinationPipelineError {
    #[error("No default image was found")]
    NoDefaultImageFound,

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error("The pipeline with id {0} was not found")]
    PipelineNotFound(i64),

    #[error("The pipeline with id {0} is not connected to destination with id {1}")]
    PipelineDestinationMismatch(i64, i64),

    #[error("A pipeline already exists for this source and destination combination")]
    DuplicatePipeline,

    #[error("The maximum number of pipelines ({limit}) has been reached for this project")]
    PipelineLimitReached { limit: i64 },

    #[error(transparent)]
    DestinationPipelinesDb(DestinationPipelinesDbError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    TrustedRootCerts(#[from] TrustedRootCertsError),

    #[error(transparent)]
    Validation(#[from] ValidationError),

    #[error(transparent)]
    K8sCore(#[from] K8sCoreError),

    #[error("The pipeline with id {0} is active. Stop it before deleting it.")]
    ActivePipeline(i64),

    #[error("{0}")]
    InvalidPipelineRequest(String),
}

impl From<DestinationPipelinesDbError> for DestinationPipelineError {
    fn from(e: DestinationPipelinesDbError) -> Self {
        match e {
            DestinationPipelinesDbError::Database(err)
                if data::utils::is_unique_constraint_violation_error(&err) =>
            {
                DestinationPipelineError::DuplicatePipeline
            }
            e => DestinationPipelineError::DestinationPipelinesDb(e),
        }
    }
}

impl DestinationPipelineError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages.
            DestinationPipelineError::DestinationPipelinesDb(
                DestinationPipelinesDbError::Database(_),
            )
            | DestinationPipelineError::DestinationsDb(DestinationsDbError::Database(_))
            | DestinationPipelineError::ImagesDb(ImagesDbError::Database(_))
            | DestinationPipelineError::SourcesDb(SourcesDbError::Database(_))
            | DestinationPipelineError::PipelinesDb(PipelinesDbError::Database(_))
            | DestinationPipelineError::Database(_)
            | DestinationPipelineError::Validation(_)
            | DestinationPipelineError::K8sCore(_) => "internal server error".to_owned(),
            // Every other message is ok, as they do not divulge sensitive information.
            e => e.to_string(),
        }
    }
}

impl ResponseError for DestinationPipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            DestinationPipelineError::NoDefaultImageFound
            | DestinationPipelineError::DestinationPipelinesDb(_)
            | DestinationPipelineError::DestinationsDb(_)
            | DestinationPipelineError::ImagesDb(_)
            | DestinationPipelineError::SourcesDb(_)
            | DestinationPipelineError::PipelinesDb(_)
            | DestinationPipelineError::Database(_)
            | DestinationPipelineError::K8sCore(_)
            | DestinationPipelineError::TrustedRootCerts(_)
            | DestinationPipelineError::Validation(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DestinationPipelineError::TenantId(_)
            | DestinationPipelineError::SourceNotFound(_)
            | DestinationPipelineError::DestinationNotFound(_)
            | DestinationPipelineError::PipelineNotFound(_)
            | DestinationPipelineError::PipelineDestinationMismatch(_, _)
            | DestinationPipelineError::InvalidPipelineRequest(_) => StatusCode::BAD_REQUEST,
            DestinationPipelineError::DuplicatePipeline
            | DestinationPipelineError::ActivePipeline(_) => StatusCode::CONFLICT,
            DestinationPipelineError::PipelineLimitReached { .. } => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage { error: self.to_message() };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code()).insert_header(ContentType::json()).body(body)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateDestinationPipelineRequest {
    #[schema(example = "My New Destination", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub destination_name: String,
    #[schema(required = true)]
    pub destination_config: FullApiDestinationConfig,
    #[schema(required = true, example = 1)]
    pub source_id: i64,
    #[schema(required = true)]
    pub pipeline_config: FullApiPipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateDestinationPipelineResponse {
    #[schema(example = 1)]
    pub destination_id: i64,
    #[schema(example = 2)]
    pub pipeline_id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateDestinationPipelineRequest {
    #[schema(example = "My Updated Destination", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub destination_name: String,
    #[schema(required = true)]
    pub destination_config: FullApiDestinationConfig,
    #[schema(required = true, example = 1)]
    pub source_id: i64,
    #[schema(required = true)]
    pub pipeline_config: FullApiPipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteDestinationPipelineResponse {
    #[schema(example = 1)]
    pub destination_id: i64,
    #[schema(example = 2)]
    pub pipeline_id: i64,
    #[schema(example = true)]
    pub destination_deleted: bool,
}

fn validate_pipeline_request(
    config: &FullApiPipelineConfig,
) -> Result<(), DestinationPipelineError> {
    config.validate().map_err(DestinationPipelineError::InvalidPipelineRequest)
}

#[utoipa::path(
    summary = "Create destination and pipeline",
    description = "Creates a destination and a pipeline linked to the specified source.",
    request_body = CreateDestinationPipelineRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination and pipeline created successfully", body = CreateDestinationPipelineResponse),
        (status = 409, description = "Conflict – a pipeline already exists for this source and destination", body = ErrorMessage),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations and Pipelines"
)]
#[post("/destinations-pipelines")]
pub async fn create_destination_and_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_and_pipeline: Json<CreateDestinationPipelineRequest>,
    encryption_key: Data<EncryptionKey>,
    feature_flags_client: Option<Data<FeatureFlagsClient>>,
) -> Result<impl Responder, DestinationPipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_and_pipeline = destination_and_pipeline.into_inner();
    validate_pipeline_request(&destination_and_pipeline.pipeline_config)?;

    let mut txn = pool.begin().await?;

    // Verify source exists
    data::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        destination_and_pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationPipelineError::SourceNotFound(destination_and_pipeline.source_id))?;

    let max_pipelines = get_max_pipelines_per_tenant(
        feature_flags_client.as_ref(),
        tenant_id,
        MAX_PIPELINES_PER_TENANT,
    )
    .await;
    let pipeline_count = count_pipelines_for_tenant(txn.deref_mut(), tenant_id).await?;
    if pipeline_count >= max_pipelines {
        return Err(DestinationPipelineError::PipelineLimitReached { limit: max_pipelines });
    }

    let image = data::images::read_default_image(&**pool)
        .await?
        .ok_or(DestinationPipelineError::NoDefaultImageFound)?;

    let (destination_id, pipeline_id) =
        data::destinations_pipelines::create_destination_and_pipeline(
            &mut txn,
            tenant_id,
            destination_and_pipeline.source_id,
            &destination_and_pipeline.destination_name,
            destination_and_pipeline.destination_config,
            image.id,
            destination_and_pipeline.pipeline_config,
            &encryption_key,
        )
        .await?;

    txn.commit().await?;

    let response = CreateDestinationPipelineResponse { destination_id, pipeline_id };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Update destination and pipeline",
    description = "Updates the destination and pipeline ensuring they remain linked.",
    request_body = UpdateDestinationPipelineRequest,
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination and pipeline updated successfully"),
        (status = 404, description = "Pipeline or destination not found", body = ErrorMessage),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations and Pipelines"
)]
#[post("/destinations-pipelines/{destination_id}/{pipeline_id}")]
pub async fn update_destination_and_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_and_pipeline_ids: Path<(i64, i64)>,
    destination_and_pipeline: Json<UpdateDestinationPipelineRequest>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, DestinationPipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (destination_id, pipeline_id) = destination_and_pipeline_ids.into_inner();
    let destination_and_pipeline = destination_and_pipeline.into_inner();
    validate_pipeline_request(&destination_and_pipeline.pipeline_config)?;

    let mut txn = pool.begin().await?;

    // Verify source exists
    data::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        destination_and_pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationPipelineError::SourceNotFound(destination_and_pipeline.source_id))?;

    if !destination_exists(txn.deref_mut(), tenant_id, destination_id).await? {
        return Err(DestinationPipelineError::DestinationNotFound(destination_id));
    }

    let pipeline = read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(DestinationPipelineError::PipelineNotFound(pipeline_id))?;

    if pipeline.destination_id != destination_id {
        return Err(DestinationPipelineError::PipelineDestinationMismatch(
            pipeline_id,
            destination_id,
        ));
    }

    data::destinations_pipelines::update_destination_and_pipeline(
        txn,
        tenant_id,
        destination_id,
        pipeline_id,
        destination_and_pipeline.source_id,
        &destination_and_pipeline.destination_name,
        destination_and_pipeline.destination_config,
        destination_and_pipeline.pipeline_config,
        &encryption_key,
    )
    .await
    .map_err(|e| match e {
        DestinationPipelinesDbError::DestinationNotFound(destination_id) => {
            DestinationPipelineError::DestinationNotFound(destination_id)
        }
        DestinationPipelinesDbError::PipelineNotFound(pipeline_id) => {
            DestinationPipelineError::PipelineNotFound(pipeline_id)
        }
        e => e.into(),
    })?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Delete destination and pipeline",
    description = "Deletes the pipeline and its destination after validation.",
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline deleted successfully, with destination deletion status included in the response body", body = DeleteDestinationPipelineResponse),
        (status = 409, description = "Pipeline is active", body = ErrorMessage),
        (status = 404, description = "Pipeline or destination not found", body = ErrorMessage),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations and Pipelines"
)]
#[delete("/destinations-pipelines/{destination_id}/{pipeline_id}")]
pub async fn delete_destination_and_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    api_config: Data<ApiConfig>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    trusted_root_certs_cache: Data<TrustedRootCertsCache>,
    destination_and_pipeline_ids: Path<(i64, i64)>,
) -> Result<impl Responder, DestinationPipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (destination_id, pipeline_id) = destination_and_pipeline_ids.into_inner();

    let pipeline = read_pipeline_for_deletion(&**pool, tenant_id, pipeline_id)
        .await?
        .ok_or(DestinationPipelineError::PipelineNotFound(pipeline_id))?;

    if pipeline.destination_id != destination_id {
        return Err(DestinationPipelineError::PipelineDestinationMismatch(
            pipeline_id,
            destination_id,
        ));
    }

    if is_replicator_active(k8s_client.as_ref(), tenant_id, pipeline.replicator_id).await? {
        return Err(DestinationPipelineError::ActivePipeline(pipeline.id));
    }

    let tls_config = trusted_root_certs_cache.get_tls_config(api_config.source.tls_enabled).await?;
    let source = data::sources::read_source_connection(
        &**pool,
        tenant_id,
        pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationPipelineError::SourceNotFound(pipeline.source_id))?;
    let source_pool = match connect_to_source_database_from_api(
        &source.config.into_connection_config(tls_config),
    )
    .await
    {
        Ok(source_pool) => Some(source_pool),
        Err(error) => {
            tracing::warn!(
                tenant_id = %tenant_id,
                destination_id,
                pipeline_id = pipeline.id,
                source_id = pipeline.source_id,
                error = %error,
                "failed to connect to source database during destination pipeline deletion, skipping source cleanup",
            );

            None
        }
    };
    let mut api_txn = pool.begin().await?;
    let mut source_txn = if let Some(source_pool) = source_pool.as_ref() {
        Some(source_pool.begin().await?)
    } else {
        None
    };
    if let Some(source_txn) = source_txn.as_mut() {
        delete_pipeline_api_and_source_state(
            api_txn.deref_mut(),
            source_txn.deref_mut(),
            tenant_id,
            &pipeline,
        )
        .await?;
    } else {
        delete_pipeline_api_state(api_txn.deref_mut(), tenant_id, &pipeline).await?;
    }

    let remaining_pipelines =
        read_pipelines_for_destination_for_deletion(api_txn.deref_mut(), tenant_id, destination_id)
            .await?;
    let destination_deleted = if remaining_pipelines.is_empty() {
        data::destinations::delete_destination(api_txn.deref_mut(), tenant_id, destination_id)
            .await?
            .ok_or(DestinationPipelineError::DestinationNotFound(destination_id))?;
        true
    } else {
        false
    };
    // Commit the API transaction first. If the source transaction committed first
    // and the API commit failed afterwards, the API database could still
    // reference pipeline state that no longer exists in the source database.
    api_txn.commit().await?;
    if let Some(source_txn) = source_txn {
        source_txn.commit().await?;
    }
    if let Some(source_pool) = source_pool.as_ref() {
        delete_pipeline_replication_slots(source_pool, pipeline.id).await?;
    }

    Ok(Json(DeleteDestinationPipelineResponse {
        destination_id,
        pipeline_id: pipeline.id,
        destination_deleted,
    }))
}
