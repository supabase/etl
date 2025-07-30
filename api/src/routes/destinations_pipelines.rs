use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use config::shared::DestinationConfig;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::ops::DerefMut;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db;
use crate::db::destinations::{DestinationsDbError, destination_exists};
use crate::db::destinations_pipelines::DestinationPipelinesDbError;
use crate::db::images::ImagesDbError;
use crate::db::pipelines::PipelineConfig;
use crate::db::sources::{SourcesDbError, source_exists};
use crate::encryption::EncryptionKey;

use super::{ErrorMessage, TenantIdError, destinations::DestinationError, extract_tenant_id};

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

    #[error(transparent)]
    Destination(#[from] DestinationError),

    #[error("A pipeline already exists for this source and destination combination")]
    DuplicatePipeline,

    #[error(transparent)]
    DestinationPipelinesDb(DestinationPipelinesDbError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<DestinationPipelinesDbError> for DestinationPipelineError {
    fn from(e: DestinationPipelinesDbError) -> Self {
        match e {
            DestinationPipelinesDbError::Database(e)
                if db::pipelines::is_duplicate_pipeline_error(&e) =>
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
            // Do not expose internal database details in error messages
            DestinationPipelineError::DestinationPipelinesDb(
                DestinationPipelinesDbError::Database(_),
            )
            | DestinationPipelineError::DestinationsDb(DestinationsDbError::Database(_))
            | DestinationPipelineError::ImagesDb(ImagesDbError::Database(_))
            | DestinationPipelineError::SourcesDb(SourcesDbError::Database(_))
            | DestinationPipelineError::Database(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for DestinationPipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            DestinationPipelineError::Destination(e) => e.status_code(),
            DestinationPipelineError::NoDefaultImageFound
            | DestinationPipelineError::DestinationPipelinesDb(_)
            | DestinationPipelineError::DestinationsDb(_)
            | DestinationPipelineError::ImagesDb(_)
            | DestinationPipelineError::SourcesDb(_)
            | DestinationPipelineError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DestinationPipelineError::TenantId(_)
            | DestinationPipelineError::SourceNotFound(_)
            | DestinationPipelineError::DestinationNotFound(_)
            | DestinationPipelineError::PipelineNotFound(_) => StatusCode::BAD_REQUEST,
            DestinationPipelineError::DuplicatePipeline => StatusCode::CONFLICT,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage {
            error: self.to_message(),
        };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(body)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateDestinationPipelineRequest {
    #[schema(example = "My New Destination", required = true)]
    pub destination_name: String,
    #[schema(required = true)]
    pub destination_config: DestinationConfig,
    #[schema(required = true, example = 1)]
    pub source_id: i64,
    #[schema(required = true)]
    pub pipeline_config: PipelineConfig,
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
    pub destination_name: String,
    #[schema(required = true)]
    pub destination_config: DestinationConfig,
    #[schema(required = true, example = 1)]
    pub source_id: i64,
    #[schema(required = true)]
    pub pipeline_config: PipelineConfig,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreateDestinationPipelineRequest,
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Create a new destination and a pipeline", body = CreateDestinationPipelineResponse),
        (status = 409, description = "A pipeline already exists for this source and destination combination", body = ErrorMessage),
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
) -> Result<impl Responder, DestinationPipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_and_pipeline = destination_and_pipeline.into_inner();

    let mut txn = pool.begin().await?;
    if !source_exists(
        txn.deref_mut(),
        tenant_id,
        destination_and_pipeline.source_id,
    )
    .await?
    {
        return Err(DestinationPipelineError::SourceNotFound(
            destination_and_pipeline.source_id,
        ));
    }

    let image = db::images::read_default_image(&**pool)
        .await?
        .ok_or(DestinationPipelineError::NoDefaultImageFound)?;
    let (destination_id, pipeline_id) =
        db::destinations_pipelines::create_destination_and_pipeline(
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

    let response = CreateDestinationPipelineResponse {
        destination_id,
        pipeline_id,
    };

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdateDestinationPipelineRequest,
    params(
        ("destination_id" = i64, Path, description = "ID of the destination to update"),
        ("pipeline_id" = i64, Path, description = "ID of the pipeline to update"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Update a destination and a pipeline"),
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

    let mut txn = pool.begin().await?;
    if !source_exists(
        txn.deref_mut(),
        tenant_id,
        destination_and_pipeline.source_id,
    )
    .await?
    {
        return Err(DestinationPipelineError::SourceNotFound(
            destination_and_pipeline.source_id,
        ));
    }

    if !destination_exists(txn.deref_mut(), tenant_id, destination_id).await? {
        return Err(DestinationPipelineError::DestinationNotFound(
            destination_id,
        ));
    }

    db::destinations_pipelines::update_destination_and_pipeline(
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
