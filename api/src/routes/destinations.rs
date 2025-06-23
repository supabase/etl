use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use config::shared::DestinationConfig;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db;
use crate::db::destinations::DestinationsDbError;
use crate::encryption::EncryptionKey;
use crate::routes::{extract_tenant_id, ErrorMessage, TenantIdError};

#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),
}

impl DestinationError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            DestinationError::DestinationsDb(DestinationsDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for DestinationError {
    fn status_code(&self) -> StatusCode {
        match self {
            DestinationError::DestinationsDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DestinationError::DestinationNotFound(_) => StatusCode::NOT_FOUND,
            DestinationError::TenantId(_) => StatusCode::BAD_REQUEST,
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

#[derive(Deserialize, ToSchema)]
pub struct PostDestinationRequest {
    pub name: String,
    #[schema(required = true)]
    pub config: DestinationConfig,
}

#[derive(Serialize, ToSchema)]
pub struct PostDestinationResponse {
    id: i64,
}

#[derive(Serialize, ToSchema)]
pub struct GetDestinationResponse {
    #[schema(example = 1)]
    id: i64,
    #[schema(example = 1)]
    tenant_id: String,
    #[schema(example = "BigQuery Destination")]
    name: String,
    config: DestinationConfig,
}

#[derive(Serialize, ToSchema)]
pub struct GetDestinationsResponse {
    destinations: Vec<GetDestinationResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostDestinationRequest,
    responses(
        (status = 200, description = "Create new destination", body = PostDestinationResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/destinations")]
pub async fn create_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    destination: Json<PostDestinationRequest>,
) -> Result<impl Responder, DestinationError> {
    let destination = destination.0;
    let tenant_id = extract_tenant_id(&req)?;
    let name = destination.name;
    let config = destination.config;
    let id = db::destinations::create_destination(&pool, tenant_id, &name, config, &encryption_key)
        .await?;
    let response = PostDestinationResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("destination_id" = i64, Path, description = "Id of the destination"),
    ),
    responses(
        (status = 200, description = "Return destination with id = destination_id", body = GetSourceResponse),
        (status = 404, description = "Destination not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/destinations/{destination_id}")]
pub async fn read_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    destination_id: Path<i64>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_id = destination_id.into_inner();
    let response =
        db::destinations::read_destination(&pool, tenant_id, destination_id, &encryption_key)
            .await?
            .map(|s| GetDestinationResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                name: s.name,
                config: s.config,
            })
            .ok_or(DestinationError::DestinationNotFound(destination_id))?;
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostDestinationRequest,
    params(
        ("destination_id" = i64, Path, description = "Id of the destination"),
    ),
    responses(
        (status = 200, description = "Update destination with id = destination_id"),
        (status = 404, description = "Destination not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/destinations/{destination_id}")]
pub async fn update_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_id: Path<i64>,
    encryption_key: Data<EncryptionKey>,
    destination: Json<PostDestinationRequest>,
) -> Result<impl Responder, DestinationError> {
    let destination = destination.0;
    let tenant_id = extract_tenant_id(&req)?;
    let destination_id = destination_id.into_inner();
    let name = destination.name;
    let config = destination.config;
    db::destinations::update_destination(
        &pool,
        tenant_id,
        &name,
        destination_id,
        config,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationError::DestinationNotFound(destination_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("destination_id" = i64, Path, description = "Id of the destination"),
    ),
    responses(
        (status = 200, description = "Delete destination with id = destination_id"),
        (status = 404, description = "Destination not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/destinations/{destination_id}")]
pub async fn delete_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_id: Path<i64>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_id = destination_id.into_inner();
    db::destinations::delete_destination(&pool, tenant_id, destination_id)
        .await?
        .ok_or(DestinationError::DestinationNotFound(destination_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all destinations"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/destinations")]
pub async fn read_all_destinations(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut destinations = vec![];
    for destination in
        db::destinations::read_all_destinations(&pool, tenant_id, &encryption_key).await?
    {
        let destination = GetDestinationResponse {
            id: destination.id,
            tenant_id: destination.tenant_id,
            name: destination.name,
            config: destination.config,
        };
        destinations.push(destination);
    }
    let response = GetDestinationsResponse { destinations };
    Ok(Json(response))
}
