use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::configs::destination::FullApiDestinationConfig;
use crate::configs::encryption::EncryptionKey;
use crate::db;
use crate::db::destinations::DestinationsDbError;
use crate::routes::{ErrorMessage, TenantIdError, extract_tenant_id};
use crate::validation::{ValidationFailure, validate_destination as run_destination_validation};

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
    pub config: FullApiDestinationConfig,
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
    #[schema(required = true)]
    pub config: FullApiDestinationConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidationFailureResponse {
    #[schema(example = "BigQuery Dataset Not Found")]
    pub name: String,
    #[schema(example = "'my_dataset' in project 'my_project'")]
    pub reason: String,
}

impl From<ValidationFailure> for ValidationFailureResponse {
    fn from(failure: ValidationFailure) -> Self {
        Self {
            name: failure.name,
            reason: failure.reason,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateDestinationResponse {
    pub validation_failures: Vec<ValidationFailureResponse>,
}

#[utoipa::path(
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
#[post("/destinations")]
pub async fn create_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    destination: Json<CreateDestinationRequest>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination = destination.into_inner();

    let id = db::destinations::create_destination(
        &**pool,
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
    summary = "Retrieve a destination",
    description = "Returns a destination identified by its ID for the given tenant.",
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination retrieved successfully", body = ReadDestinationResponse),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
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
        db::destinations::read_destination(&**pool, tenant_id, destination_id, &encryption_key)
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
    summary = "Update a destination",
    description = "Updates the destination's name and configuration.",
    request_body = UpdateDestinationRequest,
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination updated successfully"),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
#[post("/destinations/{destination_id}")]
pub async fn update_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_id: Path<i64>,
    encryption_key: Data<EncryptionKey>,
    destination: Json<UpdateDestinationRequest>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_id = destination_id.into_inner();
    let destination = destination.into_inner();

    db::destinations::update_destination(
        &**pool,
        tenant_id,
        &destination.name,
        destination_id,
        destination.config,
        &encryption_key,
    )
    .await?
    .ok_or(DestinationError::DestinationNotFound(destination_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Delete a destination",
    description = "Deletes a destination by ID for the given tenant.",
    params(
        ("destination_id" = i64, Path, description = "Unique ID of the destination"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Destination deleted successfully"),
        (status = 404, description = "Destination not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
#[delete("/destinations/{destination_id}")]
pub async fn delete_destination(
    req: HttpRequest,
    pool: Data<PgPool>,
    destination_id: Path<i64>,
) -> Result<impl Responder, DestinationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let destination_id = destination_id.into_inner();

    db::destinations::delete_destination(&**pool, tenant_id, destination_id)
        .await?
        .ok_or(DestinationError::DestinationNotFound(destination_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "List destinations",
    description = "Returns all destinations for the specified tenant.",
    responses(
        (status = 200, description = "Destinations listed successfully", body = ReadDestinationsResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    tag = "Destinations"
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
        db::destinations::read_all_destinations(&**pool, tenant_id, &encryption_key).await?
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
    summary = "Validate destination configuration",
    description = "Validates that the destination is accessible and properly configured.",
    request_body = ValidateDestinationRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Validation completed", body = ValidateDestinationResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Destinations"
)]
#[post("/destinations/validate")]
pub async fn validate_destination(
    req: HttpRequest,
    request: Json<ValidateDestinationRequest>,
) -> Result<impl Responder, DestinationError> {
    let _tenant_id = extract_tenant_id(&req)?;
    let request = request.into_inner();

    let failures = run_destination_validation(&request.config).await;
    let response = ValidateDestinationResponse {
        validation_failures: failures.into_iter().map(Into::into).collect(),
    };

    Ok(Json(response))
}
