use crate::db;
use crate::db::sources::{SourceConfig, SourcesDbError};
use crate::encryption::EncryptionKey;
use crate::routes::{extract_tenant_id, ErrorMessage, TenantIdError};
use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

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
}

impl SourceError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            SourceError::SourcesDb(SourcesDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for SourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            SourceError::SourcesDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SourceError::SourceNotFound(_) => StatusCode::NOT_FOUND,
            SourceError::TenantId(_) => StatusCode::BAD_REQUEST,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StrippedSourceConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
}

impl From<SourceConfig> for StrippedSourceConfig {
    fn from(source: SourceConfig) -> Self {
        Self {
            host: source.host,
            port: source.port,
            name: source.name,
            username: source.username,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceRequest {
    #[schema(example = "My Postgres Source", required = true)]
    pub name: String,
    #[schema(required = true)]
    pub config: SourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateSourceRequest {
    #[schema(example = "My Updated Postgres Source", required = true)]
    pub name: String,
    #[schema(required = true)]
    pub config: SourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadSourceResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = "My Postgres Source")]
    pub name: String,
    pub config: StrippedSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadSourcesResponse {
    pub sources: Vec<ReadSourceResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreateSourceRequest,
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Create new source", body = CreateSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
#[post("/sources")]
pub async fn create_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source: Json<CreateSourceRequest>,
) -> Result<impl Responder, SourceError> {
    let source = source.into_inner();
    let tenant_id = extract_tenant_id(&req)?;
    let name = source.name;
    let config = source.config;
    let id = db::sources::create_source(&pool, tenant_id, &name, config, &encryption_key).await?;
    let response = CreateSourceResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Return source with id = source_id", body = ReadSourceResponse),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
#[get("/sources/{source_id}")]
pub async fn read_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    let response = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
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
    context_path = "/v1",
    request_body = UpdateSourceRequest,
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Update source with id = source_id"),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
#[post("/sources/{source_id}")]
pub async fn update_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
    encryption_key: Data<EncryptionKey>,
    source: Json<UpdateSourceRequest>,
) -> Result<impl Responder, SourceError> {
    let source = source.into_inner();
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    let name = source.name;
    let config = source.config;
    db::sources::update_source(&pool, tenant_id, &name, source_id, config, &encryption_key)
        .await?
        .ok_or(SourceError::SourceNotFound(source_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Delete source with id = source_id"),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
#[delete("/sources/{source_id}")]
pub async fn delete_source(
    req: HttpRequest,
    pool: Data<PgPool>,
    source_id: Path<i64>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();
    db::sources::delete_source(&pool, tenant_id, source_id)
        .await?
        .ok_or(SourceError::SourceNotFound(source_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Return all sources", body = ReadSourcesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Sources"
)]
#[get("/sources")]
pub async fn read_all_sources(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, SourceError> {
    let tenant_id = extract_tenant_id(&req)?;
    let mut sources = vec![];
    for source in db::sources::read_all_sources(&pool, tenant_id, &encryption_key).await? {
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
