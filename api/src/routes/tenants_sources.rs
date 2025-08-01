use actix_web::{
    HttpResponse, Responder, ResponseError,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing_actix_web::RootSpan;
use utoipa::ToSchema;

use crate::db;
use crate::db::sources::SourceConfig;
use crate::db::tenants_sources::TenantSourceDbError;
use crate::encryption::EncryptionKey;
use crate::routes::ErrorMessage;

#[derive(Debug, Error)]
enum TenantSourceError {
    #[error(transparent)]
    TenantSourceDb(#[from] TenantSourceDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl TenantSourceError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantSourceError::TenantSourceDb(TenantSourceDbError::Database(_))
            | TenantSourceError::Database(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantSourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantSourceError::TenantSourceDb(_) | TenantSourceError::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
pub struct CreateTenantSourceRequest {
    #[schema(example = "abczjjlmfsijwrlnwatw", required = true)]
    pub tenant_id: String,
    #[schema(example = "My Tenant", required = true)]
    pub tenant_name: String,
    #[schema(example = "My Postgres Source", required = true)]
    pub source_name: String,
    #[schema(required = true)]
    pub source_config: SourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantSourceResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = 1)]
    pub source_id: i64,
}

#[utoipa::path(
    request_body = CreateTenantSourceRequest,
    responses(
        (status = 200, description = "Create a new tenant and a source", body = CreateTenantSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants & Sources"
)]
#[post("/tenants-sources")]
pub async fn create_tenant_and_source(
    pool: Data<PgPool>,
    tenant_and_source: Json<CreateTenantSourceRequest>,
    encryption_key: Data<EncryptionKey>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantSourceError> {
    let tenant_and_source = tenant_and_source.into_inner();

    root_span.record("project", &tenant_and_source.tenant_id);

    let mut txn = pool.begin().await?;
    let (tenant_id, source_id) = db::tenants_sources::create_tenant_and_source(
        &mut txn,
        &tenant_and_source.tenant_id,
        &tenant_and_source.tenant_name,
        &tenant_and_source.source_name,
        tenant_and_source.source_config,
        &encryption_key,
    )
    .await?;
    txn.commit().await?;

    let response = CreateTenantSourceResponse {
        tenant_id,
        source_id,
    };

    Ok(Json(response))
}
