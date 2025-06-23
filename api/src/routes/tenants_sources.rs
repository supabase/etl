use actix_web::{
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json},
    HttpResponse, Responder, ResponseError,
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

#[derive(Deserialize, ToSchema)]
pub struct CreateTenantSourceRequest {
    #[schema(example = "abcdefghijklmnopqrst", required = true)]
    tenant_id: String,

    #[schema(example = "Tenant Name", required = true)]
    tenant_name: String,

    #[schema(example = "Source Name", required = true)]
    source_name: String,

    #[schema(required = true)]
    source_config: SourceConfig,
}

#[derive(Debug, Error)]
enum TenantSourceError {
    #[error(transparent)]
    TenantSourceDb(#[from] TenantSourceDbError),
}

impl TenantSourceError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantSourceError::TenantSourceDb(TenantSourceDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantSourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantSourceError::TenantSourceDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
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

#[derive(Serialize, ToSchema)]
pub struct PostTenantSourceResponse {
    tenant_id: String,
    source_id: i64,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreateTenantSourceRequest,
    responses(
        (status = 200, description = "Create a new tenant and a source", body = PostTenantSourceResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/tenants-sources")]
pub async fn create_tenant_and_source(
    pool: Data<PgPool>,
    tenant_and_source: Json<CreateTenantSourceRequest>,
    encryption_key: Data<EncryptionKey>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantSourceError> {
    let tenant_and_source = tenant_and_source.0;
    let CreateTenantSourceRequest {
        tenant_id,
        tenant_name,
        source_name,
        source_config,
    } = tenant_and_source;
    root_span.record("project", &tenant_id);
    let (tenant_id, source_id) = db::tenants_sources::create_tenant_and_source(
        &pool,
        &tenant_id,
        &tenant_name,
        &source_name,
        source_config,
        &encryption_key,
    )
    .await?;
    let response = PostTenantSourceResponse {
        tenant_id,
        source_id,
    };
    Ok(Json(response))
}
