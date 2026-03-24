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

use crate::configs::source::FullApiSourceConfig;
use crate::db::tenants_sources::TenantSourceDbError;
use crate::k8s::TrustedRootCertsCache;
use crate::routes::ErrorMessage;
use crate::validation::ValidationError;
use crate::{
    config::ApiConfig, configs::encryption::EncryptionKey, configs::source::StoredSourceConfig,
    db::tenants::TenantsDbError,
};
use crate::{db, routes::common, routes::utils};

#[derive(Debug, Error)]
enum TenantSourceError {
    #[error(transparent)]
    TenantSourceDb(#[from] TenantSourceDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Validation(#[from] ValidationError),

    #[error("{0}")]
    ValidationFailed(String),
}

impl TenantSourceError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantSourceError::TenantSourceDb(TenantSourceDbError::Database(_))
            | TenantSourceError::TenantSourceDb(TenantSourceDbError::Sources(_))
            | TenantSourceError::TenantSourceDb(TenantSourceDbError::Tenants(_))
            | TenantSourceError::Database(_)
            | TenantSourceError::Validation(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantSourceError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantSourceError::TenantSourceDb(TenantSourceDbError::Tenants(
                TenantsDbError::Conflict(_),
            )) => StatusCode::CONFLICT,
            TenantSourceError::TenantSourceDb(_)
            | TenantSourceError::Database(_)
            | TenantSourceError::Validation(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantSourceError::ValidationFailed(_) => StatusCode::FORBIDDEN,
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

async fn validate_source_config(
    source_config: StoredSourceConfig,
    api_config: &ApiConfig,
    trusted_root_certs_cache: &TrustedRootCertsCache,
) -> Result<(), TenantSourceError> {
    let failures =
        common::validate_source_config(source_config, api_config, trusted_root_certs_cache).await?;

    if !failures.is_empty() {
        return Err(TenantSourceError::ValidationFailed(
            utils::format_validation_failures(failures),
        ));
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantSourceRequest {
    #[schema(example = "abczjjlmfsijwrlnwatw", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub tenant_id: String,
    #[schema(example = "My Tenant", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub tenant_name: String,
    #[schema(example = "My Postgres Source", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub source_name: String,
    #[schema(required = true)]
    pub source_config: FullApiSourceConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantSourceResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = 1)]
    pub source_id: i64,
}

#[utoipa::path(
    summary = "Create tenant and source",
    description = "Creates a new tenant and source within a single transaction.",
    request_body = CreateTenantSourceRequest,
    responses(
        (status = 200, description = "Tenant and source created successfully", body = CreateTenantSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 403, description = "Source profile validation failed", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants & Sources"
)]
#[post("/tenants-sources")]
pub async fn create_tenant_and_source(
    pool: Data<PgPool>,
    api_config: Data<ApiConfig>,
    trusted_root_certs_cache: Data<TrustedRootCertsCache>,
    tenant_and_source: Json<CreateTenantSourceRequest>,
    encryption_key: Data<EncryptionKey>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantSourceError> {
    let tenant_and_source = tenant_and_source.into_inner();

    root_span.record("project", &tenant_and_source.tenant_id);

    validate_source_config(
        tenant_and_source.source_config.clone().into(),
        api_config.as_ref(),
        trusted_root_certs_cache.as_ref(),
    )
    .await?;

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
