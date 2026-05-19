use std::sync::Arc;

use axum::{
    Extension, Json,
    http::StatusCode,
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
        source::{FullApiSourceConfig, StoredSourceConfig},
    },
    data::{self, tenants::TenantsDbError, tenants_sources::TenantSourceDbError},
    k8s::TrustedRootCertsCache,
    routes::{ErrorMessage, IntoInner, common, error_response, utils},
    validation::ValidationError,
};

#[derive(Debug, Error)]
pub(crate) enum TenantSourceError {
    #[error(transparent)]
    TenantSourceDb(#[from] TenantSourceDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Validation(#[from] ValidationError),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),
}

impl TenantSourceError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantSourceError::TenantSourceDb(
                TenantSourceDbError::Database(_)
                | TenantSourceDbError::Sources(_)
                | TenantSourceDbError::Tenants(_),
            )
            | TenantSourceError::Database(_) => "Internal server error".to_owned(),
            TenantSourceError::Validation(error) => {
                utils::validation_error_message(error).to_owned()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl IntoResponse for TenantSourceError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            TenantSourceError::TenantSourceDb(TenantSourceDbError::Tenants(
                TenantsDbError::Conflict(_),
            )) => StatusCode::CONFLICT,
            TenantSourceError::TenantSourceDb(_) | TenantSourceError::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            TenantSourceError::Validation(error) => utils::validation_error_status_code(error),
            TenantSourceError::ValidationFailed(_) => StatusCode::UNPROCESSABLE_ENTITY,
        };

        error_response(status_code, self.to_message())
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
        return Err(TenantSourceError::ValidationFailed(utils::format_validation_failures(
            failures,
        )));
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
    post,
    path = "/tenants-sources",
    summary = "Create tenant and source",
    description = "Creates a new tenant and source within a single transaction.",
    request_body = CreateTenantSourceRequest,
    responses(
        (status = 200, description = "Tenant and source created successfully", body = CreateTenantSourceResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 422, description = "Source profile validation failed", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants & Sources"
)]
pub(crate) async fn create_tenant_and_source(
    Extension(pool): Extension<PgPool>,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    Extension(encryption_key): Extension<Arc<EncryptionKey>>,
    tenant_and_source: Json<CreateTenantSourceRequest>,
) -> Result<impl IntoResponse, TenantSourceError> {
    let tenant_and_source = tenant_and_source.into_inner();

    tracing::Span::current().record("project", &tenant_and_source.tenant_id);

    validate_source_config(
        tenant_and_source.source_config.clone().into(),
        api_config.as_ref(),
        trusted_root_certs_cache.as_ref(),
    )
    .await?;

    let mut txn = pool.begin().await?;
    let (tenant_id, source_id) = data::tenants_sources::create_tenant_and_source(
        &mut txn,
        &tenant_and_source.tenant_id,
        &tenant_and_source.tenant_name,
        &tenant_and_source.source_name,
        tenant_and_source.source_config,
        &encryption_key,
    )
    .await?;
    txn.commit().await?;

    let response = CreateTenantSourceResponse { tenant_id, source_id };

    Ok(Json(response))
}
