use actix_web::HttpRequest;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::config::ApiConfig;
use crate::configs::source::StoredSourceConfig;
use crate::k8s::TrustedRootCertsCache;
use crate::validation::{self, ValidationContext, ValidationError, ValidationFailure};

pub mod destinations;
pub mod destinations_pipelines;
pub mod health_check;
pub mod images;
pub mod metrics;
pub mod pipelines;
pub mod sources;
pub mod tenants;
pub mod tenants_sources;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorMessage {
    #[schema(example = "an error occurred in the api")]
    pub error: String,
}

#[derive(Debug, Error)]
pub enum TenantIdError {
    #[error("The tenant id is missing in the request")]
    TenantIdMissing,

    #[error("The tenant id in the request is invalid")]
    TenantIdIllFormed,
}

fn extract_tenant_id(req: &HttpRequest) -> Result<&str, TenantIdError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(TenantIdError::TenantIdMissing)?
        .to_str()
        .map_err(|_| TenantIdError::TenantIdIllFormed)?;

    Ok(tenant_id)
}

/// Validates a source config against the trusted source profile, when enabled.
async fn validate_source_config(
    source_config: StoredSourceConfig,
    api_config: &ApiConfig,
    trusted_root_certs_cache: &TrustedRootCertsCache,
) -> Result<Option<ValidationFailure>, ValidationError> {
    if api_config.source.trusted_username.is_none() {
        return Ok(None);
    }

    let ctx =
        ValidationContext::build_from_source(source_config, api_config, trusted_root_certs_cache)
            .await?;
    let failures = validation::validate_source(&ctx).await?;

    Ok(failures.into_iter().next())
}
