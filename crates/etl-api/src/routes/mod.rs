use actix_web::HttpRequest;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

pub mod common;
pub mod destinations;
pub mod destinations_pipelines;
pub mod health_check;
pub mod images;
pub mod metrics;
pub mod pipelines;
pub mod sources;
pub mod tenants;
pub mod tenants_sources;
pub mod utils;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorMessage {
    #[schema(example = "an error occurred in the api")]
    pub message: String,
}

#[derive(Debug, Error)]
pub enum TenantIdError {
    #[error("The tenant id is missing from the request")]
    TenantIdMissing,

    #[error("The tenant id in the request is invalid")]
    TenantIdIllFormed,
}

pub(crate) const MAX_TENANT_ID_LEN: usize = 25;

pub(crate) fn validate_tenant_id(tenant_id: &str) -> Result<(), TenantIdError> {
    let is_valid_char =
        |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-';

    let Some(first) = tenant_id.bytes().next() else {
        return Err(TenantIdError::TenantIdIllFormed);
    };

    if tenant_id.len() > MAX_TENANT_ID_LEN
        || !(first.is_ascii_lowercase() || first.is_ascii_digit())
        || tenant_id.ends_with('-')
        || !tenant_id.bytes().all(is_valid_char)
    {
        return Err(TenantIdError::TenantIdIllFormed);
    }

    Ok(())
}

fn extract_tenant_id(req: &HttpRequest) -> Result<&str, TenantIdError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(TenantIdError::TenantIdMissing)?
        .to_str()
        .map_err(|_| TenantIdError::TenantIdIllFormed)?;

    validate_tenant_id(tenant_id)?;

    Ok(tenant_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_validation_accepts_kubernetes_safe_ids() {
        validate_tenant_id("a").unwrap();
        validate_tenant_id("abc123").unwrap();
        validate_tenant_id("etl-simulator-ducklake-0").unwrap();
        validate_tenant_id("abcdefghijklmnopqrstuvwxy").unwrap();
    }

    #[test]
    fn tenant_id_validation_rejects_kubernetes_unsafe_ids() {
        for tenant_id in [
            "",
            "abcdefghijklmnopqrstuvwxyz",
            "-tenant",
            "tenant-",
            "tenant_id",
            "Tenant",
            "tenant.id",
            "tenant/id",
        ] {
            assert_eq!(
                validate_tenant_id(tenant_id).unwrap_err().to_string(),
                TenantIdError::TenantIdIllFormed.to_string()
            );
        }
    }
}
