use axum::{
    Extension, Json,
    extract::Path,
    http::HeaderMap,
    response::{IntoResponse, Response},
};
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

fn extract_tenant_id(headers: &HeaderMap) -> Result<&str, TenantIdError> {
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(TenantIdError::TenantIdMissing)?
        .to_str()
        .map_err(|_| TenantIdError::TenantIdIllFormed)?;

    Ok(tenant_id)
}

/// Builds a JSON error response for route errors.
pub(crate) fn error_response(status_code: axum::http::StatusCode, message: String) -> Response {
    (status_code, Json(ErrorMessage { message })).into_response()
}

/// Returns the inner value from axum extractors used by route handlers.
pub(crate) trait IntoInner {
    /// The extractor's inner value.
    type Inner;

    /// Consumes the extractor and returns its inner value.
    fn into_inner(self) -> Self::Inner;
}

impl<T> IntoInner for Json<T> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.0
    }
}

impl<T> IntoInner for Path<T> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.0
    }
}

impl<T> IntoInner for Extension<T> {
    type Inner = T;

    fn into_inner(self) -> Self::Inner {
        self.0
    }
}
