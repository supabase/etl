use std::sync::Arc;

use axum::{Extension, Json, extract::Path, http::HeaderMap, response::IntoResponse};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;

use super::SourceInspectionError;
use crate::{
    configs::encryption::EncryptionKeyring,
    data::v2::schemas::{self, SourceSchema},
    k8s::SourceTlsConfig,
    routes::{ErrorMessage, v2::connect_source_database},
};

/// Response containing source schemas.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadSchemasResponse {
    /// Schemas ordered by name.
    pub schemas: Vec<SourceSchema>,
}

/// Lists source schemas available for publication selection.
#[utoipa::path(
    get,
    path = "/sources/{source_id}/schemas",
    summary = "List source schemas",
    description = "Returns source schemas that can be used for publication selection, including empty schemas.",
    tag = "V2 Source Inspection",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Schemas listed successfully", body = ReadSchemasResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 401, description = "Authentication required", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_schemas(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path(source_id): Path<i64>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    let schemas = schemas::get_schemas(&source_pool).await?;

    Ok(Json(ReadSchemasResponse { schemas }))
}
