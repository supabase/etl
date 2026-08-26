use std::sync::Arc;

use axum::{Extension, Json, extract::Path, http::HeaderMap, response::IntoResponse};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;

use super::SourceInspectionError;
use crate::{
    configs::encryption::EncryptionKeyring,
    data::v2::columns::{self, SourceColumn},
    k8s::SourceTlsConfig,
    routes::{ErrorMessage, v2::connect_source_database},
};

/// Response containing source-table columns.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadColumnsResponse {
    /// Columns ordered by ordinal position.
    pub columns: Vec<SourceColumn>,
}

/// Lists the current columns for a source table.
#[utoipa::path(
    get,
    path = "/sources/{source_id}/tables/{table_id}/columns",
    summary = "List source table columns",
    description = "Returns live column metadata for the specified source table.",
    tag = "V2 Source Inspection",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("table_id" = u32, Path, description = "Postgres OID of the source table"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Columns listed successfully", body = ReadColumnsResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 401, description = "Authentication required", body = ErrorMessage),
        (status = 404, description = "Source or table not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_columns(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path((source_id, table_id)): Path<(i64, u32)>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    let columns = columns::get_table_columns(&source_pool, table_id)
        .await?
        .ok_or(SourceInspectionError::TableNotFound(table_id))?;

    Ok(Json(ReadColumnsResponse { columns }))
}
