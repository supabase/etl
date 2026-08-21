use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query},
    http::HeaderMap,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::{IntoParams, ToSchema};

use super::SourceInspectionError;
use crate::{
    configs::encryption::EncryptionKeyring,
    data::v2::tables::{self, SourceTable},
    k8s::SourceTlsConfig,
    routes::{ErrorMessage, v2::connect_source_database},
};

/// Optional filters for source-table discovery.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ReadTablesQuery {
    /// Restricts results to this schema.
    pub schema: Option<String>,
}

/// Response containing source tables.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadTablesResponse {
    /// Tables ordered by schema and name.
    pub tables: Vec<SourceTable>,
}

/// Lists replication-eligible source tables.
#[utoipa::path(
    get,
    path = "/sources/{source_id}/tables",
    summary = "List source tables",
    description = "Returns replication-eligible tables and their direct partition hierarchy, optionally restricted to one schema.",
    tag = "V2 Source Inspection",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ReadTablesQuery,
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Tables listed successfully", body = ReadTablesResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_tables(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path(source_id): Path<i64>,
    Query(query): Query<ReadTablesQuery>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    let tables = tables::read_source_tables(&source_pool, query.schema.as_deref()).await?;

    Ok(Json(ReadTablesResponse { tables }))
}
