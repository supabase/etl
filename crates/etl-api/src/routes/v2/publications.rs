use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;

use super::SourceInspectionError;
use crate::{
    configs::encryption::EncryptionKeyring,
    data::v2::publications::{self, PublicationConfig, PublicationDetails, PublicationSummary},
    k8s::SourceTlsConfig,
    routes::{ErrorMessage, v2::connect_source_database},
};

/// Response containing source publication names.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadPublicationsResponse {
    /// Publications ordered by name.
    pub publications: Vec<PublicationSummary>,
}

/// Lists publications in a source database.
#[utoipa::path(
    get,
    path = "/sources/{source_id}/publications",
    summary = "List source publications",
    description = "Returns publication names from the specified source database.",
    tag = "V2 Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Publications listed successfully", body = ReadPublicationsResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_publications(
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
    let publications = publications::read_publications(&source_pool).await?;

    Ok(Json(ReadPublicationsResponse { publications }))
}

/// Reads a complete publication configuration and its current tables.
#[utoipa::path(
    get,
    path = "/sources/{source_id}/publications/{publication_name}",
    summary = "Read a source publication",
    description = "Returns the publication configuration and the tables it currently exposes.",
    tag = "V2 Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name within the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Publication retrieved successfully", body = PublicationDetails),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Source or publication not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_publication(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path((source_id, publication_name)): Path<(i64, String)>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    let publication = publications::read_publication(&source_pool, &publication_name)
        .await?
        .ok_or(SourceInspectionError::PublicationNotFound(publication_name))?;

    Ok(Json(publication))
}

/// Creates or replaces a named publication from a complete configuration.
#[utoipa::path(
    put,
    path = "/sources/{source_id}/publications/{publication_name}",
    summary = "Put a source publication",
    description = "Creates the named publication or replaces the table configuration of an existing explicit-table publication. Existing open-ended publications cannot be updated, and publish_via_partition_root cannot be changed after creation.",
    tag = "V2 Publications",
    request_body = PublicationConfig,
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name to create"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 201, description = "Publication created successfully", body = PublicationDetails),
        (status = 200, description = "Publication updated successfully", body = PublicationDetails),
        (status = 400, description = "Invalid publication configuration", body = ErrorMessage),
        (status = 403, description = "The source database user cannot create or update the publication", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 409, description = "The existing publication cannot be updated as requested", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn put_publication(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path((source_id, publication_name)): Path<(i64, String)>,
    Json(config): Json<PublicationConfig>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    let result = publications::put_publication(&source_pool, &publication_name, &config)
        .await
        .map_err(SourceInspectionError::publication_mutation)?;
    let status = if result.created { StatusCode::CREATED } else { StatusCode::OK };

    Ok((status, Json(result.publication)))
}

/// Deletes a named publication if it exists.
#[utoipa::path(
    delete,
    path = "/sources/{source_id}/publications/{publication_name}",
    summary = "Delete a source publication",
    description = "Deletes the named publication when it exists.",
    tag = "V2 Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name to delete"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 204, description = "Publication absent after the request"),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 403, description = "The source database user cannot delete the publication", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Your source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Your source database is unavailable", body = ErrorMessage),
        (status = 504, description = "Request to your source database timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn delete_publication(
    headers: HeaderMap,
    Extension(api_pool): Extension<PgPool>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(source_tls_config): Extension<Arc<SourceTlsConfig>>,
    Path((source_id, publication_name)): Path<(i64, String)>,
) -> Result<impl IntoResponse, SourceInspectionError> {
    let source_pool = connect_source_database(
        &headers,
        &api_pool,
        &encryption_key,
        &source_tls_config,
        source_id,
    )
    .await?;
    publications::drop_publication(&source_pool, &publication_name)
        .await
        .map_err(SourceInspectionError::publication_mutation)?;

    Ok(StatusCode::NO_CONTENT)
}
