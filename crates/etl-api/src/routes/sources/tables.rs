use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    config::ApiConfig,
    configs::encryption::EncryptionKeyring,
    data::{
        self, connect_to_source_database_from_api,
        sources::SourcesDbError,
        tables::{Table, TablesDbError},
    },
    k8s::{TrustedRootCertsCache, TrustedRootCertsError},
    routes::{
        ErrorMessage, IntoInner, TenantIdError, error_response_with_internal_error,
        extract_tenant_id, utils,
    },
};

#[derive(Debug, Error)]
pub(crate) enum TableError {
    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    TablesDb(#[from] TablesDbError),

    #[error("Database connection error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    TrustedRootCerts(#[from] TrustedRootCertsError),
}

impl TableError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TableError::SourcesDb(_) | TableError::TrustedRootCerts(_) => {
                "Internal server error".to_owned()
            }
            TableError::TablesDb(TablesDbError::Database(_)) | TableError::Database(_) => {
                "Could not query the source database".to_owned()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ReadTablesResponse {
    #[schema(required = true)]
    pub tables: Vec<Table>,
}

impl IntoResponse for TableError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            TableError::SourcesDb(_) | TableError::TrustedRootCerts(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            TableError::TablesDb(TablesDbError::Database(error)) | TableError::Database(error) => {
                utils::source_database_error_status_code(error)
            }
            TableError::SourceNotFound(_) => StatusCode::NOT_FOUND,
            TableError::TenantId(_) => StatusCode::BAD_REQUEST,
        };

        error_response_with_internal_error(status_code, self.to_message(), &self)
    }
}

#[utoipa::path(
    get,
    path = "/sources/{source_id}/tables",
    summary = "List source tables",
    description = "Returns all tables discovered for the specified source.",
    tag = "Tables",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request"),
    ),
    responses(
        (status = 200, description = "Tables listed successfully", body = ReadTablesResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 404, description = "Source not found", body = ErrorMessage),
        (status = 502, description = "Source database returned an invalid response", body = ErrorMessage),
        (status = 503, description = "Source database unavailable", body = ErrorMessage),
        (status = 504, description = "Source database request timed out", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
pub(crate) async fn read_table_names(
    headers: HeaderMap,
    Extension(pool): Extension<PgPool>,
    Extension(api_config): Extension<Arc<ApiConfig>>,
    Extension(encryption_key): Extension<Arc<EncryptionKeyring>>,
    Extension(trusted_root_certs_cache): Extension<Arc<TrustedRootCertsCache>>,
    source_id: Path<i64>,
) -> Result<impl IntoResponse, TableError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let source_id = source_id.into_inner();

    let source_config = data::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(TableError::SourceNotFound(source_id))?;

    let tls_config = trusted_root_certs_cache.get_tls_config(api_config.source.tls_enabled).await?;
    let source_pool =
        connect_to_source_database_from_api(&source_config.into_connection_config(tls_config))
            .await?;
    let tables = data::tables::get_tables(&source_pool).await?;
    let response = ReadTablesResponse { tables };

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_connection_timeout_is_reported_as_upstream_unavailable() {
        let response = TableError::Database(sqlx::Error::PoolTimedOut).into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn source_query_error_is_reported_as_bad_gateway() {
        let response = TableError::TablesDb(TablesDbError::Database(sqlx::Error::Protocol(
            "bad source response".to_owned(),
        )))
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
