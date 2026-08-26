//! V2 HTTP API routes.

use std::sync::Arc;

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use sqlx::{PgPool, error::DatabaseError};
use thiserror::Error;

use crate::{
    configs::encryption::EncryptionKeyring,
    data::{
        self, source_database,
        sources::SourcesDbError,
        tables::{TablesDbError, TablesDbError::Database as TablesDatabase},
        v2::{
            columns::{ColumnsDbError, ColumnsDbError::Database as ColumnsDatabase},
            publications::{
                PublicationsV2DbError, PublicationsV2DbError::Database as PublicationsDatabase,
            },
            schemas::{SchemasDbError, SchemasDbError::Database as SchemasDatabase},
        },
    },
    k8s::SourceTlsConfig,
    routes::{TenantIdError, error_response_with_internal_error, extract_tenant_id, utils},
};

pub mod columns;
pub mod publications;
pub mod schemas;
pub mod tables;

/// An error returned by V2 source-inspection routes.
#[derive(Debug, Error)]
pub(crate) enum SourceInspectionError {
    /// The tenant-scoped source does not exist.
    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    /// The requested publication does not exist.
    #[error("The publication `{0}` was not found")]
    PublicationNotFound(String),

    /// The requested replication-eligible table does not exist.
    #[error("The table with id {0} was not found")]
    TableNotFound(u32),

    /// The tenant ID header is invalid.
    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    /// Reading the source configuration failed.
    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    /// Interacting with source publications failed.
    #[error(transparent)]
    PublicationsDb(#[from] PublicationsV2DbError),

    /// Applying a publication mutation in the source database failed.
    #[error("Could not apply the publication mutation")]
    PublicationMutationDatabase(#[source] sqlx::Error),

    /// Reading source schemas failed.
    #[error(transparent)]
    SchemasDb(#[from] SchemasDbError),

    /// Reading source tables failed.
    #[error(transparent)]
    TablesDb(#[from] TablesDbError),

    /// Reading source columns failed.
    #[error(transparent)]
    ColumnsDb(#[from] ColumnsDbError),

    /// Connecting to the source database failed.
    #[error("Database connection error")]
    Database(#[from] sqlx::Error),
}

impl SourceInspectionError {
    /// Preserves publication input errors while marking database write errors.
    pub(super) fn publication_mutation(error: PublicationsV2DbError) -> Self {
        match error {
            PublicationsV2DbError::Database(error) => {
                SourceInspectionError::PublicationMutationDatabase(error)
            }
            error => SourceInspectionError::PublicationsDb(error),
        }
    }

    /// Returns the source database error, when present.
    fn source_database_error(&self) -> Option<&sqlx::Error> {
        match self {
            SourceInspectionError::PublicationsDb(PublicationsDatabase(error))
            | SourceInspectionError::SchemasDb(SchemasDatabase(error))
            | SourceInspectionError::TablesDb(TablesDatabase(error))
            | SourceInspectionError::ColumnsDb(ColumnsDatabase(error))
            | SourceInspectionError::PublicationMutationDatabase(error)
            | SourceInspectionError::Database(error) => Some(error),
            _ => None,
        }
    }

    /// Returns the HTTP status code for the error.
    fn status_code(&self) -> StatusCode {
        if let SourceInspectionError::PublicationMutationDatabase(sqlx::Error::Database(error)) =
            self
            && let Some((status_code, _)) = publication_request_error(error.as_ref())
        {
            return status_code;
        }

        if let Some(error) = self.source_database_error() {
            return utils::source_database_error_status_code(error);
        }

        match self {
            SourceInspectionError::SourceNotFound(_)
            | SourceInspectionError::PublicationNotFound(_)
            | SourceInspectionError::TableNotFound(_) => StatusCode::NOT_FOUND,
            SourceInspectionError::TenantId(_)
            | SourceInspectionError::PublicationsDb(
                PublicationsV2DbError::InvalidTableReference { .. }
                | PublicationsV2DbError::InvalidRowFilter { .. },
            ) => StatusCode::BAD_REQUEST,
            SourceInspectionError::PublicationsDb(
                PublicationsV2DbError::OpenEndedPublicationCannotBeUpdated
                | PublicationsV2DbError::ExistingPublicationCannotBecomeOpenEnded
                | PublicationsV2DbError::PublishViaPartitionRootCannotBeUpdated,
            ) => StatusCode::CONFLICT,
            SourceInspectionError::PublicationsDb(
                PublicationsV2DbError::UnsupportedGeneratedColumnsMode,
            ) => StatusCode::BAD_GATEWAY,
            SourceInspectionError::SourcesDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SourceInspectionError::PublicationsDb(_)
            | SourceInspectionError::PublicationMutationDatabase(_)
            | SourceInspectionError::SchemasDb(_)
            | SourceInspectionError::TablesDb(_)
            | SourceInspectionError::ColumnsDb(_)
            | SourceInspectionError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Returns a customer-facing error message.
    fn to_message(&self) -> String {
        if let SourceInspectionError::PublicationMutationDatabase(sqlx::Error::Database(error)) =
            self
            && let Some((_, message)) = publication_request_error(error.as_ref())
        {
            return message.to_owned();
        }

        if self.source_database_error().is_some() {
            return utils::source_database_query_error_message().to_owned();
        }

        match self {
            SourceInspectionError::SourcesDb(_)
            | SourceInspectionError::PublicationsDb(
                PublicationsV2DbError::CreatedPublicationNotFound,
            ) => "Internal server error".to_owned(),
            SourceInspectionError::PublicationsDb(
                PublicationsV2DbError::UnsupportedGeneratedColumnsMode,
            ) => utils::source_database_query_error_message().to_owned(),
            error => error.to_string(),
        }
    }
}

impl IntoResponse for SourceInspectionError {
    fn into_response(self) -> Response {
        error_response_with_internal_error(self.status_code(), self.to_message(), &self)
    }
}

/// Connects a V2 source route to its tenant-scoped source database.
pub(super) async fn connect_source_database(
    headers: &HeaderMap,
    api_pool: &PgPool,
    encryption_key: &Arc<EncryptionKeyring>,
    source_tls_config: &Arc<SourceTlsConfig>,
    source_id: i64,
) -> Result<PgPool, SourceInspectionError> {
    let tenant_id = extract_tenant_id(headers)?;
    let source_config = data::sources::read_source(api_pool, tenant_id, source_id, encryption_key)
        .await?
        .map(|source| source.config)
        .ok_or(SourceInspectionError::SourceNotFound(source_id))?;

    let tls_config = source_tls_config.get_tls_config();
    Ok(source_database::connect(&source_config.into_connection_config(tls_config)).await?)
}

/// Maps request-contingent PostgreSQL errors to public REST responses.
fn publication_request_error(error: &dyn DatabaseError) -> Option<(StatusCode, &'static str)> {
    match error.code().as_deref() {
        Some("42710") => Some((StatusCode::CONFLICT, "The publication already exists")),
        Some("42501") => Some((
            StatusCode::FORBIDDEN,
            "The source database user cannot apply this publication configuration",
        )),
        Some("0A000" | "3F000") => Some((
            StatusCode::BAD_REQUEST,
            "The publication configuration is not supported by the source database",
        )),
        Some(code) if code.starts_with("22") || code.starts_with("42") => Some((
            StatusCode::BAD_REQUEST,
            "The publication configuration is not supported by the source database",
        )),
        _ => None,
    }
}
