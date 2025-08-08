use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, get,
    http::{StatusCode, header::ContentType},
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::tables::TablesDbError;
use crate::routes::connect_to_source_database_with_defaults;
use crate::{
    db::{self, sources::SourcesDbError, tables::Table},
    encryption::EncryptionKey,
    routes::{ErrorMessage, TenantIdError, extract_tenant_id},
};

#[derive(Debug, Error)]
enum TableError {
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
}

impl TableError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TableError::SourcesDb(SourcesDbError::Database(_))
            | TableError::TablesDb(TablesDbError::Database(_)) => {
                "internal server error".to_string()
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

impl ResponseError for TableError {
    fn status_code(&self) -> StatusCode {
        match self {
            TableError::SourcesDb(_) | TableError::TablesDb(_) | TableError::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            TableError::SourceNotFound(_) => StatusCode::NOT_FOUND,
            TableError::TenantId(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage {
            error: self.to_message(),
        };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(body)
    }
}

#[utoipa::path(
    summary = "List source tables",
    description = "Returns all tables discovered for the specified source.",
    tag = "Tables",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
    ),
    responses(
        (status = 200, description = "Tables listed successfully", body = ReadTablesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[get("/sources/{source_id}/tables")]
pub async fn read_table_names(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
) -> Result<impl Responder, TableError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(TableError::SourceNotFound(source_id))?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;
    let tables = db::tables::get_tables(&source_pool).await?;
    let response = ReadTablesResponse { tables };

    Ok(Json(response))
}
