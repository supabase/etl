use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::publications::PublicationsDbError;
use crate::routes::connect_to_source_database_with_defaults;
use crate::{
    configs::encryption::EncryptionKey,
    db::{self, publications::Publication, sources::SourcesDbError, tables::Table},
    routes::{ErrorMessage, TenantIdError, extract_tenant_id},
};

#[derive(Debug, Error)]
enum PublicationError {
    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("The publication with name {0} was not found")]
    PublicationNotFound(String),

    #[error("Invalid publication request: {0}")]
    InvalidPublication(String),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PublicationsDb(#[from] PublicationsDbError),

    #[error("Database connection error: {0}")]
    Database(#[from] sqlx::Error),
}

impl PublicationError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PublicationError::SourcesDb(SourcesDbError::Database(_))
            | PublicationError::PublicationsDb(PublicationsDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Validation errors are safe to expose - they help users fix their input
            PublicationError::PublicationsDb(PublicationsDbError::UnsupportedColumnTypes(
                details,
            )) => {
                format!(
                    "Publication contains tables with unsupported column types: {}",
                    details
                )
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PublicationError {
    fn status_code(&self) -> StatusCode {
        match self {
            PublicationError::SourcesDb(_) | PublicationError::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PublicationError::PublicationsDb(PublicationsDbError::Database(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PublicationError::PublicationsDb(PublicationsDbError::UnsupportedColumnTypes(_))
            | PublicationError::InvalidPublication(_)
            | PublicationError::TenantId(_) => StatusCode::BAD_REQUEST,
            PublicationError::SourceNotFound(_) | PublicationError::PublicationNotFound(_) => {
                StatusCode::NOT_FOUND
            }
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

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CreatePublicationRequest {
    #[schema(example = "my_publication", required = true)]
    pub name: String,
    #[schema(required = true)]
    pub tables: Vec<Table>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct UpdatePublicationRequest {
    #[schema(required = true)]
    pub tables: Vec<Table>,
}

#[derive(Serialize, ToSchema)]
pub struct ReadPublicationsResponse {
    pub publications: Vec<Publication>,
}

#[utoipa::path(
    summary = "Create a publication",
    description = "Creates a publication on the given source with the specified tables.",
    tag = "Publications",
    request_body = CreatePublicationRequest,
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
    ),
    responses(
        (status = 200, description = "Publication created successfully"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[post("/sources/{source_id}/publications")]
pub async fn create_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
    publication: Json<CreatePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let publication = publication.into_inner();
    let publication = Publication {
        name: publication.name,
        tables: publication.tables,
    };

    validate_publication(&publication)?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;

    db::publications::validate_publication_column_types(&publication, &source_pool).await?;
    db::publications::create_publication(&publication, &source_pool).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Retrieve a publication",
    description = "Returns a publication identified by name within the given source.",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name within the source"),
    ),
    responses(
        (status = 200, description = "Publication retrieved successfully", body = Publication),
        (status = 404, description = "Publication not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[get("/sources/{source_id}/publications/{publication_name}")]
pub async fn read_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;
    let publications = db::publications::read_publication(&publication_name, &source_pool)
        .await?
        .ok_or(PublicationError::PublicationNotFound(publication_name))?;

    Ok(Json(publications))
}

#[utoipa::path(
    summary = "Update a publication",
    description = "Replaces the publication's table list on the given source.",
    tag = "Publications",
    request_body = UpdatePublicationRequest,
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name within the source"),
    ),
    responses(
        (status = 200, description = "Publication updated successfully"),
        (status = 404, description = "Publication not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[post("/sources/{source_id}/publications/{publication_name}")]
pub async fn update_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
    publication: Json<UpdatePublicationRequest>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let publication = publication.into_inner();
    let publication = Publication {
        name: publication_name,
        tables: publication.tables,
    };

    validate_publication(&publication)?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;

    db::publications::validate_publication_column_types(&publication, &source_pool).await?;
    db::publications::update_publication(&publication, &source_pool).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Delete a publication",
    description = "Deletes a publication by name on the given source.",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
        ("publication_name" = String, Path, description = "Publication name within the source"),
    ),
    responses(
        (status = 200, description = "Publication deleted successfully"),
        (status = 404, description = "Publication not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[delete("/sources/{source_id}/publications/{publication_name}")]
pub async fn delete_publication(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id_and_pub_name: Path<(i64, String)>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let (source_id, publication_name) = source_id_and_pub_name.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;
    db::publications::drop_publication(&publication_name, &source_pool).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "List publications",
    description = "Returns all publications defined on the given source.",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Unique ID of the source"),
    ),
    responses(
        (status = 200, description = "Publications listed successfully", body = ReadPublicationsResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    )
)]
#[get("/sources/{source_id}/publications")]
pub async fn read_all_publications(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    source_id: Path<i64>,
) -> Result<impl Responder, PublicationError> {
    let tenant_id = extract_tenant_id(&req)?;
    let source_id = source_id.into_inner();

    let config = db::sources::read_source(&**pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let source_pool =
        connect_to_source_database_with_defaults(&config.into_connection_config()).await?;
    let publications = db::publications::read_all_publications(&source_pool).await?;
    let response = ReadPublicationsResponse { publications };

    Ok(Json(response))
}

fn validate_publication(publication: &Publication) -> Result<(), PublicationError> {
    let mut errors: Vec<String> = Vec::new();

    if publication.name.trim().is_empty() {
        errors.push("name cannot be empty".to_string());
    }

    if publication.tables.is_empty() {
        errors.push("tables cannot be empty".to_string());
    } else {
        for (i, table) in publication.tables.iter().enumerate() {
            if table.schema.trim().is_empty() {
                errors.push(format!("table[{}]: schema cannot be empty", i));
            }
            if table.name.trim().is_empty() {
                errors.push(format!("table[{}]: name cannot be empty", i));
            }
        }
    }

    if !errors.is_empty() {
        return Err(PublicationError::InvalidPublication(errors.join(", ")));
    }

    Ok(())
}
