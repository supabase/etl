use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use config::shared::IntoConnectOptions;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::publications::PublicationsDbError;
use crate::{
    db::{self, publications::Publication, sources::SourcesDbError, tables::Table},
    encryption::EncryptionKey,
    routes::{extract_tenant_id, ErrorMessage, TenantIdError},
};

#[derive(Debug, Error)]
enum PublicationError {
    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("The publication with name {0} was not found")]
    PublicationNotFound(String),

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PublicationsDb(#[from] PublicationsDbError),
}

impl PublicationError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PublicationError::SourcesDb(SourcesDbError::Database(_))
            | PublicationError::PublicationsDb(PublicationsDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PublicationError {
    fn status_code(&self) -> StatusCode {
        match self {
            PublicationError::SourcesDb(_) | PublicationError::PublicationsDb(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            PublicationError::SourceNotFound(_) | PublicationError::PublicationNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            PublicationError::TenantId(_) => StatusCode::BAD_REQUEST,
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

#[derive(Deserialize, ToSchema)]
pub struct CreatePublicationRequest {
    #[schema(example = "my_publication", required = true)]
    name: String,
    #[schema(required = true)]
    tables: Vec<Table>,
}

#[derive(Deserialize, ToSchema)]
pub struct UpdatePublicationRequest {
    #[schema(required = true)]
    tables: Vec<Table>,
}

#[derive(Serialize, ToSchema)]
pub struct ReadPublicationsResponse {
    pub publications: Vec<Publication>,
}

#[utoipa::path(
    context_path = "/v1",
    tag = "Publications",
    request_body = CreatePublicationRequest,
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
    ),
    responses(
        (status = 200, description = "Create new publication"),
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

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.into_connection_config().with_db();
    let publication = publication.0;
    let publication = Publication {
        name: publication.name,
        tables: publication.tables,
    };
    db::publications::create_publication(&publication, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = String, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Return publication with name = publication_name from source with id = source_id", body = Publication),
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

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.into_connection_config().with_db();
    let publications = db::publications::read_publication(&publication_name, &options)
        .await?
        .ok_or(PublicationError::PublicationNotFound(publication_name))?;

    Ok(Json(publications))
}

#[utoipa::path(
    context_path = "/v1",
    tag = "Publications",
    request_body = UpdatePublicationRequest,
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = String, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Update publication with name = publication_name from source with id = source_id"),
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

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.into_connection_config().with_db();
    let publication = publication.0;
    let publication = Publication {
        name: publication_name,
        tables: publication.tables,
    };
    db::publications::update_publication(&publication, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
        ("publication_name" = String, Path, description = "Name of the publication"),
    ),
    responses(
        (status = 200, description = "Delete publication with name = publication_name from source with id = source_id"),
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

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.into_connection_config().with_db();
    db::publications::drop_publication(&publication_name, &options).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    tag = "Publications",
    params(
        ("source_id" = i64, Path, description = "Id of the source"),
    ),
    responses(
        (status = 200, description = "Return all publications", body = ReadPublicationsResponse),
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

    let config = db::sources::read_source(&pool, tenant_id, source_id, &encryption_key)
        .await?
        .map(|s| s.config)
        .ok_or(PublicationError::SourceNotFound(source_id))?;

    let options = config.into_connection_config().with_db();
    let publications = db::publications::read_all_publications(&options).await?;
    let response = ReadPublicationsResponse { publications };

    Ok(Json(response))
}
