use actix_web::{
    HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post, put,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing_actix_web::RootSpan;
use utoipa::ToSchema;

use crate::db;
use crate::db::tenants::TenantsDbError;
use crate::routes::ErrorMessage;

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("The tenant with id {0} was not found")]
    TenantNotFound(String),

    #[error(transparent)]
    TenantsDb(#[from] TenantsDbError),
}

impl TenantError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantError::TenantsDb(TenantsDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantError::TenantsDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantError::TenantNotFound(_) => StatusCode::NOT_FOUND,
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantRequest {
    #[schema(example = "abczjjlmfsijwrlnwatw", required = true)]
    pub id: String,
    #[schema(example = "My Tenant", required = true)]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateOrUpdateTenantRequest {
    #[schema(example = "My Updated Tenant", required = true)]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateOrUpdateTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateTenantRequest {
    #[schema(example = "My Updated Tenant", required = true)]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadTenantResponse {
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub id: String,
    #[schema(example = "My Tenant")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadTenantsResponse {
    pub tenants: Vec<ReadTenantResponse>,
}

#[utoipa::path(
    request_body = CreateTenantRequest,
    responses(
        (status = 200, description = "Create new tenant", body = CreateTenantResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[post("/tenants")]
pub async fn create_tenant(
    pool: Data<PgPool>,
    tenant: Json<CreateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.into_inner();

    root_span.record("project", &tenant.id);

    let id = db::tenants::create_tenant(&**pool, &tenant.id, &tenant.name).await?;

    let response = CreateTenantResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    request_body = CreateOrUpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Create a new tenant or update an existing one", body = CreateOrUpdateTenantResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[put("/tenants/{tenant_id}")]
pub async fn create_or_update_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<CreateOrUpdateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();
    let tenant = tenant.into_inner();

    root_span.record("project", &tenant_id);

    let id = db::tenants::create_or_update_tenant(&**pool, &tenant_id, &tenant.name).await?;
    let response = CreateOrUpdateTenantResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    params(
        ("tenant_id" = String, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Return tenant with id = tenant_id", body = ReadTenantResponse),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[get("/tenants/{tenant_id}")]
pub async fn read_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();

    root_span.record("project", &tenant_id);

    let response = db::tenants::read_tenant(&**pool, &tenant_id)
        .await?
        .map(|t| ReadTenantResponse {
            id: t.id,
            name: t.name,
        })
        .ok_or(TenantError::TenantNotFound(tenant_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    request_body = UpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Update tenant with id = tenant_id"),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[post("/tenants/{tenant_id}")]
pub async fn update_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    tenant: Json<UpdateTenantRequest>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant = tenant.into_inner();
    let tenant_id = tenant_id.into_inner();

    root_span.record("project", &tenant_id);

    db::tenants::update_tenant(&**pool, &tenant_id, &tenant.name)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    params(
        ("tenant_id" = String, Path, description = "Id of the tenant"),
    ),
    responses(
        (status = 200, description = "Delete tenant with id = tenant_id"),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[delete("/tenants/{tenant_id}")]
pub async fn delete_tenant(
    pool: Data<PgPool>,
    tenant_id: Path<String>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();

    root_span.record("project", &tenant_id);

    db::tenants::delete_tenant(&**pool, &tenant_id).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    responses(
        (status = 200, description = "Return all tenants", body = ReadTenantsResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[get("/tenants")]
pub async fn read_all_tenants(pool: Data<PgPool>) -> Result<impl Responder, TenantError> {
    let tenants: Vec<ReadTenantResponse> = db::tenants::read_all_tenants(&**pool)
        .await?
        .drain(..)
        .map(|t| ReadTenantResponse {
            id: t.id,
            name: t.name,
        })
        .collect();

    let response = ReadTenantsResponse { tenants };

    Ok(Json(response))
}
