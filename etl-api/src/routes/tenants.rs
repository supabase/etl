use actix_web::{
    HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post, put,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::ops::DerefMut;
use thiserror::Error;
use tracing_actix_web::RootSpan;
use utoipa::ToSchema;

use crate::config::ApiConfig;
use crate::configs::encryption::EncryptionKey;
use crate::db;
use crate::db::connect_to_source_database_from_api;
use crate::db::pipelines::{
    PipelinesDbError, delete_pipeline_replication_slots, delete_pipelines_api_and_source_state,
    read_all_pipelines_for_deletion,
};
use crate::db::sources::SourcesDbError;
use crate::db::tenants::TenantsDbError;
use crate::k8s::core::{K8sCoreError, first_active_pipeline_id};
use crate::k8s::{K8sClient, TrustedRootCertsCache, TrustedRootCertsError};
use crate::routes::ErrorMessage;

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("The tenant with id {0} was not found")]
    TenantNotFound(String),

    #[error(transparent)]
    TenantsDb(#[from] TenantsDbError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    TrustedRootCerts(#[from] TrustedRootCertsError),

    #[error(transparent)]
    K8sCore(#[from] K8sCoreError),

    #[error("The pipeline with id {0} is active. Stop it before deleting it.")]
    ActivePipeline(i64),
}

impl TenantError {
    pub fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            TenantError::TenantsDb(TenantsDbError::Database(_))
            | TenantError::SourcesDb(SourcesDbError::Database(_))
            | TenantError::PipelinesDb(PipelinesDbError::Database(_))
            | TenantError::Database(_)
            | TenantError::TrustedRootCerts(_)
            | TenantError::K8sCore(_) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for TenantError {
    fn status_code(&self) -> StatusCode {
        match self {
            TenantError::TenantsDb(TenantsDbError::Conflict(_)) => StatusCode::CONFLICT,
            TenantError::TenantsDb(_)
            | TenantError::SourcesDb(_)
            | TenantError::PipelinesDb(_)
            | TenantError::Database(_)
            | TenantError::TrustedRootCerts(_)
            | TenantError::K8sCore(_) => StatusCode::INTERNAL_SERVER_ERROR,
            TenantError::ActivePipeline(_) => StatusCode::CONFLICT,
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
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub id: String,
    #[schema(example = "My Tenant", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
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
    #[serde(deserialize_with = "crate::utils::trim_string")]
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
    #[serde(deserialize_with = "crate::utils::trim_string")]
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
    summary = "Create a tenant",
    description = "Creates a new tenant with the provided ID and name.",
    request_body = CreateTenantRequest,
    responses(
        (status = 200, description = "Tenant created successfully", body = CreateTenantResponse),
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
    summary = "Create or update a tenant",
    description = "Creates a tenant if it does not exist; otherwise updates its name.",
    request_body = CreateOrUpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant created or updated successfully", body = CreateOrUpdateTenantResponse),
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
    summary = "Retrieve a tenant",
    description = "Returns the tenant identified by the provided ID.",
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant retrieved successfully", body = ReadTenantResponse),
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
    summary = "Update a tenant",
    description = "Updates the tenant's display name.",
    request_body = UpdateTenantRequest,
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant updated successfully"),
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
    summary = "Delete a tenant",
    description = "Deletes the tenant identified by the provided ID.",
    params(
        ("tenant_id" = String, Path, description = "Unique ID of the tenant"),
    ),
    responses(
        (status = 200, description = "Tenant deleted successfully"),
        (status = 409, description = "Tenant has active pipelines or pipelines still defined", body = ErrorMessage),
        (status = 404, description = "Tenant not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Tenants"
)]
#[delete("/tenants/{tenant_id}")]
pub async fn delete_tenant(
    pool: Data<PgPool>,
    api_config: Data<ApiConfig>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    tenant_id: Path<String>,
    trusted_root_certs_cache: Data<TrustedRootCertsCache>,
    root_span: RootSpan,
) -> Result<impl Responder, TenantError> {
    let tenant_id = tenant_id.into_inner();

    root_span.record("project", &tenant_id);

    let pipelines = read_all_pipelines_for_deletion(&**pool, &tenant_id).await?;
    if let Some(pipeline_id) =
        first_active_pipeline_id(k8s_client.as_ref(), &tenant_id, &pipelines).await?
    {
        return Err(TenantError::ActivePipeline(pipeline_id));
    }

    let sources =
        db::sources::read_all_source_connections(&**pool, &tenant_id, &encryption_key).await?;
    let tls_config = trusted_root_certs_cache
        .get_tls_config(api_config.source.tls_enabled)
        .await?;
    let mut pipelines_by_source = std::collections::BTreeMap::new();
    for pipeline in pipelines {
        pipelines_by_source
            .entry(pipeline.source_id)
            .or_insert_with(Vec::new)
            .push(pipeline);
    }

    let mut api_txn = pool.begin().await?;
    let mut pending_source_deletions = Vec::new();
    for source in sources {
        let source_pool = connect_to_source_database_from_api(
            &source.config.into_connection_config(tls_config.clone()),
        )
        .await?;
        let mut source_txn = source_pool.begin().await?;
        let source_pipeline_slot_state =
            if let Some(source_pipelines) = pipelines_by_source.get(&source.id) {
                delete_pipelines_api_and_source_state(
                    api_txn.deref_mut(),
                    source_txn.deref_mut(),
                    &tenant_id,
                    source_pipelines,
                )
                .await?
            } else {
                Vec::new()
            };
        db::sources::uninstall_source_installation(source_txn.deref_mut()).await?;
        pending_source_deletions.push((source_pool, source_txn, source_pipeline_slot_state));
    }

    db::tenants::delete_tenant(api_txn.deref_mut(), &tenant_id)
        .await?
        .ok_or(TenantError::TenantNotFound(tenant_id.clone()))?;
    // Commit the API transaction before any source transaction. If a source transaction committed
    // first and the API commit failed afterwards, the API database could still expose sources or
    // pipelines whose source-side state has already been removed.
    api_txn.commit().await?;
    for (source_pool, source_txn, source_pipeline_slot_state) in pending_source_deletions {
        source_txn.commit().await?;
        for (pipeline_id, table_ids) in source_pipeline_slot_state {
            delete_pipeline_replication_slots(&source_pool, pipeline_id, table_ids).await?;
        }
    }

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "List tenants",
    description = "Returns all tenants.",
    responses(
        (status = 200, description = "Tenants listed successfully", body = ReadTenantsResponse),
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
