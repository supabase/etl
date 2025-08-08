use actix_web::HttpRequest;
use etl_config::shared::PgConnectionConfig;
use etl_postgres::replication::connect_to_source_database;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

pub mod destinations;
pub mod destinations_pipelines;
pub mod health_check;
pub mod images;
pub mod pipelines;
pub mod sources;
pub mod tenants;
pub mod tenants_sources;

/// Minimum number of connections for the source Postgres connection pool.
const MIN_POOL_CONNECTIONS: u32 = 1;
/// Maximum number of connections for the source Postgres connection pool.
const MAX_POOL_CONNECTIONS: u32 = 1;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorMessage {
    #[schema(example = "an error occurred in the api")]
    pub error: String,
}

#[derive(Debug, Error)]
pub enum TenantIdError {
    #[error("The tenant id is missing in the request")]
    TenantIdMissing,

    #[error("The tenant id in the request is invalid")]
    TenantIdIllFormed,
}

fn extract_tenant_id(req: &HttpRequest) -> Result<&str, TenantIdError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(TenantIdError::TenantIdMissing)?
        .to_str()
        .map_err(|_| TenantIdError::TenantIdIllFormed)?;

    Ok(tenant_id)
}

pub async fn connect_to_source_database_with_defaults(
    config: &PgConnectionConfig,
) -> Result<PgPool, sqlx::Error> {
    connect_to_source_database(config, MIN_POOL_CONNECTIONS, MAX_POOL_CONNECTIONS).await
}
