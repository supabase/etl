use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use config::shared::{
    DestinationConfig, PgConnectionConfig, PipelineConfig as SharedPipelineConfig,
    ReplicatorConfig, SupabaseConfig, TlsConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db;
use crate::db::destinations::{Destination, DestinationsDbError, destination_exists};
use crate::db::images::{Image, ImagesDbError};
use crate::db::pipelines::{Pipeline, PipelineConfig, PipelinesDbError};
use crate::db::replicators::{Replicator, ReplicatorsDbError};
use crate::db::sources::{Source, SourceConfig, SourcesDbError, source_exists};
use crate::encryption::EncryptionKey;
use crate::k8s_client::{
    HttpK8sClient, K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME,
};
use crate::routes::{ErrorMessage, TenantIdError, extract_tenant_id};
use secrecy::ExposeSecret;

#[derive(Debug, Error)]
enum PipelineError {
    #[error("The pipeline with id {0} was not found")]
    PipelineNotFound(i64),

    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error("The replicator with pipeline id {0} was not found")]
    ReplicatorNotFound(i64),

    #[error("The image with replicator id {0} was not found")]
    ImageNotFound(i64),

    #[error("No default image was found")]
    NoDefaultImageFound,

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error("invalid destination config")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("A K8s error occurred: {0}")]
    K8s(#[from] K8sError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error(transparent)]
    PipelinesDb(PipelinesDbError),

    #[error(transparent)]
    ReplicatorsDb(#[from] ReplicatorsDbError),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),

    #[error("The trusted root certs config was not found")]
    TrustedRootCertsConfigMissing,

    #[error("A pipeline already exists for this source and destination combination")]
    DuplicatePipeline,
}

impl From<PipelinesDbError> for PipelineError {
    fn from(e: PipelinesDbError) -> Self {
        match e {
            PipelinesDbError::Database(e) if db::pipelines::is_duplicate_pipeline_error(&e) => {
                Self::DuplicatePipeline
            }
            e => Self::PipelinesDb(e),
        }
    }
}

impl PipelineError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PipelineError::SourcesDb(SourcesDbError::Database(_))
            | PipelineError::DestinationsDb(DestinationsDbError::Database(_))
            | PipelineError::PipelinesDb(PipelinesDbError::Database(_))
            | PipelineError::ReplicatorsDb(ReplicatorsDbError::Database(_))
            | PipelineError::ImagesDb(ImagesDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            PipelineError::InvalidConfig(_)
            | PipelineError::ReplicatorNotFound(_)
            | PipelineError::ImageNotFound(_)
            | PipelineError::NoDefaultImageFound
            | PipelineError::SourcesDb(_)
            | PipelineError::DestinationsDb(_)
            | PipelineError::PipelinesDb(_)
            | PipelineError::ReplicatorsDb(_)
            | PipelineError::ImagesDb(_)
            | PipelineError::K8s(_)
            | PipelineError::TrustedRootCertsConfigMissing => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantId(_)
            | PipelineError::SourceNotFound(_)
            | PipelineError::DestinationNotFound(_) => StatusCode::BAD_REQUEST,
            PipelineError::DuplicatePipeline => StatusCode::CONFLICT,
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
pub struct CreatePipelineRequest {
    #[schema(example = 1, required = true)]
    pub source_id: i64,
    #[schema(example = 1, required = true)]
    pub destination_id: i64,
    #[schema(required = true)]
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreatePipelineResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineRequest {
    #[schema(example = 1, required = true)]
    pub source_id: i64,
    #[schema(example = 1, required = true)]
    pub destination_id: i64,
    #[schema(required = true)]
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadPipelineResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = 1)]
    pub source_id: i64,
    #[schema(example = "My Postgres Source")]
    pub source_name: String,
    #[schema(example = 1)]
    pub destination_id: i64,
    #[schema(example = "My BigQuery Destination")]
    pub destination_name: String,
    #[schema(example = 1)]
    pub replicator_id: i64,
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadPipelinesResponse {
    pub pipelines: Vec<ReadPipelineResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = CreatePipelineRequest,
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Create new pipeline", body = CreatePipelineResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub async fn create_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline: Json<CreatePipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.into_inner();
    let tenant_id = extract_tenant_id(&req)?;
    let config = pipeline.config;

    if !source_exists(&pool, tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(&pool, tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    let image = db::images::read_default_image(&pool)
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.destination_id,
        image.id,
        config,
    )
    .await?;

    let response = CreatePipelineResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Return pipeline with id = pipeline_id", body = ReadPipelineResponse),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}")]
pub async fn read_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let response = db::pipelines::read_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .map(|s| {
            Ok::<ReadPipelineResponse, serde_json::Error>(ReadPipelineResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                source_id: s.source_id,
                source_name: s.source_name,
                destination_id: s.destination_id,
                destination_name: s.destination_name,
                replicator_id: s.replicator_id,
                config: s.config,
            })
        })
        .transpose()?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdatePipelineRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Update pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}")]
pub async fn update_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    pipeline: Json<UpdatePipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.into_inner();
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    if !source_exists(&pool, tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(&pool, tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    db::pipelines::update_pipeline(
        &pool,
        tenant_id,
        pipeline_id,
        pipeline.source_id,
        pipeline.destination_id,
        &pipeline.config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Delete pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[delete("/pipelines/{pipeline_id}")]
pub async fn delete_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    db::pipelines::delete_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Return all pipelines", body = ReadPipelinesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub async fn read_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;

    let mut pipelines = vec![];
    for pipeline in db::pipelines::read_all_pipelines(&pool, tenant_id).await? {
        let pipeline = ReadPipelineResponse {
            id: pipeline.id,
            tenant_id: pipeline.tenant_id,
            source_id: pipeline.source_id,
            source_name: pipeline.source_name,
            destination_id: pipeline.destination_id,
            destination_name: pipeline.destination_name,
            replicator_id: pipeline.replicator_id,
            config: pipeline.config,
        };
        pipelines.push(pipeline);
    }

    let response = ReadPipelinesResponse { pipelines };

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Start a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let (pipeline, replicator, image, source, destination) =
        read_data(&pool, tenant_id, pipeline_id, &encryption_key).await?;

    // We wrap Supabase's related information within its own struct for a clear separation.
    let supabase_config = SupabaseConfig {
        project_ref: tenant_id.to_owned(),
    };

    let secrets = build_secrets(&source.config, &destination.config);

    let replicator_config = build_replicator_config(
        &k8s_client,
        source.config,
        destination.config,
        pipeline,
        supabase_config,
    )
    .await?;

    let prefix = create_prefix(tenant_id, replicator.id);
    create_or_update_secrets(&k8s_client, &prefix, secrets).await?;
    create_or_update_config(&k8s_client, &prefix, replicator_config).await?;
    create_or_update_replicator(&k8s_client, &prefix, image.name).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Stop a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/stop")]
pub async fn stop_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);
    delete_secrets(&k8s_client, &prefix).await?;
    delete_config(&k8s_client, &prefix).await?;
    delete_replicator(&k8s_client, &prefix).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Stop all pipelines"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/stop")]
pub async fn stop_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let replicators = db::replicators::read_replicators(&pool, tenant_id).await?;
    for replicator in replicators {
        let prefix = create_prefix(tenant_id, replicator.id);
        delete_secrets(&k8s_client, &prefix).await?;
        delete_config(&k8s_client, &prefix).await?;
        delete_replicator(&k8s_client, &prefix).await?;
    }
    Ok(HttpResponse::Ok().finish())
}

#[derive(Serialize, ToSchema)]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
    Unknown,
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Get pipeline status"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines/{pipeline_id}/status")]
pub async fn get_pipeline_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);

    let pod_phase = k8s_client.get_pod_phase(&prefix).await?;

    let status = match pod_phase {
        PodPhase::Pending => PipelineStatus::Starting,
        PodPhase::Running => PipelineStatus::Started,
        PodPhase::Succeeded => PipelineStatus::Stopped,
        PodPhase::Failed => PipelineStatus::Stopped,
        PodPhase::Unknown => PipelineStatus::Unknown,
    };

    Ok(Json(status))
}

#[derive(Debug, Serialize, Deserialize)]
struct Secrets {
    postgres_password: String,
    big_query_service_account_key: Option<String>,
}

async fn read_data(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<(Pipeline, Replicator, Image, Source, Destination), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let replicator = db::replicators::read_replicator_by_pipeline_id(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let image = db::images::read_image_by_replicator_id(pool, replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;

    let source_id = pipeline.source_id;
    let source = db::sources::read_source(pool, tenant_id, source_id, encryption_key)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;

    let destination_id = pipeline.destination_id;
    let destination =
        db::destinations::read_destination(pool, tenant_id, destination_id, encryption_key)
            .await?
            .ok_or(PipelineError::DestinationNotFound(destination_id))?;

    Ok((pipeline, replicator, image, source, destination))
}

fn build_secrets(source_config: &SourceConfig, destination_config: &DestinationConfig) -> Secrets {
    let postgres_password = source_config
        .password
        .as_ref()
        .map(|p| p.expose_secret().to_owned())
        .unwrap_or_default();
    let mut big_query_service_account_key = None;
    if let DestinationConfig::BigQuery {
        service_account_key,
        ..
    } = destination_config
    {
        big_query_service_account_key = Some(service_account_key.expose_secret().to_owned());
    };

    Secrets {
        postgres_password,
        big_query_service_account_key,
    }
}

async fn build_replicator_config(
    k8s_client: &Arc<HttpK8sClient>,
    source_config: SourceConfig,
    destination_config: DestinationConfig,
    pipeline: Pipeline,
    supabase_config: SupabaseConfig,
) -> Result<ReplicatorConfig, PipelineError> {
    let trusted_root_certs = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .get("trusted_root_certs")
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .clone();

    let pg_connection = PgConnectionConfig {
        host: source_config.host,
        port: source_config.port,
        name: source_config.name,
        username: source_config.username,
        password: source_config.password,
        tls: TlsConfig {
            trusted_root_certs,
            enabled: true,
        },
    };

    let pipeline_config = SharedPipelineConfig {
        // We are safe to perform this conversion, since the i64 -> u64 conversion performs wrap
        // around, and we won't have two different values map to the same u64, since the domain size
        // is the same.
        id: pipeline.id as u64,
        publication_name: pipeline.config.publication_name,
        pg_connection,
        // If these configs are not set, we default to the most recent default values.
        //
        // The reason for using `Option` fields in the config instead of automatically applying defaults
        // is that we want to persist a config in storage with some unset values, allowing us to interpret
        // them differently after deserialization without needing to run database migrations.
        batch: pipeline.config.batch.unwrap_or_default(),
        apply_worker_init_retry: pipeline.config.apply_worker_init_retry.unwrap_or_default(),
        // Hardcoding a value of 4 for now for maximum number of parallel table sync workers
        max_table_sync_workers: pipeline.config.max_table_sync_workers.unwrap_or(4),
    };

    let config = ReplicatorConfig {
        destination: destination_config,
        pipeline: pipeline_config,
        supabase: Some(supabase_config),
    };

    Ok(config)
}

fn create_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

async fn create_or_update_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    secrets: Secrets,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_postgres_secret(prefix, &secrets.postgres_password)
        .await?;

    if let Some(bigquery_service_account_key) = secrets.big_query_service_account_key {
        k8s_client
            .create_or_update_bq_secret(prefix, &bigquery_service_account_key)
            .await?;
    }

    Ok(())
}

async fn create_or_update_config(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    config: ReplicatorConfig,
) -> Result<(), PipelineError> {
    let base_config = "";
    let prod_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &prod_config)
        .await?;
    Ok(())
}

async fn create_or_update_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    replicator_image: String,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image)
        .await?;

    Ok(())
}

async fn delete_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;
    k8s_client.delete_bq_secret(prefix).await?;
    Ok(())
}

async fn delete_config(k8s_client: &Arc<HttpK8sClient>, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;
    Ok(())
}

async fn delete_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;
    Ok(())
}
