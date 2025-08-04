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
use postgres::schema::TableId;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, PgTransaction};
use std::ops::DerefMut;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::destinations::{Destination, DestinationsDbError, destination_exists};
use crate::db::images::{Image, ImagesDbError};
use crate::db::pipelines::{Pipeline, PipelineConfig, PipelinesDbError};
use crate::db::replicators::{Replicator, ReplicatorsDbError};
use crate::db::sources::{Source, SourceConfig, SourcesDbError, source_exists};
use crate::db::{self, pipelines::OptionalPipelineConfig};
use crate::encryption::EncryptionKey;
use crate::k8s_client::TRUSTED_ROOT_CERT_KEY_NAME;
use crate::k8s_client::{K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME};
use crate::routes::{
    ErrorMessage, TenantIdError, connect_to_source_database_with_defaults, extract_tenant_id,
};
use postgres::replication::{
    TableLookupError, TableReplicationState, get_table_name_from_oid,
    get_table_replication_state_rows,
};
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

    #[error("The table replication state is not present")]
    MissingTableReplicationState,

    #[error("The table replication state is not valid: {0}")]
    InvalidTableReplicationState(serde_json::Error),

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

    #[error("The specified image with id {0} was not found")]
    ImageNotFoundById(i64),

    #[error("There was an error while loolking up table information in the source database: {0}")]
    TableLookup(#[from] TableLookupError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
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
            | PipelineError::ImagesDb(ImagesDbError::Database(_))
            | PipelineError::Database(_) => "internal server error".to_string(),
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
            | PipelineError::TrustedRootCertsConfigMissing
            | PipelineError::Database(_)
            | PipelineError::TableLookup(_)
            | PipelineError::InvalidTableReplicationState(_)
            | PipelineError::MissingTableReplicationState => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::PipelineNotFound(_) | PipelineError::ImageNotFoundById(_) => {
                StatusCode::NOT_FOUND
            }
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
pub struct UpdatePipelineConfigRequest {
    #[schema(required = true)]
    pub config: OptionalPipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineConfigResponse {
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineImageRequest {
    #[schema(example = 1)]
    pub image_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPipelineStatusResponse {
    #[schema(example = 1)]
    pub pipeline_id: i64,
    pub status: PipelineStatus,
}

/// UI-friendly representation of table replication state
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "name")]
pub enum SimpleTableReplicationState {
    Queued,
    CopyingTable,
    CopiedTable,
    FollowingWal { lag: u64 },
    Error { message: String },
}

impl From<TableReplicationState> for SimpleTableReplicationState {
    fn from(state: TableReplicationState) -> Self {
        match state {
            TableReplicationState::Init => SimpleTableReplicationState::Queued,
            TableReplicationState::DataSync => SimpleTableReplicationState::CopyingTable,
            TableReplicationState::FinishedCopy => SimpleTableReplicationState::CopiedTable,
            // TODO: add lag metric when available.
            TableReplicationState::SyncDone { .. } => {
                SimpleTableReplicationState::FollowingWal { lag: 0 }
            }
            TableReplicationState::Ready => SimpleTableReplicationState::FollowingWal { lag: 0 },
            TableReplicationState::Errored {
                reason, solution, ..
            } => {
                let message = match solution {
                    Some(solution) => format!("{reason}: {solution}"),
                    None => reason,
                };
                SimpleTableReplicationState::Error { message }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TableReplicationStatus {
    #[schema(example = 1)]
    pub table_id: u32,
    #[schema(example = "public.users")]
    pub table_name: String,
    pub state: SimpleTableReplicationState,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPipelineReplicationStatusResponse {
    #[schema(example = 1)]
    pub pipeline_id: i64,
    pub table_statuses: Vec<TableReplicationStatus>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "name")]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
    Unknown,
    Failed {
        exit_code: Option<i32>,
        message: Option<String>,
        reason: Option<String>,
    },
}

#[utoipa::path(
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
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline = pipeline.into_inner();

    let mut txn = pool.begin().await?;
    if !source_exists(txn.deref_mut(), tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(txn.deref_mut(), tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    let image = db::images::read_default_image(txn.deref_mut())
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &mut txn,
        tenant_id,
        pipeline.source_id,
        pipeline.destination_id,
        image.id,
        pipeline.config,
    )
    .await?;
    txn.commit().await?;

    let response = CreatePipelineResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
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

    let response = db::pipelines::read_pipeline(&**pool, tenant_id, pipeline_id)
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
// Forcing {pipeline_id} to be all digits by appending :\\d+
// to avoid this route clashing with /pipelines/stop
#[post("/pipelines/{pipeline_id:\\d+}")]
pub async fn update_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    pipeline: Json<UpdatePipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let pipeline = pipeline.into_inner();

    let mut txn = pool.begin().await?;
    if !source_exists(txn.deref_mut(), tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(txn.deref_mut(), tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    db::pipelines::update_pipeline(
        txn.deref_mut(),
        tenant_id,
        pipeline_id,
        pipeline.source_id,
        pipeline.destination_id,
        &pipeline.config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
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

    db::pipelines::delete_pipeline(&**pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
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
    for pipeline in db::pipelines::read_all_pipelines(&**pool, tenant_id).await? {
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
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Start a pipeline"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let (pipeline, replicator, image, source, destination) =
        read_all_required_data(&mut txn, tenant_id, pipeline_id, &encryption_key).await?;

    // We update the pipeline in K8s.
    create_or_update_pipeline_in_k8s(
        k8s_client.as_ref(),
        tenant_id,
        pipeline,
        replicator,
        image,
        source,
        destination,
    )
    .await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Stop a pipeline"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/stop")]
pub async fn stop_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let replicator =
        db::replicators::read_replicator_by_pipeline_id(txn.deref_mut(), tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    delete_pipeline_in_k8s(k8s_client.as_ref(), tenant_id, replicator).await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    params(
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Stop all pipelines"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/stop")]
pub async fn stop_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let replicators = db::replicators::read_replicators(txn.deref_mut(), tenant_id).await?;
    for replicator in replicators {
        delete_pipeline_in_k8s(k8s_client.as_ref(), tenant_id, replicator).await?;
    }
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Get pipeline status", body = GetPipelineStatusResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}/status")]
pub async fn get_pipeline_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let replicator =
        db::replicators::read_replicator_by_pipeline_id(&**pool, tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    // We load the pod phase.
    let pod_phase = k8s_client.get_pod_phase(&prefix).await?;

    // We check the status of the replicator container.
    //
    // Note that this is dumping all the logs within the container, meaning that anything on stdout
    // and stderr will be returned. In case we want to be more careful with what we return we can
    // embed within the logs some special characters that will be used to determine which error message
    // to return to the user.
    let replicator_error = k8s_client.get_replicator_container_error(&prefix).await?;

    let status = if let Some(replicator_error) = replicator_error {
        PipelineStatus::Failed {
            exit_code: replicator_error.exit_code,
            message: replicator_error.message,
            reason: replicator_error.reason,
        }
    } else {
        match pod_phase {
            PodPhase::Pending => PipelineStatus::Starting,
            PodPhase::Running => PipelineStatus::Started,
            PodPhase::Succeeded => PipelineStatus::Stopped,
            PodPhase::Failed => PipelineStatus::Failed {
                exit_code: None,
                message: None,
                reason: None,
            },
            PodPhase::Unknown => PipelineStatus::Unknown,
        }
    };

    let response = GetPipelineStatusResponse {
        pipeline_id,
        status,
    };

    Ok(Json(response))
}

#[utoipa::path(
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Get replication status for all tables in the pipeline", body = GetPipelineReplicationStatusResponse),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}/replication-status")]
pub async fn get_pipeline_replication_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let mut txn = pool.begin().await?;

    // Read the pipeline to ensure it exists and get the source configuration
    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    // Get the source configuration
    let source = db::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(PipelineError::SourceNotFound(pipeline.source_id))?;

    txn.commit().await?;

    // Connect to the source database to read replication state
    let source_pool =
        connect_to_source_database_with_defaults(&source.config.into_connection_config()).await?;

    // Fetch replication state for all tables in this pipeline
    let state_rows = get_table_replication_state_rows(&source_pool, pipeline_id).await?;

    // Convert database states to UI-friendly format and fetch table names
    let mut tables: Vec<TableReplicationStatus> = Vec::new();
    for row in state_rows {
        let table_id = row.table_id.0;
        let table_name =
            get_table_name_from_oid(&source_pool, TableId::new(row.table_id.0)).await?;

        // Extract the metadata row from the database
        let Some(table_replication_state) = row
            .deserialize_metadata()
            .map_err(PipelineError::InvalidTableReplicationState)?
        else {
            return Err(PipelineError::MissingTableReplicationState);
        };

        tables.push(TableReplicationStatus {
            table_id,
            table_name: table_name.to_string(),
            state: table_replication_state.into(),
        });
    }

    let response = GetPipelineReplicationStatusResponse {
        pipeline_id,
        table_statuses: tables,
    };

    Ok(Json(response))
}

#[utoipa::path(
    request_body = UpdatePipelineImageRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Image updated successfully"),
        (status = 400, description = "Pipeline not running or bad request", body = ErrorMessage),
        (status = 404, description = "Pipeline or image not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/update-image")]
pub async fn update_pipeline_image(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
    update_request: Json<UpdatePipelineImageRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let update_request = update_request.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let (pipeline, replicator, current_image, source, destination) =
        read_all_required_data(&mut txn, tenant_id, pipeline_id, &encryption_key).await?;

    let target_image = match update_request.image_id {
        Some(image_id) => db::images::read_image(txn.deref_mut(), image_id)
            .await?
            .ok_or(PipelineError::ImageNotFoundById(image_id))?,
        None => db::images::read_default_image(txn.deref_mut())
            .await?
            .ok_or(PipelineError::NoDefaultImageFound)?,
    };

    // If the image ids are different, we change the database entry.
    if target_image.id != current_image.id {
        db::replicators::update_replicator_image(
            txn.deref_mut(),
            tenant_id,
            replicator.id,
            target_image.id,
        )
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;
    }

    // If the images have equal name, we don't care about their id from the K8S perspective, so we
    // won't update any resources.
    if target_image.name == current_image.name {
        txn.commit().await?;

        return Ok(HttpResponse::Ok().finish());
    }

    // We update the pipeline in K8s if client is available.
    create_or_update_pipeline_in_k8s(
        k8s_client.as_ref(),
        tenant_id,
        pipeline,
        replicator,
        target_image,
        source,
        destination,
    )
    .await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    request_body = UpdatePipelineConfigRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
        ("tenant_id" = String, Header, description = "The tenant ID")
    ),
    responses(
        (status = 200, description = "Config updated successfully", body = UpdatePipelineConfigResponse),
        (status = 400, description = "Pipeline not running or bad request", body = ErrorMessage),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/update-config")]
pub async fn update_pipeline_config(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    update_request: Json<UpdatePipelineConfigRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let update_request = update_request.into_inner();
    let mut txn = pool.begin().await?;

    let config = db::pipelines::update_pipeline_config(
        &mut txn,
        tenant_id,
        pipeline_id,
        update_request.config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    txn.commit().await?;

    let response = UpdatePipelineConfigResponse { config };

    Ok(Json(response))
}

#[derive(Debug, Serialize, Deserialize)]
struct Secrets {
    postgres_password: String,
    big_query_service_account_key: Option<String>,
}

async fn create_or_update_pipeline_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    pipeline: Pipeline,
    replicator: Replicator,
    image: Image,
    source: Source,
    destination: Destination,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    // We create the secrets.
    let secrets = build_secrets(&source.config, &destination.config);
    create_or_update_secrets(k8s_client, &prefix, secrets).await?;

    // We create the replicator configuration.
    let replicator_config = build_replicator_config(
        k8s_client,
        source.config,
        destination.config,
        pipeline,
        SupabaseConfig {
            project_ref: tenant_id.to_owned(),
        },
    )
    .await?;
    create_or_update_config(k8s_client, &prefix, replicator_config).await?;

    // We create the replicator stateful set.
    create_or_update_replicator(k8s_client, &prefix, image.name).await?;

    Ok(())
}

async fn delete_pipeline_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    replicator: Replicator,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    delete_secrets(k8s_client, &prefix).await?;
    delete_config(k8s_client, &prefix).await?;
    delete_replicator(k8s_client, &prefix).await?;

    Ok(())
}

async fn read_all_required_data(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<(Pipeline, Replicator, Image, Source, Destination), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let replicator =
        db::replicators::read_replicator_by_pipeline_id(txn.deref_mut(), tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let image = db::images::read_image_by_replicator_id(txn.deref_mut(), replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;

    let source_id = pipeline.source_id;
    let source = db::sources::read_source(txn.deref_mut(), tenant_id, source_id, encryption_key)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;

    let destination_id = pipeline.destination_id;
    let destination = db::destinations::read_destination(
        txn.deref_mut(),
        tenant_id,
        destination_id,
        encryption_key,
    )
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
    k8s_client: &dyn K8sClient,
    source_config: SourceConfig,
    destination_config: DestinationConfig,
    pipeline: Pipeline,
    supabase_config: SupabaseConfig,
) -> Result<ReplicatorConfig, PipelineError> {
    // We load the trusted root certificates from the config map.
    let trusted_root_certs = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .get(TRUSTED_ROOT_CERT_KEY_NAME)
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
        // Hardcoding 10s for now
        table_error_retry_delay_ms: pipeline.config.table_error_retry_delay_ms.unwrap_or(10000),
        // Hardcoding a value of 4 for now for maximum number of parallel table sync workers
        max_table_sync_workers: pipeline.config.max_table_sync_workers.unwrap_or(4),
    };

    let config = ReplicatorConfig {
        destination: destination_config,
        pipeline: pipeline_config,
        // The Sentry config will be injected via env variables for security purposes.
        sentry: None,
        supabase: Some(supabase_config),
    };

    Ok(config)
}

fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

async fn create_or_update_secrets(
    k8s_client: &dyn K8sClient,
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
    k8s_client: &dyn K8sClient,
    prefix: &str,
    config: ReplicatorConfig,
) -> Result<(), PipelineError> {
    // For now the base config is empty.
    let base_config = "";
    let prod_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &prod_config)
        .await?;

    Ok(())
}

async fn create_or_update_replicator(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    replicator_image: String,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image)
        .await?;

    Ok(())
}

async fn delete_secrets(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;
    k8s_client.delete_bq_secret(prefix).await?;

    Ok(())
}

async fn delete_config(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;

    Ok(())
}

async fn delete_replicator(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;

    Ok(())
}
