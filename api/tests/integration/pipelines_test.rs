use api::db::pipelines::{OptionalPipelineConfig, PipelineConfig};
use api::db::sources::SourceConfig;
use api::routes::pipelines::{
    CreatePipelineRequest, CreatePipelineResponse, GetPipelineReplicationStatusResponse,
    ReadPipelineResponse, ReadPipelinesResponse, RollbackTableStateRequest,
    RollbackTableStateResponse, RollbackType, SimpleTableReplicationState,
    UpdatePipelineConfigRequest, UpdatePipelineConfigResponse, UpdatePipelineImageRequest,
    UpdatePipelineRequest,
};
use api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use config::SerializableSecretString;
use config::shared::BatchConfig;
use postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use sqlx::postgres::types::Oid;
use telemetry::init_test_tracing;
use uuid::Uuid;

use crate::{
    common::test_app::{TestApp, spawn_test_app},
    integration::destination_test::create_destination,
    integration::images_test::create_default_image,
    integration::sources_test::create_source,
    integration::tenants_test::create_tenant,
    integration::tenants_test::create_tenant_with_id_and_name,
};

pub fn new_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "publication".to_owned(),
        batch: Some(BatchConfig {
            max_size: 1000,
            max_fill_ms: 5,
        }),
        table_error_retry_delay_ms: Some(10000),
        max_table_sync_workers: Some(2),
    }
}

pub fn updated_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "updated_publication".to_owned(),
        batch: Some(BatchConfig {
            max_size: 2000,
            max_fill_ms: 10,
        }),
        table_error_retry_delay_ms: Some(20000),
        max_table_sync_workers: Some(4),
    }
}

pub enum ConfigUpdateType {
    Batch(BatchConfig),
    TableErrorRetryDelayMs(u64),
    MaxTableSyncWorkers(u16),
}

pub fn partially_updated_optional_pipeline_config(
    update: ConfigUpdateType,
) -> OptionalPipelineConfig {
    match update {
        ConfigUpdateType::Batch(batch_config) => OptionalPipelineConfig {
            batch: Some(batch_config),
            table_error_retry_delay_ms: None,
            max_table_sync_workers: None,
        },
        ConfigUpdateType::TableErrorRetryDelayMs(table_error_retry_delay_ms) => {
            OptionalPipelineConfig {
                batch: None,
                table_error_retry_delay_ms: Some(table_error_retry_delay_ms),
                max_table_sync_workers: None,
            }
        }
        ConfigUpdateType::MaxTableSyncWorkers(n) => OptionalPipelineConfig {
            batch: None,
            table_error_retry_delay_ms: None,
            max_table_sync_workers: Some(n),
        },
    }
}

pub fn updated_optional_pipeline_config() -> OptionalPipelineConfig {
    OptionalPipelineConfig {
        batch: Some(BatchConfig {
            max_size: 1_000_000,
            max_fill_ms: 100,
        }),
        table_error_retry_delay_ms: Some(10000),
        max_table_sync_workers: Some(8),
    }
}

pub async fn create_pipeline_with_config(
    app: &TestApp,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    config: PipelineConfig,
) -> i64 {
    create_default_image(app).await;
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config,
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

pub struct PipelineTestSetup {
    pub app: TestApp,
    pub tenant_id: String,
    pub source_pool: sqlx::PgPool,
    pub pipeline_id: i64,
    pub source_db_config: config::shared::PgConnectionConfig,
}

impl PipelineTestSetup {
    pub async fn new_with_source_db() -> Self {
        let app = spawn_test_app().await;
        create_default_image(&app).await;
        let tenant_id = create_tenant(&app).await;
        let (source_pool, source_id, source_db_config) =
            create_test_source_database(&app, &tenant_id).await;
        let destination_id = create_destination(&app, &tenant_id).await;
        let pipeline_id = create_pipeline_with_config(
            &app,
            &tenant_id,
            source_id,
            destination_id,
            new_pipeline_config(),
        )
        .await;

        Self {
            app,
            tenant_id,
            source_pool,
            pipeline_id,
            source_db_config,
        }
    }

    pub async fn new_simple() -> (TestApp, String, i64, i64, i64) {
        let app = spawn_test_app().await;
        create_default_image(&app).await;
        let tenant_id = create_tenant(&app).await;
        let source_id = create_source(&app, &tenant_id).await;
        let destination_id = create_destination(&app, &tenant_id).await;
        let pipeline_id = create_pipeline_with_config(
            &app,
            &tenant_id,
            source_id,
            destination_id,
            new_pipeline_config(),
        )
        .await;

        (app, tenant_id, source_id, destination_id, pipeline_id)
    }

    pub async fn create_table_with_state(
        &self,
        table_name: &str,
        state_chain: &[(&str, &str)],
    ) -> i64 {
        create_etl_table_schema(&self.source_pool).await;
        let table_oid = create_test_table(&self.source_pool, table_name).await;

        let mut prev_id: Option<i64> = None;
        for (i, (state, metadata)) in state_chain.iter().enumerate() {
            let is_current = i == state_chain.len() - 1;
            let id = sqlx::query_scalar::<_, i64>(
                "INSERT INTO etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current) VALUES ($1, $2, $3::etl.table_state, $4::jsonb, $5, $6) RETURNING id"
            )
            .bind(self.pipeline_id)
            .bind(table_oid)
            .bind(state)
            .bind(metadata)
            .bind(prev_id)
            .bind(is_current)
            .fetch_one(&self.source_pool)
            .await
            .unwrap();

            if i < state_chain.len() - 1 {
                prev_id = Some(id);
            }
        }

        table_oid
    }

    pub async fn create_tables_with_states(
        &self,
        tables: &[(&str, &str, &str)],
    ) -> Vec<(i64, String)> {
        create_etl_table_schema(&self.source_pool).await;
        let mut results = Vec::new();

        for (table_name, state, metadata) in tables {
            let table_oid = create_test_table(&self.source_pool, table_name).await;

            sqlx::query(
                "INSERT INTO etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current) VALUES ($1, $2, $3::etl.table_state, $4::jsonb, NULL, true)"
            )
            .bind(self.pipeline_id)
            .bind(table_oid)
            .bind(state)
            .bind(metadata)
            .execute(&self.source_pool)
            .await
            .unwrap();

            results.push((table_oid, format!("public.{}", table_name)));
        }

        results
    }

    pub async fn test_rollback(
        &self,
        table_oid: i64,
        rollback_type: RollbackType,
        expected_status: StatusCode,
    ) -> Option<RollbackTableStateResponse> {
        let response = self
            .app
            .rollback_table_state(
                &self.tenant_id,
                self.pipeline_id,
                &RollbackTableStateRequest {
                    table_id: table_oid as u32,
                    rollback_type,
                },
            )
            .await;

        assert_eq!(response.status(), expected_status);

        if expected_status.is_success() {
            Some(response.json().await.unwrap())
        } else {
            None
        }
    }
}

async fn create_test_source_database(
    app: &TestApp,
    tenant_id: &str,
) -> (sqlx::PgPool, i64, config::shared::PgConnectionConfig) {
    let mut source_db_config = app.database_config().clone();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());
    let source_pool = create_pg_database(&source_db_config).await;

    let source_config = SourceConfig {
        host: source_db_config.host.clone(),
        port: source_db_config.port,
        name: source_db_config.name.clone(),
        username: source_db_config.username.clone(),
        password: source_db_config
            .password
            .as_ref()
            .map(|p| SerializableSecretString::from(p.expose_secret().to_string())),
    };

    let source = CreateSourceRequest {
        name: "Test Source".to_string(),
        config: source_config,
    };

    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    (source_pool, response.id, source_db_config)
}

async fn create_test_table(source_pool: &sqlx::PgPool, table_name: &str) -> i64 {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, name TEXT)"
    ))
    .execute(source_pool)
    .await
    .expect("Failed to create test table");

    sqlx::query_scalar::<_, Oid>(
        "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = $1 AND n.nspname = $2"
    )
    .bind(table_name)
    .bind("public")
    .fetch_one(source_pool)
    .await
    .expect("Failed to get table OID").0 as i64
}

pub async fn create_etl_table_schema(source_pool: &sqlx::PgPool) {
    // Create etl schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS etl")
        .execute(source_pool)
        .await
        .expect("Failed to create etl schema");

    // Create the table_state enum
    sqlx::query(
        r#"
        CREATE TYPE etl.table_state AS ENUM (
            'init',
            'data_sync',
            'finished_copy',
            'sync_done',
            'ready',
            'errored'
        )
    "#,
    )
    .execute(source_pool)
    .await
    .expect("Failed to create table_state enum");

    // Create the replication_state table
    sqlx::query(
        r#"
        CREATE TABLE etl.replication_state (
            id BIGSERIAL PRIMARY KEY,
            pipeline_id BIGINT NOT NULL,
            table_id OID NOT NULL,
            state etl.table_state NOT NULL,
            metadata JSONB NULL,
            prev BIGINT NULL REFERENCES etl.replication_state(id),
            is_current BOOLEAN NOT NULL DEFAULT true
        )
    "#,
    )
    .execute(source_pool)
    .await
    .expect("Failed to create replication_state table");
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_source_cant_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source2_id = create_source(&app, tenant2_id).await;
    let destinaion1_id = create_destination(&app, tenant1_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source2_id,
        destination_id: destinaion1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_destination_cant_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source1_id = create_source(&app, tenant1_id).await;
    let destination2_id = create_destination(&app, tenant2_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination2_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.read_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_ne!(response.replicator_id, 0);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_source_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source1_id = create_source(&app, tenant1_id).await;
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source2_id,
        destination_id: destination1_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_destination_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source1_id = create_source(&app, tenant1_id).await;
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let destination2_id = create_destination(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id,
        destination_id: destination2_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Act
    let updated_config = UpdatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app.update_pipeline(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.delete_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_pipelines_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id = create_source(&app, tenant_id).await;
    let source2_id = create_source(&app, tenant_id).await;
    let destination1_id = create_destination(&app, tenant_id).await;
    let destination2_id = create_destination(&app, tenant_id).await;

    let pipeline1_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source1_id,
        destination1_id,
        new_pipeline_config(),
    )
    .await;
    let pipeline2_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source2_id,
        destination2_id,
        updated_pipeline_config(),
    )
    .await;

    // Act
    let response = app.read_all_pipelines(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadPipelinesResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for pipeline in response.pipelines {
        if pipeline.id == pipeline1_id {
            assert_eq!(&pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source1_id);
            assert_eq!(pipeline.destination_id, destination1_id);
            insta::assert_debug_snapshot!(pipeline.config);
        } else if pipeline.id == pipeline2_id {
            assert_eq!(&pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source2_id);
            assert_eq!(pipeline.destination_id, destination2_id);
            insta::assert_debug_snapshot!(pipeline.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_source_cascade_deletes_the_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    app.delete_source(tenant_id, source_id).await;

    // Assert
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_destination_cascade_deletes_the_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    app.delete_destination(tenant_id, destination_id).await;

    // Assert
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_pipeline_with_same_source_and_destination_cant_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create first pipeline
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(response.status().is_success());

    // Act - Try to create duplicate pipeline with same source and destination
    let duplicate_pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &duplicate_pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_pipeline_to_duplicate_source_destination_combination_fails() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id = create_source(&app, tenant_id).await;
    let source2_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create first pipeline
    let pipeline1 = CreatePipelineRequest {
        source_id: source1_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline1).await;
    assert!(response.status().is_success());

    // Create second pipeline with different source
    let pipeline2 = CreatePipelineRequest {
        source_id: source2_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline2).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline2_id = response.id;

    // Act - Try to update second pipeline to have same source as first
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id, // This would create a duplicate
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant_id, pipeline2_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_image_can_be_updated_with_specific_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act
    let update_request = UpdatePipelineImageRequest {
        image_id: Some(1), // Use the default image ID
    };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_image_can_be_updated_to_default_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act - update to default image (no image_id specified)
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_non_existing_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, 42, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_non_existing_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act
    let update_request = UpdatePipelineImageRequest {
        image_id: Some(999), // Non-existing image ID
    };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_pipeline_from_another_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant(&app).await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant1_id,
        source1_id,
        destination1_id,
        new_pipeline_config(),
    )
    .await;

    // Act - Try to update image using wrong tenant credentials
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image("wrong-tenant-id", pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_when_no_default_image_exists() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    // Don't create default image
    let tenant_id = &create_tenant(&app).await;

    // Act - Try to update to default image when none exists
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, 1, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_config_can_be_updated() {
    init_test_tracing();
    let (app, tenant_id, _source_id, _destination_id, pipeline_id) =
        PipelineTestSetup::new_simple().await;

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(ConfigUpdateType::Batch(BatchConfig {
            max_size: 10_000,
            max_fill_ms: 100,
        })),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(
            ConfigUpdateType::TableErrorRetryDelayMs(20000),
        ),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(ConfigUpdateType::MaxTableSyncWorkers(
            8,
        )),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_config_fails_for_non_existing_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: updated_optional_pipeline_config(),
    };
    let response = app
        .update_pipeline_config(tenant_id, 42, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_config_fails_for_pipeline_from_another_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant1_id = &create_tenant(&app).await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant1_id,
        source1_id,
        destination1_id,
        new_pipeline_config(),
    )
    .await;

    // Act - Try to update config using
    let update_request = UpdatePipelineConfigRequest {
        config: updated_optional_pipeline_config(),
    };
    let response = app
        .update_pipeline_config("wrong-tenant-id", pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_started() {
    init_test_tracing();
    let (app, tenant_id, _source_id, _destination_id, pipeline_id) =
        PipelineTestSetup::new_simple().await;

    // Act
    let response = app.start_pipeline(&tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_stopped() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.stop_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn all_pipelines_can_be_stopped() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let _response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    // Act
    let response = app.stop_all_pipelines(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_replication_status_returns_table_states_and_names() {
    init_test_tracing();
    let setup = PipelineTestSetup::new_with_source_db().await;

    // Create tables with different states
    let tables = setup
        .create_tables_with_states(&[
            ("test_table_users", "data_sync", r#"{"type": "data_sync"}"#),
            ("test_table_orders", "ready", r#"{"type": "ready"}"#),
        ])
        .await;

    // Test the endpoint
    let response = setup
        .app
        .get_pipeline_replication_status(&setup.tenant_id, setup.pipeline_id)
        .await;
    let response: GetPipelineReplicationStatusResponse = response.json().await.unwrap();

    assert_eq!(response.pipeline_id, setup.pipeline_id);
    assert_eq!(response.table_statuses.len(), 2);

    // Verify table states
    for (table_oid, table_name) in &tables {
        let table_status = response
            .table_statuses
            .iter()
            .find(|s| s.table_name == *table_name)
            .expect("Table not found in response");

        assert_eq!(table_status.table_id, *table_oid as u32);

        match table_name.as_str() {
            "public.test_table_users" => assert!(matches!(
                table_status.state,
                SimpleTableReplicationState::CopyingTable
            )),
            "public.test_table_orders" => assert!(matches!(
                table_status.state,
                SimpleTableReplicationState::FollowingWal { .. }
            )),
            _ => panic!("Unexpected table name: {}", table_name),
        }
    }

    drop_pg_database(&setup.source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_succeeds_for_manual_retry_errors() {
    init_test_tracing();
    let setup = PipelineTestSetup::new_with_source_db().await;

    let table_oid = setup
        .create_table_with_state(
            "test_users",
            &[
                ("ready", r#"{"type": "ready"}"#),
                (
                    "errored",
                    r#"{"type": "errored", "retry_policy": {"type": "manual_retry"}}"#,
                ),
            ],
        )
        .await;

    let response = setup
        .test_rollback(table_oid, RollbackType::Individual, StatusCode::OK)
        .await
        .unwrap();
    assert_eq!(response.pipeline_id, setup.pipeline_id);
    assert_eq!(response.table_id, table_oid as u32);
    assert!(matches!(
        response.new_state,
        SimpleTableReplicationState::FollowingWal { .. }
    ));

    drop_pg_database(&setup.source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_fails_for_non_manual_retry_errors() {
    init_test_tracing();
    let setup = PipelineTestSetup::new_with_source_db().await;

    let table_oid = setup
        .create_table_with_state(
            "test_users",
            &[(
                "errored",
                r#"{"type": "errored", "retry_policy": {"type": "no_retry"}}"#,
            )],
        )
        .await;

    setup
        .test_rollback(table_oid, RollbackType::Individual, StatusCode::BAD_REQUEST)
        .await;

    drop_pg_database(&setup.source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_with_full_reset_succeeds() {
    init_test_tracing();
    let setup = PipelineTestSetup::new_with_source_db().await;

    let table_oid = setup
        .create_table_with_state(
            "test_users",
            &[
                ("ready", r#"{"type": "ready"}"#),
                (
                    "errored",
                    r#"{"type": "errored", "retry_policy": {"type": "manual_retry"}}"#,
                ),
            ],
        )
        .await;

    let response = setup
        .test_rollback(table_oid, RollbackType::Full, StatusCode::OK)
        .await
        .unwrap();
    assert_eq!(response.pipeline_id, setup.pipeline_id);
    assert_eq!(response.table_id, table_oid as u32);
    assert!(matches!(
        response.new_state,
        SimpleTableReplicationState::Queued
    ));

    // Verify only one row exists (the reset init state)
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM etl.replication_state WHERE pipeline_id = $1 AND table_id = $2",
    )
    .bind(setup.pipeline_id)
    .bind(table_oid)
    .fetch_one(&setup.source_pool)
    .await
    .unwrap();
    assert_eq!(count, 1);

    drop_pg_database(&setup.source_db_config).await;
}
