use etl_api::{
    configs::{
        destination::{ApiDestinationConfig, UpdateApiDestinationConfig, UpdateApiIcebergConfig},
        update::UpdateField,
    },
    k8s::PodStatus,
    routes::{
        destinations::{
            CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
            ReadDestinationsResponse, UpdateDestinationRequest,
        },
        pipelines::{CreatePipelineRequest, CreatePipelineResponse},
    },
    startup::get_connection_pool,
};
use etl_config::{SerializableSecretString, shared::DestinationConfig};
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::support::{
    database::{create_test_source_database, run_etl_migrations_on_source_database},
    mocks::{
        create_default_image,
        destinations::{
            create_destination, create_destination_with_config, new_bigquery_destination_config,
            new_ducklake_destination_config, new_iceberg_supabase_destination_config, new_name,
            new_snowflake_destination_config, updated_destination_config,
            updated_iceberg_supabase_destination_config, updated_name,
            updated_snowflake_destination_config,
        },
        pipelines::new_pipeline_config,
        sources::create_source,
        tenants::create_tenant,
    },
    test_app::{TestApp, spawn_test_app},
};

async fn create_destination_pipeline_for_source(
    app: &TestApp,
    tenant_id: &str,
    source_id: i64,
) -> (i64, i64) {
    let destination_id = create_destination(app, tenant_id).await;
    let pipeline =
        CreatePipelineRequest { source_id, destination_id, config: new_pipeline_config() };
    let pipeline_response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(pipeline_response.status().is_success());
    let pipeline_response: CreatePipelineResponse =
        pipeline_response.json().await.expect("failed to deserialize response");

    (destination_id, pipeline_response.id)
}

async fn read_stored_destination_config(app: &TestApp, destination_id: i64) -> serde_json::Value {
    let pool = get_connection_pool(app.database_config());
    sqlx::query_scalar::<_, serde_json::Value>("select config from app.destinations where id = $1")
        .bind(destination_id)
        .fetch_one(&pool)
        .await
        .expect("failed to read stored destination config")
}

#[tokio::test(flavor = "multi_thread")]
async fn bigquery_destination_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_supabase_destination_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let destination = CreateDestinationRequest {
        name: "Iceberg Supabase Destination".to_owned(),
        config: new_iceberg_supabase_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let response = app.read_destination(tenant_id, destination_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, destination.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_destination_cannot_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_destination(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_bigquery_destination_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::from_api_config(updated_destination_config()),
    };
    let response = app.update_destination(tenant_id, destination_id, &updated_config).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_config_update_preserves_omitted_fields_and_resets_default_fields() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    let update_request = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Preserve,
            dataset_id: UpdateField::Set("dataset-id-patched".to_owned()),
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Clear,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");

    match response.config {
        etl_api::configs::destination::ApiDestinationConfig::BigQuery {
            project_id,
            dataset_id,
            connection_pool_size,
            ..
        } => {
            assert_eq!(project_id, "project-id");
            assert_eq!(dataset_id, "dataset-id-patched");
            assert_eq!(connection_pool_size, Some(DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE));
        }
        _ => panic!("Config types don't match"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_config_update_preserves_and_reencrypts_secret_fields_in_storage() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination_id = create_destination_with_config(
        &app,
        tenant_id,
        new_name(),
        new_bigquery_destination_config(),
    )
    .await;
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    let stored_service_account_key = stored_config["big_query"]["service_account_key"].clone();

    let update_request = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Preserve,
            dataset_id: UpdateField::Set("dataset-id-patched".to_owned()),
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    assert_eq!(stored_config["big_query"]["dataset_id"], "dataset-id-patched");
    assert_eq!(stored_config["big_query"]["service_account_key"], stored_service_account_key);

    let update_request = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Preserve,
            dataset_id: UpdateField::Preserve,
            service_account_key: UpdateField::Set(SerializableSecretString::from(
                "service-account-key-updated".to_owned(),
            )),
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    assert_ne!(stored_config["big_query"]["service_account_key"], stored_service_account_key);
    assert!(!stored_config.to_string().contains("service-account-key-updated"));
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_config_update_preserves_absent_stored_default_fields() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;
    let pool = get_connection_pool(app.database_config());

    sqlx::query(
        r#"
        update app.destinations
        set config = config #- '{big_query,connection_pool_size}'
        where id = $1
        "#,
    )
    .bind(destination_id)
    .execute(&pool)
    .await
    .expect("failed to remove stored destination default field");

    let update_request = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Preserve,
            dataset_id: UpdateField::Set("dataset-id-patched".to_owned()),
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = sqlx::query_scalar::<_, serde_json::Value>(
        "select config from app.destinations where id = $1",
    )
    .bind(destination_id)
    .fetch_one(&pool)
    .await
    .expect("failed to read stored destination config");
    let bigquery_config =
        stored_config.get("big_query").expect("stored destination should use BigQuery config");
    assert_eq!(bigquery_config["dataset_id"], "dataset-id-patched");
    assert!(bigquery_config.get("connection_pool_size").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_config_update_rejects_null_required_field() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    let update_request = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Clear,
            dataset_id: UpdateField::Preserve,
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_destination_with_running_pipeline_restarts_replicator() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    let source_id = create_source(&app, tenant_id).await;
    let (destination_id, _pipeline_id) =
        create_destination_pipeline_for_source(&app, tenant_id, source_id).await;

    let create_calls_before = app.k8s_state.create_calls();
    let updated_config = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::from_api_config(updated_destination_config()),
    };

    let response = app.update_destination(tenant_id, destination_id, &updated_config).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(app.k8s_state.create_calls() > create_calls_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_iceberg_supabase_destination_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: "Iceberg Supabase Destination".to_owned(),
        config: new_iceberg_supabase_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: "Iceberg Supabase Destination (Updated)".to_owned(),
        config: UpdateApiDestinationConfig::from_api_config(
            updated_iceberg_supabase_destination_config(),
        ),
    };
    let response = app.update_destination(tenant_id, destination_id, &updated_config).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_supabase_destination_update_preserves_nested_encrypted_fields() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination_id = create_destination_with_config(
        &app,
        tenant_id,
        "Iceberg Supabase Destination".to_owned(),
        new_iceberg_supabase_destination_config(),
    )
    .await;
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    let stored_catalog_token = stored_config["iceberg"]["supabase"]["catalog_token"].clone();

    let update_request = UpdateDestinationRequest {
        name: "Iceberg Supabase Destination (Partial)".to_owned(),
        config: UpdateApiDestinationConfig::Iceberg {
            config: UpdateApiIcebergConfig::Supabase {
                project_ref: UpdateField::Preserve,
                warehouse_name: UpdateField::Set("warehouse-patched".to_owned()),
                namespace: UpdateField::Clear,
                catalog_token: UpdateField::Preserve,
                s3_access_key_id: UpdateField::Preserve,
                s3_secret_access_key: UpdateField::Preserve,
                s3_region: UpdateField::Preserve,
            },
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    assert_eq!(stored_config["iceberg"]["supabase"]["warehouse_name"], "warehouse-patched");
    assert!(stored_config["iceberg"]["supabase"].get("namespace").is_none());
    assert_eq!(stored_config["iceberg"]["supabase"]["catalog_token"], stored_catalog_token);
}

#[tokio::test(flavor = "multi_thread")]
async fn ducklake_destination_update_preserves_and_clears_encrypted_fields() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination_id = create_destination_with_config(
        &app,
        tenant_id,
        "DuckLake Destination".to_owned(),
        new_ducklake_destination_config(),
    )
    .await;
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    let stored_catalog_url = stored_config["ducklake"]["catalog_url"].clone();

    let update_request = UpdateDestinationRequest {
        name: "DuckLake Destination (Partial)".to_owned(),
        config: UpdateApiDestinationConfig::Ducklake {
            catalog_url: UpdateField::Preserve,
            data_path: UpdateField::Set("s3://ducklake/patched/".to_owned()),
            pool_size: UpdateField::Preserve,
            s3_access_key_id: UpdateField::Clear,
            s3_secret_access_key: UpdateField::Preserve,
            s3_region: UpdateField::Preserve,
            s3_endpoint: UpdateField::Preserve,
            s3_url_style: UpdateField::Preserve,
            s3_use_ssl: UpdateField::Preserve,
            metadata_schema: UpdateField::Preserve,
            maintenance_target_file_size: UpdateField::Preserve,
            expire_snapshots_older_than: UpdateField::Preserve,
            maintenance_mode: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    assert_eq!(stored_config["ducklake"]["data_path"], "s3://ducklake/patched/");
    assert_eq!(stored_config["ducklake"]["catalog_url"], stored_catalog_url);
    assert!(stored_config["ducklake"].get("s3_access_key_id").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_destination_cannot_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: updated_name(),
        config: UpdateApiDestinationConfig::from_api_config(updated_destination_config()),
    };
    let response = app.update_destination(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination =
        CreateDestinationRequest { name: new_name(), config: new_bigquery_destination_config() };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let response = app.delete_destination(tenant_id, destination_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_destination_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_destination(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_destinations_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let destination1_id = create_destination_with_config(
        &app,
        tenant_id,
        new_name(),
        new_bigquery_destination_config(),
    )
    .await;
    let destination2_id = create_destination_with_config(
        &app,
        tenant_id,
        updated_name(),
        updated_destination_config(),
    )
    .await;

    // Act
    let response = app.read_all_destinations(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadDestinationsResponse =
        response.json().await.expect("failed to deserialize response");
    for destination in response.destinations {
        if destination.id == destination1_id {
            let name = new_name();
            assert_eq!(&destination.tenant_id, tenant_id);
            assert_eq!(destination.name, name);
            insta::assert_debug_snapshot!(destination.config);
        } else if destination.id == destination2_id {
            let name = updated_name();
            assert_eq!(&destination.tenant_id, tenant_id);
            assert_eq!(destination.name, name);
            insta::assert_debug_snapshot!(destination.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_with_active_pipeline_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    // Create source and destination
    let source_id = create_source(&app, tenant_id).await;
    let (destination_id, _pipeline_id) =
        create_destination_pipeline_for_source(&app, tenant_id, source_id).await;

    // Act - Try to delete the destination
    let response = app.delete_destination(tenant_id, destination_id).await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Verify destination still exists
    let destination_response = app.read_destination(tenant_id, destination_id).await;
    assert!(destination_response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_with_inactive_pipeline_cannot_be_deleted() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    let (_source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;
    let (destination_id, pipeline_id) =
        create_destination_pipeline_for_source(&app, tenant_id, source_id).await;

    app.k8s_state.set_pod_status(PodStatus::Stopped).await;

    let response = app.delete_destination(tenant_id, destination_id).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let destination_response = app.read_destination(tenant_id, destination_id).await;
    assert!(destination_response.status().is_success());

    let pipeline_response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert!(pipeline_response.status().is_success());

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snowflake_destination_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let destination = CreateDestinationRequest {
        name: "Snowflake Destination".to_owned(),
        config: new_snowflake_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_snowflake_destination_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: "Snowflake Destination".to_owned(),
        config: new_snowflake_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let response = app.read_destination(tenant_id, destination_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, destination.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_snowflake_destination_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: "Snowflake Destination".to_owned(),
        config: new_snowflake_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: "Snowflake Destination (Updated)".to_owned(),
        config: UpdateApiDestinationConfig::from_api_config(updated_snowflake_destination_config()),
    };
    let response = app.update_destination(tenant_id, destination_id, &updated_config).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn snowflake_destination_update_preserves_and_clears_encrypted_fields() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let mut config = new_snowflake_destination_config();
    if let ApiDestinationConfig::Snowflake { private_key_passphrase, .. } = &mut config {
        *private_key_passphrase = Some(SerializableSecretString::from("passphrase".to_owned()));
    }

    let destination_id =
        create_destination_with_config(&app, tenant_id, "Snowflake Destination".to_owned(), config)
            .await;
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    let stored_private_key = stored_config["snowflake"]["private_key"].clone();

    let update_request = UpdateDestinationRequest {
        name: "Snowflake Destination (Partial)".to_owned(),
        config: UpdateApiDestinationConfig::Snowflake {
            account_id: UpdateField::Preserve,
            user: UpdateField::Preserve,
            private_key: UpdateField::Preserve,
            private_key_passphrase: UpdateField::Clear,
            database: UpdateField::Set("patched_database".to_owned()),
            schema: UpdateField::Preserve,
            role: UpdateField::Preserve,
        },
    };
    let response = app.update_destination(tenant_id, destination_id, &update_request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let stored_config = read_stored_destination_config(&app, destination_id).await;
    assert_eq!(stored_config["snowflake"]["database"], "patched_database");
    assert_eq!(stored_config["snowflake"]["private_key"], stored_private_key);
    assert!(stored_config["snowflake"].get("private_key_passphrase").is_none());
}
