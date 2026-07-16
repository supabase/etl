use etl_api::{
    configs::source::FullApiSourceConfig,
    k8s::PodStatus,
    routes::{
        pipelines::{CreatePipelineRequest, CreatePipelineResponse},
        sources::{
            CreateSourceRequest, CreateSourceResponse, ReadSourceResponse, ReadSourcesResponse,
            UpdateSourceRequest, ValidateSourceRequest, ValidateSourceResponse,
            publications::ReadPublicationsResponse, tables::ReadTablesResponse,
        },
    },
};
use etl_config::{SerializableSecretString, shared::PgConnectionConfig};
use etl_postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use sqlx::Executor;
use uuid::Uuid;

use crate::support::{
    database::{
        create_test_source_database, create_trusted_source_database, drop_trusted_source_database,
        get_test_db_config, run_etl_migrations_on_source_database,
    },
    mocks::{
        create_default_image,
        destinations::create_destination,
        pipelines::new_pipeline_config,
        sources::{
            create_source, create_source_with_config, new_name, new_source_config, updated_name,
            updated_source_config,
        },
        tenants::create_tenant,
    },
    test_app::{
        TestApp, spawn_test_app, spawn_test_app_with_trusted_username,
        spawn_test_app_without_k8s_client,
    },
};

fn source_config_from_db_config(source_db_config: &PgConnectionConfig) -> FullApiSourceConfig {
    FullApiSourceConfig {
        host: source_db_config.host.clone(),
        hostaddr: source_db_config.hostaddr,
        port: source_db_config.port,
        name: source_db_config.name.clone(),
        username: source_db_config.username.clone(),
        password: source_db_config
            .password
            .as_ref()
            .map(|password| SerializableSecretString::from(password.expose_secret().to_owned())),
    }
}

async fn create_pipeline_for_source(app: &TestApp, tenant_id: &str, source_id: i64) -> i64 {
    let destination_id = create_destination(app, tenant_id).await;
    let pipeline =
        CreatePipelineRequest { source_id, destination_id, config: new_pipeline_config() };
    let pipeline_response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(pipeline_response.status().is_success());
    let pipeline_response: CreatePipelineResponse =
        pipeline_response.json().await.expect("failed to deserialize response");

    pipeline_response.id
}

#[tokio::test(flavor = "multi_thread")]
async fn source_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let source = CreateSourceRequest { name: new_name(), config: new_source_config() };
    let response = app.create_source(tenant_id, &source).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_table_and_publication_responses_use_partition_identity_ids() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute(
            "create table public.partitioned_copy_selection_test (id bigint) partition by range \
             (id)",
        )
        .await
        .expect("failed to create partitioned source table");
    source_pool
        .execute(
            "create table public.partitioned_copy_selection_test_low partition of \
             public.partitioned_copy_selection_test for values from (minvalue) to (100)",
        )
        .await
        .expect("failed to create low source table partition");
    source_pool
        .execute(
            "create table public.partitioned_copy_selection_test_high partition of \
             public.partitioned_copy_selection_test for values from (100) to (maxvalue)",
        )
        .await
        .expect("failed to create high source table partition");
    source_pool
        .execute(
            "create publication root_copy_selection_pub for table \
             public.partitioned_copy_selection_test with (publish_via_partition_root = true)",
        )
        .await
        .expect("failed to create partition-root source publication");
    source_pool
        .execute(
            "create publication leaf_copy_selection_pub for table \
             public.partitioned_copy_selection_test with (publish_via_partition_root = false)",
        )
        .await
        .expect("failed to create partition-leaf source publication");
    source_pool
        .execute("create publication a_empty_copy_selection_pub")
        .await
        .expect("failed to create leading empty source publication");
    source_pool
        .execute("create publication z_empty_copy_selection_pub")
        .await
        .expect("failed to create trailing empty source publication");

    let expected_partition_tables: Vec<(i64, String, String)> = sqlx::query_as(
        r#"
        select c.oid::bigint, n.nspname, c.relname
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
            and c.relname in (
                'partitioned_copy_selection_test',
                'partitioned_copy_selection_test_high',
                'partitioned_copy_selection_test_low'
            )
        order by n.nspname, c.relname
        "#,
    )
    .fetch_all(&source_pool)
    .await
    .expect("failed to read partition table oids");
    let expected_partition_tables: Vec<(u32, String, String)> = expected_partition_tables
        .into_iter()
        .map(|(id, schema, name)| {
            (u32::try_from(id).expect("Postgres OIDs fit in u32"), schema, name)
        })
        .collect();
    assert_eq!(expected_partition_tables.len(), 3);
    let expected_root_table = expected_partition_tables
        .iter()
        .find(|(_, _, name)| name == "partitioned_copy_selection_test")
        .cloned()
        .expect("partition root missing from pg_class");
    let expected_leaf_tables: Vec<_> = expected_partition_tables
        .iter()
        .filter(|(_, _, name)| name != "partitioned_copy_selection_test")
        .cloned()
        .collect();

    let tables_response = app.read_source_tables(tenant_id, source_id).await;
    assert!(tables_response.status().is_success());
    let tables_response: ReadTablesResponse =
        tables_response.json().await.expect("failed to deserialize table response");
    assert!(tables_response.tables.is_sorted_by(|left, right| {
        (&left.schema, &left.name) <= (&right.schema, &right.name)
    }));
    let partition_tables: Vec<_> = tables_response
        .tables
        .iter()
        .filter(|table| {
            table.schema == "public"
                && matches!(
                    table.name.as_str(),
                    "partitioned_copy_selection_test"
                        | "partitioned_copy_selection_test_high"
                        | "partitioned_copy_selection_test_low"
                )
        })
        .map(|table| (table.id, table.schema.clone(), table.name.clone()))
        .collect();
    assert_eq!(partition_tables, expected_partition_tables);

    let publications_response = app.read_source_publications(tenant_id, source_id).await;
    assert!(publications_response.status().is_success());
    let publications_response: ReadPublicationsResponse =
        publications_response.json().await.expect("failed to deserialize publication response");
    assert!(
        publications_response
            .publications
            .is_sorted_by(|left, right| left.name.as_str() <= right.name.as_str())
    );
    assert_eq!(
        publications_response
            .publications
            .iter()
            .map(|publication| publication.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "a_empty_copy_selection_pub",
            "leaf_copy_selection_pub",
            "root_copy_selection_pub",
            "z_empty_copy_selection_pub",
        ]
    );
    assert!(publications_response.publications.iter().all(|publication| {
        publication
            .tables
            .is_sorted_by(|left, right| (&left.schema, &left.name) <= (&right.schema, &right.name))
    }));
    assert!(
        publications_response
            .publications
            .iter()
            .filter(|publication| publication.name.contains("empty_copy_selection_pub"))
            .all(|publication| publication.tables.is_empty())
    );

    let root_publication = publications_response
        .publications
        .iter()
        .find(|publication| publication.name == "root_copy_selection_pub")
        .expect("partition-root publication missing from response");
    let root_publication_tables: Vec<_> = root_publication
        .tables
        .iter()
        .map(|table| (table.id, table.schema.clone(), table.name.clone()))
        .collect();
    assert_eq!(root_publication_tables, vec![expected_root_table.clone()]);

    let leaf_publication = publications_response
        .publications
        .iter()
        .find(|publication| publication.name == "leaf_copy_selection_pub")
        .expect("partition-leaf publication missing from response");
    let leaf_publication_tables: Vec<_> = leaf_publication
        .tables
        .iter()
        .map(|table| (table.id, table.schema.clone(), table.name.clone()))
        .collect();
    assert_eq!(leaf_publication_tables, expected_leaf_tables);
    assert!(
        leaf_publication.tables.iter().all(|table| table.id != expected_root_table.0),
        "partition-leaf publication should not expose the partition root id"
    );

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_and_recreating_a_table_yields_a_different_id() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute("create table public.recreated_table (id bigint primary key)")
        .await
        .expect("failed to create source table");
    let original_id: i64 =
        sqlx::query_scalar("select 'public.recreated_table'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .expect("failed to read source table oid");

    source_pool
        .execute("drop table public.recreated_table")
        .await
        .expect("failed to drop source table");
    source_pool
        .execute("create table public.recreated_table (id bigint primary key)")
        .await
        .expect("failed to recreate source table");
    let recreated_id: i64 =
        sqlx::query_scalar("select 'public.recreated_table'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .expect("failed to read recreated source table oid");

    assert_ne!(
        original_id, recreated_id,
        "recreating a table should assign a new OID, since a pipeline's table_sync_copy ids are \
         only meaningful for the relation that held them when selected"
    );

    let tables_response = app.read_source_tables(tenant_id, source_id).await;
    assert!(tables_response.status().is_success());
    let tables_response: ReadTablesResponse =
        tables_response.json().await.expect("failed to deserialize table response");
    let recreated_id = u32::try_from(recreated_id).expect("Postgres OIDs fit in u32");
    let table = tables_response
        .tables
        .iter()
        .find(|table| table.schema == "public" && table.name == "recreated_table")
        .expect("recreated table missing from response");
    assert_eq!(table.id, recreated_id);

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest { name: new_name(), config: new_source_config() };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let response = app.read_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, source.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest { name: new_name(), config: new_source_config() };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let updated_config =
        UpdateSourceRequest { name: updated_name(), config: updated_source_config() };
    let response = app.update_source(tenant_id, source_id, &updated_config).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    let response: ReadSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_source_with_running_pipeline_restarts_replicator() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest { name: new_name(), config: new_source_config() };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    create_default_image(&app).await;
    create_pipeline_for_source(&app, tenant_id, source_id).await;

    let create_calls_before = app.k8s_state.create_calls();
    let updated_config =
        UpdateSourceRequest { name: updated_name(), config: updated_source_config() };

    let response = app.update_source(tenant_id, source_id, &updated_config).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(app.k8s_state.create_calls() > create_calls_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_source_with_pipeline_succeeds_without_k8s_client() {
    init_test_tracing();
    let app = spawn_test_app_without_k8s_client().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest { name: new_name(), config: new_source_config() };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    create_default_image(&app).await;
    create_pipeline_for_source(&app, tenant_id, source_id).await;

    let updated_config =
        UpdateSourceRequest { name: updated_name(), config: updated_source_config() };
    let response = app.update_source(tenant_id, source_id, &updated_config).await;

    assert_eq!(response.status(), StatusCode::OK);
    let response = app.read_source(tenant_id, source_id).await;
    let response: ReadSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.name, updated_config.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let updated_config =
        UpdateSourceRequest { name: updated_name(), config: updated_source_config() };
    let response = app.update_source(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let (_source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    // Act
    let response = app.delete_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_sources_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id =
        create_source_with_config(&app, tenant_id, new_name(), new_source_config()).await;
    let source2_id =
        create_source_with_config(&app, tenant_id, updated_name(), updated_source_config()).await;

    // Act
    let response = app.read_all_sources(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadSourcesResponse =
        response.json().await.expect("failed to deserialize response");
    for source in response.sources {
        if source.id == source1_id {
            let name = new_name();
            assert_eq!(&source.tenant_id, tenant_id);
            assert_eq!(source.name, name);
            insta::assert_debug_snapshot!(source.config);
        } else if source.id == source2_id {
            let name = updated_name();
            assert_eq!(&source.tenant_id, tenant_id);
            assert_eq!(source.name, name);
            insta::assert_debug_snapshot!(source.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_with_active_pipeline_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    // Create source and destination
    let source_id = create_source(&app, tenant_id).await;
    let _pipeline_id = create_pipeline_for_source(&app, tenant_id, source_id).await;

    // Act - Try to delete the source
    let response = app.delete_source(tenant_id, source_id).await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Verify source still exists
    let source_response = app.read_source(tenant_id, source_id).await;
    assert!(source_response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn source_creation_with_matching_trusted_username_succeeds() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;
    let tenant_id = &create_tenant(&app).await;
    let source_config = source_config_from_db_config(&trusted_source.trusted_config);

    let source = CreateSourceRequest { name: new_name(), config: source_config };

    // Act
    let response = app.create_source(tenant_id, &source).await;

    // Assert
    assert!(
        response.status().is_success(),
        "Expected successful source creation with matching trusted username, got status: {}",
        response.status()
    );

    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_validation_with_matching_trusted_username_succeeds() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;
    let tenant_id = &create_tenant(&app).await;
    let request = ValidateSourceRequest {
        config: source_config_from_db_config(&trusted_source.trusted_config),
    };

    let response = app.validate_source(tenant_id, &request).await;

    assert!(response.status().is_success());
    let response: ValidateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert!(response.validation_failures.is_empty());

    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_creation_with_non_matching_trusted_username_fails() {
    init_test_tracing();

    // Arrange
    let mut source_db_config = get_test_db_config();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());

    let different_username = "different_user".to_owned();
    let app = spawn_test_app_with_trusted_username(Some(different_username)).await;
    let tenant_id = &create_tenant(&app).await;

    // Create the actual source database.
    let _source_pool = create_pg_database(&source_db_config).await;

    // Create a source config that connects to the test database with the actual
    // username.
    let source_config = source_config_from_db_config(&source_db_config);

    let source = CreateSourceRequest { name: new_name(), config: source_config };

    // Act
    let response = app.create_source(tenant_id, &source).await;

    // Assert
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Expected UNPROCESSABLE_ENTITY status for non-matching trusted username"
    );

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_validation_with_non_matching_trusted_username_returns_failure() {
    init_test_tracing();

    let mut source_db_config = get_test_db_config();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());

    let different_username = "different_user".to_owned();
    let app = spawn_test_app_with_trusted_username(Some(different_username)).await;
    let tenant_id = &create_tenant(&app).await;

    let _source_pool = create_pg_database(&source_db_config).await;

    let request = ValidateSourceRequest { config: source_config_from_db_config(&source_db_config) };

    let response = app.validate_source(tenant_id, &request).await;

    assert!(response.status().is_success());
    let response: ValidateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    assert!(
        !response.validation_failures.is_empty(),
        "Expected validation failures for non-matching trusted username"
    );

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_update_with_matching_trusted_username_succeeds() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;
    let tenant_id = &create_tenant(&app).await;
    let source_config = source_config_from_db_config(&trusted_source.trusted_config);

    let source = CreateSourceRequest { name: new_name(), config: source_config.clone() };

    let create_response = app.create_source(tenant_id, &source).await;
    assert!(create_response.status().is_success());
    let create_response: CreateSourceResponse =
        create_response.json().await.expect("failed to deserialize response");
    let source_id = create_response.id;

    // Act - Update the source with the same config.
    let updated_source = UpdateSourceRequest { name: updated_name(), config: source_config };

    let response = app.update_source(tenant_id, source_id, &updated_source).await;

    // Assert
    assert!(
        response.status().is_success(),
        "Expected successful source update with matching trusted username, got status: {}",
        response.status()
    );

    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_with_inactive_pipeline_cannot_be_deleted() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;
    let tenant_id = &create_tenant(&app).await;
    let source_config = source_config_from_db_config(&trusted_source.trusted_config);
    let source = CreateSourceRequest { name: new_name(), config: source_config };

    let response = app.create_source(tenant_id, &source).await;
    assert!(response.status().is_success());
    let response: CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    run_etl_migrations_on_source_database(&trusted_source.trusted_config).await;

    create_default_image(&app).await;
    let pipeline_id = create_pipeline_for_source(&app, tenant_id, source_id).await;

    app.k8s_state.set_pod_status(PodStatus::Stopped).await;

    trusted_source
        .admin_pool
        .execute("create table public.uninstall_test_users (id bigint primary key)")
        .await
        .expect("failed to create test table");
    trusted_source
        .admin_pool
        .execute("create publication uninstall_pub for table public.uninstall_test_users")
        .await
        .expect("failed to create publication");

    let response = app.delete_source(tenant_id, source_id).await;

    assert_eq!(response.status(), StatusCode::CONFLICT);

    let source_response = app.read_source(tenant_id, source_id).await;
    assert!(source_response.status().is_success());

    let pipeline_response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert!(pipeline_response.status().is_success());

    let etl_schema_exists: bool =
        sqlx::query_scalar("select exists(select 1 from pg_namespace where nspname = 'etl')")
            .fetch_one(&trusted_source.admin_pool)
            .await
            .expect("failed to check etl schema");
    assert!(etl_schema_exists);

    let publication_exists: bool = sqlx::query_scalar(
        "select exists(select 1 from pg_publication where pubname = 'uninstall_pub')",
    )
    .fetch_one(&trusted_source.admin_pool)
    .await
    .expect("failed to check publication");
    assert!(publication_exists);
    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_creation_with_matching_trusted_username_but_invalid_role_profile_fails() {
    init_test_tracing();

    let mut source_db_config = get_test_db_config();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());

    let trusted_username = source_db_config.username.clone();
    let app = spawn_test_app_with_trusted_username(Some(trusted_username)).await;
    let tenant_id = &create_tenant(&app).await;

    let _source_pool = create_pg_database(&source_db_config).await;

    let source = CreateSourceRequest {
        name: new_name(),
        config: source_config_from_db_config(&source_db_config),
    };

    let response = app.create_source(tenant_id, &source).await;
    let status = response.status();
    let body = response.text().await.expect("failed to read response body");

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body.contains("does not have sufficient access to use this source database"));

    drop_pg_database(&source_db_config).await;
}
