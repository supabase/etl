use etl_api::db::sources::SourceConfig;
use etl_api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use etl_config::SerializableSecretString;
use etl_config::shared::PgConnectionConfig;
use etl_postgres::replication::connect_to_source_database;
use etl_postgres::sqlx::test_utils::create_pg_database;
use secrecy::ExposeSecret;
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::test_app::TestApp;

/// Creates and configures a new PostgreSQL database for the API.
///
/// Similar to [`create_pg_database`], but additionally runs all database migrations
/// from the "./migrations" directory after creation. Returns a [`PgPool`]
/// connected to the newly created and migrated database. Panics if database
/// creation or migration fails.
pub async fn create_etl_api_database(config: &PgConnectionConfig) -> PgPool {
    let connection_pool = create_pg_database(config).await;

    sqlx::migrate!("./migrations")
        .run(&connection_pool)
        .await
        .expect("Failed to migrate the database");

    connection_pool
}

pub async fn create_test_source_database(
    app: &TestApp,
    tenant_id: &str,
) -> (PgPool, i64, PgConnectionConfig) {
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

pub async fn run_etl_migrations_on_source_database(source_db_config: &PgConnectionConfig) {
    // We create a pool just for the migrations.
    let source_pool = connect_to_source_database(source_db_config, 1, 1)
        .await
        .unwrap();

    // Create the `etl` schema first.
    sqlx::query("create schema if not exists etl")
        .execute(&source_pool)
        .await
        .unwrap();

    // Set the `etl` schema as search path (this is done to have the migrations metadata table created
    // by sqlx within the `etl` schema).
    sqlx::query("set search_path = 'etl';")
        .execute(&source_pool)
        .await
        .unwrap();

    // Run replicator migrations to create the state store tables.
    sqlx::migrate!("../etl-replicator/migrations")
        .run(&source_pool)
        .await
        .unwrap();
}
