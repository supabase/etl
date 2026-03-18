#![allow(dead_code)]

use etl_api::configs::source::FullApiSourceConfig;
use etl_api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use etl_config::SerializableSecretString;
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use etl_postgres::replication::connect_to_source_database;
use etl_postgres::sqlx::test_utils::create_pg_database;
use pg_escape::{quote_identifier, quote_literal};
use secrecy::ExposeSecret;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

use crate::support::test_app::TestApp;

/// Creates a database configuration from TESTS_DATABASE_* environment variables.
///
/// Generates a unique database name using a UUID suffix to avoid conflicts
/// between concurrent test runs.
pub fn get_test_db_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set"),
        port: std::env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        name: format!("test_db_{}", Uuid::new_v4()),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .expect("TESTS_DATABASE_USERNAME must be set"),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .map(Into::into),
        tls: TlsConfig::disabled(),
        keepalive: TcpKeepaliveConfig::default(),
    }
}

/// Creates and configures a new Postgres database for the API.
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

    let source_config = FullApiSourceConfig {
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

pub struct TrustedSourceDatabase {
    pub admin_pool: PgPool,
    pub admin_config: PgConnectionConfig,
    pub trusted_config: PgConnectionConfig,
    pub trusted_username: String,
}

pub async fn create_trusted_source_database() -> TrustedSourceDatabase {
    let mut admin_config = get_test_db_config();
    admin_config.name = format!("test_trusted_source_db_{}", Uuid::new_v4());

    let admin_pool = create_pg_database(&admin_config).await;
    let trusted_username = format!(
        "supabase_etl_admin_{}",
        &Uuid::new_v4().simple().to_string()[..12]
    );
    let trusted_password = format!("trusted-{}", Uuid::new_v4().simple());

    let mut connection = PgConnection::connect_with(&admin_config.without_db(None))
        .await
        .expect("Failed to connect to Postgres");

    connection
        .execute(&*format!(
            "create role {} with login password {} nosuperuser inherit nocreaterole nocreatedb replication bypassrls connection limit -1",
            quote_identifier(&trusted_username),
            quote_literal(&trusted_password),
        ))
        .await
        .expect("Failed to create trusted ETL role");

    connection
        .execute(&*format!(
            "grant create on database {} to {}",
            quote_identifier(&admin_config.name),
            quote_identifier(&trusted_username),
        ))
        .await
        .expect("Failed to grant CREATE on database to trusted ETL role");

    let mut trusted_config = admin_config.clone();
    trusted_config.username = trusted_username.clone();
    trusted_config.password = Some(trusted_password.into());

    TrustedSourceDatabase {
        admin_pool,
        admin_config,
        trusted_config,
        trusted_username,
    }
}

pub async fn drop_trusted_source_database(database: TrustedSourceDatabase) {
    let TrustedSourceDatabase {
        admin_pool,
        admin_config,
        trusted_config,
        trusted_username,
    } = database;

    drop(admin_pool);

    etl_postgres::sqlx::test_utils::drop_pg_database(&admin_config).await;

    let mut connection = PgConnection::connect_with(&admin_config.without_db(None))
        .await
        .expect("Failed to connect to Postgres");

    connection
        .execute(&*format!(
            "drop role if exists {}",
            quote_identifier(&trusted_username),
        ))
        .await
        .expect("Failed to drop trusted ETL role");

    drop(trusted_config);
}

/// Runs ETL migrations on the source database.
///
/// Sets up the `etl` schema and runs Postgres state store migrations to create the
/// tables ETL uses to persist the replication state needed for ETL operations.
///
/// # Panics
/// Panics if database connection fails, schema creation fails, or migrations fail.
pub async fn run_etl_migrations_on_source_database(source_db_config: &PgConnectionConfig) {
    // We create a pool just for the migrations.
    let source_pool = connect_to_source_database(source_db_config, 1, 1, None)
        .await
        .expect("failed to connect to source database");

    // Create the `etl` schema first.
    sqlx::query("create schema if not exists etl")
        .execute(&source_pool)
        .await
        .expect("failed to create etl schema");

    // Set the `etl` schema as search path (this is done to have the migrations metadata table created
    // by sqlx within the `etl` schema).
    sqlx::query("set search_path = 'etl';")
        .execute(&source_pool)
        .await
        .expect("failed to set search path");

    // Run etl migrations to create the state store tables.
    sqlx::migrate!("../etl/migrations")
        .run(&source_pool)
        .await
        .expect("failed to run etl migrations");
}
