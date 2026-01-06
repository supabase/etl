use etl_config::shared::{PgConnectionConfig, TlsConfig};
use etl_postgres::replication::connect_to_source_database;
use etl_postgres::tokio::test_utils::PgDatabase;
use etl_postgres::types::TableName;
use tokio_postgres::Client;
use uuid::Uuid;

/// The schema name used for organizing test tables.
///
/// This constant defines the default schema where test tables are created,
/// providing isolation from other database objects.
pub const TEST_DATABASE_SCHEMA: &str = "test";

/// Creates a [`TableName`] in the test schema.
///
/// This helper function constructs a [`TableName`] with the schema set to the test schema
/// and the provided name as the table name. It's used to ensure consistent table naming
/// across test scenarios.
pub fn test_table_name(name: &str) -> TableName {
    TableName {
        schema: TEST_DATABASE_SCHEMA.to_owned(),
        name: name.to_owned(),
    }
}

/// Generates Postgres connection configuration for isolated test databases.
///
/// This function creates connection parameters for a local Postgres instance with
/// test-specific settings designed for isolation, reproducibility, and ease of debugging.
/// Each invocation creates a unique database name to prevent test interference.
///
/// Configuration is read from environment variables:
/// - `TESTS_DATABASE_HOST`: Postgres server hostname (required)
/// - `TESTS_DATABASE_PORT`: Postgres server port (required)
/// - `TESTS_DATABASE_USERNAME`: Database user (required)
/// - `TESTS_DATABASE_PASSWORD`: Database password (optional)
fn local_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set"),
        port: std::env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        // Generate unique database name for test isolation
        name: Uuid::new_v4().to_string(),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .expect("TESTS_DATABASE_USERNAME must be set"),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .map(Into::into),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
        keepalive: None,
    }
}

/// Creates a new test database instance with a unique name.
///
/// This function spawns a new Postgres database with a random UUID as its name,
/// using default credentials and disabled SSL. It automatically creates the test schema
/// for organizing test tables.
///
/// # Panics
///
/// Panics if the test schema cannot be created.
pub async fn spawn_source_database() -> PgDatabase<Client> {
    // We create the database via tokio postgres.
    let config = local_pg_connection_config();
    let database = PgDatabase::new(config).await;

    // We create the test schema, where all tables will be added.
    database
        .client
        .as_ref()
        .unwrap()
        .execute(&format!("create schema {TEST_DATABASE_SCHEMA}"), &[])
        .await
        .expect("Failed to create test schema");

    database
}

/// Creates a new test database instance with a unique name and all the ETL migrations run.
///
/// This function spawns a new Postgres database with a random UUID as its name,
/// using default credentials and disabled SSL. It automatically creates the test schema
/// for organizing test tables.
///
/// # Panics
///
/// Panics if the test schema cannot be created.
pub async fn spawn_source_database_for_store() -> PgDatabase<Client> {
    // We create the database via tokio postgres.
    let config = local_pg_connection_config();
    let database = PgDatabase::new(config.clone()).await;

    // We now connect via sqlx just to run the migrations, but we still use the original tokio postgres
    // connection for the db object returned.
    let pool = connect_to_source_database(&config, 1, 1)
        .await
        .expect("Failed to connect with sqlx");

    // Create the `etl` schema first.
    sqlx::query("create schema if not exists etl")
        .execute(&pool)
        .await
        .expect("Failed to create 'etl' schema");

    // Set the `etl` schema as search path (this is done to have the migrations metadata table created
    // by sqlx within the `etl` schema).
    sqlx::query("set search_path = 'etl';")
        .execute(&pool)
        .await
        .expect("Failed to set search path to 'etl'");

    // Run replicator migrations to create the state store tables.
    sqlx::migrate!("../etl-replicator/migrations")
        .run(&pool)
        .await
        .expect("Failed to run replicator migrations");

    database
}
