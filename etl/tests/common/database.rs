use postgres::schema::TableName;
use postgres::tokio::config::{PgConnectionConfig, PgTlsConfig};
use postgres::tokio::test_utils::PgDatabase;
use secrecy::Secret;
use tokio_postgres::config::SslMode;
use tokio_postgres::Client;
use uuid::Uuid;

/// The schema name used for organizing test tables.
///
/// This constant defines the default schema where test tables are created,
/// providing isolation from other database objects.
const TEST_DATABASE_SCHEMA: &str = "test";

/// Creates a [`TableName`] in the test schema.
///
/// This helper function constructs a [`TableName`] with the schema set to [`TEST_DATABASE_SCHEMA`]
/// and the provided name as the table name. It's used to ensure consistent table naming
/// across test scenarios.
pub fn test_table_name(name: &str) -> TableName {
    TableName {
        schema: TEST_DATABASE_SCHEMA.to_owned(),
        name: name.to_owned(),
    }
}

/// Creates a new test database instance with a unique name.
///
/// This function spawns a new PostgreSQL database with a random UUID as its name,
/// using default credentials and disabled SSL. It automatically creates the test schema
/// for organizing test tables.
///
/// # Panics
///
/// Panics if the test schema cannot be created.
pub async fn spawn_database() -> PgDatabase<Client> {
    let options = PgConnectionConfig {
        host: "localhost".to_owned(),
        port: 5430,
        // We create a random database name to avoid conflicts with existing databases.
        name: Uuid::new_v4().to_string(),
        username: "postgres".to_owned(),
        password: Some(Secret::from("postgres".to_owned())),
        tls_config: PgTlsConfig {
            ssl_mode: SslMode::Disable,
            trusted_root_certs: vec![],
        },
    };

    let database = PgDatabase::new(options).await;

    // Create the test schema.
    database
        .client
        .as_ref()
        .unwrap()
        .execute(&format!("CREATE SCHEMA {TEST_DATABASE_SCHEMA}"), &[])
        .await
        .expect("Failed to create test schema");

    database
}
