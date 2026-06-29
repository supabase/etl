//! Test database utilities for spawning isolated PostgreSQL instances.
//!
//! # PostgreSQL Configuration Requirements
//!
//! For tests to complete in a timely manner, the PostgreSQL instance must be
//! configured with a low `wal_sender_timeout` value. PostgreSQL uses
//! `wal_sender_timeout / 2` to determine the interval for sending status
//! updates (keep-alive messages) during idle periods in logical replication.
//!
//! For example, with `wal_sender_timeout = 10s`, keep-alive messages are sent
//! every 5 seconds. This is important because the apply loop relies on these
//! messages to trigger table synchronization state transitions when there is no
//! WAL activity.
//!
//! The recommended configuration for tests is:
//! ```text
//! wal_sender_timeout = 10s
//! ```
//!
//! See `scripts/docker/docker-compose.yaml` for the test database
//! configuration.

use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig};
use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
use pg_escape::quote_identifier;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::{postgres::migrations, schema::TableName};

/// The schema name used for organizing test tables.
///
/// This constant defines the default schema where test tables are created,
/// providing isolation from other database objects.
pub const TEST_DATABASE_SCHEMA: &str = "test";

const DEFAULT_DATABASE_HOST: &str = "localhost";
const DEFAULT_DATABASE_PORT: &str = "5430";
const DEFAULT_DATABASE_USERNAME: &str = "postgres";
const DEFAULT_DATABASE_PASSWORD: &str = "postgres";
const READ_REPLICA_PORT_OFFSET: u16 = 1000;
/// Creates a [`TableName`] in the test schema.
///
/// This helper function constructs a [`TableName`] with the schema set to the
/// test schema and the provided name as the table name. It's used to ensure
/// consistent table naming across test scenarios.
pub fn test_table_name(name: &str) -> TableName {
    TableName { schema: TEST_DATABASE_SCHEMA.to_owned(), name: name.to_owned() }
}

/// Generates Postgres connection configuration for isolated test databases.
///
/// This function creates connection parameters for a local Postgres instance
/// with test-specific settings designed for isolation, reproducibility, and
/// ease of debugging. Each invocation creates a unique database name to prevent
/// test interference.
///
/// Configuration is read from environment variables, falling back to defaults
/// suitable for the local Docker Compose setup:
/// - `TESTS_DATABASE_HOST`: Postgres server hostname (default: `localhost`)
/// - `TESTS_DATABASE_PORT`: Postgres server port (default: `5430`)
/// - `TESTS_DATABASE_USERNAME`: Database user (default: `postgres`)
/// - `TESTS_DATABASE_PASSWORD`: Database password (default: `postgres`)
/// - `TESTS_DATABASE_TLS_ENABLED`: Whether test clients use TLS (default:
///   `false`)
/// - `TESTS_DATABASE_TLS_ROOT_CERT`: Path to the trusted root certificate
fn local_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").unwrap_or(DEFAULT_DATABASE_HOST.into()),
        hostaddr: None,
        port: std::env::var("TESTS_DATABASE_PORT")
            .unwrap_or(DEFAULT_DATABASE_PORT.into())
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        name: Uuid::new_v4().to_string(),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .unwrap_or(DEFAULT_DATABASE_USERNAME.into()),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .or(Some(DEFAULT_DATABASE_PASSWORD.into()))
            .map(Into::into),
        tls: local_tls_config_from_env(),
        keepalive: TcpKeepaliveConfig::default(),
    }
}

/// Generates Postgres connection configuration for the test read replica.
///
/// The database name and credentials are inherited from `source_config`, while
/// the host and port default to the local Docker Compose replica convention.
/// Override them with `TESTS_DATABASE_REPLICA_HOST` and
/// `TESTS_DATABASE_REPLICA_PORT` when running against a custom standby.
pub fn local_pg_read_replica_connection_config(
    source_config: &PgConnectionConfig,
) -> PgConnectionConfig {
    let default_replica_port = source_config
        .port
        .checked_add(READ_REPLICA_PORT_OFFSET)
        .expect("source Postgres port plus read replica offset should fit in the valid port range");

    let mut replica_config = source_config.clone();
    replica_config.host =
        std::env::var("TESTS_DATABASE_REPLICA_HOST").unwrap_or_else(|_| source_config.host.clone());
    replica_config.hostaddr = None;
    replica_config.port = std::env::var("TESTS_DATABASE_REPLICA_PORT")
        .unwrap_or_else(|_| default_replica_port.to_string())
        .parse()
        .expect("TESTS_DATABASE_REPLICA_PORT must be a valid port number");

    replica_config
}

/// Creates a new test database instance with a unique name and runs
/// migrations.
///
/// This function spawns a new Postgres database with a random UUID as its name,
/// using default credentials and disabled SSL. It automatically creates the
/// test schema for organizing test tables and runs all ETL
/// migrations.
///
/// # Panics
///
/// Panics if the test schema cannot be created or migrations fail.
pub async fn spawn_source_database() -> PgDatabase<Client> {
    // We create the database via tokio postgres.
    let config = local_pg_connection_config();
    let database = PgDatabase::new(config.clone()).await;

    // We create the test schema, where all tables will be added.
    database
        .client
        .as_ref()
        .expect("database client should be initialized")
        .execute(&format!("create schema {}", quote_identifier(TEST_DATABASE_SCHEMA)), &[])
        .await
        .expect("Failed to create test schema");

    migrations::run_source_migrations(&config).await.expect("Failed to run source migrations");

    database
}
