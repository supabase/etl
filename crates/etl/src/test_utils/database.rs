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

use std::time::Duration;

use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig};
use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
use pg_escape::quote_identifier;
use tokio_postgres::{Client, types::PgLsn};
use uuid::Uuid;

use crate::{postgres::migrations, schema::TableName, test_utils::notify::DEFAULT_NOTIFY_TIMEOUT};

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

/// Queries the replication slot's confirmed flush LSN and active walsender PID.
async fn query_replication_slot_state(
    client: &Client,
    slot_name: &str,
) -> Result<(PgLsn, Option<i32>), tokio_postgres::Error> {
    let row = client
        .query_one(
            "select confirmed_flush_lsn, active_pid from pg_replication_slots where slot_name = $1",
            &[&slot_name],
        )
        .await?;

    Ok((row.get(0), row.get(1)))
}

/// Returns the replication slot's confirmed flush LSN and active walsender PID.
pub async fn replication_slot_state(client: &Client, slot_name: &str) -> (PgLsn, Option<i32>) {
    query_replication_slot_state(client, slot_name)
        .await
        .expect("Failed to query replication slot state")
}

/// Result of trying to terminate the active walsender for a replication slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalsenderTermination {
    /// The replication slot has no active walsender.
    Inactive,
    /// Postgres did not terminate the active walsender.
    NotTerminated {
        /// PID of the walsender that Postgres did not terminate.
        pid: i32,
    },
    /// Postgres terminated the active walsender.
    Terminated {
        /// PID of the terminated walsender.
        pid: i32,
    },
}

/// Requests termination of the walsender with `pid`.
///
/// Returns whether Postgres terminated the backend.
///
/// # Errors
///
/// Returns an error if Postgres cannot run the termination query.
pub async fn terminate_walsender(client: &Client, pid: i32) -> Result<bool, tokio_postgres::Error> {
    let row = client.query_one("select pg_terminate_backend($1)", &[&pid]).await?;

    Ok(row.get(0))
}

/// Tries to terminate the active walsender for `slot_name`.
///
/// # Errors
///
/// Returns an error if Postgres cannot query the replication slot or run the
/// termination query.
pub async fn terminate_active_walsender(
    client: &Client,
    slot_name: &str,
) -> Result<WalsenderTermination, tokio_postgres::Error> {
    let (_, active_pid) = query_replication_slot_state(client, slot_name).await?;
    let Some(pid) = active_pid else {
        return Ok(WalsenderTermination::Inactive);
    };

    if terminate_walsender(client, pid).await? {
        Ok(WalsenderTermination::Terminated { pid })
    } else {
        Ok(WalsenderTermination::NotTerminated { pid })
    }
}

/// Waits until a replication slot has confirmed at least `expected_lsn`.
///
/// # Panics
///
/// Panics after [`DEFAULT_NOTIFY_TIMEOUT`] if the slot does not confirm the
/// expected LSN.
pub async fn wait_for_replication_slot_flush_lsn(
    client: &Client,
    slot_name: &str,
    expected_lsn: PgLsn,
) {
    tokio::time::timeout(DEFAULT_NOTIFY_TIMEOUT, async {
        loop {
            let (confirmed_flush_lsn, _) = replication_slot_state(client, slot_name).await;
            if confirmed_flush_lsn >= expected_lsn {
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for the replication slot to confirm the expected LSN");
}

/// Waits until the replication slot is served by a walsender other than
/// `old_pid`.
///
/// Returns the new walsender PID, or [`None`] if no new walsender becomes
/// active within `timeout`.
///
/// # Errors
///
/// Returns an error if Postgres cannot query the replication slot state.
pub async fn wait_for_new_walsender(
    client: &Client,
    slot_name: &str,
    old_pid: i32,
    timeout: Duration,
) -> Result<Option<i32>, tokio_postgres::Error> {
    let result = tokio::time::timeout(timeout, async {
        loop {
            let (_, active_pid) = query_replication_slot_state(client, slot_name).await?;
            if let Some(pid) = active_pid
                && pid != old_pid
            {
                return Ok::<i32, tokio_postgres::Error>(pid);
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    match result {
        Ok(result) => result.map(Some),
        Err(_) => Ok(None),
    }
}
