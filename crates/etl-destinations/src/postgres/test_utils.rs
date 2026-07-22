//! Test utilities for the Postgres destination.

use etl::store::{SchemaStore, StateStore};
use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig};
use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::postgres::PostgresDestination;

/// Default schema override used by destination integration tests.
pub const TEST_DESTINATION_SCHEMA: &str = "replica";

/// Builds a destination database config on the same server as the test source.
pub fn destination_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_owned()),
        hostaddr: None,
        port: std::env::var("TESTS_DATABASE_PORT")
            .unwrap_or_else(|_| "5430".to_owned())
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        name: format!("etl_pg_dest_{}", Uuid::new_v4().simple()),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .unwrap_or_else(|_| "postgres".to_owned()),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .or_else(|| Some("postgres".to_owned()))
            .map(Into::into),
        tls: local_tls_config_from_env(),
        keepalive: TcpKeepaliveConfig::default(),
    }
}

/// Isolated Postgres database used as a destination in tests.
pub struct PostgresTestDatabase {
    database: PgDatabase<Client>,
}

impl PostgresTestDatabase {
    /// Creates a unique destination database on the test Postgres server.
    pub async fn setup() -> Self {
        let config = destination_pg_connection_config();
        Self { database: PgDatabase::new(config).await }
    }

    /// Returns the destination connection config.
    pub fn config(&self) -> &PgConnectionConfig {
        &self.database.config
    }

    /// Builds a [`PostgresDestination`] scoped to this database.
    pub fn build_destination<S>(&self, store: S) -> PostgresDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        PostgresDestination::new(
            self.config().clone(),
            Some(TEST_DESTINATION_SCHEMA.to_owned()),
            store,
        )
    }

    /// Runs a SQL statement against the destination database.
    pub async fn run_sql(&self, sql: &str) {
        self.database.run_sql(sql).await.expect("destination sql failed");
    }

    /// Returns raw query rows.
    pub async fn query(&self, sql: &str) -> Vec<Row> {
        let client = self.database.client.as_ref().expect("destination client missing");
        client.query(sql, &[]).await.expect("destination query failed")
    }

    /// Returns column names for a destination table in ordinal order.
    pub async fn column_names(&self, schema: &str, table: &str) -> Vec<String> {
        let client = self.database.client.as_ref().expect("destination client missing");
        let rows = client
            .query(
                "select column_name from information_schema.columns where table_schema = $1 and \
                 table_name = $2 order by ordinal_position",
                &[&schema, &table],
            )
            .await
            .expect("column lookup failed");
        rows.into_iter().map(|row| row.get(0)).collect()
    }
}

/// Sets up a destination database for tests.
pub async fn setup_postgres_destination_database() -> PostgresTestDatabase {
    PostgresTestDatabase::setup().await
}
