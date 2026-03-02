//! Test utilities for ClickHouse destinations.

use clickhouse::Client;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::PipelineId;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::clickhouse::{ClickHouseDestination, ClickHouseInserterConfig};

/// ClickHouse HTTP URL (e.g. `http://localhost:8123`).
pub const CLICKHOUSE_URL_ENV: &str = "TESTS_CLICKHOUSE_URL";
/// ClickHouse user name (required).
pub const CLICKHOUSE_USER_ENV: &str = "TESTS_CLICKHOUSE_USER";
/// ClickHouse password (optional — omit or leave empty for passwordless access).
pub const CLICKHOUSE_PASSWORD_ENV: &str = "TESTS_CLICKHOUSE_PASSWORD";

/// Returns whether ClickHouse integration tests should be skipped.
///
/// Prints a warning and returns `true` when any required env var is missing.
/// Required: [`CLICKHOUSE_URL_ENV`], [`CLICKHOUSE_USER_ENV`].
/// Optional: [`CLICKHOUSE_PASSWORD_ENV`].
pub fn skip_if_missing_clickhouse_env_vars() -> bool {
    let missing: Vec<&str> = [CLICKHOUSE_URL_ENV, CLICKHOUSE_USER_ENV]
        .iter()
        .copied()
        .filter(|var| std::env::var_os(var).is_none())
        .collect();

    if missing.is_empty() {
        return false;
    }

    eprintln!(
        "skipping clickhouse integration test: missing {}",
        missing.join(", ")
    );
    true
}

/// Returns the ClickHouse HTTP URL from the environment.
///
/// # Panics
///
/// Panics if [`CLICKHOUSE_URL_ENV`] is not set.
pub fn get_clickhouse_url() -> String {
    std::env::var(CLICKHOUSE_URL_ENV)
        .unwrap_or_else(|_| panic!("{CLICKHOUSE_URL_ENV} must be set"))
}

/// Returns the ClickHouse user name from the environment.
///
/// # Panics
///
/// Panics if [`CLICKHOUSE_USER_ENV`] is not set.
pub fn get_clickhouse_user() -> String {
    std::env::var(CLICKHOUSE_USER_ENV)
        .unwrap_or_else(|_| panic!("{CLICKHOUSE_USER_ENV} must be set"))
}

/// Returns the ClickHouse password from the environment, or `None` if unset.
pub fn get_clickhouse_password() -> Option<String> {
    std::env::var(CLICKHOUSE_PASSWORD_ENV).ok().filter(|s| !s.is_empty())
}

/// Generates a unique database name for test isolation.
pub fn random_database_name() -> String {
    format!("etl_tests_{}", Uuid::new_v4().simple())
}

/// ClickHouse connection for testing.
///
/// Wraps a [`Client`] and automatically drops the test database on [`Drop`].
pub struct ClickHouseTestDatabase {
    /// Root client (no database selected) used for CREATE/DROP DATABASE.
    root_client: Client,
    /// Client scoped to the test database for queries.
    db_client: Client,
    url: String,
    user: String,
    password: Option<String>,
    database: String,
}

impl ClickHouseTestDatabase {
    fn new(url: String, user: String, password: Option<String>, database: String) -> Self {
        let build_client = |db: Option<&str>| {
            let mut c = Client::default().with_url(&url).with_user(&user);
            if let Some(db) = db {
                c = c.with_database(db);
            }
            if let Some(pw) = &password {
                c = c.with_password(pw);
            }
            c
        };

        Self {
            root_client: build_client(None),
            db_client: build_client(Some(&database)),
            url,
            user,
            password,
            database,
        }
    }

    /// Creates the test database in ClickHouse.
    pub async fn create_database(&self) {
        self.root_client
            .query(&format!(
                "CREATE DATABASE IF NOT EXISTS `{}`",
                self.database
            ))
            .execute()
            .await
            .expect("Failed to create test ClickHouse database");
    }

    /// Drops the test database from ClickHouse.
    pub async fn drop_database(&self) {
        self.root_client
            .query(&format!(
                "DROP DATABASE IF EXISTS `{}`",
                self.database
            ))
            .execute()
            .await
            .expect("Failed to drop test ClickHouse database");
    }

    /// Builds a [`ClickHouseDestination`] scoped to this test database.
    pub fn build_destination<S>(
        &self,
        _pipeline_id: PipelineId,
        store: S,
    ) -> ClickHouseDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        ClickHouseDestination::new(
            &self.url,
            &self.user,
            self.password.clone(),
            &self.database,
            ClickHouseInserterConfig {
                // 100 MiB — large enough that tests never hit an intermediate flush.
                max_bytes_per_insert: 100 * 1024 * 1024,
            },
            store,
        )
        .expect("Failed to create ClickHouseDestination for test")
    }

    /// Fetches all rows from a ClickHouse table using the given SQL query.
    ///
    /// `T` must be an owned row type (i.e. `Value<'a> = Self`) and implement
    /// [`serde::de::DeserializeOwned`]. The caller is responsible for writing a
    /// SELECT whose columns match `T`'s fields in the correct order.
    pub async fn query<T>(&self, sql: &str) -> Vec<T>
    where
        T: for<'a> clickhouse::Row<Value<'a> = T> + serde::de::DeserializeOwned + 'static,
    {
        self.db_client
            .query(sql)
            .fetch_all::<T>()
            .await
            .expect("ClickHouse query failed")
    }
}

impl Drop for ClickHouseTestDatabase {
    fn drop(&mut self) {
        if let Ok(handle) = Handle::try_current() {
            handle.block_on(self.drop_database());
        }
    }
}

/// Creates a fresh, isolated ClickHouse database for a single test.
///
/// Reads connection parameters from environment variables:
/// - [`CLICKHOUSE_URL_ENV`] — required
/// - [`CLICKHOUSE_USER_ENV`] — required
/// - [`CLICKHOUSE_PASSWORD_ENV`] — optional
///
/// The database is dropped automatically when the returned handle is dropped.
pub async fn setup_clickhouse_database() -> ClickHouseTestDatabase {
    let url = get_clickhouse_url();
    let user = get_clickhouse_user();
    let password = get_clickhouse_password();
    let database = random_database_name();
    let db = ClickHouseTestDatabase::new(url, user, password, database);
    db.create_database().await;
    db
}
