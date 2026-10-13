//! Test utilities for ClickHouse destinations.

use clickhouse::Client;
use etl::store::{SchemaStore, StateStore};
use tokio::runtime::Handle;
use url::Url;
use uuid::Uuid;

use crate::clickhouse::{
    ClickHouseClientConfig, ClickHouseDestination, ClickHouseInserterConfig,
    sql::{quote_identifier, quote_string_literal},
};

/// ClickHouse HTTP URL (e.g. `http://localhost:8123`).
pub const CLICKHOUSE_URL_ENV: &str = "TESTS_CLICKHOUSE_URL";
/// ClickHouse user name (required).
pub const CLICKHOUSE_USER_ENV: &str = "TESTS_CLICKHOUSE_USER";
/// ClickHouse password (optional -- omit or leave empty for passwordless
/// access).
pub const CLICKHOUSE_PASSWORD_ENV: &str = "TESTS_CLICKHOUSE_PASSWORD";

/// Returns the ClickHouse HTTP URL from the environment.
///
/// # Panics
///
/// Panics if [`CLICKHOUSE_URL_ENV`] is not set or is not a valid URL.
pub fn get_clickhouse_url() -> Url {
    let value = std::env::var(CLICKHOUSE_URL_ENV)
        .unwrap_or_else(|_| panic!("{CLICKHOUSE_URL_ENV} must be set"));
    Url::parse(&value)
        .unwrap_or_else(|error| panic!("{CLICKHOUSE_URL_ENV} must be a valid URL: {error}"))
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
    url: Url,
    user: String,
    password: Option<String>,
    database: String,
    /// User created by [`Self::use_user_with_settings`], dropped with the
    /// database.
    settings_user: Option<String>,
}

impl ClickHouseTestDatabase {
    fn new(url: Url, user: String, password: Option<String>, database: String) -> Self {
        let build_client = |db: Option<&str>| {
            let mut c = Client::default().with_url(url.as_str()).with_user(&user);
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
            settings_user: None,
        }
    }

    /// Creates the test database in ClickHouse, retrying on transient errors.
    pub async fn create_database(&self) {
        let database = quote_identifier(&self.database);
        let query = format!("CREATE DATABASE IF NOT EXISTS {database}");
        for attempt in 1..=5 {
            match self.root_client.query(&query).execute().await {
                Ok(()) => return,
                Err(e) if attempt < 5 => {
                    eprintln!(
                        "warning: create_database attempt {attempt}/5 failed: {e}, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200 * attempt)).await;
                }
                Err(e) => panic!("Failed to create test ClickHouse database after 5 attempts: {e}"),
            }
        }
    }

    /// Drops the test database from ClickHouse.
    pub async fn drop_database(&self) {
        let database = quote_identifier(&self.database);
        self.root_client
            .query(&format!("DROP DATABASE IF EXISTS {database}"))
            .execute()
            .await
            .expect("Failed to drop test ClickHouse database");
    }

    /// Routes destinations built afterward through a new user whose profile
    /// applies `settings`, a ClickHouse `SETTINGS` list such as
    /// `async_insert = 1`.
    ///
    /// The user can access only this database and is dropped with it. Query
    /// helpers keep using the original user.
    pub async fn use_user_with_settings(&mut self, settings: &str) {
        let user = format!("etl_tests_user_{}", Uuid::new_v4().simple());
        let password = Uuid::new_v4().simple().to_string();
        let quoted_user = quote_identifier(&user);
        self.root_client
            .query(&format!(
                "create user {quoted_user} identified with plaintext_password by {} settings \
                 {settings}",
                quote_string_literal(&password)
            ))
            .execute()
            .await
            .expect("Failed to create test ClickHouse user");
        // Record the user before granting so `Drop` removes it if the grant
        // fails.
        self.settings_user = Some(user.clone());
        self.root_client
            .query(&format!("grant all on {}.* to {quoted_user}", quote_identifier(&self.database)))
            .execute()
            .await
            .expect("Failed to grant test ClickHouse user access");
        self.user = user;
        self.password = Some(password);
    }

    /// Builds a [`ClickHouseDestination`] scoped to this test database with
    /// default inserter config (100 MiB per INSERT -- large enough that tests
    /// never hit an intermediate flush). Validates engine support eagerly so
    /// tests fail fast on engine/version mismatch.
    pub async fn build_destination<S>(&self, store: S) -> ClickHouseDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        self.build_destination_with_engine(store, etl_config::shared::ClickHouseEngine::default())
            .await
    }

    /// Builds a [`ClickHouseDestination`] for the given engine.
    pub async fn build_destination_with_engine<S>(
        &self,
        store: S,
        engine: etl_config::shared::ClickHouseEngine,
    ) -> ClickHouseDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        self.build_destination_with_config(
            store,
            ClickHouseInserterConfig { max_bytes_per_insert: 100 * 1024 * 1024, engine },
        )
        .await
    }

    /// Builds a [`ClickHouseDestination`] scoped to this test database with a
    /// caller-supplied [`ClickHouseInserterConfig`]. Validates engine support
    /// eagerly so tests fail fast on engine/version mismatch.
    pub async fn build_destination_with_config<S>(
        &self,
        store: S,
        config: ClickHouseInserterConfig,
    ) -> ClickHouseDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        let destination = ClickHouseDestination::new(
            self.url.clone(),
            &self.user,
            self.password.clone(),
            &self.database,
            config,
            ClickHouseClientConfig::default(),
            store,
        )
        .expect("Failed to create ClickHouseDestination for test");
        destination
            .validate_engine_support()
            .await
            .expect("ClickHouse engine support check failed in test setup");
        destination
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
        self.db_client.query(sql).fetch_all::<T>().await.expect("ClickHouse query failed")
    }

    /// Returns the underlying ClickHouse client for fallible queries.
    pub fn db_client(&self) -> &Client {
        &self.db_client
    }

    /// Returns the column names of a ClickHouse table in position order,
    /// excluding both engines' trailing CDC columns.
    pub async fn column_names(&self, table_name: &str) -> Vec<String> {
        self.column_types(table_name).await.into_iter().map(|(name, _)| name).collect()
    }

    /// Returns the column names and ClickHouse type strings in position order,
    /// excluding both engines' trailing CDC columns (`cdc_operation`,
    /// `cdc_lsn`, `cdc_tx_ordinal`, `_etl_version`, `_etl_deleted`).
    pub async fn column_types(&self, table_name: &str) -> Vec<(String, String)> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Col {
            name: String,
            type_name: String,
        }
        self.db_client
            .query(
                "SELECT name, type AS type_name FROM system.columns WHERE database = ? AND table \
                 = ? AND name NOT IN ('cdc_operation', 'cdc_lsn', 'cdc_tx_ordinal', \
                 '_etl_version', '_etl_deleted') ORDER BY position",
            )
            .bind(&self.database)
            .bind(table_name)
            .fetch_all::<Col>()
            .await
            .expect("failed to query system.columns")
            .into_iter()
            .map(|c| (c.name, c.type_name))
            .collect()
    }
}

impl Drop for ClickHouseTestDatabase {
    fn drop(&mut self) {
        let root_client = self.root_client.clone();
        let database = quote_identifier(&self.database);
        let settings_user = self.settings_user.as_deref().map(quote_identifier);

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(move || {
                Handle::current().block_on(async move {
                    if let Err(error) = root_client
                        .query(&format!("DROP DATABASE IF EXISTS {database}"))
                        .execute()
                        .await
                    {
                        eprintln!("warning: failed to drop test ClickHouse database: {error}");
                    }
                    if let Some(user) = settings_user
                        && let Err(error) = root_client
                            .query(&format!("drop user if exists {user}"))
                            .execute()
                            .await
                    {
                        eprintln!("warning: failed to drop test ClickHouse user: {error}");
                    }
                });
            });
        }));
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
