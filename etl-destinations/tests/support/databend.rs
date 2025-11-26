#![allow(dead_code)]
#![cfg(feature = "databend")]

use databend_driver::Client as DatabendDriverClient;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::TableName;
use etl_destinations::databend::{DatabendDestination, table_name_to_databend_table_id};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Environment variable name for the Databend DSN.
const DATABEND_DSN_ENV_NAME: &str = "TESTS_DATABEND_DSN";
/// Environment variable name for the Databend database name.
const DATABEND_DATABASE_ENV_NAME: &str = "TESTS_DATABEND_DATABASE";

/// Generates a unique database name for test isolation.
fn random_database_name() -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("etl_tests_{}", uuid)
}

/// Databend database connection for testing.
///
/// Provides a unified interface for Databend operations in tests.
pub struct DatabendDatabase {
    dsn: String,
    database: String,
    client: Option<DatabendDriverClient>,
}

impl DatabendDatabase {
    /// Creates a new Databend database instance for testing.
    ///
    /// # Panics
    ///
    /// Panics if the required environment variables are not set.
    pub async fn new() -> Self {
        let dsn = std::env::var(DATABEND_DSN_ENV_NAME)
            .unwrap_or_else(|_| panic!("The env variable {} must be set", DATABEND_DSN_ENV_NAME));

        // Use provided database name or generate a random one
        let database = std::env::var(DATABEND_DATABASE_ENV_NAME)
            .unwrap_or_else(|_| random_database_name());

        let client = DatabendDriverClient::new(dsn.clone());

        // Initialize the database
        initialize_databend(&client, &database).await;

        Self {
            dsn,
            database,
            client: Some(client),
        }
    }

    /// Creates a [`DatabendDestination`] configured for this database instance.
    pub async fn build_destination<S>(&self, store: S) -> DatabendDestination<S>
    where
        S: StateStore + SchemaStore,
    {
        DatabendDestination::new(self.dsn.clone(), self.database.clone(), store)
            .await
            .unwrap()
    }

    /// Executes a SELECT * query against the specified table.
    pub async fn query_table(&self, table_name: TableName) -> Option<Vec<DatabendRow>> {
        use futures::stream::StreamExt;

        let client = self.client.as_ref().unwrap();
        let table_id = table_name_to_databend_table_id(&table_name);

        let query = format!("SELECT * FROM `{}`.`{}`", self.database, table_id);

        let conn = client.get_conn().await.unwrap();
        let mut rows = conn.query_iter(&query).await.ok()?;

        let mut results = Vec::new();
        while let Some(row) = rows.next().await {
            let row = row.unwrap();
            results.push(DatabendRow { row });
        }

        Some(results)
    }

    /// Returns the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Takes ownership of the client for cleanup.
    fn take_client(&mut self) -> Option<DatabendDriverClient> {
        self.client.take()
    }
}

impl Drop for DatabendDatabase {
    /// Cleans up the test database when dropped.
    fn drop(&mut self) {
        let Some(client) = self.take_client() else {
            return;
        };

        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                destroy_databend(&client, &self.database).await;
            });
        });
    }
}

/// Creates a new database for testing.
async fn initialize_databend(client: &DatabendDriverClient, database: &str) {
    let conn = client.get_conn().await.unwrap();
    let query = format!("CREATE DATABASE IF NOT EXISTS `{}`", database);
    conn.exec(&query).await.unwrap();
}

/// Deletes a database and all its contents.
async fn destroy_databend(client: &DatabendDriverClient, database: &str) {
    let conn = client.get_conn().await.unwrap();
    let query = format!("DROP DATABASE IF EXISTS `{}`", database);
    conn.exec(&query).await.unwrap();
}

/// Sets up a Databend database connection for testing.
///
/// # Panics
///
/// Panics if required environment variables are not set.
pub async fn setup_databend_connection() -> DatabendDatabase {
    DatabendDatabase::new().await
}

/// Wrapper for a Databend row result.
pub struct DatabendRow {
    row: databend_driver::Row,
}

impl DatabendRow {
    /// Gets a value from the row by index.
    pub fn get<T>(&self, index: usize) -> Option<T>
    where
        T: FromStr,
        <T as FromStr>::Err: fmt::Debug,
    {
        let value = self.row.values().get(index)?;
        let string_value = value.to_string();
        string_value.parse().ok()
    }

    /// Gets a string value from the row by index.
    pub fn get_string(&self, index: usize) -> Option<String> {
        let value = self.row.values().get(index)?;
        Some(value.to_string())
    }

    /// Gets an optional value from the row by index.
    pub fn get_optional<T>(&self, index: usize) -> Option<Option<T>>
    where
        T: FromStr,
        <T as FromStr>::Err: fmt::Debug,
    {
        let value = self.row.values().get(index)?;
        if value.to_string() == "NULL" || value.to_string().is_empty() {
            Some(None)
        } else {
            Some(value.to_string().parse().ok())
        }
    }
}

/// Helper struct for test data - User.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DatabendUser {
    pub id: i32,
    pub name: String,
    pub age: i32,
}

impl DatabendUser {
    pub fn new(id: i32, name: &str, age: i32) -> Self {
        Self {
            id,
            name: name.to_owned(),
            age,
        }
    }

    pub fn from_row(row: &DatabendRow) -> Self {
        Self {
            id: row.get(0).unwrap(),
            name: row.get_string(1).unwrap(),
            age: row.get(2).unwrap(),
        }
    }
}

/// Helper struct for test data - Order.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DatabendOrder {
    pub id: i32,
    pub description: String,
}

impl DatabendOrder {
    pub fn new(id: i32, description: &str) -> Self {
        Self {
            id,
            description: description.to_owned(),
        }
    }

    pub fn from_row(row: &DatabendRow) -> Self {
        Self {
            id: row.get(0).unwrap(),
            description: row.get_string(1).unwrap(),
        }
    }
}

/// Parses Databend table rows into a sorted vector of typed structs.
pub fn parse_databend_table_rows<T, F>(rows: Vec<DatabendRow>, parser: F) -> Vec<T>
where
    T: Ord,
    F: Fn(&DatabendRow) -> T,
{
    let mut parsed_rows: Vec<T> = rows.iter().map(parser).collect();
    parsed_rows.sort();
    parsed_rows
}
