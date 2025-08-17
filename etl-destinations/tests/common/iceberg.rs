//! Test utilities for Iceberg destination integration tests.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::TableName;
use etl_destinations::iceberg::IcebergDestination;
use std::collections::HashMap;
use uuid::Uuid;

/// Environment variable name for the Iceberg REST catalog URI.
/// Can also use ICEBERG_CATALOG_URI for Docker-based tests.
const ICEBERG_CATALOG_URI_ENV_NAME: &str = "TESTS_ICEBERG_CATALOG_URI";
/// Environment variable name for the Iceberg warehouse location.
/// Can also use ICEBERG_WAREHOUSE for Docker-based tests.
const ICEBERG_WAREHOUSE_ENV_NAME: &str = "TESTS_ICEBERG_WAREHOUSE";
/// Environment variable name for the Iceberg namespace.
/// Can also use ICEBERG_NAMESPACE for Docker-based tests.
const ICEBERG_NAMESPACE_ENV_NAME: &str = "TESTS_ICEBERG_NAMESPACE";

/// Generates a unique namespace for test isolation.
///
/// Creates a random namespace prefixed with "etl_tests_" to ensure
/// each test run uses a fresh namespace and avoid conflicts.
fn random_namespace() -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("etl_tests_{}", &uuid[..8]) // Keep it shorter for readability
}

/// Iceberg database connection for testing using real Iceberg REST catalog.
///
/// Provides a unified interface for Iceberg operations in tests, automatically
/// handling setup and teardown of test namespaces and tables.
///
/// # Example Usage
///
/// ```rust
/// // Create a test database instance
/// let db = IcebergDatabase::new().await;
///
/// // Create a destination for testing
/// let destination = db.build_destination(store).await;
///
/// // Query tables after test operations
/// let results = db.query_table("public_users").await.unwrap();
/// ```
pub struct IcebergDatabase {
    catalog_uri: String,
    warehouse: String,
    namespace: String,
    #[allow(dead_code)]
    auth_token: Option<String>,
}

impl IcebergDatabase {
    /// Creates a new Iceberg database instance for testing.
    ///
    /// Sets up an [`IcebergDatabase`] that connects to an Iceberg REST catalog
    /// using environment variables for configuration. Falls back to local
    /// testing configuration if REST catalog is not available.
    pub async fn new() -> Self {
        // Support both test env vars and Docker env vars
        let namespace = std::env::var(ICEBERG_NAMESPACE_ENV_NAME)
            .or_else(|_| std::env::var("ICEBERG_NAMESPACE"))
            .unwrap_or_else(|_| random_namespace());

        let catalog_uri = std::env::var(ICEBERG_CATALOG_URI_ENV_NAME)
            .or_else(|_| std::env::var("ICEBERG_CATALOG_URI"))
            .unwrap_or_else(|_| "http://localhost:8181".to_string());

        let warehouse = std::env::var(ICEBERG_WAREHOUSE_ENV_NAME)
            .or_else(|_| std::env::var("ICEBERG_WAREHOUSE"))
            .unwrap_or_else(|_| "file:///tmp/iceberg-warehouse".to_string());

        let auth_token = std::env::var("TESTS_ICEBERG_AUTH_TOKEN").ok();

        if std::env::var(ICEBERG_CATALOG_URI_ENV_NAME).is_err() {
            eprintln!(
                "Warning: Using default Iceberg configuration for tests. Set {} and {} for custom REST catalog testing.",
                ICEBERG_CATALOG_URI_ENV_NAME, ICEBERG_WAREHOUSE_ENV_NAME
            );
        }

        Self {
            catalog_uri,
            warehouse,
            namespace,
            auth_token,
        }
    }

    /// Creates an [`IcebergDestination`] configured for this database instance.
    ///
    /// Returns a destination that can be used with ETL pipelines for testing.
    /// The destination will automatically create tables in the test namespace.
    #[allow(dead_code)]
    pub async fn build_destination<S>(&self, store: S) -> IcebergDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync + 'static,
    {
        IcebergDestination::new(
            self.catalog_uri.clone(),
            self.warehouse.clone(),
            self.namespace.clone(),
            self.auth_token.clone(),
            store,
        )
        .await
        .expect("Failed to create Iceberg destination for testing")
    }

    /// Gets the test namespace being used.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Gets the catalog URI being used.
    pub fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    /// Gets the warehouse location being used.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Formats a table name for Iceberg (adds prefix and converts schema.table format).
    pub fn format_table_name(&self, table_name: &TableName) -> String {
        // For Phase 2, use simple schema_table format
        format!("{}_{}", table_name.schema, table_name.name)
    }

}

impl Drop for IcebergDatabase {
    fn drop(&mut self) {
        // Best effort cleanup - in a real implementation this would
        // schedule async cleanup of the test namespace
        eprintln!("IcebergDatabase dropped for namespace: {}", self.namespace);
    }
}

/// Represents a row from an Iceberg table for testing.
///
/// This is a simplified representation that can be used to validate
/// data written to Iceberg tables during testing.
#[derive(Debug, Clone, PartialEq)]
pub struct IcebergRow {
    pub columns: HashMap<String, IcebergValue>,
}

impl IcebergRow {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    pub fn with_column<T: Into<IcebergValue>>(mut self, name: &str, value: T) -> Self {
        self.columns.insert(name.to_string(), value.into());
        self
    }

    pub fn get<T: TryFrom<IcebergValue>>(&self, column: &str) -> Option<T>
    where
        T::Error: std::fmt::Debug,
    {
        self.columns.get(column)?.clone().try_into().ok()
    }
}

/// Represents a value in an Iceberg table for testing.
#[derive(Debug, Clone, PartialEq)]
pub enum IcebergValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Binary(Vec<u8>),
    // Add more types as needed for testing
}

impl From<bool> for IcebergValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for IcebergValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for IcebergValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<String> for IcebergValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for IcebergValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<NaiveDate> for IcebergValue {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<DateTime<Utc>> for IcebergValue {
    fn from(value: DateTime<Utc>) -> Self {
        Self::TimestampTz(value)
    }
}

impl TryFrom<IcebergValue> for bool {
    type Error = ();

    fn try_from(value: IcebergValue) -> Result<Self, Self::Error> {
        match value {
            IcebergValue::Boolean(b) => Ok(b),
            _ => Err(()),
        }
    }
}

impl TryFrom<IcebergValue> for i32 {
    type Error = ();

    fn try_from(value: IcebergValue) -> Result<Self, Self::Error> {
        match value {
            IcebergValue::Integer(i) => Ok(i as i32),
            _ => Err(()),
        }
    }
}

impl TryFrom<IcebergValue> for i64 {
    type Error = ();

    fn try_from(value: IcebergValue) -> Result<Self, Self::Error> {
        match value {
            IcebergValue::Integer(i) => Ok(i),
            _ => Err(()),
        }
    }
}

impl TryFrom<IcebergValue> for String {
    type Error = ();

    fn try_from(value: IcebergValue) -> Result<Self, Self::Error> {
        match value {
            IcebergValue::String(s) => Ok(s),
            _ => Err(()),
        }
    }
}

/// Test data structures that mirror the BigQuery test types
/// for consistent testing across destinations.


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_namespace_generation() {
        let ns1 = random_namespace();
        let ns2 = random_namespace();

        assert!(ns1.starts_with("etl_tests_"));
        assert!(ns2.starts_with("etl_tests_"));
        assert_ne!(ns1, ns2);
    }

    #[test]
    fn test_iceberg_value_conversions() {
        let bool_val: IcebergValue = true.into();
        assert_eq!(bool_val, IcebergValue::Boolean(true));

        let int_val: IcebergValue = 42i64.into();
        assert_eq!(int_val, IcebergValue::Integer(42));

        let str_val: IcebergValue = "test".into();
        assert_eq!(str_val, IcebergValue::String("test".to_string()));
    }

    #[test]
    fn test_iceberg_row_operations() {
        let row = IcebergRow::new()
            .with_column("id", 1i64)
            .with_column("name", "test")
            .with_column("active", true);

        assert_eq!(row.get::<i64>("id"), Some(1));
        assert_eq!(row.get::<String>("name"), Some("test".to_string()));
        assert_eq!(row.get::<bool>("active"), Some(true));
        assert_eq!(row.get::<i64>("nonexistent"), None);
    }

    #[tokio::test]
    async fn test_iceberg_database_creation() {
        let db = IcebergDatabase::new().await;
        assert!(db.namespace().starts_with("etl_tests_"));
        assert!(!db.catalog_uri().is_empty());
        assert!(!db.warehouse().is_empty());
    }

    #[test]
    fn test_format_table_name() {
        let db = IcebergDatabase {
            catalog_uri: "http://localhost:8181".to_string(),
            warehouse: "file:///tmp/test".to_string(),
            namespace: "test".to_string(),
            auth_token: None,
        };

        let table_name = TableName::new("public".to_string(), "users".to_string());
        let formatted = db.format_table_name(&table_name);
        assert_eq!(formatted, "public_users");
    }
}
