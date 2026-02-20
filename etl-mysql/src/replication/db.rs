use std::num::NonZeroI32;

use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions};
use thiserror::Error;

use crate::types::{TableId, TableName};

/// MySQL database connection configuration placeholder.
///
/// This is a simplified version for the MySQL implementation.
/// In production, this should be defined in etl-config.
#[derive(Debug, Clone)]
pub struct MySqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: Option<String>,
}

impl MySqlConnectionConfig {
    /// Creates MySQL connection options for connecting to the configured database.
    pub fn with_db(&self) -> sqlx::mysql::MySqlConnectOptions {
        let mut options = sqlx::mysql::MySqlConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.username)
            .database(&self.name);

        if let Some(password) = &self.password {
            options = options.password(password);
        }

        options
    }
}

/// Errors that can occur during table lookups.
#[derive(Debug, Error)]
pub enum TableLookupError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Table with ID {0} not found")]
    TableNotFound(TableId),
}

/// Connects to the source database with a connection pool.
///
/// Creates a MySQL connection pool with the specified minimum and maximum
/// connection counts for accessing the source database.
#[cfg(feature = "replication")]
pub async fn connect_to_source_database(
    config: &MySqlConnectionConfig,
    min_connections: u32,
    max_connections: u32,
) -> Result<MySqlPool, sqlx::Error> {
    let options = config.with_db();

    let pool = MySqlPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Retrieves table name from table identifier by querying information_schema.
///
/// Looks up the schema and table name for the given table identifier using MySQL's
/// information_schema database.
pub async fn get_table_name_from_id(
    pool: &MySqlPool,
    table_id: TableId,
) -> Result<TableName, TableLookupError> {
    let query = "
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = ?
    ";

    let row = sqlx::query(query)
        .bind(table_id.into_inner().to_string())
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => {
            let schema_name: String = row.try_get("table_schema")?;
            let table_name: String = row.try_get("table_name")?;

            Ok(TableName {
                schema: schema_name,
                name: table_name,
            })
        }
        None => Err(TableLookupError::TableNotFound(table_id)),
    }
}

/// Extracts the MySQL server version from a version string.
///
/// This function parses version strings like "8.0.35" or "5.7.44-log"
/// and converts them to the numeric format used by MySQL.
///
/// Returns the version in the format: MAJOR * 10000 + MINOR * 100 + PATCH
/// For example: MySQL 8.0.35 = 80035, MySQL 5.7.44 = 50744
///
/// Returns `None` if the version string cannot be parsed or results in zero.
pub fn extract_server_version(server_version_str: impl AsRef<str>) -> Option<NonZeroI32> {
    let version_part = server_version_str
        .as_ref()
        .split_whitespace()
        .next()
        .unwrap_or("0.0.0");

    let version_part = version_part.split('-').next().unwrap_or("0.0.0");

    let version_components: Vec<&str> = version_part.split('.').collect();

    let major = version_components
        .first()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let minor = version_components
        .get(1)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let patch = version_components
        .get(2)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let version = major * 10000 + minor * 100 + patch;

    NonZeroI32::new(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_server_version_basic_versions() {
        assert_eq!(extract_server_version("8.0.35"), NonZeroI32::new(80035));
        assert_eq!(extract_server_version("5.7.44"), NonZeroI32::new(50744));
        assert_eq!(extract_server_version("8.1.0"), NonZeroI32::new(80100));
        assert_eq!(extract_server_version("8.2.5"), NonZeroI32::new(80205));
    }

    #[test]
    fn test_extract_server_version_with_suffixes() {
        assert_eq!(extract_server_version("8.0.35-log"), NonZeroI32::new(80035));
        assert_eq!(
            extract_server_version("5.7.44-0ubuntu0.18.04.1"),
            NonZeroI32::new(50744)
        );
        assert_eq!(
            extract_server_version("8.0.32-0ubuntu0.22.04.2"),
            NonZeroI32::new(80032)
        );
    }

    #[test]
    fn test_extract_server_version_invalid_inputs() {
        assert_eq!(extract_server_version(""), None);
        assert_eq!(extract_server_version("invalid"), None);
        assert_eq!(extract_server_version("not.a.version"), None);
        assert_eq!(extract_server_version("MySQL"), None);
        assert_eq!(extract_server_version("   "), None);
    }

    #[test]
    fn test_extract_server_version_zero_versions() {
        assert_eq!(extract_server_version("0.0.0"), None);
        assert_eq!(extract_server_version("0.0"), None);
    }

    #[test]
    fn test_extract_server_version_whitespace_handling() {
        assert_eq!(extract_server_version("  8.0.35  "), NonZeroI32::new(80035));
        assert_eq!(extract_server_version("8.0.35\n"), NonZeroI32::new(80035));
    }
}
