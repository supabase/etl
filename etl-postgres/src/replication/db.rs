use std::num::NonZeroI32;

use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;

use crate::types::{TableId, TableName};

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
/// Creates a Postgres connection pool with the specified minimum and maximum
/// connection counts for accessing the source database.
#[cfg(feature = "replication")]
pub async fn connect_to_source_database(
    config: &PgConnectionConfig,
    min_connections: u32,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    let options = config.with_db();

    let pool = PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Retrieves table name from table OID by querying system catalogs.
///
/// Looks up the schema and table name for the given table OID using Postgres's
/// pg_class and pg_namespace system tables.
pub async fn get_table_name_from_oid(
    pool: &PgPool,
    table_id: TableId,
) -> Result<TableName, TableLookupError> {
    let query = "
        select n.nspname as schema_name, c.relname as table_name
        from pg_class c
        join pg_namespace n on c.relnamespace = n.oid
        where c.oid = $1
    ";

    let row = sqlx::query(query)
        .bind(table_id.into_inner() as i64)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => {
            let schema_name: String = row.try_get("schema_name")?;
            let table_name: String = row.try_get("table_name")?;

            Ok(TableName {
                schema: schema_name,
                name: table_name,
            })
        }
        None => Err(TableLookupError::TableNotFound(table_id)),
    }
}

/// Extracts the PostgreSQL server version from a version string.
///
/// This function parses version strings like "15.5 (Homebrew)" or "14.2"
/// and converts them to the numeric format used by PostgreSQL.
///
/// Returns the version in the format: MAJOR * 10000 + MINOR * 100 + PATCH
/// For example: PostgreSQL 14.2 = 140200, PostgreSQL 15.1 = 150100
///
/// Returns `None` if the version string cannot be parsed or results in zero.
pub fn extract_server_version(server_version_str: impl AsRef<str>) -> Option<NonZeroI32> {
    // Parse version string like "15.5 (Homebrew)" or "14.2"
    let version_part = server_version_str
        .as_ref()
        .split_whitespace()
        .next()
        .unwrap_or("0.0");

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
        assert_eq!(extract_server_version("15.5"), NonZeroI32::new(150500));
        assert_eq!(extract_server_version("14.2"), NonZeroI32::new(140200));
        assert_eq!(extract_server_version("13.0"), NonZeroI32::new(130000));
        assert_eq!(extract_server_version("16.1"), NonZeroI32::new(160100));
    }

    #[test]
    fn test_extract_server_version_with_suffixes() {
        assert_eq!(
            extract_server_version("15.5 (Homebrew)"),
            NonZeroI32::new(150500)
        );
        assert_eq!(
            extract_server_version("14.2 on x86_64-pc-linux-gnu"),
            NonZeroI32::new(140200)
        );
        assert_eq!(
            extract_server_version("13.7 (Ubuntu 13.7-1.pgdg20.04+1)"),
            NonZeroI32::new(130700)
        );
        assert_eq!(
            extract_server_version("16.0 devel"),
            NonZeroI32::new(160000)
        );
    }

    #[test]
    fn test_extract_server_version_patch_versions() {
        // Test versions with patch numbers
        assert_eq!(extract_server_version("15.5.1"), NonZeroI32::new(150501));
        assert_eq!(extract_server_version("14.10.3"), NonZeroI32::new(141003));
        assert_eq!(extract_server_version("13.12.25"), NonZeroI32::new(131225));
    }

    #[test]
    fn test_extract_server_version_invalid_inputs() {
        // Test invalid inputs that should return None
        assert_eq!(extract_server_version(""), None);
        assert_eq!(extract_server_version("invalid"), None);
        assert_eq!(extract_server_version("not.a.version"), None);
        assert_eq!(extract_server_version("PostgreSQL"), None);
        assert_eq!(extract_server_version("   "), None);
    }

    #[test]
    fn test_extract_server_version_zero_versions() {
        assert_eq!(extract_server_version("0.0.0"), None);
        assert_eq!(extract_server_version("0.0"), None);
    }

    #[test]
    fn test_extract_server_version_whitespace_handling() {
        assert_eq!(extract_server_version("  15.5  "), NonZeroI32::new(150500));
        assert_eq!(
            extract_server_version("15.5\t(Homebrew)"),
            NonZeroI32::new(150500)
        );
        assert_eq!(extract_server_version("15.5\n"), NonZeroI32::new(150500));
    }
}
