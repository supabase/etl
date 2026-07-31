//! SQL identifier helpers for the Postgres destination.

use etl::schema::TableName;
use pg_escape::quote_identifier as pg_quote_identifier;

/// Quotes a SQL identifier for safe inclusion in DDL/DML.
pub(crate) fn quote_identifier(identifier: &str) -> String {
    pg_quote_identifier(identifier).to_string()
}

/// Returns the quoted `"schema"."table"` form of [`TableName`].
pub(crate) fn quote_table_name(table_name: &TableName) -> String {
    table_name.as_quoted_identifier()
}
