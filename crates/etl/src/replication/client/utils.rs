use std::fmt;

use tokio_postgres::SimpleQueryRow;

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

/// Extracts and parses a value from a [`SimpleQueryRow`].
///
/// Returns an error if the column is not found or if the value cannot be parsed
/// to the target type.
pub(super) fn get_row_value<T: std::str::FromStr>(
    row: &SimpleQueryRow,
    column_name: &str,
    table_name: &str,
) -> EtlResult<T>
where
    T::Err: fmt::Debug,
{
    let value = row.try_get(column_name)?.ok_or(etl_error!(
        ErrorKind::SourceSchemaError,
        "Column not found in source table",
        format!("Column '{}' not found in table '{}'", column_name, table_name)
    ))?;

    value.parse().map_err(|e: T::Err| {
        etl_error!(
            ErrorKind::ConversionError,
            "Column parsing failed",
            format!(
                "Failed to parse value from column '{}' in table '{}': {:?}",
                column_name, table_name, e
            )
        )
    })
}
