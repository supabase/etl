use std::{fmt, pin::pin};

use futures::TryStreamExt;
use tokio_postgres::{Client, Error, SimpleQueryMessage, SimpleQueryRow};

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

/// Processes rows as they arrive and checks the entire response for query
/// errors.
pub(super) async fn for_each_simple_query_row<E>(
    client: &Client,
    query: &str,
    mut visit: impl FnMut(SimpleQueryRow) -> Result<(), E>,
) -> Result<(), E>
where
    E: From<Error>,
{
    let mut messages = pin!(client.simple_query_raw(query).await?);
    let mut result = Ok(());

    // Preserve the precedence of query errors over row processing errors.
    while let Some(message) = messages.try_next().await? {
        if let SimpleQueryMessage::Row(row) = message
            && result.is_ok()
        {
            result = visit(row);
        }
    }

    result
}

/// Returns the first row after checking the complete response for errors.
pub(super) async fn simple_query_first_row(
    client: &Client,
    query: &str,
) -> Result<Option<SimpleQueryRow>, Error> {
    let mut first = None;
    for_each_simple_query_row::<Error>(client, query, |row| {
        first.get_or_insert(row);

        Ok(())
    })
    .await?;

    Ok(first)
}

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
