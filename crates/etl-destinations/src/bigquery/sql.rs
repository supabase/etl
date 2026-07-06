use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

use crate::bigquery::client::{BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};

/// Escapes a BigQuery identifier using GoogleSQL quoted-identifier escape
/// sequences.
///
/// BigQuery quoted identifiers are enclosed in backticks and use the same
/// escape sequences as string literals. Empty quoted identifiers are invalid,
/// and control characters are rejected to avoid relying on less-readable
/// numeric escapes.
fn escape_identifier(identifier: &str, context: &str) -> EtlResult<String> {
    if identifier.is_empty() {
        return Err(etl_error!(
            ErrorKind::DestinationTableNameInvalid,
            "Invalid BigQuery identifier",
            format!("{context} cannot be empty")
        ));
    }

    if identifier.chars().any(char::is_control) {
        return Err(etl_error!(
            ErrorKind::DestinationTableNameInvalid,
            "Invalid BigQuery identifier",
            format!("{context} contains control characters")
        ));
    }

    let mut escaped = String::with_capacity(identifier.len());

    for ch in identifier.chars() {
        match ch {
            '`' => escaped.push_str("\\`"),
            '\\' => escaped.push_str("\\\\"),
            _ => escaped.push(ch),
        }
    }

    Ok(escaped)
}

/// Quotes one BigQuery SQL identifier with backticks.
pub(super) fn quote_identifier(identifier: &str, context: &str) -> EtlResult<String> {
    Ok(format!("`{}`", escape_identifier(identifier, context)?))
}

/// Quotes a fully qualified BigQuery table path as one GoogleSQL path
/// expression.
///
/// BigQuery treats backtick-quoted table paths as dotted path expressions, so
/// the project, dataset, and table parts are escaped separately and then joined
/// inside one quoted path.
pub(super) fn quote_table_path(
    project_id: &BigQueryProjectId,
    dataset_id: &BigQueryDatasetId,
    table_id: &BigQueryTableId,
) -> EtlResult<String> {
    let project_id = escape_identifier(project_id, "BigQuery project id")?;
    let dataset_id = escape_identifier(dataset_id, "BigQuery dataset id")?;
    let table_id = escape_identifier(table_id, "BigQuery table id")?;

    Ok(format!("`{project_id}.{dataset_id}.{table_id}`"))
}

/// Quotes the BigQuery `INFORMATION_SCHEMA.TABLES` path for a dataset.
///
/// The fixed `INFORMATION_SCHEMA.TABLES` suffix is intentionally not escaped as
/// user input.
pub(super) fn quote_information_schema_tables_path(
    project_id: &BigQueryProjectId,
    dataset_id: &BigQueryDatasetId,
) -> EtlResult<String> {
    let project_id = escape_identifier(project_id, "BigQuery project id")?;
    let dataset_id = escape_identifier(dataset_id, "BigQuery dataset id")?;

    Ok(format!("`{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`"))
}

/// Quotes the BigQuery `INFORMATION_SCHEMA.COLUMNS` path for a dataset.
///
/// The fixed `INFORMATION_SCHEMA.COLUMNS` suffix is intentionally not escaped
/// as user input.
pub(super) fn quote_information_schema_columns_path(
    project_id: &BigQueryProjectId,
    dataset_id: &BigQueryDatasetId,
) -> EtlResult<String> {
    let project_id = escape_identifier(project_id, "BigQuery project id")?;
    let dataset_id = escape_identifier(dataset_id, "BigQuery dataset id")?;

    Ok(format!("`{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_escapes_backticks_and_backslashes() {
        assert_eq!(
            quote_identifier(r#"pwn`name\suffix"#, "column").expect("quoted identifier"),
            r#"`pwn\`name\\suffix`"#
        );
    }

    #[test]
    fn quote_identifier_rejects_control_chars() {
        let result = quote_identifier("bad\nname", "column");

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::DestinationTableNameInvalid
        ));
    }

    #[test]
    fn quote_table_path_quotes_entire_path() {
        assert_eq!(
            quote_table_path(
                &"project".to_owned(),
                &"data`set".to_owned(),
                &"table\\name".to_owned()
            )
            .expect("quoted path"),
            r#"`project.data\`set.table\\name`"#
        );
    }
}
