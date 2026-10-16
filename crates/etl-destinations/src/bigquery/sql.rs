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

/// Quotes a GoogleSQL string literal.
///
/// Takes the decoded string value, not a literal from another SQL dialect.
/// PostgreSQL literals must be decoded first because PostgreSQL doubles single
/// quotes and treats a backslash as an ordinary character, while GoogleSQL
/// rejects doubled quotes, treats a backslash as an escape, and does not allow
/// raw newlines inside single-quoted literals.
pub(super) fn quote_string_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');

    for ch in value.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '\'' => quoted.push_str("\\'"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if ch.is_control() => quoted.push_str(&format!("\\u{:04x}", u32::from(ch))),
            _ => quoted.push(ch),
        }
    }

    quoted.push('\'');
    quoted
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

/// Renders the query of an ETL logical view over one physical table.
pub(super) fn view_query(
    project_id: &BigQueryProjectId,
    dataset_id: &BigQueryDatasetId,
    table_id: &BigQueryTableId,
) -> EtlResult<String> {
    Ok(format!("select * from {}", quote_table_path(project_id, dataset_id, table_id)?))
}

/// Consumes an ASCII SQL keyword without changing identifier case.
fn consume_keyword<'a>(query: &'a str, keyword: &str) -> Option<&'a str> {
    let (prefix, rest) = query.split_at_checked(keyword.len())?;
    prefix.eq_ignore_ascii_case(keyword).then_some(rest)
}

/// Decodes one backtick-quoted path using the escapes ETL emits.
///
/// Other GoogleSQL escapes are deliberately rejected rather than decoded
/// incorrectly. Both current and legacy ETL view definitions only escape
/// backticks and backslashes.
fn consume_quoted_path(query: &str) -> Option<(String, &str)> {
    let mut chars = query.strip_prefix('`')?.chars();
    let mut path = String::new();

    loop {
        match chars.next()? {
            '`' => return Some((path, chars.as_str())),
            '\\' => match chars.next()? {
                ch @ ('\\' | '`') => path.push(ch),
                _ => return None,
            },
            ch if ch.is_control() => return None,
            ch => path.push(ch),
        }
    }
}

/// Parses the physical table ID from an ETL logical view definition.
///
/// Accepts only `SELECT * FROM` one fully qualified, backtick-quoted table in
/// the expected project and dataset, with optional ASCII whitespace and one
/// trailing semicolon. Keywords are case-insensitive, identifiers are not.
/// Current and legacy ETL renderers both use this shape. Joins, filters,
/// aliases, wildcard tables, decorators, and additional statements fail closed.
pub(super) fn parse_view_query_table_id(
    query: &str,
    project_id: &BigQueryProjectId,
    dataset_id: &BigQueryDatasetId,
) -> Option<BigQueryTableId> {
    escape_identifier(project_id, "BigQuery project id").ok()?;
    escape_identifier(dataset_id, "BigQuery dataset id").ok()?;

    let query = consume_keyword(query.trim_ascii_start(), "select")?.trim_ascii_start();
    let query = query.strip_prefix('*')?.trim_ascii_start();
    let query = consume_keyword(query, "from")?.trim_ascii_start();
    let (path, rest) = consume_quoted_path(query)?;
    let rest = rest.trim_ascii();
    if !rest.is_empty() && rest != ";" {
        return None;
    }

    let prefix = format!("{project_id}.{dataset_id}.");
    let table_id = path.strip_prefix(&prefix)?;
    if table_id.is_empty() || table_id.contains(['.', '*', '$', '@']) {
        return None;
    }

    Some(table_id.to_owned())
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
    fn quote_string_literal_escapes_quotes_backslashes_and_controls() {
        assert_eq!(quote_string_literal(r"don't C:\temp"), r"'don\'t C:\\temp'");
        assert_eq!(quote_string_literal("line\nnext\ttab\u{1}"), r"'line\nnext\ttab\u0001'");
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

    #[test]
    fn parse_view_query_table_id_round_trips_rendered_queries() {
        let project_id = "project".to_owned();
        let dataset_id = "data`set".to_owned();
        for table_id in [
            "public_orders_1",
            "public_orders_0000000000000000000000001",
            "supabase_etl_1_42_0000000000000000000000001",
            "注文 Ä-1",
            "pwn`name\\suffix",
        ] {
            let query = view_query(&project_id, &dataset_id, &table_id.to_owned()).unwrap();
            assert_eq!(
                parse_view_query_table_id(&query, &project_id, &dataset_id).as_deref(),
                Some(table_id)
            );
        }
    }

    /// Harmless formatting does not change the selected physical table.
    #[test]
    fn parse_view_query_table_id_accepts_keyword_case_and_whitespace() {
        for query in [
            "SELECT * FROM `project.dataset.table`",
            "\tSeLeCt\n*\r\nFrOm\t`project.dataset.table` ;\n",
            "select*from`project.dataset.table`;",
        ] {
            assert_eq!(
                parse_view_query_table_id(query, &"project".to_owned(), &"dataset".to_owned())
                    .as_deref(),
                Some("table")
            );
        }
    }

    #[test]
    fn parse_view_query_table_id_rejects_foreign_queries() {
        let project_id = "project".to_owned();
        let dataset_id = "dataset".to_owned();
        for query in [
            "",
            "select * from `project.dataset.`",
            "select * from `project.other.table`",
            "select * from `other.dataset.table`",
            "select id from `project.dataset.table`",
            "select * from `project.dataset.ta`ble`",
            "select * from `project.dataset.table\\",
            "select * from `project.dataset.table` where true",
            "select * from `project.dataset.table` join `project.dataset.other` using (id)",
            "select * from `project.dataset.table` t",
            "select * from `project.dataset.table`; select * from `project.dataset.other`",
            "select * from `project.dataset.table`;;",
            "select * from `project.dataset.table.*`",
            "select * from `project.dataset.table*`",
            "select * from `project.dataset.table$20260101`",
            "select * from `project.dataset.table@123`",
            "select * from `project.dataset.table\n`",
            r"select * from `project.dataset.ta\nble`",
            r"select * from `project.dataset.ta\x62le`",
            r"select * from `project.dataset.ta\u0062le`",
            r"select * from `project.dataset.ta\qble`",
            "select * from `project.dataset.table\\`",
            "select * from `project.dataset.table",
            "sélèct * from `project.dataset.table`",
            "selec表 * from `project.dataset.table`",
            "select * fröm `project.dataset.table`",
            "select * fro表 `project.dataset.table`",
            "select * from `project.dataset.表` où true",
        ] {
            assert_eq!(parse_view_query_table_id(query, &project_id, &dataset_id), None);
        }
    }
}
