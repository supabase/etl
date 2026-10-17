use etl::schema::ReplicatedTableSchema;

use crate::snowflake::{
    Error, Result, SNOWFLAKE_COLUMN_NAME_MAPPING,
    schema::{
        CDC_MISSING_COLUMNS_COLUMN, CDC_OPERATION_COLUMN, CDC_SEQUENCE_COLUMN, type_name,
        validate_primary_key_identity,
    },
    sql::{quote_identifier, quote_string_literal},
};

/// Generates a Snowflake `SELECT` query for materializing current table state
/// from one CDC table generation.
///
/// Returns SQL as a [`String`]. The caller executes it; this function does not
/// execute SQL or create, refresh, or manage views or tables.
///
/// Supply unquoted database, namespace, and physical table names. The query
/// quotes each identifier independently and returns source columns plus
/// `_cdc_anchored`, `_cdc_unresolved_columns`, and `_cdc_last_sequence`.
/// Consumers must treat an unanchored row or a nonempty unresolved list as
/// incomplete history, not discard it. These indicators cannot detect arbitrary
/// publication gaps or pruned events.
///
/// The table must retain all ETL-owned history from a fresh snapshot under the
/// documented stable-key and uninterrupted-publication contract. Supply the
/// schema snapshot ETL has applied to the destination after column renames or
/// drops. A live source catalog can be ahead of the destination. Omission
/// metadata uses stable PostgreSQL attribute numbers, not names or current
/// positions. Added columns follow the destination's existing default-value
/// limitations. The query does not supply cross-table transaction atomicity. It
/// scans retained history for lifetime boundaries, but resolves values only
/// from each key's latest complete insert/update and later partial updates.
/// This does not permit pruning stored history.
pub fn materialization_query(
    database: &str,
    namespace: &str,
    table: &str,
    schema: &ReplicatedTableSchema,
) -> Result<String> {
    query(
        &format!(
            "{}.{}.{}",
            quote_identifier(database),
            quote_identifier(namespace),
            quote_identifier(table)
        ),
        schema,
    )
}

/// Uses the view's own schema so database clones do not read the original
/// table.
pub(super) fn view_query(table: &str, schema: &ReplicatedTableSchema) -> Result<String> {
    query(&quote_identifier(table), schema)
}

/// Checks the schema requirements shared by query generation and view setup.
pub(super) fn validate_schema(schema: &ReplicatedTableSchema) -> Result<()> {
    validate_primary_key_identity(schema)?;
    if schema.column_schemas().any(|column| column.name.starts_with("_cdc_")) {
        return Err(Error::Config(
            "Source column names beginning with _cdc_ are reserved by the materialization query"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Builds the shared query over an already quoted table reference.
fn query(table: &str, schema: &ReplicatedTableSchema) -> Result<String> {
    validate_schema(schema)?;
    let columns =
        schema.destination_column_schemas(SNOWFLAKE_COLUMN_NAME_MAPPING).collect::<Vec<_>>();
    let keys = columns
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.primary_key().then_some(format!("c{i}")))
        .collect::<Vec<_>>()
        .join(", ");
    let mut projection = Vec::with_capacity(columns.len() + 3);
    let mut group_keys = vec![keys.clone()];
    let mut resolved = vec![keys.clone()];
    let mut output = Vec::with_capacity(columns.len() + 3);
    let mut unresolved = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let name = quote_identifier(&column.name);
        let value = if column.primary_key() && type_name(&column.typ) == "VARCHAR" {
            format!("collate({name}, 'utf8')")
        } else {
            name.clone()
        };
        projection.push(format!("{value} as c{i}"));
        if column.primary_key() {
            // Keep UTF-8 key equality without changing the output collation.
            projection.push(format!("{name} as o{i}"));
            group_keys.push(format!("o{i}"));
            resolved.push(format!("o{i}"));
            output.push(format!("o{i} as {name}"));
        } else {
            let supplied =
                format!("iff(array_contains({}::variant, missing), 0, 1)", column.ordinal_position);
            // NULL ordering keys exclude omitted values, while a supplied SQL
            // NULL can still win. Aggregation avoids a distinct window ordering
            // per column, which makes wide queries expensive to compile.
            resolved.push(format!("max_by(c{i}, iff({supplied} = 1, seq, null)) as r{i}"));
            resolved.push(format!("max({supplied}) as k{i}"));
            output.push(format!("r{i} as {name}"));
            unresolved.push(format!("iff(k{i} = 0, {}, null)", quote_string_literal(&column.name)));
        }
    }
    projection.push(format!("{} as op", quote_identifier(CDC_OPERATION_COLUMN)));
    projection.push(format!("collate({}, 'utf8') as seq", quote_identifier(CDC_SEQUENCE_COLUMN)));
    projection.push(format!(
        "ifnull({}, array_construct()) as missing",
        quote_identifier(CDC_MISSING_COLUMNS_COLUMN)
    ));
    // Groups are nonempty and the physical sequence is NOT NULL. The fallback
    // is unreachable; it preserves the result's non-nullability metadata.
    resolved.push("coalesce(max(seq), collate('', 'utf8')) as seq".to_owned());
    resolved.push(
        "max(latest_insert) is not null and (max(latest_delete) is null or max(latest_insert) > \
         max(latest_delete)) as anchored"
            .to_owned(),
    );
    output.push("anchored as \"_cdc_anchored\"".to_owned());
    let unresolved = if unresolved.is_empty() {
        "array_construct()".to_owned()
    } else {
        format!("array_construct_compact({})", unresolved.join(", "))
    };
    output.push(format!("{unresolved} as \"_cdc_unresolved_columns\""));
    output.push("seq as \"_cdc_last_sequence\"".to_owned());
    // A complete record supplies every value, including explicit NULLs, so
    // earlier suppliers cannot affect the query result. Compute lifetime
    // boundaries first so filtering out an older insert does not lose the
    // anchor diagnostic.
    Ok(format!(
        r#"with input as (
  select {projection} from {table}
), boundaries as (
  select *,
    max(iff(op = 'insert', seq, null)) over (partition by {keys}) as latest_insert,
    max(iff(op = 'delete', seq, null)) over (partition by {keys}) as latest_delete,
    max(iff(op in ('insert', 'update') and array_size(missing) = 0, seq, null))
      over (partition by {keys}) as latest_complete
  from input
), eligible as (
  select * from boundaries
  where op in ('insert', 'update')
    and (latest_delete is null or seq > latest_delete)
    and (latest_insert is null or seq >= latest_insert)
    and (latest_complete is null or seq >= latest_complete)
), resolved as (
  select {resolved}
  from eligible
  group by {group_keys}
)
select {output} from resolved"#,
        projection = projection.join(", "),
        resolved = resolved.join(",\n    "),
        group_keys = group_keys.join(", "),
        output = output.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type};

    use crate::snowflake::{
        AuthManager, Config, SqlClient,
        materialization::{query, view_query},
    };

    /// Builds a schema for generated-query regression tests.
    fn schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".into(), "test".into()),
            columns,
        )))
    }

    /// Wide schemas must not reintroduce per-column window planning costs.
    #[test]
    fn window_count_does_not_grow_with_column_count() {
        let counts = [0, 1, 128].map(|width| {
            let mut columns =
                vec![ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1)];
            columns.extend(
                (0..width)
                    .map(|i| ColumnSchema::new(format!("value{i}"), Type::TEXT, -1, i + 2, true)),
            );
            let query = view_query("test", &schema(columns)).unwrap();

            query.matches("over (").count()
        });

        assert_eq!(counts, [counts[0]; 3]);
    }

    /// Aggregation must preserve source collations and non-null sequence
    /// metadata.
    #[tokio::test]
    #[ignore = "requires Snowflake test credentials"]
    async fn materialization_preserves_result_metadata() {
        let auth = Arc::new(AuthManager::new(Config::require_tests_env().unwrap()).unwrap());
        let sql =
            SqlClient::new(auth.config().clone_without_credentials(), auth, reqwest::Client::new());
        let schema = schema(vec![
            ColumnSchema::new("id".into(), Type::TEXT, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
        ]);
        let input = "(select 'A'::varchar collate 'en-ci' as \"id\", null::varchar collate \
                     'en-ci' as \"payload\", 'insert' as \"_cdc_operation\", '01'::varchar as \
                     \"_cdc_sequence_number\", array_construct() as \"_cdc_missing_columns\")";
        let reference = format!(
            "select \"id\", \"payload\", collate(\"_cdc_sequence_number\", 'utf8') as \
             \"_cdc_last_sequence\" from {input}"
        );
        let mut metadata = Vec::new();
        for statement in [reference, query(input, &schema).unwrap()] {
            let response = sql.execute_statement(&statement).await.unwrap();
            assert_eq!(
                response
                    .column_nullability()
                    .into_iter()
                    .filter(|(name, _)| matches!(*name, "id" | "payload" | "_cdc_last_sequence"))
                    .collect::<Vec<_>>(),
                [("id", Some(false)), ("payload", Some(true)), ("_cdc_last_sequence", Some(false))]
            );
            let handle = response.statement_handle.unwrap();
            let response =
                sql.execute_statement(&format!("describe result '{handle}'")).await.unwrap();
            let columns = response
                .data
                .unwrap()
                .into_iter()
                .filter(|row| {
                    matches!(row[0].as_str(), Some("id" | "payload" | "_cdc_last_sequence"))
                })
                .collect::<Vec<_>>();
            assert_eq!(columns.len(), 3);
            metadata.push(columns);
        }

        assert_eq!(metadata[0], metadata[1]);
    }
}
