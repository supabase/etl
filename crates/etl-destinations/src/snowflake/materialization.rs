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
/// documented stable-key and fixed-schema history contract. The query does not
/// supply cross-table transaction atomicity. It scans retained history for
/// lifetime boundaries, but resolves values only from each key's latest
/// complete insert/update and later partial updates. This does not permit
/// pruning stored history.
pub fn materialization_query(
    database: &str,
    namespace: &str,
    table: &str,
    schema: &ReplicatedTableSchema,
) -> Result<String> {
    validate_primary_key_identity(schema)?;
    if schema.column_schemas().any(|column| column.name.starts_with("_cdc_")) {
        return Err(Error::Config(
            "Source column names beginning with _cdc_ are reserved by the materialization query"
                .to_owned(),
        ));
    }
    let columns =
        schema.destination_column_schemas(SNOWFLAKE_COLUMN_NAME_MAPPING).collect::<Vec<_>>();
    let keys = columns
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.primary_key().then_some(format!("c{i}")))
        .collect::<Vec<_>>()
        .join(", ");
    let mut projection = Vec::with_capacity(columns.len() + 3);
    let mut resolved = Vec::with_capacity(columns.len() * 2);
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
            // Partitioning must use UTF-8 equality without changing the output
            // collation.
            projection.push(format!("{name} as o{i}"));
            output.push(format!("o{i} as {name}"));
        } else {
            let supplied = format!(
                "iff(array_contains({}::variant, missing), 0, 1)",
                quote_string_literal(&column.name)
            );
            resolved.push(format!(
                "first_value(c{i}) respect nulls over (partition by {keys} order by {supplied} \
                 desc, seq desc rows between unbounded preceding and unbounded following) as r{i}"
            ));
            resolved.push(format!("max({supplied}) over (partition by {keys}) as k{i}"));
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
    resolved.push(
        "latest_insert is not null and (latest_delete is null or latest_insert > latest_delete) \
         as anchored"
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
    let table = format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(namespace),
        quote_identifier(table)
    );
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
  select *, {resolved}
  from eligible
  qualify row_number() over (partition by {keys} order by seq desc) = 1
)
select {output} from resolved"#,
        projection = projection.join(", "),
        resolved = resolved.join(",\n    "),
        output = output.join(", ")
    ))
}
