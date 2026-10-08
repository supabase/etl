//! Source-name fixtures shared by the destination naming lifecycle tests.

use std::sync::Arc;

use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type};

/// Returns an ambiguous legacy pair, repeated boundary underscores, an
/// unchanged legacy name, and maximum-length standard PostgreSQL identifiers.
pub(crate) fn table_name_schemas() -> Vec<(ReplicatedTableSchema, String)> {
    // Snowflake tests share a schema, so every invocation owns unique names.
    let namespace = format!("n{}", uuid::Uuid::new_v4().simple());
    let trailing_namespace = format!("{namespace}_");
    let long_ascii_schema = format!("_{namespace}{}_", "a".repeat(28));
    let long_table = "_".repeat(63);
    assert_eq!(long_ascii_schema.len(), 63);
    [
        (namespace.clone(), "_b".to_owned(), format!("_3302_{namespace}__b")),
        (trailing_namespace.clone(), "b".to_owned(), format!("_3401_{trailing_namespace}_b")),
        (namespace.clone(), "__b__".to_owned(), format!("_3305_{namespace}___b__")),
        (namespace.clone(), "legacy".to_owned(), format!("{namespace}_legacy")),
        (
            long_ascii_schema.clone(),
            long_table.clone(),
            format!("_6363_{long_ascii_schema}_{long_table}"),
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (schema, table, expected))| {
        let schema = TableSchema::new(
            TableId::new(u32::try_from(index + 1).unwrap()),
            TableName::new(schema, table),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
            ],
        );
        (ReplicatedTableSchema::all(Arc::new(schema)), expected)
    })
    .collect()
}
