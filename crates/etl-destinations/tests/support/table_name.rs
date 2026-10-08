//! Source-name fixtures shared by the destination naming lifecycle tests.

use std::sync::Arc;

use data_encoding::BASE32_NOPAD;
use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type};

/// Returns an ambiguous legacy pair, a case variant, an unchanged legacy name,
/// and a maximum-byte-length name, each with its expected destination base.
pub(crate) fn table_name_schemas() -> Vec<(ReplicatedTableSchema, String)> {
    // Snowflake tests share a schema, so every invocation owns unique names.
    let namespace = format!("n{}", uuid::Uuid::new_v4().simple());
    let encoded_namespace = BASE32_NOPAD.encode(namespace.as_bytes());
    let trailing_namespace = format!("{namespace}_");
    let long_schema = format!("_{namespace}{}_", "é".repeat(14));
    let long_table = "_".repeat(63);
    assert_eq!(long_schema.len(), 63);
    [
        (namespace.clone(), "_b".to_owned(), format!("_ETL1_{encoded_namespace}_L5RA")),
        (
            trailing_namespace.clone(),
            "b".to_owned(),
            format!("_ETL1_{}_MI", BASE32_NOPAD.encode(trailing_namespace.as_bytes())),
        ),
        (namespace.clone(), "_B".to_owned(), format!("_ETL1_{encoded_namespace}_L5BA")),
        (namespace.clone(), "legacy".to_owned(), format!("{namespace}_legacy")),
        (
            long_schema.clone(),
            long_table.clone(),
            format!(
                "_ETL1_{}_{}",
                BASE32_NOPAD.encode(long_schema.as_bytes()),
                BASE32_NOPAD.encode(long_table.as_bytes())
            ),
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
