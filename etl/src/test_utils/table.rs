use etl_postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use std::collections::HashMap;

/// Asserts that a table schema matches the expected schema.
///
/// Compares all aspects of the table schema including table ID, name, and column
/// definitions. Each column's properties (name, type, modifier, nullability, and
/// primary key status) are verified.
///
/// # Panics
///
/// Panics if the table ID doesn't exist in the provided schemas, or if any aspect
/// of the schema doesn't match the expected values.
pub fn assert_table_schema(
    table_schemas: &HashMap<TableId, TableSchema>,
    table_id: TableId,
    expected_table_name: TableName,
    expected_columns: &[ColumnSchema],
) {
    let table_schema = table_schemas.get(&table_id).unwrap();

    assert_eq!(table_schema.id, table_id);
    assert_eq!(table_schema.name, expected_table_name);

    let columns = &table_schema.column_schemas;
    assert_eq!(columns.len(), expected_columns.len());

    for (actual, expected) in columns.iter().zip(expected_columns.iter()) {
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.typ, expected.typ);
        assert_eq!(actual.modifier, expected.modifier);
        assert_eq!(actual.nullable, expected.nullable);
        assert_eq!(actual.primary, expected.primary);
    }
}
