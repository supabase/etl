use crate::types::Type;
use etl_postgres::types::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema};
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
        assert_eq!(actual.primary_key(), expected.primary_key());
    }
}

/// Asserts that a replicated table schema has the expected columns with all columns replicated.
///
/// Verifies that:
/// - The column names match in order.
/// - The column types match in order.
/// - All columns are marked as replicated (replication mask is all 1s).
///
/// # Panics
///
/// Panics if the column count doesn't match, or if any column name, type, or
/// replication status doesn't match expectations.
pub fn assert_replicated_columns(
    replicated_schema: &ReplicatedTableSchema,
    expected_columns: &[(&str, Type)],
) {
    let columns: Vec<_> = replicated_schema
        .column_schemas()
        .map(|c| (c.name.as_str(), c.typ.clone()))
        .collect();

    assert_eq!(
        columns.len(),
        expected_columns.len(),
        "column count mismatch: got {} columns, expected {}",
        columns.len(),
        expected_columns.len()
    );

    for (i, ((actual_name, actual_type), (expected_name, expected_type))) in
        columns.iter().zip(expected_columns.iter()).enumerate()
    {
        assert_eq!(
            actual_name, expected_name,
            "column name mismatch at index {i}: got '{actual_name}', expected '{expected_name}'"
        );
        assert_eq!(
            actual_type, expected_type,
            "column type mismatch for '{actual_name}' at index {i}: got {actual_type:?}, expected {expected_type:?}"
        );
    }

    let mask = replicated_schema.replication_mask().as_slice();
    assert_eq!(
        mask.len(),
        expected_columns.len(),
        "replication mask length mismatch"
    );
    assert!(
        mask.iter().all(|&bit| bit == 1),
        "expected all columns to be replicated, but mask is {mask:?}"
    );
}
