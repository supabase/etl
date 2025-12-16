use crate::types::Type;
use etl_postgres::types::{ColumnSchema, ReplicatedTableSchema, SnapshotId, TableSchema};

/// Asserts that two column schemas are equal.
pub fn assert_column_schema_eq(actual: &ColumnSchema, expected: &ColumnSchema) {
    assert_eq!(
        actual.name, expected.name,
        "column name mismatch: got '{}', expected '{}'",
        actual.name, expected.name
    );
    assert_eq!(
        actual.typ, expected.typ,
        "column '{}' type mismatch: got {:?}, expected {:?}",
        actual.name, actual.typ, expected.typ
    );
    assert_eq!(
        actual.modifier, expected.modifier,
        "column '{}' modifier mismatch: got {}, expected {}",
        actual.name, actual.modifier, expected.modifier
    );
    assert_eq!(
        actual.nullable, expected.nullable,
        "column '{}' nullable mismatch: got {}, expected {}",
        actual.name, actual.nullable, expected.nullable
    );
    assert_eq!(
        actual.primary_key(),
        expected.primary_key(),
        "column '{}' primary_key mismatch: got {}, expected {}",
        actual.name,
        actual.primary_key(),
        expected.primary_key()
    );
}

/// Asserts that a column has the expected name and type.
pub fn assert_column_name_type(column: &ColumnSchema, expected_name: &str, expected_type: &Type) {
    assert_eq!(
        column.name, expected_name,
        "column name mismatch: got '{}', expected '{expected_name}'",
        column.name
    );
    assert_eq!(
        &column.typ, expected_type,
        "column '{expected_name}' type mismatch: got {:?}, expected {expected_type:?}",
        column.typ
    );
}

/// Asserts that a column has the expected name.
pub fn assert_column_name(column: &ColumnSchema, expected_name: &str) {
    assert_eq!(
        column.name, expected_name,
        "column name mismatch: got '{}', expected '{expected_name}'",
        column.name
    );
}

/// Asserts that columns match the expected column schemas.
pub fn assert_columns_eq<'a>(
    columns: impl Iterator<Item = &'a ColumnSchema>,
    expected_columns: &[ColumnSchema],
) {
    let columns: Vec<_> = columns.collect();
    assert_eq!(
        columns.len(),
        expected_columns.len(),
        "column count mismatch: got {}, expected {}",
        columns.len(),
        expected_columns.len()
    );

    for (actual, expected) in columns.iter().zip(expected_columns.iter()) {
        assert_column_schema_eq(actual, expected);
    }
}

/// Asserts that columns have the expected names and types.
pub fn assert_columns_names_types<'a>(
    columns: impl Iterator<Item = &'a ColumnSchema>,
    expected_columns: &[(&str, Type)],
) {
    let columns: Vec<_> = columns.collect();
    assert_eq!(
        columns.len(),
        expected_columns.len(),
        "column count mismatch: got {}, expected {}",
        columns.len(),
        expected_columns.len()
    );

    for (i, (actual, (expected_name, expected_type))) in
        columns.iter().zip(expected_columns.iter()).enumerate()
    {
        assert_eq!(
            actual.name, *expected_name,
            "column name mismatch at index {i}: got '{}', expected '{expected_name}'",
            actual.name
        );
        assert_eq!(
            actual.typ, *expected_type,
            "column '{expected_name}' type mismatch at index {i}: got {:?}, expected {expected_type:?}",
            actual.typ
        );
    }
}

/// Asserts that columns have the expected names.
pub fn assert_columns_names<'a>(
    columns: impl Iterator<Item = &'a ColumnSchema>,
    expected_names: &[&str],
) {
    let columns: Vec<_> = columns.collect();
    assert_eq!(
        columns.len(),
        expected_names.len(),
        "column count mismatch: got {}, expected {}",
        columns.len(),
        expected_names.len()
    );

    for (i, (actual, expected_name)) in columns.iter().zip(expected_names.iter()).enumerate() {
        assert_eq!(
            actual.name, *expected_name,
            "column name mismatch at index {i}: got '{}', expected '{expected_name}'",
            actual.name
        );
    }
}

/// Asserts that a table schema has columns matching the expected column schemas.
pub fn assert_table_schema_columns(schema: &TableSchema, expected_columns: &[ColumnSchema]) {
    assert_columns_eq(schema.column_schemas.iter(), expected_columns);
}

/// Asserts that a table schema has columns with the expected names and types.
pub fn assert_table_schema_column_names_types(
    schema: &TableSchema,
    expected_columns: &[(&str, Type)],
) {
    assert_columns_names_types(schema.column_schemas.iter(), expected_columns);
}

/// Asserts that a table schema has columns with the expected names.
pub fn assert_table_schema_column_names(schema: &TableSchema, expected_names: &[&str]) {
    assert_columns_names(schema.column_schemas.iter(), expected_names);
}

/// Asserts that a replicated table schema has columns matching the expected column schemas,
/// and that all columns are replicated.
pub fn assert_replicated_schema_columns(
    schema: &ReplicatedTableSchema,
    expected_columns: &[ColumnSchema],
) {
    assert_columns_eq(schema.column_schemas(), expected_columns);
    assert_all_columns_replicated(schema, expected_columns.len());
}

/// Asserts that a replicated table schema has columns with the expected names and types,
/// and that all columns are replicated.
pub fn assert_replicated_schema_column_names_types(
    schema: &ReplicatedTableSchema,
    expected_columns: &[(&str, Type)],
) {
    assert_columns_names_types(schema.column_schemas(), expected_columns);
    assert_all_columns_replicated(schema, expected_columns.len());
}

/// Asserts that a replicated table schema has columns with the expected names,
/// and that all columns are replicated.
pub fn assert_replicated_schema_column_names(
    schema: &ReplicatedTableSchema,
    expected_names: &[&str],
) {
    assert_columns_names(schema.column_schemas(), expected_names);
    assert_all_columns_replicated(schema, expected_names.len());
}

/// Asserts that all columns in the replication mask are set to 1.
fn assert_all_columns_replicated(schema: &ReplicatedTableSchema, expected_len: usize) {
    let mask = schema.replication_mask().as_slice();
    assert_eq!(
        mask.len(),
        expected_len,
        "replication mask length mismatch: got {}, expected {}",
        mask.len(),
        expected_len
    );
    assert!(
        mask.iter().all(|&bit| bit == 1),
        "expected all columns to be replicated, but mask is {mask:?}"
    );
}

/// Asserts that schema snapshots are in strictly increasing order by snapshot ID.
///
/// If `first_is_zero` is true, the first snapshot ID must be 0.
/// If `first_is_zero` is false, the first snapshot ID must be > 0.
/// Each subsequent snapshot ID must be strictly greater than the previous one.
pub fn assert_schema_snapshots_ordering(
    snapshots: &[(SnapshotId, TableSchema)],
    first_is_zero: bool,
) {
    assert!(
        !snapshots.is_empty(),
        "expected at least one schema snapshot"
    );

    let (first_snapshot_id, _) = &snapshots[0];
    if first_is_zero {
        assert_eq!(
            *first_snapshot_id,
            SnapshotId::initial(),
            "first snapshot_id is {first_snapshot_id}, expected 0"
        );
    } else {
        assert!(
            *first_snapshot_id > SnapshotId::initial(),
            "first snapshot_id is {first_snapshot_id}, expected > 0"
        );
    }

    for i in 1..snapshots.len() {
        let (prev_snapshot_id, _) = &snapshots[i - 1];
        let (snapshot_id, _) = &snapshots[i];
        assert!(
            *snapshot_id > *prev_snapshot_id,
            "snapshot at index {i} has snapshot_id {snapshot_id} which is not greater than previous snapshot_id {prev_snapshot_id}"
        );
    }
}
