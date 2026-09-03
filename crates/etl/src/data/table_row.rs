use std::mem::size_of;

use crate::data::{ArrayCell, Cell, PgNumeric, PgTimeTz, SizeHint, owned_heap_size_hint};

/// Represents a complete row of data from a database table.
///
/// [`TableRow`] contains a vector of [`Cell`] values corresponding to the
/// columns of a database table. The values are ordered to match the table's
/// column order and include proper type information for each cell.
#[derive(Debug)]
pub struct TableRow {
    /// Cached decoded in-memory size, or zero after mutable access.
    size_hint_bytes: usize,
    /// Column values in table column order.
    values: Vec<Cell>,
}

impl TableRow {
    /// Creates a new table row with the given cell values.
    ///
    /// The values should be ordered to match the target table's column schema.
    /// Each [`Cell`] should contain properly typed data for its corresponding
    /// column.
    pub fn new(values: Vec<Cell>) -> Self {
        let size_hint_bytes = estimate_table_row_allocated_bytes(&values, values.capacity());

        Self { size_hint_bytes, values }
    }

    /// Returns the row values in table column order.
    pub fn values(&self) -> &[Cell] {
        &self.values
    }

    /// Returns mutable access to row values in table column order.
    pub fn values_mut(&mut self) -> &mut Vec<Cell> {
        // The returned Vec can change both its allocation and nested cell
        // allocations, so a later size query must recompute the estimate.
        self.size_hint_bytes = 0;
        &mut self.values
    }

    /// Consumes the row and returns its values in table column order.
    pub fn into_values(self) -> Vec<Cell> {
        self.values
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Clone for TableRow {
    fn clone(&self) -> Self {
        Self::new(self.values.clone())
    }
}

impl PartialEq for TableRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl SizeHint for TableRow {
    fn size_hint(&self) -> usize {
        if self.size_hint_bytes == 0 {
            estimate_table_row_allocated_bytes(&self.values, self.values.capacity())
        } else {
            self.size_hint_bytes
        }
    }
}

/// Represents a partial row image from a replication event.
///
/// Partial rows preserve the present values in replicated-schema order and
/// separately record which replicated-column positions are missing.
#[derive(Debug)]
pub struct PartialTableRow {
    /// Cached decoded in-memory size.
    size_hint_bytes: usize,
    /// Total number of replicated columns for the table schema.
    total_columns: usize,
    /// Present values in replicated-schema order, excluding missing columns.
    table_row: TableRow,
    /// Zero-based replicated-column indexes that are missing from the row.
    missing_column_indexes: Vec<usize>,
}

impl PartialTableRow {
    /// Creates a new partial row.
    pub fn new(
        total_columns: usize,
        table_row: TableRow,
        missing_column_indexes: Vec<usize>,
    ) -> Self {
        let size_hint_bytes = estimate_partial_table_row_allocated_bytes(
            &table_row,
            missing_column_indexes.capacity(),
        );

        Self { size_hint_bytes, total_columns, table_row, missing_column_indexes }
    }

    /// Returns the total number of replicated columns for this table.
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }

    /// Returns the present row values.
    pub fn table_row(&self) -> &TableRow {
        &self.table_row
    }

    /// Returns the present row values in replicated table-column order,
    /// excluding missing columns.
    pub fn values(&self) -> &[Cell] {
        self.table_row.values()
    }

    /// Returns the missing replicated-column indexes.
    pub fn missing_column_indexes(&self) -> &[usize] {
        &self.missing_column_indexes
    }

    /// Consumes the row and returns the present values and missing indexes.
    pub fn into_parts(self) -> (TableRow, Vec<usize>) {
        (self.table_row, self.missing_column_indexes)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Clone for PartialTableRow {
    fn clone(&self) -> Self {
        Self::new(self.total_columns, self.table_row.clone(), self.missing_column_indexes.clone())
    }
}

impl PartialEq for PartialTableRow {
    fn eq(&self, other: &Self) -> bool {
        self.total_columns == other.total_columns
            && self.table_row == other.table_row
            && self.missing_column_indexes == other.missing_column_indexes
    }
}

impl SizeHint for PartialTableRow {
    fn size_hint(&self) -> usize {
        self.size_hint_bytes
    }
}

/// Represents a row image that may be full or partial.
#[derive(Debug, PartialEq)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub enum UpdatedTableRow {
    /// A complete row image with all replicated columns present.
    Full(TableRow),
    /// A partial row image containing only the source values we could
    /// reconstruct, plus indexes for the missing replicated columns.
    Partial(PartialTableRow),
}

impl UpdatedTableRow {
    /// Returns the full row when available.
    pub fn as_full(&self) -> Option<&TableRow> {
        match self {
            Self::Full(row) => Some(row),
            Self::Partial(_) => None,
        }
    }
}

impl SizeHint for UpdatedTableRow {
    fn size_hint(&self) -> usize {
        let owned_heap_bytes = match self {
            Self::Full(row) => owned_heap_size_hint(row),
            Self::Partial(row) => owned_heap_size_hint(row),
        };

        size_of::<Self>().saturating_add(owned_heap_bytes)
    }
}

/// Old-row image carried by logical replication for updates and deletes.
///
/// This enum preserves the old-side tuple shape that PostgreSQL exposed to the
/// replication stream:
///
/// - [`OldTableRow::Full`] means PostgreSQL emitted a full old tuple. In
///   practice this is the `REPLICA IDENTITY FULL` case.
/// - [`OldTableRow::Key`] means PostgreSQL emitted only the replica-identity
///   columns.
///
/// Key rows are stored densely in replicated table-column order after
/// filtering to just the identity columns. They are therefore not necessarily
/// the table's primary key; they represent whatever the source table exposed as
/// replica identity.
#[derive(Debug, PartialEq)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub enum OldTableRow {
    /// Complete old row in replicated table-column order.
    Full(TableRow),
    /// Replica-identity columns only, in replicated table-column order.
    Key(TableRow),
}

impl OldTableRow {
    /// Returns the full row payload when available.
    pub fn as_full(&self) -> Option<&TableRow> {
        match self {
            Self::Full(row) => Some(row),
            Self::Key(_) => None,
        }
    }
}

impl SizeHint for OldTableRow {
    fn size_hint(&self) -> usize {
        let owned_heap_bytes = match self {
            Self::Full(row) | Self::Key(row) => owned_heap_size_hint(row),
        };

        size_of::<Self>().saturating_add(owned_heap_bytes)
    }
}

/// Returns an estimate of allocated bytes for a table row payload.
fn estimate_table_row_allocated_bytes(values: &[Cell], values_capacity: usize) -> usize {
    let mut total =
        size_of::<TableRow>().saturating_add(values_capacity.saturating_mul(size_of::<Cell>()));

    for cell in values {
        total = total.saturating_add(estimate_cell_allocated_bytes(cell));
    }

    total
}

/// Returns an estimate of allocated bytes for a partial row payload.
fn estimate_partial_table_row_allocated_bytes(
    table_row: &TableRow,
    missing_column_indexes_capacity: usize,
) -> usize {
    let mut total = size_of::<PartialTableRow>().saturating_add(owned_heap_size_hint(table_row));
    total =
        total.saturating_add(missing_column_indexes_capacity.saturating_mul(size_of::<usize>()));

    total
}

/// Returns an estimate of additional heap bytes owned by a single [`Cell`].
fn estimate_cell_allocated_bytes(cell: &Cell) -> usize {
    match cell {
        Cell::Null
        | Cell::Bool(_)
        | Cell::I16(_)
        | Cell::I32(_)
        | Cell::U32(_)
        | Cell::I64(_)
        | Cell::F32(_)
        | Cell::F64(_)
        | Cell::Date(_)
        | Cell::Time(_)
        | Cell::TimeTz(_)
        | Cell::Timestamp(_)
        | Cell::TimestampTz(_)
        | Cell::Uuid(_) => 0,
        Cell::Numeric(value) => estimated_pg_numeric_allocated_bytes(value),
        Cell::String(value) => value.capacity(),
        Cell::Bytes(value) => value.capacity(),
        Cell::Json(value) => estimate_json_allocated_bytes(value),
        Cell::Array(value) => estimate_array_allocated_bytes(value),
    }
}

/// Returns an estimate of additional heap bytes owned by a [`PgNumeric`].
fn estimated_pg_numeric_allocated_bytes(value: &PgNumeric) -> usize {
    match &value {
        PgNumeric::Value { digits, .. } => digits.capacity().saturating_mul(size_of::<i16>()),
        PgNumeric::NaN | PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => 0,
    }
}

/// Returns the owned text bytes of an arbitrary-precision JSON number.
fn estimate_json_number_allocated_bytes(value: &serde_json::Number) -> usize {
    value.as_str().len()
}

/// Returns an estimate of additional heap bytes owned by a JSON value.
fn estimate_json_allocated_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) => 0,
        serde_json::Value::Number(value) => estimate_json_number_allocated_bytes(value),
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => {
            let mut total = values.capacity().saturating_mul(size_of::<serde_json::Value>());
            for value in values {
                total = total.saturating_add(estimate_json_allocated_bytes(value));
            }
            total
        }
        serde_json::Value::Object(values) => {
            // Count stable key/value storage without depending on serde_json's
            // private map-node layout or allocation strategy.
            let mut total = values
                .len()
                .saturating_mul(size_of::<String>().saturating_add(size_of::<serde_json::Value>()));

            for (key, value) in values {
                total = total
                    .saturating_add(key.capacity())
                    .saturating_add(estimate_json_allocated_bytes(value));
            }

            total
        }
    }
}

/// Returns an estimate of additional heap bytes owned by an [`ArrayCell`].
fn estimate_array_allocated_bytes(value: &ArrayCell) -> usize {
    match value {
        ArrayCell::Bool(values) => values.capacity().saturating_mul(size_of::<Option<bool>>()),
        ArrayCell::I16(values) => values.capacity().saturating_mul(size_of::<Option<i16>>()),
        ArrayCell::I32(values) => values.capacity().saturating_mul(size_of::<Option<i32>>()),
        ArrayCell::U32(values) => values.capacity().saturating_mul(size_of::<Option<u32>>()),
        ArrayCell::I64(values) => values.capacity().saturating_mul(size_of::<Option<i64>>()),
        ArrayCell::F32(values) => values.capacity().saturating_mul(size_of::<Option<f32>>()),
        ArrayCell::F64(values) => values.capacity().saturating_mul(size_of::<Option<f64>>()),
        ArrayCell::Numeric(values) => {
            let mut total = values.capacity().saturating_mul(size_of::<Option<PgNumeric>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(estimated_pg_numeric_allocated_bytes(value));
            }
            total
        }
        ArrayCell::Date(values) => {
            values.capacity().saturating_mul(size_of::<Option<chrono::NaiveDate>>())
        }
        ArrayCell::Time(values) => {
            values.capacity().saturating_mul(size_of::<Option<chrono::NaiveTime>>())
        }
        ArrayCell::TimeTz(values) => {
            values.capacity().saturating_mul(size_of::<Option<PgTimeTz>>())
        }
        ArrayCell::Timestamp(values) => {
            values.capacity().saturating_mul(size_of::<Option<chrono::NaiveDateTime>>())
        }
        ArrayCell::TimestampTz(values) => {
            values.capacity().saturating_mul(size_of::<Option<chrono::DateTime<chrono::Utc>>>())
        }
        ArrayCell::Uuid(values) => {
            values.capacity().saturating_mul(size_of::<Option<uuid::Uuid>>())
        }
        ArrayCell::String(values) => {
            let mut total = values.capacity().saturating_mul(size_of::<Option<String>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(value.capacity());
            }
            total
        }
        ArrayCell::Json(values) => {
            let mut total =
                values.capacity().saturating_mul(size_of::<Option<serde_json::Value>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(estimate_json_allocated_bytes(value));
            }
            total
        }
        ArrayCell::Bytes(values) => {
            let mut total = values.capacity().saturating_mul(size_of::<Option<Vec<u8>>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(value.capacity());
            }
            total
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn table_row_size_hint_uses_container_and_nested_capacities() {
        let mut text = String::with_capacity(127);
        text.push_str("text");
        let text_capacity = text.capacity();

        let mut bytes = Vec::with_capacity(257);
        bytes.extend_from_slice(&[1, 2, 3]);
        let bytes_capacity = bytes.capacity();

        let numeric = PgNumeric::from_str("123456789012345678901234567890.1234").unwrap();
        let numeric_bytes = match &numeric {
            PgNumeric::Value { digits, .. } => digits.capacity() * size_of::<i16>(),
            _ => unreachable!(),
        };

        let mut strings = Vec::with_capacity(5);
        let mut nested_text = String::with_capacity(63);
        nested_text.push('x');
        let nested_text_capacity = nested_text.capacity();
        strings.push(Some(nested_text));
        strings.push(None);
        let strings_bytes = strings.capacity() * size_of::<Option<String>>() + nested_text_capacity;

        let mut values = Vec::with_capacity(8);
        values.push(Cell::String(text));
        values.push(Cell::Bytes(bytes));
        values.push(Cell::Numeric(numeric));
        values.push(Cell::Array(ArrayCell::String(strings)));
        let values_capacity = values.capacity();

        let row = TableRow::new(values);
        let expected = size_of::<TableRow>()
            + values_capacity * size_of::<Cell>()
            + text_capacity
            + bytes_capacity
            + numeric_bytes
            + strings_bytes;

        assert_eq!(row.size_hint(), expected);
        assert_eq!(row.size_hint(), expected);
    }

    #[test]
    fn mutable_row_access_invalidates_the_cached_size_hint() {
        let mut row = TableRow::new(vec![Cell::I64(1)]);
        let initial_size_hint = row.size_hint();

        let mut text = String::with_capacity(1_024);
        text.push_str("new value");
        row.values_mut().push(Cell::String(text));

        assert_eq!(row.size_hint_bytes, 0);
        assert_eq!(
            row.size_hint(),
            estimate_table_row_allocated_bytes(row.values(), row.values.capacity())
        );
        assert!(row.size_hint() > initial_size_hint);
    }

    #[test]
    fn cloned_rows_rebuild_size_hint_caches_for_cloned_capacities() {
        let mut text = String::with_capacity(1_024);
        text.push_str("payload");
        let mut row = TableRow::new(Vec::with_capacity(8));
        row.values_mut().push(Cell::String(text));

        let cloned_row = row.clone();
        assert_ne!(cloned_row.size_hint_bytes, 0);
        assert_eq!(
            cloned_row.size_hint(),
            estimate_table_row_allocated_bytes(cloned_row.values(), cloned_row.values.capacity(),)
        );

        let mut missing_column_indexes = Vec::with_capacity(8);
        missing_column_indexes.push(1);
        let partial = PartialTableRow::new(2, cloned_row, missing_column_indexes);
        let cloned_partial = partial.clone();
        assert_eq!(partial.total_columns(), 2);
        assert_eq!(
            cloned_partial.size_hint(),
            estimate_partial_table_row_allocated_bytes(
                cloned_partial.table_row(),
                cloned_partial.missing_column_indexes.capacity(),
            )
        );
    }

    #[test]
    fn json_size_hint_covers_owned_numbers_arrays_and_objects() {
        let number: serde_json::Value =
            serde_json::from_str("123456789012345678901234567890").unwrap();
        assert_eq!(estimate_json_allocated_bytes(&number), 30);

        let array: serde_json::Value =
            serde_json::from_str(r#"[1,2,3,4,5,"a string payload that owns heap memory"]"#)
                .unwrap();
        let serde_json::Value::Array(array_values) = &array else {
            unreachable!();
        };
        let expected_array_bytes = array_values.capacity() * size_of::<serde_json::Value>()
            + array_values.iter().map(estimate_json_allocated_bytes).sum::<usize>();
        assert_eq!(estimate_json_allocated_bytes(&array), expected_array_bytes);

        let object: serde_json::Value =
            serde_json::from_str(r#"{"key":"nested string payload"}"#).unwrap();
        let serde_json::Value::Object(object_values) = &object else {
            unreachable!();
        };
        let (key, value) = object_values.iter().next().unwrap();
        let expected_object_bytes = size_of::<String>()
            + size_of::<serde_json::Value>()
            + key.capacity()
            + estimate_json_allocated_bytes(value);
        assert_eq!(estimate_json_allocated_bytes(&object), expected_object_bytes);
    }

    #[test]
    fn array_size_hints_cover_scalar_and_nested_allocations() {
        let mut integers = Vec::with_capacity(7);
        integers.push(Some(1));
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::I64(integers)),
            7 * size_of::<Option<i64>>()
        );

        let numeric = PgNumeric::from_str("123456789012345678901234567890.1234").unwrap();
        let numeric_heap_bytes = estimated_pg_numeric_allocated_bytes(&numeric);
        let mut numerics = Vec::with_capacity(5);
        numerics.push(Some(numeric));
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::Numeric(numerics)),
            5 * size_of::<Option<PgNumeric>>() + numeric_heap_bytes
        );

        let mut bytes = Vec::with_capacity(257);
        bytes.push(1);
        let mut byte_arrays = Vec::with_capacity(3);
        byte_arrays.push(Some(bytes));
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::Bytes(byte_arrays)),
            3 * size_of::<Option<Vec<u8>>>() + 257
        );

        let json: serde_json::Value =
            serde_json::from_str(r#"{"object":[1,2,3],"text":"payload"}"#).unwrap();
        let json_heap_bytes = estimate_json_allocated_bytes(&json);
        let mut json_values = Vec::with_capacity(4);
        json_values.push(Some(json));
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::Json(json_values)),
            4 * size_of::<Option<serde_json::Value>>() + json_heap_bytes
        );
    }

    #[test]
    fn row_wrapper_hints_do_not_double_count_inline_rows() {
        let mut text = String::with_capacity(211);
        text.push('x');
        let table_row = TableRow::new(vec![Cell::String(text)]);
        let table_row_heap_bytes = owned_heap_size_hint(&table_row);

        let mut missing_column_indexes = Vec::with_capacity(4);
        missing_column_indexes.push(1);
        let missing_column_indexes_capacity = missing_column_indexes.capacity();
        let partial = PartialTableRow::new(2, table_row, missing_column_indexes);

        assert_eq!(
            partial.size_hint(),
            size_of::<PartialTableRow>()
                + table_row_heap_bytes
                + missing_column_indexes_capacity * size_of::<usize>()
        );
        let cloned_partial = partial.clone();
        assert_eq!(partial.total_columns(), 2);
        let cloned_partial_heap_bytes = owned_heap_size_hint(&cloned_partial);
        assert_eq!(
            UpdatedTableRow::Partial(cloned_partial).size_hint(),
            size_of::<UpdatedTableRow>() + cloned_partial_heap_bytes
        );
        assert_eq!(
            OldTableRow::Key(TableRow::new(vec![Cell::I64(1)])).size_hint(),
            size_of::<OldTableRow>() + size_of::<Cell>()
        );
    }
}
