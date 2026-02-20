use crate::types::SizeHint;
use crate::types::cell::{ArrayCell, Cell};
use std::mem::size_of;

/// Represents a complete row of data from a database table.
///
/// [`TableRow`] contains a vector of [`Cell`] values corresponding to the columns
/// of a database table. The values are ordered to match the table's column order
/// and include proper type information for each cell.
#[derive(Debug, PartialEq)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct TableRow {
    /// Approximate row size in bytes.
    size_hint_bytes: usize,
    /// Column values in table column order
    values: Vec<Cell>,
}

impl TableRow {
    /// Creates a new table row with the given cell values.
    ///
    /// The values should be ordered to match the target table's column schema.
    /// Each [`Cell`] should contain properly typed data for its corresponding column.
    pub fn new(values: Vec<Cell>) -> Self {
        let estimated_allocated_bytes =
            estimate_table_row_allocated_bytes(&values, values.capacity());
        let size_hint_bytes = estimated_allocated_bytes.max(1);

        Self {
            size_hint_bytes,
            values,
        }
    }

    /// Returns the row values in table column order.
    pub fn values(&self) -> &[Cell] {
        &self.values
    }

    /// Returns mutable access to row values in table column order.
    pub fn values_mut(&mut self) -> &mut Vec<Cell> {
        &mut self.values
    }

    /// Consumes the row and returns its values in table column order.
    pub fn into_values(self) -> Vec<Cell> {
        self.values
    }
}

impl SizeHint for TableRow {
    fn size_hint(&self) -> usize {
        self.size_hint_bytes
    }
}

/// Returns an estimate of allocated bytes for a table row payload.
fn estimate_table_row_allocated_bytes(values: &[Cell], values_capacity: usize) -> usize {
    let mut total = size_of::<TableRow>();
    total = total.saturating_add(values_capacity.saturating_mul(size_of::<Cell>()));

    for cell in values {
        total = total.saturating_add(estimate_cell_allocated_bytes(cell));
    }

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
        | Cell::Numeric(_)
        | Cell::Date(_)
        | Cell::Time(_)
        | Cell::Timestamp(_)
        | Cell::TimestampTz(_)
        | Cell::Uuid(_) => 0,
        Cell::String(value) => value.capacity(),
        Cell::Bytes(value) => value.capacity(),
        Cell::Json(value) => estimate_json_allocated_bytes(value),
        Cell::Array(value) => estimate_array_allocated_bytes(value),
    }
}

/// Returns an estimate of additional heap bytes owned by a JSON value.
fn estimate_json_allocated_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => {
            let mut total = values
                .capacity()
                .saturating_mul(size_of::<serde_json::Value>());
            for value in values {
                total = total.saturating_add(estimate_json_allocated_bytes(value));
            }
            total
        }
        serde_json::Value::Object(values) => values.iter().fold(0usize, |acc, (key, value)| {
            acc.saturating_add(key.capacity())
                .saturating_add(estimate_json_allocated_bytes(value))
        }),
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
        ArrayCell::Numeric(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<crate::conversions::numeric::PgNumeric>>()),
        ArrayCell::Date(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<chrono::NaiveDate>>()),
        ArrayCell::Time(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<chrono::NaiveTime>>()),
        ArrayCell::Timestamp(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<chrono::NaiveDateTime>>()),
        ArrayCell::TimestampTz(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<chrono::DateTime<chrono::Utc>>>()),
        ArrayCell::Uuid(values) => values
            .capacity()
            .saturating_mul(size_of::<Option<uuid::Uuid>>()),
        ArrayCell::String(values) => {
            let mut total = values
                .capacity()
                .saturating_mul(size_of::<Option<String>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(value.capacity());
            }
            total
        }
        ArrayCell::Json(values) => {
            let mut total = values
                .capacity()
                .saturating_mul(size_of::<Option<serde_json::Value>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(estimate_json_allocated_bytes(value));
            }
            total
        }
        ArrayCell::Bytes(values) => {
            let mut total = values
                .capacity()
                .saturating_mul(size_of::<Option<Vec<u8>>>());
            for value in values.iter().flatten() {
                total = total.saturating_add(value.capacity());
            }
            total
        }
    }
}
