use crate::types::cell::{ArrayCell, Cell};
use crate::types::{PgNumeric, SizeHint};
use std::mem::size_of;
use tracing::warn;

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
        let size_hint_bytes = estimate_table_row_allocated_bytes(&values, values.capacity());

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
    total = checked_add_or_saturating(
        total,
        checked_mul_or_saturating(
            values_capacity,
            size_of::<Cell>(),
            "table_row.values_capacity_mul_cell_size",
        ),
        "table_row.base_add_values_capacity",
    );

    for cell in values {
        total = checked_add_or_saturating(
            total,
            estimate_cell_allocated_bytes(cell),
            "table_row.add_cell_heap_bytes",
        );
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
        | Cell::Date(_)
        | Cell::Time(_)
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
        PgNumeric::Value { digits, .. } => checked_mul_or_saturating(
            digits.capacity(),
            size_of::<i16>(),
            "pg_numeric.digits_capacity_mul_i16_size",
        ),
        PgNumeric::NaN | PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => 0,
    }
}

/// Returns an estimate of additional heap bytes owned by a JSON value.
fn estimate_json_allocated_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => {
            let mut total = checked_mul_or_saturating(
                values.capacity(),
                size_of::<serde_json::Value>(),
                "json.array_capacity_mul_value_size",
            );
            for value in values {
                total = checked_add_or_saturating(
                    total,
                    estimate_json_allocated_bytes(value),
                    "json.add_nested_value_bytes",
                );
            }
            total
        }
        serde_json::Value::Object(values) => values.iter().fold(0usize, |acc, (key, value)| {
            let with_key =
                checked_add_or_saturating(acc, key.capacity(), "json.object_add_key_capacity");
            checked_add_or_saturating(
                with_key,
                estimate_json_allocated_bytes(value),
                "json.object_add_nested_value_bytes",
            )
        }),
    }
}

/// Returns an estimate of additional heap bytes owned by an [`ArrayCell`].
fn estimate_array_allocated_bytes(value: &ArrayCell) -> usize {
    match value {
        ArrayCell::Bool(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<bool>>(),
            "array.bool_capacity_mul_option_size",
        ),
        ArrayCell::I16(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<i16>>(),
            "array.i16_capacity_mul_option_size",
        ),
        ArrayCell::I32(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<i32>>(),
            "array.i32_capacity_mul_option_size",
        ),
        ArrayCell::U32(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<u32>>(),
            "array.u32_capacity_mul_option_size",
        ),
        ArrayCell::I64(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<i64>>(),
            "array.i64_capacity_mul_option_size",
        ),
        ArrayCell::F32(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<f32>>(),
            "array.f32_capacity_mul_option_size",
        ),
        ArrayCell::F64(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<f64>>(),
            "array.f64_capacity_mul_option_size",
        ),
        ArrayCell::Numeric(values) => {
            let mut total = checked_mul_or_saturating(
                values.capacity(),
                size_of::<Option<PgNumeric>>(),
                "array.numeric_capacity_mul_option_size",
            );
            for value in values.iter().flatten() {
                total = checked_add_or_saturating(
                    total,
                    estimated_pg_numeric_allocated_bytes(value),
                    "array.numeric_add_element_heap_bytes",
                );
            }
            total
        }
        ArrayCell::Date(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<chrono::NaiveDate>>(),
            "array.date_capacity_mul_option_size",
        ),
        ArrayCell::Time(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<chrono::NaiveTime>>(),
            "array.time_capacity_mul_option_size",
        ),
        ArrayCell::Timestamp(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<chrono::NaiveDateTime>>(),
            "array.timestamp_capacity_mul_option_size",
        ),
        ArrayCell::TimestampTz(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<chrono::DateTime<chrono::Utc>>>(),
            "array.timestamptz_capacity_mul_option_size",
        ),
        ArrayCell::Uuid(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<uuid::Uuid>>(),
            "array.uuid_capacity_mul_option_size",
        ),
        ArrayCell::String(values) => {
            let mut total = checked_mul_or_saturating(
                values.capacity(),
                size_of::<Option<String>>(),
                "array.string_capacity_mul_option_size",
            );
            for value in values.iter().flatten() {
                total = checked_add_or_saturating(
                    total,
                    value.capacity(),
                    "array.string_add_element_capacity",
                );
            }
            total
        }
        ArrayCell::Json(values) => {
            let mut total = checked_mul_or_saturating(
                values.capacity(),
                size_of::<Option<serde_json::Value>>(),
                "array.json_capacity_mul_option_size",
            );
            for value in values.iter().flatten() {
                total = checked_add_or_saturating(
                    total,
                    estimate_json_allocated_bytes(value),
                    "array.json_add_element_heap_bytes",
                );
            }
            total
        }
        ArrayCell::Bytes(values) => {
            let mut total = checked_mul_or_saturating(
                values.capacity(),
                size_of::<Option<Vec<u8>>>(),
                "array.bytes_capacity_mul_option_size",
            );
            for value in values.iter().flatten() {
                total = checked_add_or_saturating(
                    total,
                    value.capacity(),
                    "array.bytes_add_element_capacity",
                );
            }
            total
        }
    }
}

/// Returns `left + right`, saturating on overflow while emitting a warning.
fn checked_add_or_saturating(left: usize, right: usize, context: &'static str) -> usize {
    match left.checked_add(right) {
        Some(value) => value,
        None => {
            warn!(
                context,
                left, right, "size hint addition overflowed, saturating to usize::MAX"
            );

            usize::MAX
        }
    }
}

/// Returns `left * right`, saturating on overflow while emitting a warning.
fn checked_mul_or_saturating(left: usize, right: usize, context: &'static str) -> usize {
    match left.checked_mul(right) {
        Some(value) => value,
        None => {
            warn!(
                context,
                left, right, "size hint multiplication overflowed, saturating to usize::MAX"
            );

            usize::MAX
        }
    }
}
