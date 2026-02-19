use crate::types::SizeHint;
use crate::types::cell::Cell;

/// Represents a complete row of data from a database table.
///
/// [`TableRow`] contains a vector of [`Cell`] values corresponding to the columns
/// of a database table. The values are ordered to match the table's column order
/// and include proper type information for each cell.
#[derive(Debug, Clone, PartialEq)]
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
        Self::new_with_size_hint(values, 0)
    }

    /// Creates a new table row with an explicit approximate size hint in bytes.
    pub fn new_with_size_hint(values: Vec<Cell>, size_hint_bytes: usize) -> Self {
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
