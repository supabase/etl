use crate::types::cell::Cell;

/// Represents a complete row of data from a database table.
///
/// [`TableRow`] contains a vector of [`Cell`] values corresponding to the columns
/// of a database table. The values are ordered to match the table's column order
/// and include proper type information for each cell.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRow {
    /// Column values in table column order
    pub values: Vec<Cell>,
}

impl TableRow {
    /// Creates a new table row with the given cell values.
    ///
    /// The values should be ordered to match the target table's column schema.
    /// Each [`Cell`] should contain properly typed data for its corresponding column.
    pub fn new(values: Vec<Cell>) -> Self {
        Self { values }
    }
}
