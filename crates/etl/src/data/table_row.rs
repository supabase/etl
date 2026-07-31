use std::mem::size_of;

use crate::data::{
    PgNumeric, PgTimeTz, SizeHint,
    cell::{ArrayCell, Cell},
};

/// Represents a complete row of data from a database table.
///
/// [`TableRow`] contains a vector of [`Cell`] values corresponding to the
/// columns of a database table. The values are ordered to match the table's
/// column order and include proper type information for each cell.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct TableRow {
    /// Approximate decoded in-memory size hint in bytes.
    size_hint_bytes: usize,
    /// Column values in table column order
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
        &mut self.values
    }

    /// Consumes the row and returns its values in table column order.
    pub fn into_values(self) -> Vec<Cell> {
        self.values
    }
}

impl PartialEq for TableRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl SizeHint for TableRow {
    fn size_hint(&self) -> usize {
        self.size_hint_bytes
    }
}

/// Represents a partial row image from a replication event.
///
/// Partial rows preserve the present values in replicated-schema order and
/// separately record which replicated-column positions are missing.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct PartialTableRow {
    /// Approximate decoded in-memory size hint in bytes.
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
            &missing_column_indexes,
            missing_column_indexes.capacity(),
            total_columns,
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

    /// Consumes the row and returns the present values.
    pub fn into_values(self) -> Vec<Cell> {
        self.table_row.into_values()
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
    /// Returns whether this row image is partial.
    pub fn is_partial(&self) -> bool {
        matches!(self, Self::Partial(_))
    }

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
        match self {
            Self::Full(row) => row.size_hint(),
            Self::Partial(row) => row.size_hint(),
        }
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
    /// Returns whether this image contains only replica-identity columns.
    pub fn is_key(&self) -> bool {
        matches!(self, Self::Key(_))
    }

    /// Returns the full row payload when available.
    pub fn as_full(&self) -> Option<&TableRow> {
        match self {
            Self::Full(row) => Some(row),
            Self::Key(_) => None,
        }
    }

    /// Consumes the image and returns the full row payload when available.
    pub fn into_full(self) -> Option<TableRow> {
        match self {
            Self::Full(row) => Some(row),
            Self::Key(_) => None,
        }
    }

    /// Returns the key row payload when available.
    pub fn as_key(&self) -> Option<&TableRow> {
        match self {
            Self::Full(_) => None,
            Self::Key(row) => Some(row),
        }
    }

    /// Consumes the image and returns the key row payload when available.
    pub fn into_key(self) -> Option<TableRow> {
        match self {
            Self::Full(_) => None,
            Self::Key(row) => Some(row),
        }
    }
}

impl SizeHint for OldTableRow {
    fn size_hint(&self) -> usize {
        match self {
            Self::Full(row) | Self::Key(row) => row.size_hint(),
        }
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
    missing_column_indexes: &[usize],
    missing_column_indexes_capacity: usize,
    _total_columns: usize,
) -> usize {
    let mut total = size_of::<PartialTableRow>().saturating_add(table_row.size_hint());
    total =
        total.saturating_add(missing_column_indexes_capacity.saturating_mul(size_of::<usize>()));
    let _ = missing_column_indexes;

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

/// Returns an estimate of additional heap bytes owned by a JSON value.
fn estimate_json_allocated_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => {
            let mut total = values.capacity().saturating_mul(size_of::<serde_json::Value>());
            for value in values {
                total = total.saturating_add(estimate_json_allocated_bytes(value));
            }
            total
        }
        serde_json::Value::Object(values) => values.iter().fold(0usize, |acc, (key, value)| {
            let with_key = acc.saturating_add(key.capacity());
            with_key.saturating_add(estimate_json_allocated_bytes(value))
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
