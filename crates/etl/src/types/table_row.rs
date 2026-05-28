use std::mem::size_of;

use tracing::warn;

use crate::types::{
    PgDate, PgNumeric, PgTimestamp, PgTimestampTz, SizeHint,
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
    /// Approximate row size in bytes.
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
    /// Approximate row size in bytes.
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

/// Returns an estimate of allocated bytes for a partial row payload.
fn estimate_partial_table_row_allocated_bytes(
    table_row: &TableRow,
    missing_column_indexes: &[usize],
    missing_column_indexes_capacity: usize,
    _total_columns: usize,
) -> usize {
    let mut total = size_of::<PartialTableRow>() + table_row.size_hint();
    total = checked_add_or_saturating(
        total,
        checked_mul_or_saturating(
            missing_column_indexes_capacity,
            size_of::<usize>(),
            "partial_table_row.missing_indexes_capacity_mul_usize_size",
        ),
        "partial_table_row.base_add_missing_indexes_capacity",
    );
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
        | Cell::Time(_)
        | Cell::Uuid(_) => 0,
        Cell::Date(value) => estimated_pg_date_allocated_bytes(value),
        Cell::Timestamp(value) => estimated_pg_timestamp_allocated_bytes(value),
        Cell::TimestampTz(value) => estimated_pg_timestamptz_allocated_bytes(value),
        Cell::Numeric(value) => estimated_pg_numeric_allocated_bytes(value),
        Cell::String(value) => value.capacity(),
        Cell::Bytes(value) => value.capacity(),
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

/// Returns an estimate of additional heap bytes owned by a [`PgDate`].
fn estimated_pg_date_allocated_bytes(value: &PgDate) -> usize {
    match value {
        PgDate::OutOfRange(value) => value.text_capacity(),
        PgDate::Finite(_) | PgDate::PosInfinity | PgDate::NegInfinity => 0,
    }
}

/// Returns an estimate of additional heap bytes owned by a [`PgTimestamp`].
fn estimated_pg_timestamp_allocated_bytes(value: &PgTimestamp) -> usize {
    match value {
        PgTimestamp::OutOfRange(value) => value.text_capacity(),
        PgTimestamp::Finite(_) | PgTimestamp::PosInfinity | PgTimestamp::NegInfinity => 0,
    }
}

/// Returns an estimate of additional heap bytes owned by a [`PgTimestampTz`].
fn estimated_pg_timestamptz_allocated_bytes(value: &PgTimestampTz) -> usize {
    match value {
        PgTimestampTz::OutOfRange(value) => value.text_capacity(),
        PgTimestampTz::Finite(_) | PgTimestampTz::PosInfinity | PgTimestampTz::NegInfinity => 0,
    }
}

/// Returns an estimate of additional heap bytes owned by temporal arrays.
fn estimate_temporal_array_allocated_bytes<T>(
    values: &Vec<Option<T>>,
    element_size: usize,
    capacity_metric: &'static str,
    element_heap_metric: &'static str,
    estimate_element_allocated_bytes: impl Fn(&T) -> usize,
) -> usize {
    let mut total = checked_mul_or_saturating(values.capacity(), element_size, capacity_metric);
    for value in values.iter().flatten() {
        total = checked_add_or_saturating(
            total,
            estimate_element_allocated_bytes(value),
            element_heap_metric,
        );
    }

    total
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
        ArrayCell::Date(values) => estimate_temporal_array_allocated_bytes(
            values,
            size_of::<Option<crate::types::PgDate>>(),
            "array.date_capacity_mul_option_size",
            "array.date_add_element_heap_bytes",
            estimated_pg_date_allocated_bytes,
        ),
        ArrayCell::Time(values) => checked_mul_or_saturating(
            values.capacity(),
            size_of::<Option<crate::types::PgTime>>(),
            "array.time_capacity_mul_option_size",
        ),
        ArrayCell::Timestamp(values) => estimate_temporal_array_allocated_bytes(
            values,
            size_of::<Option<crate::types::PgTimestamp>>(),
            "array.timestamp_capacity_mul_option_size",
            "array.timestamp_add_element_heap_bytes",
            estimated_pg_timestamp_allocated_bytes,
        ),
        ArrayCell::TimestampTz(values) => estimate_temporal_array_allocated_bytes(
            values,
            size_of::<Option<crate::types::PgTimestampTz>>(),
            "array.timestamptz_capacity_mul_option_size",
            "array.timestamptz_add_element_heap_bytes",
            estimated_pg_timestamptz_allocated_bytes,
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
            warn!(context, left, right, "size hint addition overflowed, saturating to usize::MAX");

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PgTemporalBound, PgTemporalOutOfRange};

    const OUT_OF_RANGE_TEXT: &str = "100000000-01-01";
    const OUT_OF_RANGE_TEXT_CAPACITY: usize = 64;

    fn out_of_range_temporal() -> PgTemporalOutOfRange {
        let mut text = String::with_capacity(OUT_OF_RANGE_TEXT_CAPACITY);
        text.push_str(OUT_OF_RANGE_TEXT);

        PgTemporalOutOfRange::new(text, PgTemporalBound::Upper)
    }

    #[test]
    fn temporal_out_of_range_estimates_text_capacity() {
        assert_eq!(
            estimated_pg_date_allocated_bytes(&PgDate::OutOfRange(out_of_range_temporal())),
            OUT_OF_RANGE_TEXT_CAPACITY
        );
        assert_eq!(
            estimated_pg_timestamp_allocated_bytes(&PgTimestamp::OutOfRange(
                out_of_range_temporal()
            )),
            OUT_OF_RANGE_TEXT_CAPACITY
        );
        assert_eq!(
            estimated_pg_timestamptz_allocated_bytes(&PgTimestampTz::OutOfRange(
                out_of_range_temporal()
            )),
            OUT_OF_RANGE_TEXT_CAPACITY
        );
    }

    #[test]
    fn temporal_array_estimation_includes_out_of_range_text_capacity() {
        let mut dates = Vec::with_capacity(4);
        dates.push(Some(PgDate::OutOfRange(out_of_range_temporal())));
        dates.push(Some(PgDate::PosInfinity));
        dates.push(None);
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::Date(dates)),
            4 * size_of::<Option<PgDate>>() + OUT_OF_RANGE_TEXT_CAPACITY
        );

        let mut timestamps = Vec::with_capacity(3);
        timestamps.push(Some(PgTimestamp::OutOfRange(out_of_range_temporal())));
        timestamps.push(None);
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::Timestamp(timestamps)),
            3 * size_of::<Option<PgTimestamp>>() + OUT_OF_RANGE_TEXT_CAPACITY
        );

        let mut timestamptzs = Vec::with_capacity(2);
        timestamptzs.push(Some(PgTimestampTz::OutOfRange(out_of_range_temporal())));
        assert_eq!(
            estimate_array_allocated_bytes(&ArrayCell::TimestampTz(timestamptzs)),
            2 * size_of::<Option<PgTimestampTz>>() + OUT_OF_RANGE_TEXT_CAPACITY
        );
    }

    #[test]
    fn table_row_size_hint_includes_temporal_array_heap_bytes() {
        let mut dates = Vec::with_capacity(4);
        dates.push(Some(PgDate::OutOfRange(out_of_range_temporal())));
        dates.push(None);

        let mut cells = Vec::with_capacity(2);
        cells.push(Cell::Array(ArrayCell::Date(dates)));

        let row = TableRow::new(cells);

        assert_eq!(
            row.size_hint(),
            size_of::<TableRow>()
                + 2 * size_of::<Cell>()
                + 4 * size_of::<Option<PgDate>>()
                + OUT_OF_RANGE_TEXT_CAPACITY
        );
    }
}
