use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

use crate::data::{PgNumeric, PgTimeTz};

/// Represents a single database cell value with support for Postgres types.
///
/// [`Cell`] is the primary data container for individual values during ETL
/// processing. It represents the ergonomic Rust value shape passed to
/// destinations, not the source database schema type. The original Postgres
/// type remains available on the corresponding
/// [`crate::schema::ColumnSchema`], so values without specialized Rust
/// semantics can be preserved as [`Cell::String`] while destinations still know
/// the source column type.
///
/// The enum is designed to make destination conversion efficient while
/// maintaining data fidelity.
#[derive(Debug, PartialEq, Clone)]
pub enum Cell {
    /// Represents a NULL database value
    Null,
    /// Boolean value (true/false)
    Bool(bool),
    /// Text or character data
    String(String),
    /// 16-bit signed integer
    I16(i16),
    /// 32-bit signed integer
    I32(i32),
    /// 32-bit unsigned integer
    U32(u32),
    /// 64-bit signed integer
    I64(i64),
    /// 32-bit floating point number
    F32(f32),
    /// 64-bit floating point number
    F64(f64),
    /// Postgres NUMERIC/DECIMAL type with arbitrary precision
    Numeric(PgNumeric),
    /// Date without time information.
    Date(NaiveDate),
    /// Time of day without time zone information.
    Time(NaiveTime),
    /// Time of day with a fixed UTC offset.
    TimeTz(PgTimeTz),
    /// Timestamp without time zone information.
    Timestamp(NaiveDateTime),
    /// Timestamp with time zone information normalized to UTC.
    TimestampTz(DateTime<Utc>),
    /// UUID (Universally Unique Identifier)
    Uuid(Uuid),
    /// JSON data as parsed value
    Json(serde_json::Value),
    /// Raw byte data
    Bytes(Vec<u8>),
    /// Array of values with nullable elements
    Array(ArrayCell),
}

impl Cell {
    /// Clears the cell value to its default state.
    pub fn clear(&mut self) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => *b = false,
            Cell::String(s) => s.clear(),
            Cell::I16(i) => *i = 0,
            Cell::I32(i) => *i = 0,
            Cell::I64(i) => *i = 0,
            Cell::F32(i) => *i = 0.,
            Cell::F64(i) => *i = 0.,
            Cell::Numeric(n) => *n = PgNumeric::default(),
            Cell::Date(t) => *t = NaiveDate::default(),
            Cell::Time(t) => *t = NaiveTime::default(),
            Cell::TimeTz(t) => *t = PgTimeTz::default(),
            Cell::Timestamp(t) => *t = NaiveDateTime::default(),
            Cell::TimestampTz(t) => *t = DateTime::<Utc>::default(),
            Cell::Uuid(u) => *u = Uuid::default(),
            Cell::Json(j) => *j = serde_json::Value::default(),
            Cell::U32(u) => *u = 0,
            Cell::Bytes(b) => b.clear(),
            Cell::Array(vec) => {
                vec.clear();
            }
        }
    }
}

/// Represents array data from Postgres with nullable elements.
///
/// [`ArrayCell`] handles Postgres array types where individual elements can be
/// NULL. Each variant corresponds to a Postgres array type and maintains the
/// nullable nature of array elements as they exist in the source database.
///
/// Destinations that do not support NULL values within arrays are responsible
/// for rejecting them at write time.
#[derive(Debug, PartialEq, Clone)]
pub enum ArrayCell {
    /// Array of nullable boolean values
    Bool(Vec<Option<bool>>),
    /// Array of nullable string values
    String(Vec<Option<String>>),
    /// Array of nullable 16-bit integers
    I16(Vec<Option<i16>>),
    /// Array of nullable 32-bit integers
    I32(Vec<Option<i32>>),
    /// Array of nullable 32-bit unsigned integers
    U32(Vec<Option<u32>>),
    /// Array of nullable 64-bit integers
    I64(Vec<Option<i64>>),
    /// Array of nullable 32-bit floats
    F32(Vec<Option<f32>>),
    /// Array of nullable 64-bit floats
    F64(Vec<Option<f64>>),
    /// Array of nullable Postgres numeric values
    Numeric(Vec<Option<PgNumeric>>),
    /// Array of nullable dates.
    Date(Vec<Option<NaiveDate>>),
    /// Array of nullable times of day without time zone information.
    Time(Vec<Option<NaiveTime>>),
    /// Array of nullable times of day with fixed UTC offsets.
    TimeTz(Vec<Option<PgTimeTz>>),
    /// Array of nullable timestamps without time zone information.
    Timestamp(Vec<Option<NaiveDateTime>>),
    /// Array of nullable timestamps with time zone information normalized to
    /// UTC.
    TimestampTz(Vec<Option<DateTime<Utc>>>),
    /// Array of nullable UUIDs
    Uuid(Vec<Option<Uuid>>),
    /// Array of nullable JSON values
    Json(Vec<Option<serde_json::Value>>),
    /// Array of nullable byte arrays
    Bytes(Vec<Option<Vec<u8>>>),
}

impl ArrayCell {
    /// Clears all elements from the array while preserving the variant type.
    fn clear(&mut self) {
        match self {
            ArrayCell::Bool(vec) => vec.clear(),
            ArrayCell::String(vec) => vec.clear(),
            ArrayCell::I16(vec) => vec.clear(),
            ArrayCell::I32(vec) => vec.clear(),
            ArrayCell::U32(vec) => vec.clear(),
            ArrayCell::I64(vec) => vec.clear(),
            ArrayCell::F32(vec) => vec.clear(),
            ArrayCell::F64(vec) => vec.clear(),
            ArrayCell::Numeric(vec) => vec.clear(),
            ArrayCell::Date(vec) => vec.clear(),
            ArrayCell::Time(vec) => vec.clear(),
            ArrayCell::TimeTz(vec) => vec.clear(),
            ArrayCell::Timestamp(vec) => vec.clear(),
            ArrayCell::TimestampTz(vec) => vec.clear(),
            ArrayCell::Uuid(vec) => vec.clear(),
            ArrayCell::Json(vec) => vec.clear(),
            ArrayCell::Bytes(vec) => vec.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_types_equality() {
        // Test that equal cells are actually equal
        assert_eq!(Cell::I32(42), Cell::I32(42));
        assert_ne!(Cell::I32(42), Cell::I32(43));

        assert_eq!(Cell::String("test".to_owned()), Cell::String("test".to_owned()));
        assert_ne!(Cell::String("test".to_owned()), Cell::String("different".to_owned()));

        assert_eq!(Cell::Null, Cell::Null);
        assert_ne!(Cell::Null, Cell::I32(0));
    }

    #[test]
    fn cell_types_clone() {
        let cell = Cell::String("test".to_owned());
        let cloned = cell.clone();
        assert_eq!(cell, cloned);

        let array_cell = Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]));
        let cloned_array = array_cell.clone();
        assert_eq!(array_cell, cloned_array);
    }
}
