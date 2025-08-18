use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use numeric::PgNumeric;
use std::fmt::Debug;
use uuid::Uuid;

use crate::bail;
use crate::error::ErrorKind;
use crate::error::EtlError;

pub mod bool;
pub mod event;
pub mod hex;
pub mod numeric;
pub mod table_row;
pub mod text;

/// Represents a single database cell value with support for Postgres types.
///
/// [`Cell`] is the primary data container for individual values during ETL processing.
/// It supports all common Postgres data types including arrays, JSON, and temporal types.
/// Each variant handles nullable data appropriately for the destination system.
///
/// The enum is designed to preserve type information and enable efficient conversion
/// to destination formats while maintaining data fidelity.
#[derive(Debug, Clone, PartialEq)]
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
    /// Date without time information
    Date(NaiveDate),
    /// Time without date information
    Time(NaiveTime),
    /// Timestamp without timezone information
    TimeStamp(NaiveDateTime),
    /// Timestamp with timezone information in UTC
    TimeStampTz(DateTime<Utc>),
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
            Cell::TimeStamp(t) => *t = NaiveDateTime::default(),
            Cell::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
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
/// [`ArrayCell`] handles Postgres array types where individual elements can be NULL.
/// Each variant corresponds to a Postgres array type and maintains the nullable
/// nature of array elements as they exist in the source database.
///
/// This type is used internally during data conversion and can be converted to
/// [`ArrayCellNonOptional`] for destinations that don't support nullable array elements.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCell {
    /// NULL array value
    Null,
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
    /// Array of nullable dates
    Date(Vec<Option<NaiveDate>>),
    /// Array of nullable times
    Time(Vec<Option<NaiveTime>>),
    /// Array of nullable timestamps
    TimeStamp(Vec<Option<NaiveDateTime>>),
    /// Array of nullable timestamps with timezone
    TimeStampTz(Vec<Option<DateTime<Utc>>>),
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
            ArrayCell::Null => {}
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
            ArrayCell::TimeStamp(vec) => vec.clear(),
            ArrayCell::TimeStampTz(vec) => vec.clear(),
            ArrayCell::Uuid(vec) => vec.clear(),
            ArrayCell::Json(vec) => vec.clear(),
            ArrayCell::Bytes(vec) => vec.clear(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CellNonOptional {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Array(ArrayCellNonOptional),
}

impl TryFrom<Cell> for CellNonOptional {
    type Error = EtlError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(CellNonOptional::Null),
            Cell::Bool(val) => Ok(CellNonOptional::Bool(val)),
            Cell::String(val) => Ok(CellNonOptional::String(val)),
            Cell::I16(val) => Ok(CellNonOptional::I16(val)),
            Cell::I32(val) => Ok(CellNonOptional::I32(val)),
            Cell::U32(val) => Ok(CellNonOptional::U32(val)),
            Cell::I64(val) => Ok(CellNonOptional::I64(val)),
            Cell::F32(val) => Ok(CellNonOptional::F32(val)),
            Cell::F64(val) => Ok(CellNonOptional::F64(val)),
            Cell::Numeric(val) => Ok(CellNonOptional::Numeric(val)),
            Cell::Date(val) => Ok(CellNonOptional::Date(val)),
            Cell::Time(val) => Ok(CellNonOptional::Time(val)),
            Cell::TimeStamp(val) => Ok(CellNonOptional::TimeStamp(val)),
            Cell::TimeStampTz(val) => Ok(CellNonOptional::TimeStampTz(val)),
            Cell::Uuid(val) => Ok(CellNonOptional::Uuid(val)),
            Cell::Json(val) => Ok(CellNonOptional::Json(val)),
            Cell::Bytes(val) => Ok(CellNonOptional::Bytes(val)),
            Cell::Array(array_cell) => {
                let array_cell_non_optional = ArrayCellNonOptional::try_from(array_cell)?;
                Ok(CellNonOptional::Array(array_cell_non_optional))
            }
        }
    }
}

impl CellNonOptional {
    pub fn clear(&mut self) {
        match self {
            CellNonOptional::Null => {}
            CellNonOptional::Bool(b) => *b = false,
            CellNonOptional::String(s) => s.clear(),
            CellNonOptional::I16(i) => *i = 0,
            CellNonOptional::I32(i) => *i = 0,
            CellNonOptional::I64(i) => *i = 0,
            CellNonOptional::F32(i) => *i = 0.,
            CellNonOptional::F64(i) => *i = 0.,
            CellNonOptional::Numeric(n) => *n = PgNumeric::default(),
            CellNonOptional::Date(t) => *t = NaiveDate::default(),
            CellNonOptional::Time(t) => *t = NaiveTime::default(),
            CellNonOptional::TimeStamp(t) => *t = NaiveDateTime::default(),
            CellNonOptional::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
            CellNonOptional::Uuid(u) => *u = Uuid::default(),
            CellNonOptional::Json(j) => *j = serde_json::Value::default(),
            CellNonOptional::U32(u) => *u = 0,
            CellNonOptional::Bytes(b) => b.clear(),
            CellNonOptional::Array(vec) => {
                vec.clear();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCellNonOptional {
    Null,
    Bool(Vec<bool>),
    String(Vec<String>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Numeric(Vec<PgNumeric>),
    Date(Vec<NaiveDate>),
    Time(Vec<NaiveTime>),
    TimeStamp(Vec<NaiveDateTime>),
    TimeStampTz(Vec<DateTime<Utc>>),
    Uuid(Vec<Uuid>),
    Json(Vec<serde_json::Value>),
    Bytes(Vec<Vec<u8>>),
}

macro_rules! convert_array_variant {
    ($variant:ident, $vec:expr) => {
        if $vec.iter().any(|v| v.is_none()) {
            bail!(
                ErrorKind::NullValuesNotSupportedInArray,
                "NULL values in arrays are not supported",
                format!(
                    "Remove the NULL values from the array {:?} and try again",
                    $vec
                )
            )
        } else {
            Ok(ArrayCellNonOptional::$variant(
                $vec.into_iter().flatten().collect(),
            ))
        }
    };
}

impl TryFrom<ArrayCell> for ArrayCellNonOptional {
    type Error = EtlError;

    fn try_from(array_cell: ArrayCell) -> Result<Self, Self::Error> {
        match array_cell {
            ArrayCell::Null => Ok(ArrayCellNonOptional::Null),
            ArrayCell::Bool(vec) => convert_array_variant!(Bool, vec),
            ArrayCell::String(vec) => convert_array_variant!(String, vec),
            ArrayCell::I16(vec) => convert_array_variant!(I16, vec),
            ArrayCell::I32(vec) => convert_array_variant!(I32, vec),
            ArrayCell::U32(vec) => convert_array_variant!(U32, vec),
            ArrayCell::I64(vec) => convert_array_variant!(I64, vec),
            ArrayCell::F32(vec) => convert_array_variant!(F32, vec),
            ArrayCell::F64(vec) => convert_array_variant!(F64, vec),
            ArrayCell::Numeric(vec) => convert_array_variant!(Numeric, vec),
            ArrayCell::Date(vec) => convert_array_variant!(Date, vec),
            ArrayCell::Time(vec) => convert_array_variant!(Time, vec),
            ArrayCell::TimeStamp(vec) => convert_array_variant!(TimeStamp, vec),
            ArrayCell::TimeStampTz(vec) => convert_array_variant!(TimeStampTz, vec),
            ArrayCell::Uuid(vec) => convert_array_variant!(Uuid, vec),
            ArrayCell::Json(vec) => convert_array_variant!(Json, vec),
            ArrayCell::Bytes(vec) => convert_array_variant!(Bytes, vec),
        }
    }
}

impl ArrayCellNonOptional {
    fn clear(&mut self) {
        match self {
            ArrayCellNonOptional::Null => {}
            ArrayCellNonOptional::Bool(vec) => vec.clear(),
            ArrayCellNonOptional::String(vec) => vec.clear(),
            ArrayCellNonOptional::I16(vec) => vec.clear(),
            ArrayCellNonOptional::I32(vec) => vec.clear(),
            ArrayCellNonOptional::U32(vec) => vec.clear(),
            ArrayCellNonOptional::I64(vec) => vec.clear(),
            ArrayCellNonOptional::F32(vec) => vec.clear(),
            ArrayCellNonOptional::F64(vec) => vec.clear(),
            ArrayCellNonOptional::Numeric(vec) => vec.clear(),
            ArrayCellNonOptional::Date(vec) => vec.clear(),
            ArrayCellNonOptional::Time(vec) => vec.clear(),
            ArrayCellNonOptional::TimeStamp(vec) => vec.clear(),
            ArrayCellNonOptional::TimeStampTz(vec) => vec.clear(),
            ArrayCellNonOptional::Uuid(vec) => vec.clear(),
            ArrayCellNonOptional::Json(vec) => vec.clear(),
            ArrayCellNonOptional::Bytes(vec) => vec.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_try_from_to_cell_non_optional() {
        // Test successful conversions
        let cell = Cell::I32(42);
        let non_opt = CellNonOptional::try_from(cell).unwrap();
        assert_eq!(non_opt, CellNonOptional::I32(42));

        let cell = Cell::String("test".to_string());
        let non_opt = CellNonOptional::try_from(cell).unwrap();
        assert_eq!(non_opt, CellNonOptional::String("test".to_string()));

        let cell = Cell::Null;
        let non_opt = CellNonOptional::try_from(cell).unwrap();
        assert_eq!(non_opt, CellNonOptional::Null);
    }

    #[test]
    fn cell_try_from_array_conversion() {
        let array_cell = ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let cell = Cell::Array(array_cell);

        let non_opt = CellNonOptional::try_from(cell).unwrap();
        if let CellNonOptional::Array(ArrayCellNonOptional::I32(vec)) = non_opt {
            assert_eq!(vec, vec![1, 2, 3]);
        } else {
            panic!("Expected non-optional i32 array");
        }
    }

    #[test]
    fn array_cell_try_from_with_nulls() {
        let array_cell = ArrayCell::String(vec![
            Some("test".to_string()),
            None,
            Some("hello".to_string()),
        ]);

        let result = ArrayCellNonOptional::try_from(array_cell);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("NULL values in arrays are not supported")
        );
    }

    #[test]
    fn array_cell_try_from_without_nulls() {
        let array_cell = ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);

        let result = ArrayCellNonOptional::try_from(array_cell).unwrap();
        if let ArrayCellNonOptional::I32(vec) = result {
            assert_eq!(vec, vec![1, 2, 3]);
        } else {
            panic!("Expected i32 array");
        }
    }

    #[test]
    fn array_cell_try_from_null_variant() {
        let array_cell = ArrayCell::Null;

        let result = ArrayCellNonOptional::try_from(array_cell).unwrap();
        assert_eq!(result, ArrayCellNonOptional::Null);
    }

    #[test]
    fn cell_types_equality() {
        // Test that equal cells are actually equal
        assert_eq!(Cell::I32(42), Cell::I32(42));
        assert_ne!(Cell::I32(42), Cell::I32(43));

        assert_eq!(
            Cell::String("test".to_string()),
            Cell::String("test".to_string())
        );
        assert_ne!(
            Cell::String("test".to_string()),
            Cell::String("different".to_string())
        );

        assert_eq!(Cell::Null, Cell::Null);
        assert_ne!(Cell::Null, Cell::I32(0));
    }

    #[test]
    fn cell_types_clone() {
        let cell = Cell::String("test".to_string());
        let cloned = cell.clone();
        assert_eq!(cell, cloned);

        let array_cell = Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]));
        let cloned_array = array_cell.clone();
        assert_eq!(array_cell, cloned_array);
    }

    #[test]
    fn complex_array_conversions() {
        // Test complex array type conversions
        let mixed_types = vec![
            ArrayCell::Bool(vec![Some(true), Some(false)]),
            ArrayCell::String(vec![Some("hello".to_string()), Some("world".to_string())]),
            ArrayCell::I32(vec![Some(1), Some(2), Some(3)]),
            ArrayCell::Null,
        ];

        for array_cell in mixed_types {
            let cell = Cell::Array(array_cell);

            // Test that we can convert to CellNonOptional if no nulls are present
            let non_opt_result = CellNonOptional::try_from(cell);

            // Some will succeed (Bool, String, I32 without nulls, Null)
            // Others may fail if they contain nulls
            if let Ok(non_opt) = non_opt_result {
                // Test that clear works on the result
                let mut non_opt_clone = non_opt.clone();
                non_opt_clone.clear();
            }
        }
    }
}
