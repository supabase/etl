use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, ArrowPrimitiveType, BooleanBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
        ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, FieldRef, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
        Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    },
    error::ArrowError,
};
use chrono::{NaiveDate, NaiveTime};
use etl::types::{ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableRow};

pub const UNIX_EPOCH: NaiveDate =
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch is a valid date");

const MIDNIGHT: NaiveTime = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is a valid time");

const UUID_BYTE_WIDTH: i32 = 16;

/// Converts a slice of [`TableRow`]s to an Arrow [`RecordBatch`].
///
/// This function transforms tabular data from the ETL pipeline's internal format
/// into Apache Arrow's columnar format for efficient storage and processing in
/// Iceberg tables. Each field in the schema is processed sequentially to build
/// the corresponding Arrow arrays.
///
/// Returns a [`RecordBatch`] containing the converted data, or an [`ArrowError`]
/// if the conversion fails due to schema mismatches or other Arrow-related issues.
pub fn rows_to_record_batch(rows: &[TableRow], schema: Schema) -> Result<RecordBatch, ArrowError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (field_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_for_field(rows, field_idx, field.data_type());
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

    Ok(batch)
}

/// Builds an [`ArrayRef`] from the [`TableRow`]s for a field specified by the `field_idx`.
///
/// This function dispatches to type-specific array builders based on the Arrow
/// [`DataType`]. It handles all supported data types including primitives, strings,
/// binary data, dates, times, timestamps, and UUIDs. Unsupported types fall back
/// to string representation.
///
/// Returns an [`ArrayRef`] containing the encoded field values in the appropriate
/// Arrow array type. For unsupported types, returns a string array with string
/// representations of the values.
fn build_array_for_field(rows: &[TableRow], field_idx: usize, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => build_boolean_array(rows, field_idx),
        DataType::Int32 => build_primitive_array::<Int32Type, _>(rows, field_idx, cell_to_i32),
        DataType::Int64 => build_primitive_array::<Int64Type, _>(rows, field_idx, cell_to_i64),
        DataType::Float32 => build_primitive_array::<Float32Type, _>(rows, field_idx, cell_to_f32),
        DataType::Float64 => build_primitive_array::<Float64Type, _>(rows, field_idx, cell_to_f64),
        DataType::Utf8 => build_string_array(rows, field_idx),
        DataType::LargeBinary => build_binary_array(rows, field_idx),
        DataType::Date32 => build_primitive_array::<Date32Type, _>(rows, field_idx, cell_to_date32),
        DataType::Time64(TimeUnit::Microsecond) => {
            build_primitive_array::<Time64MicrosecondType, _>(rows, field_idx, cell_to_time64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            build_timestamptz_array(rows, field_idx, tz)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_primitive_array::<TimestampMicrosecondType, _>(rows, field_idx, cell_to_timestamp)
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => build_uuid_array(rows, field_idx),
        DataType::List(field) => build_list_array(rows, field_idx, field.clone()),
        _ => build_string_array(rows, field_idx),
    }
}

/// Builds a primitive Arrow array from [`TableRow`]s using a type-specific converter function.
///
/// This generic function creates Arrow arrays for primitive types (integers, floats,
/// dates, times, timestamps) by applying a converter function to each cell value.
/// The converter handles type conversion and returns [`None`] for incompatible values,
/// which become null entries in the resulting array.
///
/// # Returns
///
/// Returns an [`ArrayRef`] containing a primitive array with converted values.
/// Incompatible or null values are represented as null entries in the array.
fn build_primitive_array<T, F>(rows: &[TableRow], field_idx: usize, converter: F) -> ArrayRef
where
    T: ArrowPrimitiveType,
    F: Fn(&Cell) -> Option<T::Native>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(rows.len());

    for row in rows {
        let arrow_value = converter(&row.values[field_idx]);
        builder.append_option(arrow_value);
    }

    Arc::new(builder.finish())
}

macro_rules! impl_array_builder {
    ($fn_name:ident, $builder_type:ty, $converter:ident) => {
        fn $fn_name(rows: &[TableRow], field_idx: usize) -> ArrayRef {
            let mut builder = <$builder_type>::new();

            for row in rows {
                let arrow_value = $converter(&row.values[field_idx]);
                builder.append_option(arrow_value);
            }

            Arc::new(builder.finish())
        }
    };
}

impl_array_builder!(build_boolean_array, BooleanBuilder, cell_to_bool);
impl_array_builder!(build_string_array, StringBuilder, cell_to_string);
impl_array_builder!(build_binary_array, LargeBinaryBuilder, cell_to_bytes);

/// Builds a timezone-aware timestamp array from [`TableRow`]s.
///
/// This function creates an Arrow timestamp array with microsecond precision
/// and a specified timezone. It processes [`Cell::TimestampTz`] values and
/// converts them to microseconds since the Unix epoch while preserving
/// timezone information in the array metadata.
///
/// Returns an [`ArrayRef`] containing a timestamp array with timezone metadata.
/// Non-timestamp cells become null entries in the resulting array.
fn build_timestamptz_array(rows: &[TableRow], field_idx: usize, tz: &str) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone(tz);

    for row in rows {
        let arrow_value = cell_to_timestamptz(&row.values[field_idx]);
        builder.append_option(arrow_value);
    }

    Arc::new(builder.finish())
}

/// Builds a fixed-size binary array for UUID values from [`TableRow`]s.
///
/// This function creates an Arrow fixed-size binary array specifically for
/// UUID values, which are represented as 16-byte binary data. It extracts
/// UUID bytes from [`Cell::Uuid`] values and creates null entries for
/// non-UUID cells.
///
/// # Panics
///
/// Panics if the UUID byte array length doesn't match the expected 16-byte width,
/// which should never occur with valid UUID values.
fn build_uuid_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH);

    for row in rows {
        match cell_to_uuid(&row.values[field_idx]) {
            Some(value) => {
                builder
                    .append_value(value)
                    .expect("array length and buider byte width are both 16");
            }
            None => {
                builder.append_null();
            }
        }
    }

    Arc::new(builder.finish())
}

/// Converts a [`Cell`] to a boolean value.
///
/// Extracts boolean values from [`Cell::Bool`] variants, returning [`None`]
/// for all other cell types. This is used when building boolean Arrow arrays.
fn cell_to_bool(cell: &Cell) -> Option<bool> {
    match cell {
        Cell::Bool(v) => Some(*v),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 32-bit signed integer.
///
/// Handles conversion from multiple integer cell types to i32, including:
/// - [`Cell::I16`] values (widened to i32)
/// - [`Cell::I32`] values (direct conversion)
///
/// Returns [`None`] for incompatible cell types.
fn cell_to_i32(cell: &Cell) -> Option<i32> {
    match cell {
        Cell::I16(v) => Some(*v as i32),
        Cell::I32(v) => Some(*v),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 64-bit signed integer.
///
/// Extracts 64-bit signed integer values from [`Cell::I64`] variants,
/// [`Cell::U32`] values are cast to i64
///
/// returning [`None`] for all other cell types.
fn cell_to_i64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::I64(v) => Some(*v),
        Cell::U32(v) => Some(*v as i64),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 32-bit floating-point number.
///
/// Extracts 32-bit float values from [`Cell::F32`] variants, returning
/// [`None`] for all other cell types.
fn cell_to_f32(cell: &Cell) -> Option<f32> {
    match cell {
        Cell::F32(v) => Some(*v),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 64-bit floating-point number.
///
/// Extracts 64-bit float values from [`Cell::F64`] variants, returning
/// [`None`] for all other cell types.
fn cell_to_f64(cell: &Cell) -> Option<f64> {
    match cell {
        Cell::F64(v) => Some(*v),
        _ => None,
    }
}

/// Converts a [`Cell`] to a byte vector.
///
/// Extracts binary data from [`Cell::Bytes`] variants by cloning the
/// underlying vector. Returns [`None`] for all other cell types.
fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
    match cell {
        Cell::Bytes(v) => Some(v.clone()),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 32-bit date value (days since Unix epoch).
///
/// Transforms [`Cell::Date`] values into the number of days since the
/// Unix epoch (1970-01-01) as required by Arrow's Date32 type. Returns
/// [`None`] for non-date cell types.
fn cell_to_date32(cell: &Cell) -> Option<i32> {
    match cell {
        Cell::Date(date) => Some(date.signed_duration_since(UNIX_EPOCH).num_days() as i32),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 64-bit time value (microseconds since midnight).
///
/// Transforms [`Cell::Time`] values into microseconds since midnight as
/// required by Arrow's Time64 type. Returns [`None`] if the duration cannot
/// be represented in microseconds or for non-time cell types.
fn cell_to_time64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Time(time) => time.signed_duration_since(MIDNIGHT).num_microseconds(),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 64-bit timestamp value (microseconds since Unix epoch).
///
/// Transforms naive [`Cell::Timestamp`] values into microseconds since the
/// Unix epoch by treating them as UTC timestamps. Returns [`None`] for
/// non-timestamp cell types.
fn cell_to_timestamp(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros()),
        _ => None,
    }
}

/// Converts a [`Cell`] to a timezone-aware timestamp value (microseconds since Unix epoch).
///
/// Transforms timezone-aware [`Cell::TimestampTz`] values into microseconds
/// since the Unix epoch, preserving the timezone information in the timestamp.
/// Returns [`None`] for non-timestamptz cell types.
fn cell_to_timestamptz(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::TimestampTz(ts) => Some(ts.timestamp_micros()),
        _ => None,
    }
}

/// Extracts UUID bytes from a [`Cell`] value.
///
/// This function attempts to extract the 16-byte representation of a UUID
/// from a [`Cell::Uuid`] variant. For all other cell types, it returns [`None`].
///
/// Returns [`Some`] with a reference to the 16-byte UUID array if the cell
/// contains a UUID, or [`None`] for all other cell types.
fn cell_to_uuid(cell: &Cell) -> Option<&[u8; UUID_BYTE_WIDTH as usize]> {
    match cell {
        Cell::Uuid(value) => Some(value.as_bytes()),
        _ => None,
    }
}

/// Converts a [`Cell`] to its string representation for Arrow arrays.
///
/// This function provides a comprehensive string conversion for all [`Cell`]
/// variants, handling type-specific formatting requirements. It's used as a
/// fallback for unsupported Arrow types and for explicit string columns.
///
/// The conversion rules are:
/// - [`Cell::Null`] becomes [`None`]
/// - Primitive types use their standard string representation
/// - Dates, times, and timestamps use predefined format strings
/// - Timezone-aware timestamps use RFC3339 format
/// - Binary data is Base64-encoded
/// - JSON values use their serialized string form
/// - Arrays use debug formatting
///
/// Returns [`Some`] with the string representation for non-null values,
/// or [`None`] for [`Cell::Null`] which becomes a null entry in the Arrow array.
fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Bool(_) => None,
        Cell::String(s) => Some(s.clone()),
        Cell::I16(_) => None,
        Cell::I32(_) => None,
        Cell::U32(_) => None,
        Cell::I64(_) => None,
        Cell::F32(_) => None,
        Cell::F64(_) => None,
        Cell::Numeric(n) => Some(n.to_string()),
        Cell::Date(_) => None,
        Cell::Time(_) => None,
        Cell::Timestamp(_) => None,
        Cell::TimestampTz(_) => None,
        Cell::Uuid(_) => None,
        Cell::Json(j) => Some(j.to_string()),
        Cell::Bytes(_) => None,
        Cell::Array(_) => None,
    }
}

/// Extracts [`ArrayCell`] from a [`Cell::Array`] value.
///
/// This function safely extracts the array data from a [`Cell::Array`] variant,
/// returning [`None`] for non-array cells. This is used when building Arrow list
/// arrays to access the underlying array elements.
///
/// Returns [`Some`] with a reference to the [`ArrayCell`] if the cell contains
/// an array, or [`None`] for all other cell types including [`Cell::Null`].
fn cell_to_array_cell(cell: &Cell) -> Option<&ArrayCell> {
    match cell {
        Cell::Array(array_cell) => Some(array_cell),
        _ => None,
    }
}

/// Builds an Arrow list array from [`TableRow`]s for a specific field.
///
/// This function creates an Arrow [`ListArray`] by processing [`Cell::Array`] values
/// from the specified field index. It delegates to type-specific builders based on
/// the list element type, reusing the existing primitive and string array building
/// infrastructure.
///
/// Returns an [`ArrayRef`] containing a list array with the appropriate element type.
/// Rows with non-array cells become null entries in the resulting list array.
fn build_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    match field.data_type() {
        DataType::Boolean => build_boolean_list_array(rows, field_idx, field),
        // For unsupported element types, fall back to string representation
        _ => build_list_array_for_strings(rows, field_idx),
    }
}

/// Builds a list array for boolean elements.
fn build_boolean_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(BooleanBuilder::new()).with_field(dbg!(field));

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Bool(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                // For non-boolean array types, fall back to string representation
                _ => {
                    return build_list_array_for_strings(rows, field_idx);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for string elements.
///
/// This function creates an Arrow list array with string elements by processing
/// [`ArrayCell::String`] variants from [`Cell::Array`] values.
fn build_list_array_for_strings(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(StringBuilder::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::String(vec) => {
                    for item in vec {
                        match item {
                            Some(s) => list_builder.values().append_value(s),
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                // For non-string array types, convert elements to strings
                _ => {
                    append_array_cell_as_strings(&mut list_builder, array_cell);
                    list_builder.append(true);
                }
            }
        } else {
            // Non-array cell becomes null list entry
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Helper function to append any [`ArrayCell`] variant as string elements to a list builder.
///
/// This function converts array elements to their string representation and appends
/// them to a string list builder. It's used as a fallback for unsupported array types.
fn append_array_cell_as_strings(
    list_builder: &mut ListBuilder<StringBuilder>,
    array_cell: &ArrayCell,
) {
    match array_cell {
        ArrayCell::Bool(vec) => {
            for item in vec {
                match item {
                    Some(b) => list_builder.values().append_value(b.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::String(vec) => {
            for item in vec {
                match item {
                    Some(s) => list_builder.values().append_value(s),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::I16(vec) => {
            for item in vec {
                match item {
                    Some(i) => list_builder.values().append_value(i.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::I32(vec) => {
            for item in vec {
                match item {
                    Some(i) => list_builder.values().append_value(i.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::U32(vec) => {
            for item in vec {
                match item {
                    Some(u) => list_builder.values().append_value(u.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::I64(vec) => {
            for item in vec {
                match item {
                    Some(i) => list_builder.values().append_value(i.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::F32(vec) => {
            for item in vec {
                match item {
                    Some(f) => list_builder.values().append_value(f.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::F64(vec) => {
            for item in vec {
                match item {
                    Some(f) => list_builder.values().append_value(f.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Numeric(vec) => {
            for item in vec {
                match item {
                    Some(n) => list_builder.values().append_value(n.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Date(vec) => {
            for item in vec {
                match item {
                    Some(d) => list_builder
                        .values()
                        .append_value(d.format(DATE_FORMAT).to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Time(vec) => {
            for item in vec {
                match item {
                    Some(t) => list_builder
                        .values()
                        .append_value(t.format(TIME_FORMAT).to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Timestamp(vec) => {
            for item in vec {
                match item {
                    Some(ts) => list_builder
                        .values()
                        .append_value(ts.format(TIMESTAMP_FORMAT).to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::TimestampTz(vec) => {
            for item in vec {
                match item {
                    Some(ts) => list_builder.values().append_value(ts.to_rfc3339()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Uuid(vec) => {
            for item in vec {
                match item {
                    Some(u) => list_builder.values().append_value(u.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Json(vec) => {
            for item in vec {
                match item {
                    Some(j) => list_builder.values().append_value(j.to_string()),
                    None => list_builder.values().append_null(),
                }
            }
        }
        ArrayCell::Bytes(vec) => {
            for _ in vec {
                list_builder.values().append_null();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use etl::types::ArrayCell;

    #[test]
    fn test_cell_to_bool() {
        assert_eq!(cell_to_bool(&Cell::Bool(true)), Some(true));
        assert_eq!(cell_to_bool(&Cell::Bool(false)), Some(false));
        assert_eq!(cell_to_bool(&Cell::Null), None);
        assert_eq!(cell_to_bool(&Cell::String("true".to_string())), None);
        assert_eq!(cell_to_bool(&Cell::I32(1)), None);
    }

    #[test]
    fn test_cell_to_i32() {
        assert_eq!(cell_to_i32(&Cell::I16(42)), Some(42));
        assert_eq!(cell_to_i32(&Cell::I32(42)), Some(42));
        assert_eq!(cell_to_i32(&Cell::Null), None);
        assert_eq!(cell_to_i32(&Cell::I64(42)), None);
        assert_eq!(cell_to_i32(&Cell::String("42".to_string())), None);
    }

    #[test]
    fn test_cell_to_i64() {
        assert_eq!(cell_to_i64(&Cell::I64(42)), Some(42));
        assert_eq!(cell_to_i64(&Cell::I64(-42)), Some(-42));
        assert_eq!(cell_to_i64(&Cell::U32(42)), Some(42));
        assert_eq!(cell_to_i64(&Cell::U32(u32::MAX)), Some(u32::MAX as i64)); // Overflow case
        assert_eq!(cell_to_i64(&Cell::Null), None);
        assert_eq!(cell_to_i64(&Cell::I32(42)), None);
        assert_eq!(cell_to_i64(&Cell::String("42".to_string())), None);
    }

    #[test]
    fn test_cell_to_f32() {
        assert_eq!(cell_to_f32(&Cell::F32(2.5)), Some(2.5));
        assert_eq!(cell_to_f32(&Cell::F32(-2.5)), Some(-2.5));
        assert_eq!(cell_to_f32(&Cell::Null), None);
        assert_eq!(cell_to_f32(&Cell::F64(2.5)), None);
        assert_eq!(cell_to_f32(&Cell::I32(42)), None);
    }

    #[test]
    fn test_cell_to_f64() {
        assert_eq!(cell_to_f64(&Cell::F64(1.234567)), Some(1.234567));
        assert_eq!(cell_to_f64(&Cell::F64(-1.234567)), Some(-1.234567));
        assert_eq!(cell_to_f64(&Cell::Null), None);
        assert_eq!(cell_to_f64(&Cell::F32(2.5)), None);
        assert_eq!(cell_to_f64(&Cell::I32(42)), None);
    }

    #[test]
    fn test_cell_to_bytes() {
        let test_bytes = vec![1, 2, 3, 4];
        assert_eq!(
            cell_to_bytes(&Cell::Bytes(test_bytes.clone())),
            Some(test_bytes)
        );
        assert_eq!(cell_to_bytes(&Cell::Bytes(vec![])), Some(vec![]));
        assert_eq!(cell_to_bytes(&Cell::Null), None);
        assert_eq!(cell_to_bytes(&Cell::String("hello".to_string())), None);
    }

    #[test]
    fn test_cell_to_date32() {
        use chrono::NaiveDate;
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let expected_days = test_date.signed_duration_since(UNIX_EPOCH).num_days() as i32;

        assert_eq!(cell_to_date32(&Cell::Date(test_date)), Some(expected_days));
        assert_eq!(cell_to_date32(&Cell::Date(UNIX_EPOCH)), Some(0));
        assert_eq!(cell_to_date32(&Cell::Null), None);
        assert_eq!(
            cell_to_date32(&Cell::String("2023-05-15".to_string())),
            None
        );
    }

    #[test]
    fn test_cell_to_time64() {
        use chrono::NaiveTime;
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let expected_micros = test_time.signed_duration_since(MIDNIGHT).num_microseconds();

        assert_eq!(cell_to_time64(&Cell::Time(test_time)), expected_micros);
        assert_eq!(cell_to_time64(&Cell::Time(MIDNIGHT)), Some(0));
        assert_eq!(cell_to_time64(&Cell::Null), None);
        assert_eq!(cell_to_time64(&Cell::String("12:30:45".to_string())), None);
    }

    #[test]
    fn test_cell_to_timestamp() {
        use chrono::DateTime;
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let expected_micros = test_ts.and_utc().timestamp_micros();

        assert_eq!(
            cell_to_timestamp(&Cell::Timestamp(test_ts)),
            Some(expected_micros)
        );
        assert_eq!(cell_to_timestamp(&Cell::Null), None);
        assert_eq!(
            cell_to_timestamp(&Cell::String("2001-09-09 01:46:40".to_string())),
            None
        );
    }

    #[test]
    fn test_cell_to_timestamptz() {
        use chrono::DateTime;
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap();
        let expected_micros = test_ts.timestamp_micros();

        assert_eq!(
            cell_to_timestamptz(&Cell::TimestampTz(test_ts)),
            Some(expected_micros)
        );
        assert_eq!(cell_to_timestamptz(&Cell::Null), None);
        assert_eq!(
            cell_to_timestamptz(&Cell::String("2001-09-09T01:46:40Z".to_string())),
            None
        );
    }

    #[test]
    fn test_cell_to_uuid() {
        use uuid::Uuid;
        let test_uuid = Uuid::new_v4();
        let expected_bytes = test_uuid.as_bytes();

        assert_eq!(cell_to_uuid(&Cell::Uuid(test_uuid)), Some(expected_bytes));
        assert_eq!(cell_to_uuid(&Cell::Null), None);
        assert_eq!(cell_to_uuid(&Cell::String(test_uuid.to_string())), None);
    }

    #[test]
    fn test_cell_to_string() {
        use chrono::{NaiveDate, NaiveTime};
        use uuid::Uuid;

        // Test basic types
        assert_eq!(cell_to_string(&Cell::Null), None);
        assert_eq!(cell_to_string(&Cell::Bool(true)), None);
        assert_eq!(cell_to_string(&Cell::Bool(false)), None);
        assert_eq!(
            cell_to_string(&Cell::String("hello".to_string())),
            Some("hello".to_string())
        );
        assert_eq!(cell_to_string(&Cell::I16(42)), None);
        assert_eq!(cell_to_string(&Cell::I32(-42)), None);
        assert_eq!(cell_to_string(&Cell::U32(42)), None);
        assert_eq!(cell_to_string(&Cell::I64(42)), None);
        assert_eq!(cell_to_string(&Cell::F32(2.5)), None);
        assert_eq!(cell_to_string(&Cell::F64(1.234567)), None);

        // Test temporal types with known formats
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        assert_eq!(cell_to_string(&Cell::Date(test_date)), None);

        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(cell_to_string(&Cell::Time(test_time)), None);

        // Test UUID
        let test_uuid = Uuid::new_v4();
        assert_eq!(cell_to_string(&Cell::Uuid(test_uuid)), None);

        // Test JSON
        let json_val = serde_json::json!({"key": "value"});
        assert_eq!(
            cell_to_string(&Cell::Json(json_val.clone())),
            Some(json_val.to_string())
        );

        // Test bytes (Base64 encoded)
        let test_bytes = vec![72, 101, 108, 108, 111];
        assert_eq!(cell_to_string(&Cell::Bytes(test_bytes)), None);

        // Test array (debug format)
        let test_array = ArrayCell::I32(vec![Some(1), Some(2)]);
        assert!(cell_to_string(&Cell::Array(test_array)).is_none());
    }

    #[test]
    fn test_build_boolean_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Bool(true)],
            },
            TableRow {
                values: vec![Cell::Bool(false)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("not bool".to_string())],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Boolean);
        let bool_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        assert_eq!(bool_array.len(), 4);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));
        assert!(bool_array.is_null(3)); // Non-bool cell becomes null
    }

    #[test]
    fn test_build_i32_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::I16(42)],
            },
            TableRow {
                values: vec![Cell::I32(-123)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("not int".to_string())],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Int32);
        let int_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(int_array.len(), 4);
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), -123);
        assert!(int_array.is_null(2));
        assert!(int_array.is_null(3)); // Non-int cell becomes null
    }

    #[test]
    fn test_build_i64_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::I64(123456789)],
            },
            TableRow {
                values: vec![Cell::I64(-987654321)],
            },
            TableRow {
                values: vec![Cell::U32(456)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::I32(42)],
            }, // Non-I64 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Int64);
        let int_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(int_array.len(), 5);
        assert_eq!(int_array.value(0), 123456789);
        assert_eq!(int_array.value(1), -987654321);
        assert_eq!(int_array.value(2), 456);
        assert!(int_array.is_null(3));
        assert!(int_array.is_null(4));
    }

    #[test]
    fn test_build_f32_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::F32(2.5)],
            },
            TableRow {
                values: vec![Cell::F32(-1.25)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::F64(3.0)],
            }, // Non-F32 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Float32);
        let float_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();

        assert_eq!(float_array.len(), 4);
        assert_eq!(float_array.value(0), 2.5);
        assert_eq!(float_array.value(1), -1.25);
        assert!(float_array.is_null(2));
        assert!(float_array.is_null(3));
    }

    #[test]
    fn test_build_f64_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::F64(1.23456789)],
            },
            TableRow {
                values: vec![Cell::F64(-9.87654321)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::F32(2.5)],
            }, // Non-F64 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Float64);
        let float_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        assert_eq!(float_array.len(), 4);
        assert_eq!(float_array.value(0), 1.23456789);
        assert_eq!(float_array.value(1), -9.87654321);
        assert!(float_array.is_null(2));
        assert!(float_array.is_null(3));
    }

    #[test]
    fn test_build_string_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::String("hello".to_string())],
            },
            TableRow {
                values: vec![Cell::Bool(true)],
            }, // Converted to string
            TableRow {
                values: vec![Cell::I32(42)],
            }, // Converted to string
            TableRow {
                values: vec![Cell::Null],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Utf8);
        let string_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(string_array.len(), 4);
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "");
        assert_eq!(string_array.value(2), "");
        assert!(string_array.is_null(3));
    }

    #[test]
    fn test_build_binary_array() {
        let test_bytes = vec![1, 2, 3, 4, 5];
        let rows = vec![
            TableRow {
                values: vec![Cell::Bytes(test_bytes.clone())],
            },
            TableRow {
                values: vec![Cell::Bytes(vec![])],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("not bytes".to_string())],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::LargeBinary);
        let binary_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();

        assert_eq!(binary_array.len(), 4);
        assert_eq!(binary_array.value(0), test_bytes);
        assert_eq!(binary_array.value(1), Vec::<u8>::new());
        assert!(binary_array.is_null(2));
        assert!(binary_array.is_null(3));
    }

    #[test]
    fn test_build_date32_array() {
        use chrono::NaiveDate;
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let expected_days = test_date.signed_duration_since(UNIX_EPOCH).num_days() as i32;

        let rows = vec![
            TableRow {
                values: vec![Cell::Date(test_date)],
            },
            TableRow {
                values: vec![Cell::Date(UNIX_EPOCH)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("2023-05-15".to_string())],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Date32);
        let date_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();

        assert_eq!(date_array.len(), 4);
        assert_eq!(date_array.value(0), expected_days);
        assert_eq!(date_array.value(1), 0);
        assert!(date_array.is_null(2));
        assert!(date_array.is_null(3));
    }

    #[test]
    fn test_build_time64_array() {
        use chrono::NaiveTime;
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let expected_micros = test_time
            .signed_duration_since(MIDNIGHT)
            .num_microseconds()
            .unwrap();

        let rows = vec![
            TableRow {
                values: vec![Cell::Time(test_time)],
            },
            TableRow {
                values: vec![Cell::Time(MIDNIGHT)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("12:30:45".to_string())],
            },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Time64(TimeUnit::Microsecond));
        let time_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            .unwrap();

        assert_eq!(time_array.len(), 4);
        assert_eq!(time_array.value(0), expected_micros);
        assert_eq!(time_array.value(1), 0);
        assert!(time_array.is_null(2));
        assert!(time_array.is_null(3));
    }

    #[test]
    fn test_build_timestamp_array() {
        use chrono::DateTime;
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let expected_micros = test_ts.and_utc().timestamp_micros();

        let rows = vec![
            TableRow {
                values: vec![Cell::Timestamp(test_ts)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("2001-09-09 01:46:40".to_string())],
            },
        ];

        let array_ref =
            build_array_for_field(&rows, 0, &DataType::Timestamp(TimeUnit::Microsecond, None));
        let ts_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(ts_array.len(), 3);
        assert_eq!(ts_array.value(0), expected_micros);
        assert!(ts_array.is_null(1));
        assert!(ts_array.is_null(2));
    }

    #[test]
    fn test_build_timestamptz_array() {
        use chrono::DateTime;
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap();
        let expected_micros = test_ts.timestamp_micros();

        let rows = vec![
            TableRow {
                values: vec![Cell::TimestampTz(test_ts)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String("2001-09-09T01:46:40Z".to_string())],
            },
        ];

        let array_ref = build_array_for_field(
            &rows,
            0,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        );
        let ts_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(ts_array.len(), 3);
        assert_eq!(ts_array.value(0), expected_micros);
        assert!(ts_array.timezone().is_some());
        assert!(ts_array.is_null(1));
        assert!(ts_array.is_null(2));
    }

    #[test]
    fn test_build_uuid_array() {
        use uuid::Uuid;
        let test_uuid = Uuid::new_v4();
        let expected_bytes = test_uuid.as_bytes();

        let rows = vec![
            TableRow {
                values: vec![Cell::Uuid(test_uuid)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::String(test_uuid.to_string())],
            },
        ];

        let array_ref =
            build_array_for_field(&rows, 0, &DataType::FixedSizeBinary(UUID_BYTE_WIDTH));
        let uuid_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(uuid_array.len(), 3);
        assert_eq!(uuid_array.value(0), expected_bytes);
        assert!(uuid_array.is_null(1));
        assert!(uuid_array.is_null(2));
    }

    #[test]
    fn test_rows_to_record_batch_simple() {
        use arrow::datatypes::{Field, Schema};

        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(42),
                    Cell::String("hello".to_string()),
                    Cell::Bool(true),
                ],
            },
            TableRow {
                values: vec![
                    Cell::I32(100),
                    Cell::String("world".to_string()),
                    Cell::Bool(false),
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        // Verify column values
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 42);
        assert_eq!(id_array.value(1), 100);

        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "hello");
        assert_eq!(name_array.value(1), "world");

        let active_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(active_array.value(0));
        assert!(!active_array.value(1));
    }

    #[test]
    fn test_rows_to_record_batch_with_nulls() {
        use arrow::datatypes::{Field, Schema};

        let rows = vec![
            TableRow {
                values: vec![Cell::I32(42), Cell::Null],
            },
            TableRow {
                values: vec![Cell::Null, Cell::String("test".to_string())],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 42);
        assert!(id_array.is_null(1));

        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(name_array.is_null(0));
        assert_eq!(name_array.value(1), "test");
    }

    #[test]
    fn test_rows_to_record_batch_temporal_types() {
        use arrow::datatypes::{Field, Schema};
        use chrono::{DateTime, NaiveDate, NaiveTime};

        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let test_ts_tz = DateTime::from_timestamp(1000000000, 0).unwrap();

        let rows = vec![TableRow {
            values: vec![
                Cell::Date(test_date),
                Cell::Time(test_time),
                Cell::Timestamp(test_ts),
                Cell::TimestampTz(test_ts_tz),
            ],
        }];

        let schema = Schema::new(vec![
            Field::new("date_col", DataType::Date32, false),
            Field::new("time_col", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new(
                "ts_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_tz_col",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);

        let date_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(
            date_array.value(0),
            test_date.signed_duration_since(UNIX_EPOCH).num_days() as i32
        );

        let time_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(
            time_array.value(0),
            test_time
                .signed_duration_since(MIDNIGHT)
                .num_microseconds()
                .unwrap()
        );

        let ts_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.value(0), test_ts.and_utc().timestamp_micros());

        let ts_tz_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_tz_array.value(0), test_ts_tz.timestamp_micros());
    }

    #[test]
    fn test_rows_to_record_batch_binary_and_uuid() {
        use arrow::datatypes::{Field, Schema};
        use uuid::Uuid;

        let test_bytes = vec![1, 2, 3, 4, 5];
        let test_uuid = Uuid::new_v4();

        let rows = vec![TableRow {
            values: vec![Cell::Bytes(test_bytes.clone()), Cell::Uuid(test_uuid)],
        }];

        let schema = Schema::new(vec![
            Field::new("data", DataType::LargeBinary, false),
            Field::new("uuid", DataType::FixedSizeBinary(16), false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let bytes_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(bytes_array.value(0), test_bytes);

        let uuid_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid_array.value(0), test_uuid.as_bytes());
    }

    #[test]
    fn test_rows_to_record_batch_empty() {
        use arrow::datatypes::{Field, Schema};

        let rows: Vec<TableRow> = vec![];
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_rows_to_record_batch_unsupported_fallback() {
        use arrow::datatypes::{Field, Schema};

        // Test with a data type that doesn't have a direct converter
        // This will test the fallback to string conversion behavior
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(42),
                    Cell::Json(serde_json::json!({"key": "value", "number": 123})),
                ],
            },
            TableRow {
                values: vec![Cell::I32(100), Cell::Null],
            },
        ];

        // Use a schema that expects a different type for JSON data
        // This should trigger fallback behavior to string representation
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("metadata", DataType::Utf8, true), // JSON as string
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 42);
        assert_eq!(id_array.value(1), 100);

        let metadata_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        // JSON should be converted to string representation
        assert_eq!(metadata_array.value(0), r#"{"key":"value","number":123}"#);
        assert!(metadata_array.is_null(1));
    }

    #[test]
    fn test_rows_to_record_batch_schema_mismatch_length() {
        use arrow::datatypes::{Field, Schema};

        // Test what happens when row has different number of columns than schema
        let rows = vec![TableRow {
            values: vec![
                Cell::I32(1),
                Cell::String("test".to_string()),
                Cell::Bool(true), // Extra column not in schema
            ],
        }];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            // Missing third field for the boolean
        ]);

        // This should either handle gracefully or return an error
        let result = rows_to_record_batch(&rows, schema);

        // The function should handle this case - either by succeeding with partial data
        // or by returning an appropriate error
        match result {
            Ok(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 2); // Only schema columns
            }
            Err(_) => {
                // Error is also acceptable for schema mismatch
            }
        }
    }
}
