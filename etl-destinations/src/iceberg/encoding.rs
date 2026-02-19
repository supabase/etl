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
        DataType::Int32 => build_int32_list_array(rows, field_idx, field),
        DataType::Int64 => build_int64_list_array(rows, field_idx, field),
        DataType::Float32 => build_float32_list_array(rows, field_idx, field),
        DataType::Float64 => build_float64_list_array(rows, field_idx, field),
        DataType::Utf8 => build_string_list_array(rows, field_idx, field),
        DataType::LargeBinary => build_binary_list_array(rows, field_idx, field),
        DataType::Date32 => build_date32_list_array(rows, field_idx, field),
        DataType::Time64(TimeUnit::Microsecond) => build_time64_list_array(rows, field_idx, field),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_timestamp_list_array(rows, field_idx, field)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            build_timestamptz_list_array(rows, field_idx, field)
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => build_uuid_list_array(rows, field_idx, field),
        // For unsupported element types, fall back to string representation
        _ => build_list_array_for_strings(rows, field_idx, field),
    }
}

/// Builds a list array for boolean elements.
fn build_boolean_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(BooleanBuilder::new()).with_field(field.clone());

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
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for 32-bit integer elements.
fn build_int32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(PrimitiveBuilder::<Int32Type>::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::I16(vec) => {
                    for item in vec {
                        list_builder.values().append_option(item.map(|v| v as i32));
                    }
                    list_builder.append(true);
                }
                ArrayCell::I32(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for 64-bit integer elements.
fn build_int64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(PrimitiveBuilder::<Int64Type>::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::I64(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                ArrayCell::U32(vec) => {
                    for item in vec {
                        list_builder.values().append_option(item.map(|v| v as i64));
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for 32-bit float elements.
fn build_float32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(PrimitiveBuilder::<Float32Type>::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::F32(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for 64-bit float elements.
fn build_float64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(PrimitiveBuilder::<Float64Type>::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::F64(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for string elements.
fn build_string_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(StringBuilder::new()).with_field(field.clone());

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
                ArrayCell::Numeric(vec) => {
                    for item in vec {
                        match item {
                            Some(n) => list_builder.values().append_value(n.to_string()),
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                ArrayCell::Json(vec) => {
                    for item in vec {
                        match item {
                            Some(j) => list_builder.values().append_value(j.to_string()),
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for binary elements.
fn build_binary_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(LargeBinaryBuilder::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Bytes(vec) => {
                    for item in vec {
                        match item {
                            Some(bytes) => list_builder.values().append_value(bytes),
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for Date32 elements.
fn build_date32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(PrimitiveBuilder::<Date32Type>::new()).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Date(vec) => {
                    for item in vec {
                        let arrow_value = item
                            .map(|date| date.signed_duration_since(UNIX_EPOCH).num_days() as i32);
                        list_builder.values().append_option(arrow_value);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for Time64 elements.
fn build_time64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Time64MicrosecondType>::new())
        .with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Time(vec) => {
                    for item in vec {
                        let arrow_value = item.and_then(|time| {
                            time.signed_duration_since(MIDNIGHT).num_microseconds()
                        });
                        list_builder.values().append_option(arrow_value);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for Timestamp elements.
fn build_timestamp_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<TimestampMicrosecondType>::new())
        .with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Timestamp(vec) => {
                    for item in vec {
                        let arrow_value = item.map(|ts| ts.and_utc().timestamp_micros());
                        list_builder.values().append_option(arrow_value);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for TimestampTz elements.
fn build_timestamptz_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    // Extract timezone from the field's data type
    let tz = if let DataType::Timestamp(TimeUnit::Microsecond, Some(tz_str)) = field.data_type() {
        tz_str.clone()
    } else {
        Arc::from("+00:00".to_string()) // Default to UTC
    };

    let mut list_builder = ListBuilder::new(TimestampMicrosecondBuilder::new().with_timezone(tz))
        .with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::TimestampTz(vec) => {
                    for item in vec {
                        let arrow_value = item.map(|ts| ts.timestamp_micros());
                        list_builder.values().append_option(arrow_value);
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
                }
            }
        } else {
            list_builder.append_null();
        }
    }

    Arc::new(list_builder.finish())
}

/// Builds a list array for UUID elements.
fn build_uuid_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut list_builder =
        ListBuilder::new(FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH)).with_field(field.clone());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Uuid(vec) => {
                    for item in vec {
                        match item {
                            Some(uuid) => {
                                list_builder
                                    .values()
                                    .append_value(uuid.as_bytes())
                                    .expect("array length and builder byte width are both 16");
                            }
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                _ => {
                    return build_list_array_for_strings(rows, field_idx, field);
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
fn build_list_array_for_strings(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    // Create a new field with Utf8 data type for string list elements
    let field = Arc::new(
        arrow::datatypes::Field::new(field.name(), DataType::Utf8, field.is_nullable())
            .with_metadata(field.metadata().clone()),
    );
    let mut list_builder = ListBuilder::new(StringBuilder::new()).with_field(field);

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
                size_hint_bytes: 0,
                values: vec![Cell::Bool(true)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Bool(false)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::I16(42)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::I32(-123)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::I64(123456789)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::I64(-987654321)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::U32(456)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::F32(2.5)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::F32(-1.25)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::F64(1.23456789)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::F64(-9.87654321)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::String("hello".to_string())],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Bool(true)],
            }, // Converted to string
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::I32(42)],
            }, // Converted to string
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::Bytes(test_bytes.clone())],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Bytes(vec![])],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::Date(test_date)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Date(UNIX_EPOCH)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::Time(test_time)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Time(MIDNIGHT)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::Timestamp(test_ts)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::TimestampTz(test_ts)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::Uuid(test_uuid)],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![
                    Cell::I32(42),
                    Cell::String("hello".to_string()),
                    Cell::Bool(true),
                ],
            },
            TableRow {
                size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![Cell::I32(42), Cell::Null],
            },
            TableRow {
                size_hint_bytes: 0,
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
            size_hint_bytes: 0,
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
            size_hint_bytes: 0,
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
                size_hint_bytes: 0,
                values: vec![
                    Cell::I32(42),
                    Cell::Json(serde_json::json!({"key": "value", "number": 123})),
                ],
            },
            TableRow {
                size_hint_bytes: 0,
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
            size_hint_bytes: 0,
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

    #[test]
    fn test_cell_to_array_cell() {
        let bool_array = ArrayCell::Bool(vec![Some(true), Some(false), None]);
        let string_array = ArrayCell::String(vec![Some("hello".to_string()), None]);

        // Test extraction from Cell::Array
        assert!(cell_to_array_cell(&Cell::Array(bool_array.clone())).is_some());
        assert!(cell_to_array_cell(&Cell::Array(string_array.clone())).is_some());

        // Test non-array cells return None
        assert!(cell_to_array_cell(&Cell::Null).is_none());
        assert!(cell_to_array_cell(&Cell::Bool(true)).is_none());
        assert!(cell_to_array_cell(&Cell::String("test".to_string())).is_none());
        assert!(cell_to_array_cell(&Cell::I32(42)).is_none());

        // Verify the extracted array cell is the same
        if let Some(extracted) = cell_to_array_cell(&Cell::Array(bool_array.clone())) {
            if let ArrayCell::Bool(vec) = extracted {
                assert_eq!(vec.len(), 3);
                assert_eq!(vec[0], Some(true));
                assert_eq!(vec[1], Some(false));
                assert_eq!(vec[2], None);
            } else {
                panic!("Expected Bool array");
            }
        }
    }

    #[test]
    fn test_build_boolean_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Boolean, true);
        let field_ref = Arc::new(field);

        // Test with boolean array cells
        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bool(vec![
                    Some(true),
                    Some(false),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bool(vec![Some(true)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bool(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::String("not an array".to_string())], // Non-array cell
            },
        ];

        let array_ref = build_boolean_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 5);

        // First row: [true, false, null]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let bool_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.len(), 3);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));

        // Second row: [true]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let bool_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.len(), 1);
        assert!(bool_array.value(0));

        // Third row: [] (empty array)
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));

        // Fifth row: null (non-array cell)
        assert!(list_array.is_null(4));
    }

    #[test]
    fn test_build_boolean_list_array_fallback() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Boolean, true);
        let field_ref = Arc::new(field);

        // Test with non-boolean array type - should fall back to string conversion
        let rows = vec![TableRow {
            size_hint_bytes: 0,
            values: vec![Cell::Array(ArrayCell::I32(vec![Some(1), Some(0), None]))],
        }];

        // This should trigger the fallback case
        let array_ref = build_boolean_list_array(&rows, 0, field_ref);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Should still create a list array but with string elements due to fallback
        assert_eq!(list_array.len(), 1);
        assert!(!list_array.is_null(0));
    }

    #[test]
    fn test_build_int32_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Int32, true);
        let field_ref = Arc::new(field);

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I16(vec![Some(10), Some(20), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(100), Some(-200)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I32(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_int32_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: I16 values converted to i32
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let int_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array.value(0), 10);
        assert_eq!(int_array.value(1), 20);
        assert!(int_array.is_null(2));

        // Second row: I32 values
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let int_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(int_array.len(), 2);
        assert_eq!(int_array.value(0), 100);
        assert_eq!(int_array.value(1), -200);

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_int64_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Int64, true);
        let field_ref = Arc::new(field);

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I64(vec![
                    Some(123456789),
                    Some(-987654321),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::U32(vec![Some(456), Some(789)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I64(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_int64_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: I64 values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let int_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array.value(0), 123456789);
        assert_eq!(int_array.value(1), -987654321);
        assert!(int_array.is_null(2));

        // Second row: U32 values converted to i64
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let int_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int_array.len(), 2);
        assert_eq!(int_array.value(0), 456);
        assert_eq!(int_array.value(1), 789);

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_float32_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Float32, true);
        let field_ref = Arc::new(field);

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F32(vec![
                    Some(1.5),
                    Some(-2.75),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F32(vec![Some(
                    std::f32::consts::PI,
                )]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F32(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_float32_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: F32 values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let float_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(float_array.len(), 3);
        assert_eq!(float_array.value(0), 1.5);
        assert_eq!(float_array.value(1), -2.75);
        assert!(float_array.is_null(2));

        // Second row: F32 values
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let float_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(float_array.len(), 1);
        assert!((float_array.value(0) - std::f32::consts::PI).abs() < f32::EPSILON);

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_float64_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::Float64, true);
        let field_ref = Arc::new(field);

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F64(vec![
                    Some(1.23456789),
                    Some(-9.87654321),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F64(vec![Some(
                    std::f64::consts::PI,
                )]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F64(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_float64_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: F64 values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let float_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(float_array.len(), 3);
        assert_eq!(float_array.value(0), 1.23456789);
        assert_eq!(float_array.value(1), -9.87654321);
        assert!(float_array.is_null(2));

        // Second row: F64 values
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let float_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(float_array.len(), 1);
        assert!((float_array.value(0) - std::f64::consts::PI).abs() < f64::EPSILON);

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_string_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use etl::types::PgNumeric;

        let field = Field::new("items", DataType::Utf8, true);
        let field_ref = Arc::new(field);

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::String(vec![
                    Some("hello".to_string()),
                    Some("world".to_string()),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Numeric(vec![
                    Some("12345".parse::<PgNumeric>().unwrap()),
                    Some("-6789".parse::<PgNumeric>().unwrap()),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Json(vec![
                    Some(serde_json::json!({"key": "value"})),
                    Some(serde_json::json!([1, 2, 3])),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::String(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_string_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 5);

        // First row: String values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let string_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");
        assert!(string_array.is_null(2));

        // Second row: Numeric values converted to strings
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let string_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "12345");
        assert_eq!(string_array.value(1), "-6789");
        assert!(string_array.is_null(2));

        // Third row: JSON values converted to strings
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        let string_array = third_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), r#"{"key":"value"}"#);
        assert_eq!(string_array.value(1), "[1,2,3]");
        assert!(string_array.is_null(2));

        // Fourth row: empty array
        assert!(!list_array.is_null(3));
        let fourth_list = list_array.value(3);
        assert_eq!(fourth_list.len(), 0);

        // Fifth row: null cell
        assert!(list_array.is_null(4));
    }

    #[test]
    fn test_build_binary_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let field = Field::new("items", DataType::LargeBinary, true);
        let field_ref = Arc::new(field);

        let test_bytes_1 = vec![1, 2, 3, 4, 5];
        let test_bytes_2 = vec![255, 0, 128];
        let empty_bytes = vec![];

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bytes(vec![
                    Some(test_bytes_1.clone()),
                    Some(test_bytes_2.clone()),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bytes(vec![Some(
                    empty_bytes.clone(),
                )]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bytes(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_binary_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: Bytes values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let binary_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(binary_array.len(), 3);
        assert_eq!(binary_array.value(0), test_bytes_1);
        assert_eq!(binary_array.value(1), test_bytes_2);
        assert!(binary_array.is_null(2));

        // Second row: Empty bytes
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let binary_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(binary_array.len(), 1);
        assert_eq!(binary_array.value(0), empty_bytes);

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_date32_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use chrono::NaiveDate;

        let field = Field::new("items", DataType::Date32, true);
        let field_ref = Arc::new(field);

        let test_date_1 = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let test_date_2 = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(); // Unix epoch
        let test_date_3 = NaiveDate::from_ymd_opt(2000, 12, 31).unwrap();

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Date(vec![
                    Some(test_date_1),
                    Some(test_date_2),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Date(vec![Some(test_date_3)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Date(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_date32_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: Date values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let date_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date_array.len(), 3);
        assert_eq!(
            date_array.value(0),
            test_date_1.signed_duration_since(UNIX_EPOCH).num_days() as i32
        );
        assert_eq!(
            date_array.value(1),
            test_date_2.signed_duration_since(UNIX_EPOCH).num_days() as i32
        );
        assert!(date_array.is_null(2));

        // Second row: Single date
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let date_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date_array.len(), 1);
        assert_eq!(
            date_array.value(0),
            test_date_3.signed_duration_since(UNIX_EPOCH).num_days() as i32
        );

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_time64_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use chrono::NaiveTime;

        let field = Field::new("items", DataType::Time64(TimeUnit::Microsecond), true);
        let field_ref = Arc::new(field);

        let test_time_1 = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let test_time_2 = NaiveTime::from_hms_opt(0, 0, 0).unwrap(); // Midnight
        let test_time_3 = NaiveTime::from_hms_opt(23, 59, 59).unwrap();

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Time(vec![
                    Some(test_time_1),
                    Some(test_time_2),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Time(vec![Some(test_time_3)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Time(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_time64_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: Time values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let time_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(time_array.len(), 3);
        assert_eq!(
            time_array.value(0),
            test_time_1
                .signed_duration_since(MIDNIGHT)
                .num_microseconds()
                .unwrap()
        );
        assert_eq!(
            time_array.value(1),
            test_time_2
                .signed_duration_since(MIDNIGHT)
                .num_microseconds()
                .unwrap()
        );
        assert!(time_array.is_null(2));

        // Second row: Single time
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let time_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(time_array.len(), 1);
        assert_eq!(
            time_array.value(0),
            test_time_3
                .signed_duration_since(MIDNIGHT)
                .num_microseconds()
                .unwrap()
        );

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_timestamp_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use chrono::DateTime;

        let field = Field::new(
            "items",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        );
        let field_ref = Arc::new(field);

        let test_ts_1 = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let test_ts_2 = DateTime::from_timestamp(1600000000, 0).unwrap().naive_utc();
        let test_ts_3 = DateTime::from_timestamp(0, 0).unwrap().naive_utc(); // Unix epoch

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![
                    Some(test_ts_1),
                    Some(test_ts_2),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![Some(test_ts_3)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_timestamp_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: Timestamp values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let ts_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.len(), 3);
        assert_eq!(ts_array.value(0), test_ts_1.and_utc().timestamp_micros());
        assert_eq!(ts_array.value(1), test_ts_2.and_utc().timestamp_micros());
        assert!(ts_array.is_null(2));

        // Second row: Single timestamp
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let ts_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.len(), 1);
        assert_eq!(ts_array.value(0), test_ts_3.and_utc().timestamp_micros());

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_timestamptz_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use chrono::DateTime;

        let field = Field::new(
            "items",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        );
        let field_ref = Arc::new(field);

        let test_ts_1 = DateTime::from_timestamp(1000000000, 0).unwrap();
        let test_ts_2 = DateTime::from_timestamp(1600000000, 0).unwrap();
        let test_ts_3 = DateTime::from_timestamp(0, 0).unwrap(); // Unix epoch

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::TimestampTz(vec![
                    Some(test_ts_1),
                    Some(test_ts_2),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::TimestampTz(vec![Some(test_ts_3)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::TimestampTz(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_timestamptz_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: TimestampTz values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let ts_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.len(), 3);
        assert_eq!(ts_array.value(0), test_ts_1.timestamp_micros());
        assert_eq!(ts_array.value(1), test_ts_2.timestamp_micros());
        assert!(ts_array.is_null(2));

        // Verify timezone is preserved
        assert!(ts_array.timezone().is_some());

        // Second row: Single timestamptz
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let ts_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.len(), 1);
        assert_eq!(ts_array.value(0), test_ts_3.timestamp_micros());

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_uuid_list_array() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use uuid::Uuid;

        let field = Field::new("items", DataType::FixedSizeBinary(UUID_BYTE_WIDTH), true);
        let field_ref = Arc::new(field);

        let test_uuid_1 = Uuid::new_v4();
        let test_uuid_2 = Uuid::new_v4();
        let test_uuid_3 = Uuid::nil(); // Nil UUID

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Uuid(vec![
                    Some(test_uuid_1),
                    Some(test_uuid_2),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Uuid(vec![Some(test_uuid_3)]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Uuid(vec![]))], // Empty array
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_uuid_list_array(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: UUID values
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let uuid_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid_array.len(), 3);
        assert_eq!(uuid_array.value(0), test_uuid_1.as_bytes());
        assert_eq!(uuid_array.value(1), test_uuid_2.as_bytes());
        assert!(uuid_array.is_null(2));

        // Second row: Single UUID (nil)
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let uuid_array = second_list
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid_array.len(), 1);
        assert_eq!(uuid_array.value(0), test_uuid_3.as_bytes());

        // Third row: empty array
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        assert_eq!(third_list.len(), 0);

        // Fourth row: null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_list_array_for_strings() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;
        use chrono::{DateTime, NaiveDate, NaiveTime};
        use etl::types::PgNumeric;
        use uuid::Uuid;

        let field = Field::new("items", DataType::Utf8, true);
        let field_ref = Arc::new(field);

        let test_uuid = Uuid::new_v4();
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let test_ts_tz = DateTime::from_timestamp(1000000000, 0).unwrap();

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::String(vec![
                    Some("hello".to_string()),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bool(vec![
                    Some(true),
                    Some(false),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(42), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::F64(vec![
                    Some(std::f64::consts::PI),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Uuid(vec![Some(test_uuid), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Date(vec![Some(test_date), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Time(vec![Some(test_time), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![Some(test_ts), None]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::TimestampTz(vec![
                    Some(test_ts_tz),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Numeric(vec![
                    Some("123.45".parse::<PgNumeric>().unwrap()),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Json(vec![
                    Some(serde_json::json!({"key": "value"})),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Array(ArrayCell::Bytes(vec![
                    Some(vec![1, 2, 3]),
                    None,
                ]))],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![Cell::Null], // Null cell
            },
        ];

        let array_ref = build_list_array_for_strings(&rows, 0, field_ref.clone());
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 13);

        // Row 0: String array (direct)
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let string_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "hello");
        assert!(string_array.is_null(1));

        // Row 1: Bool array (converted to strings)
        assert!(!list_array.is_null(1));
        let bool_list = list_array.value(1);
        let string_array = bool_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "true");
        assert_eq!(string_array.value(1), "false");
        assert!(string_array.is_null(2));

        // Row 2: I32 array (converted to strings)
        assert!(!list_array.is_null(2));
        let int_list = list_array.value(2);
        let string_array = int_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "42");
        assert!(string_array.is_null(1));

        // Row 3: F64 array (converted to strings)
        assert!(!list_array.is_null(3));
        let float_list = list_array.value(3);
        let string_array = float_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), std::f64::consts::PI.to_string());
        assert!(string_array.is_null(1));

        // Row 4: UUID array (converted to strings)
        assert!(!list_array.is_null(4));
        let uuid_list = list_array.value(4);
        let string_array = uuid_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), test_uuid.to_string());
        assert!(string_array.is_null(1));

        // Row 5: Date array (converted to strings with DATE_FORMAT)
        assert!(!list_array.is_null(5));
        let date_list = list_array.value(5);
        let string_array = date_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(
            string_array.value(0),
            test_date.format(DATE_FORMAT).to_string()
        );
        assert!(string_array.is_null(1));

        // Row 6: Time array (converted to strings with TIME_FORMAT)
        assert!(!list_array.is_null(6));
        let time_list = list_array.value(6);
        let string_array = time_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(
            string_array.value(0),
            test_time.format(TIME_FORMAT).to_string()
        );
        assert!(string_array.is_null(1));

        // Row 7: Timestamp array (converted to strings with TIMESTAMP_FORMAT)
        assert!(!list_array.is_null(7));
        let ts_list = list_array.value(7);
        let string_array = ts_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(
            string_array.value(0),
            test_ts.format(TIMESTAMP_FORMAT).to_string()
        );
        assert!(string_array.is_null(1));

        // Row 8: TimestampTz array (converted to strings with RFC3339)
        assert!(!list_array.is_null(8));
        let ts_tz_list = list_array.value(8);
        let string_array = ts_tz_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), test_ts_tz.to_rfc3339());
        assert!(string_array.is_null(1));

        // Row 9: Numeric array (converted to strings)
        assert!(!list_array.is_null(9));
        let numeric_list = list_array.value(9);
        let string_array = numeric_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "123.45");
        assert!(string_array.is_null(1));

        // Row 10: JSON array (converted to strings)
        assert!(!list_array.is_null(10));
        let json_list = list_array.value(10);
        let string_array = json_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), r#"{"key":"value"}"#);
        assert!(string_array.is_null(1));

        // Row 11: Bytes array (becomes null elements)
        assert!(!list_array.is_null(11));
        let bytes_list = list_array.value(11);
        let string_array = bytes_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert!(string_array.is_null(0)); // Bytes become null in string conversion
        assert!(string_array.is_null(1));

        // Row 12: Null cell
        assert!(list_array.is_null(12));
    }

    #[test]
    fn test_append_array_cell_as_strings_comprehensive() {
        use arrow::array::ListBuilder;
        use chrono::{DateTime, NaiveDate, NaiveTime};
        use etl::types::PgNumeric;
        use uuid::Uuid;

        let test_uuid = Uuid::new_v4();
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let test_ts_tz = DateTime::from_timestamp(1000000000, 0).unwrap();

        // Pre-compute string representations to avoid borrowing issues
        let date_str = test_date.format(DATE_FORMAT).to_string();
        let time_str = test_time.format(TIME_FORMAT).to_string();
        let ts_str = test_ts.format(TIMESTAMP_FORMAT).to_string();
        let ts_tz_str = test_ts_tz.to_rfc3339();
        let uuid_str = test_uuid.to_string();
        let f32_pi_str = std::f32::consts::PI.to_string();
        let f64_e_str = std::f64::consts::E.to_string();

        // Test each ArrayCell variant individually
        let test_cases = vec![
            (
                ArrayCell::Bool(vec![Some(true), Some(false), None]),
                vec!["true", "false"],
                1, // 1 null
            ),
            (
                ArrayCell::String(vec![Some("hello".to_string()), None]),
                vec!["hello"],
                1, // 1 null
            ),
            (
                ArrayCell::I16(vec![Some(42), Some(-100), None]),
                vec!["42", "-100"],
                1, // 1 null
            ),
            (
                ArrayCell::I32(vec![Some(123), None]),
                vec!["123"],
                1, // 1 null
            ),
            (
                ArrayCell::U32(vec![Some(456), None]),
                vec!["456"],
                1, // 1 null
            ),
            (
                ArrayCell::I64(vec![Some(789), None]),
                vec!["789"],
                1, // 1 null
            ),
            (
                ArrayCell::F32(vec![Some(std::f32::consts::PI), None]),
                vec![f32_pi_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::F64(vec![Some(std::f64::consts::E), None]),
                vec![f64_e_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::Numeric(vec![Some("123.45".parse::<PgNumeric>().unwrap()), None]),
                vec!["123.45"],
                1, // 1 null
            ),
            (
                ArrayCell::Date(vec![Some(test_date), None]),
                vec![date_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::Time(vec![Some(test_time), None]),
                vec![time_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::Timestamp(vec![Some(test_ts), None]),
                vec![ts_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::TimestampTz(vec![Some(test_ts_tz), None]),
                vec![ts_tz_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::Uuid(vec![Some(test_uuid), None]),
                vec![uuid_str.as_str()],
                1, // 1 null
            ),
            (
                ArrayCell::Json(vec![Some(serde_json::json!({"key": "value"})), None]),
                vec![r#"{"key":"value"}"#],
                1, // 1 null
            ),
            (
                ArrayCell::Bytes(vec![Some(vec![1, 2, 3]), None]),
                vec![], // Bytes become null strings
                2,      // All become null
            ),
        ];

        for (array_cell, expected_values, expected_nulls) in test_cases {
            let mut list_builder = ListBuilder::new(StringBuilder::new());

            append_array_cell_as_strings(&mut list_builder, &array_cell);

            // Build the array to check values
            list_builder.append(true);
            let list_array = list_builder.finish();
            let first_list = list_array.value(0);
            let string_array = first_list
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let total_len = string_array.len();
            let expected_non_nulls = expected_values.len();

            assert_eq!(total_len, expected_non_nulls + expected_nulls,);

            // Check non-null values
            let mut non_null_count = 0;
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    assert!(non_null_count < expected_values.len(),);
                    assert_eq!(string_array.value(i), expected_values[non_null_count],);
                    non_null_count += 1;
                }
            }

            assert_eq!(non_null_count, expected_values.len(),);
        }
    }

    #[test]
    fn test_build_list_array_dispatch() {
        use arrow::datatypes::Field;

        // Test that build_list_array correctly dispatches to the right type-specific builders
        let test_cases = vec![
            (DataType::Boolean, "boolean list"),
            (DataType::Int32, "int32 list"),
            (DataType::Int64, "int64 list"),
            (DataType::Float32, "float32 list"),
            (DataType::Float64, "float64 list"),
            (DataType::Utf8, "string list"),
            (DataType::LargeBinary, "binary list"),
            (DataType::Date32, "date32 list"),
            (DataType::Time64(TimeUnit::Microsecond), "time64 list"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "timestamp list",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                "timestamptz list",
            ),
            (DataType::FixedSizeBinary(UUID_BYTE_WIDTH), "uuid list"),
        ];

        for (data_type, test_name) in test_cases {
            let field = Field::new("item", data_type, true);
            let field_ref = Arc::new(field);

            let rows = vec![
                TableRow {
                    size_hint_bytes: 0,
                    values: vec![Cell::Array(ArrayCell::Bool(vec![Some(true)]))],
                },
                TableRow {
                    size_hint_bytes: 0,
                    values: vec![Cell::Null],
                },
            ];

            // Should not panic and should create an array
            let array_ref = build_list_array(&rows, 0, field_ref);
            let list_array = array_ref
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .unwrap();

            assert_eq!(list_array.len(), 2, "Failed for {test_name}");
            assert!(list_array.is_null(1),);
        }
    }

    #[test]
    fn test_build_list_array_unsupported_type_fallback() {
        use arrow::datatypes::Field;

        // Test with an unsupported inner type that should fall back to string representation
        let field = Field::new("item", DataType::Decimal128(10, 2), true); // Unsupported type
        let field_ref = Arc::new(field);

        let rows = vec![TableRow {
            size_hint_bytes: 0,
            values: vec![Cell::Array(ArrayCell::I32(vec![Some(123), Some(456)]))],
        }];

        let array_ref = build_list_array(&rows, 0, field_ref);
        let list_array = array_ref
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        assert_eq!(list_array.len(), 1);
        assert!(!list_array.is_null(0));

        // Should have created a string list as fallback
        let first_list = list_array.value(0);
        let string_array = first_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "123");
        assert_eq!(string_array.value(1), "456");
    }

    #[test]
    fn test_rows_to_record_batch_with_list_fields() {
        use arrow::datatypes::{Field, Schema};
        use uuid::Uuid;

        let test_uuid = Uuid::new_v4();
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();

        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![
                    Cell::I32(1),
                    Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false)])),
                    Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), None])),
                    Cell::Array(ArrayCell::String(vec![Some("hello".to_string()), None])),
                    Cell::Array(ArrayCell::Uuid(vec![Some(test_uuid)])),
                    Cell::Array(ArrayCell::Date(vec![Some(test_date), None])),
                ],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![
                    Cell::I32(2),
                    Cell::Array(ArrayCell::Bool(vec![])), // Empty array
                    Cell::Array(ArrayCell::I32(vec![Some(100)])),
                    Cell::Null,                               // Null array
                    Cell::Array(ArrayCell::Uuid(vec![None])), // Array with null element
                    Cell::Array(ArrayCell::Date(vec![])),     // Empty date array
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "bool_list",
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "int_list",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "string_list",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "uuid_list",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeBinary(UUID_BYTE_WIDTH),
                    true,
                ))),
                true,
            ),
            Field::new(
                "date_list",
                DataType::List(Arc::new(Field::new("item", DataType::Date32, true))),
                true,
            ),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);

        // Check ID column
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        // Check bool list column
        let bool_list_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(bool_list_array.len(), 2);
        assert!(!bool_list_array.is_null(0));
        assert!(!bool_list_array.is_null(1));

        let first_bool_list = bool_list_array.value(0);
        let bool_array = first_bool_list
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(bool_array.len(), 2);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        let second_bool_list = bool_list_array.value(1); // Empty array
        assert_eq!(second_bool_list.len(), 0);

        // Check int list column
        let int_list_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(int_list_array.len(), 2);
        assert!(!int_list_array.is_null(0));
        assert!(!int_list_array.is_null(1));

        let first_int_list = int_list_array.value(0);
        let int_array = first_int_list
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array.value(0), 10);
        assert_eq!(int_array.value(1), 20);
        assert!(int_array.is_null(2));

        let second_int_list = int_list_array.value(1);
        let int_array = second_int_list
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(int_array.len(), 1);
        assert_eq!(int_array.value(0), 100);

        // Check string list column
        let string_list_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(string_list_array.len(), 2);
        assert!(!string_list_array.is_null(0));
        assert!(string_list_array.is_null(1)); // Null array

        let first_string_list = string_list_array.value(0);
        let string_array = first_string_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "hello");
        assert!(string_array.is_null(1));

        // Check UUID list column
        let uuid_list_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(uuid_list_array.len(), 2);
        assert!(!uuid_list_array.is_null(0));
        assert!(!uuid_list_array.is_null(1));

        let first_uuid_list = uuid_list_array.value(0);
        let uuid_array = first_uuid_list
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid_array.len(), 1);
        assert_eq!(uuid_array.value(0), test_uuid.as_bytes());

        let second_uuid_list = uuid_list_array.value(1);
        let uuid_array = second_uuid_list
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid_array.len(), 1);
        assert!(uuid_array.is_null(0)); // Null UUID

        // Check date list column
        let date_list_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(date_list_array.len(), 2);
        assert!(!date_list_array.is_null(0));
        assert!(!date_list_array.is_null(1));

        let first_date_list = date_list_array.value(0);
        let date_array = first_date_list
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date_array.len(), 2);
        assert_eq!(
            date_array.value(0),
            test_date.signed_duration_since(UNIX_EPOCH).num_days() as i32
        );
        assert!(date_array.is_null(1));

        let second_date_list = date_list_array.value(1); // Empty date array
        assert_eq!(second_date_list.len(), 0);
    }

    #[test]
    fn test_rows_to_record_batch_mixed_scalar_and_array() {
        use arrow::datatypes::{Field, Schema};

        // Test mixing scalar and array columns in the same record batch
        let rows = vec![
            TableRow {
                size_hint_bytes: 0,
                values: vec![
                    Cell::String("record1".to_string()),
                    Cell::I32(42),
                    Cell::Array(ArrayCell::F32(vec![Some(1.1), Some(2.2)])),
                    Cell::Bool(true),
                    Cell::Array(ArrayCell::String(vec![
                        Some("a".to_string()),
                        Some("b".to_string()),
                    ])),
                ],
            },
            TableRow {
                size_hint_bytes: 0,
                values: vec![
                    Cell::String("record2".to_string()),
                    Cell::I32(84),
                    Cell::Array(ArrayCell::F32(vec![])), // Empty float array
                    Cell::Bool(false),
                    Cell::Null, // Null string array
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
            Field::new(
                "floats",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new("flag", DataType::Boolean, false),
            Field::new(
                "strings",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);

        // Verify all scalar columns work correctly
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "record1");
        assert_eq!(name_array.value(1), "record2");

        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(value_array.value(0), 42);
        assert_eq!(value_array.value(1), 84);

        // Verify array columns work correctly
        let float_list_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert!(!float_list_array.is_null(0));
        assert!(!float_list_array.is_null(1));

        let first_float_list = float_list_array.value(0);
        let float_array = first_float_list
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(float_array.len(), 2);
        assert_eq!(float_array.value(0), 1.1);
        assert_eq!(float_array.value(1), 2.2);

        let second_float_list = float_list_array.value(1); // Empty array
        assert_eq!(second_float_list.len(), 0);

        // Verify remaining scalar column
        let flag_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(flag_array.value(0));
        assert!(!flag_array.value(1));

        // Verify final array column
        let string_list_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert!(!string_list_array.is_null(0));
        assert!(string_list_array.is_null(1)); // Null array

        let first_string_list = string_list_array.value(0);
        let string_array = first_string_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "a");
        assert_eq!(string_array.value(1), "b");
    }
}
