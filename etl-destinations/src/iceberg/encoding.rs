use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, ArrowPrimitiveType, BooleanBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
        ListBuilder, PrimitiveBuilder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
        Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    },
    error::ArrowError,
};
use base64::{Engine, prelude::BASE64_STANDARD};
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
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances containing the data to convert
/// * `schema` - The Arrow [`Schema`] defining the structure and types of the output batch
///
/// # Returns
///
/// Returns a [`RecordBatch`] containing the converted data, or an [`ArrowError`]
/// if the conversion fails due to schema mismatches or other Arrow-related issues.
///
/// # Examples
///
/// The function is typically used in the Iceberg destination to prepare data
/// for writing to Parquet files within Iceberg tables.
pub fn rows_to_record_batch(rows: &[TableRow], schema: Schema) -> Result<RecordBatch, ArrowError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (field_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_for_field(rows, field_idx, field.data_type());
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

    Ok(batch)
}

/// Builds an Arrow list array from [`TableRow`]s for a specific field.
///
/// This function creates an Arrow [`ListArray`] by processing [`Cell::Array`] values
/// from the specified field index. It delegates to type-specific builders based on
/// the list element type, reusing the existing primitive and string array building
/// infrastructure.
///
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances to extract list values from
/// * `field_idx` - The zero-based index of the list field within each row
/// * `element_type` - The Arrow [`DataType`] of the list elements
///
/// # Returns
///
/// Returns an [`ArrayRef`] containing a list array with the appropriate element type.
/// Rows with non-array cells become null entries in the resulting list array.
fn build_list_array(rows: &[TableRow], field_idx: usize, element_type: &DataType) -> ArrayRef {
    match element_type {
        DataType::Boolean => build_boolean_list_array(rows, field_idx),
        DataType::Int32 => build_i32_list_array(rows, field_idx),
        DataType::Int64 => build_i64_list_array(rows, field_idx),
        DataType::Float32 => build_f32_list_array(rows, field_idx),
        DataType::Float64 => build_f64_list_array(rows, field_idx),
        DataType::Utf8 => build_list_array_for_strings(rows, field_idx),
        DataType::LargeBinary => build_list_array_for_bytes(rows, field_idx),
        DataType::Date32 => build_date32_list_array(rows, field_idx),
        DataType::Time64(TimeUnit::Microsecond) => build_time64_list_array(rows, field_idx),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_timestamp_list_array(rows, field_idx)
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => build_list_array_for_uuids(rows, field_idx),
        // For unsupported element types, fall back to string representation
        _ => build_list_array_for_strings(rows, field_idx),
    }
}

// /// Builds a list array for primitive types using a type-specific converter function.
// ///
// /// This generic function creates Arrow list arrays for primitive types by using
// /// a `ListBuilder` wrapped around the appropriate primitive builder. It processes
// /// [`ArrayCell`] variants and appends their elements using the provided converter.
// ///
// /// # Type Parameters
// ///
// /// * `T` - The native Rust type for the list elements
// /// * `F` - The converter function type taking a [`Cell`] reference and returning [`Option<T>`]
// ///
// /// # Arguments
// ///
// /// * `rows` - A slice of [`TableRow`] instances to extract list values from
// /// * `field_idx` - The zero-based index of the list field within each row
// /// * `converter` - A function that converts individual [`Cell`] values to the target type
// ///
// /// # Returns
// ///
// /// Returns an [`ArrayRef`] containing a list array with primitive elements.
// fn build_list_array_for_type<T, F>(rows: &[TableRow], field_idx: usize, _converter: F) -> ArrayRef
// where
//     T: Default + Clone,
//     F: Fn(&Cell) -> Option<T>,
// {
//     // For primitives, we need to implement specific builders for each type
//     // For now, fall back to string representation until we implement typed builders
//     build_list_array_for_strings(rows, field_idx)
// }

/// Builds a list array for boolean elements.
fn build_boolean_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(BooleanBuilder::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
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

/// Builds a list array for i32 elements.
fn build_i32_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::I32(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                ArrayCell::I16(vec) => {
                    // Convert i16 to i32
                    for item in vec {
                        list_builder.values().append_option(item.map(|v| v as i32));
                    }
                    list_builder.append(true);
                }
                ArrayCell::U32(vec) => {
                    // Convert u32 to i32 (may lose data for large values)
                    for item in vec {
                        list_builder.values().append_option(item.map(|v| v as i32));
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for i64 elements.
fn build_i64_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Int64Type>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::I64(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for f32 elements.
fn build_f32_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Float32Type>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::F32(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for f64 elements.
fn build_f64_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Float64Type>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::F64(vec) => {
                    for item in vec {
                        list_builder.values().append_option(*item);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for Date32 elements.
fn build_date32_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Date32Type>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::Date(vec) => {
                    for item in vec {
                        let date32_value =
                            item.map(|d| d.signed_duration_since(UNIX_EPOCH).num_days() as i32);
                        list_builder.values().append_option(date32_value);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for Time64 elements.
fn build_time64_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Time64MicrosecondType>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::Time(vec) => {
                    for item in vec {
                        let time64_value =
                            item.and_then(|t| t.signed_duration_since(MIDNIGHT).num_microseconds());
                        list_builder.values().append_option(time64_value);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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

/// Builds a list array for Timestamp elements.
fn build_timestamp_list_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut list_builder = ListBuilder::new(PrimitiveBuilder::<TimestampMicrosecondType>::new());

    for row in rows {
        if let Some(array_cell) = cell_to_array_cell(&row.values[field_idx]) {
            match array_cell {
                ArrayCell::Null => {
                    list_builder.append_null();
                }
                ArrayCell::Timestamp(vec) => {
                    for item in vec {
                        let timestamp_value = item.map(|ts| ts.and_utc().timestamp_micros());
                        list_builder.values().append_option(timestamp_value);
                    }
                    list_builder.append(true);
                }
                // For incompatible array types, fall back to string representation
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
                ArrayCell::Null => {
                    list_builder.append_null();
                }
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

/// Builds a list array for byte array elements.
///
/// This function creates an Arrow list array with binary elements by processing
/// [`ArrayCell::Bytes`] variants from [`Cell::Array`] values.
fn build_list_array_for_bytes(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    // For now, fall back to string representation
    // This will be improved to use proper binary list builders
    build_list_array_for_strings(rows, field_idx)
}

/// Builds a list array for UUID elements.
///
/// This function creates an Arrow list array with UUID elements by processing
/// [`ArrayCell::Uuid`] variants from [`Cell::Array`] values.
fn build_list_array_for_uuids(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    // For now, fall back to string representation
    // This will be improved to use proper UUID list builders
    build_list_array_for_strings(rows, field_idx)
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
        ArrayCell::Null => {
            // Empty - this case is handled by the caller
        }
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
            for item in vec {
                match item {
                    Some(b) => list_builder
                        .values()
                        .append_value(BASE64_STANDARD.encode(b)),
                    None => list_builder.values().append_null(),
                }
            }
        }
    }
}

/// Builds an [`ArrayRef`] from the [`TableRow`]s for a field specified by the `field_idx`.
///
/// This function dispatches to type-specific array builders based on the Arrow
/// [`DataType`]. It handles all supported data types including primitives, strings,
/// binary data, dates, times, timestamps, and UUIDs. Unsupported types fall back
/// to string representation.
///
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances to extract field values from
/// * `field_idx` - The zero-based index of the field within each row to process
/// * `data_type` - The Arrow [`DataType`] specifying how to interpret and encode the field
///
/// # Returns
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
        DataType::List(field) => build_list_array(rows, field_idx, field.data_type()),
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
/// # Type Parameters
///
/// * `T` - The Arrow primitive type implementing [`ArrowPrimitiveType`]
/// * `F` - The converter function type taking a [`Cell`] reference and returning [`Option<T::Native>`]
///
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances to extract field values from
/// * `field_idx` - The zero-based index of the field within each row to process
/// * `converter` - A function that converts [`Cell`] values to the target primitive type
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
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances to extract timestamp values from
/// * `field_idx` - The zero-based index of the timestamp field within each row
/// * `tz` - The timezone string (e.g., "UTC", "America/New_York") for the array
///
/// # Returns
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
/// # Arguments
///
/// * `rows` - A slice of [`TableRow`] instances to extract UUID values from
/// * `field_idx` - The zero-based index of the UUID field within each row
///
/// # Returns
///
/// Returns an [`ArrayRef`] containing a fixed-size binary array with 16-byte
/// UUID values. Non-UUID cells become null entries in the resulting array.
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
/// - [`Cell::U32`] values (cast to i32, may overflow for large values)
///
/// Returns [`None`] for incompatible cell types.
fn cell_to_i32(cell: &Cell) -> Option<i32> {
    match cell {
        Cell::I16(v) => Some(*v as i32),
        Cell::I32(v) => Some(*v),
        Cell::U32(v) => Some(*v as i32),
        _ => None,
    }
}

/// Converts a [`Cell`] to a 64-bit signed integer.
///
/// Extracts 64-bit signed integer values from [`Cell::I64`] variants,
/// returning [`None`] for all other cell types.
fn cell_to_i64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::I64(v) => Some(*v),
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
/// # Arguments
///
/// * `cell` - The [`Cell`] to extract UUID bytes from
///
/// # Returns
///
/// Returns [`Some`] with a reference to the 16-byte UUID array if the cell
/// contains a UUID, or [`None`] for all other cell types.
fn cell_to_uuid(cell: &Cell) -> Option<&[u8; UUID_BYTE_WIDTH as usize]> {
    match cell {
        Cell::Uuid(value) => Some(value.as_bytes()),
        _ => None,
    }
}

/// Extracts [`ArrayCell`] from a [`Cell::Array`] value.
///
/// This function safely extracts the array data from a [`Cell::Array`] variant,
/// returning [`None`] for non-array cells. This is used when building Arrow list
/// arrays to access the underlying array elements.
///
/// # Arguments
///
/// * `cell` - The [`Cell`] to extract array data from
///
/// # Returns
///
/// Returns [`Some`] with a reference to the [`ArrayCell`] if the cell contains
/// an array, or [`None`] for all other cell types including [`Cell::Null`].
fn cell_to_array_cell(cell: &Cell) -> Option<&ArrayCell> {
    match cell {
        Cell::Array(array_cell) => Some(array_cell),
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
/// # Arguments
///
/// * `cell` - The [`Cell`] value to convert to a string
///
/// # Returns
///
/// Returns [`Some`] with the string representation for non-null values,
/// or [`None`] for [`Cell::Null`] which becomes a null entry in the Arrow array.
fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Bool(b) => Some(b.to_string()),
        Cell::String(s) => Some(s.clone()),
        Cell::I16(i) => Some(i.to_string()),
        Cell::I32(i) => Some(i.to_string()),
        Cell::U32(u) => Some(u.to_string()),
        Cell::I64(i) => Some(i.to_string()),
        Cell::F32(f) => Some(f.to_string()),
        Cell::F64(f) => Some(f.to_string()),
        Cell::Numeric(n) => Some(n.to_string()),
        Cell::Date(d) => Some(d.format(DATE_FORMAT).to_string()),
        Cell::Time(t) => Some(t.format(TIME_FORMAT).to_string()),
        Cell::Timestamp(ts) => Some(ts.format(TIMESTAMP_FORMAT).to_string()),
        Cell::TimestampTz(ts) => Some(ts.to_rfc3339()),
        Cell::Uuid(u) => Some(u.to_string()),
        Cell::Json(j) => Some(j.to_string()),
        Cell::Bytes(b) => Some(BASE64_STANDARD.encode(b)),
        Cell::Array(arr) => Some(format!("{arr:?}")), // Simple debug representation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray};
    use etl::types::ArrayCell;

    #[test]
    fn test_build_list_array_with_i32_elements() {
        // Create test data with i32 arrays
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), Some(3)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(4), None, Some(6)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Null)],
            },
            TableRow {
                values: vec![Cell::Null],
            },
        ];

        // Build the list array
        let array_ref = build_list_array(&rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Verify the structure
        assert_eq!(list_array.len(), 4);

        // First row: [1, 2, 3]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_i32 = first_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_list_i32.len(), 3);
        assert_eq!(first_list_i32.value(0), 1);
        assert_eq!(first_list_i32.value(1), 2);
        assert_eq!(first_list_i32.value(2), 3);

        // Second row: [4, null, 6]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_i32 = second_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second_list_i32.len(), 3);
        assert_eq!(second_list_i32.value(0), 4);
        assert!(second_list_i32.is_null(1));
        assert_eq!(second_list_i32.value(2), 6);

        // Third row: null array
        assert!(list_array.is_null(2));

        // Fourth row: null cell
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_build_list_array_with_boolean_elements() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Bool(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Bool(vec![Some(false), None]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Boolean);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Verify the structure
        assert_eq!(list_array.len(), 2);

        // First row: [true, false, true]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_bool = first_list
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(first_list_bool.len(), 3);
        assert!(first_list_bool.value(0));
        assert!(!first_list_bool.value(1));
        assert!(first_list_bool.value(2));

        // Second row: [false, null]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_bool = second_list
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(second_list_bool.len(), 2);
        assert!(!second_list_bool.value(0));
        assert!(second_list_bool.is_null(1));
    }

    #[test]
    fn test_build_list_array_with_string_elements() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::String(vec![
                    Some("hello".to_string()),
                    Some("world".to_string()),
                ]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::String(vec![
                    Some("foo".to_string()),
                    None,
                    Some("bar".to_string()),
                ]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Utf8);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        // Verify the structure
        assert_eq!(list_array.len(), 2);

        // First row: ["hello", "world"]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_str = first_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(first_list_str.len(), 2);
        assert_eq!(first_list_str.value(0), "hello");
        assert_eq!(first_list_str.value(1), "world");

        // Second row: ["foo", null, "bar"]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_str = second_list
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(second_list_str.len(), 3);
        assert_eq!(second_list_str.value(0), "foo");
        assert!(second_list_str.is_null(1));
        assert_eq!(second_list_str.value(2), "bar");
    }

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
        assert_eq!(cell_to_i32(&Cell::U32(42)), Some(42));
        assert_eq!(cell_to_i32(&Cell::U32(u32::MAX)), Some(-1)); // Overflow case
        assert_eq!(cell_to_i32(&Cell::Null), None);
        assert_eq!(cell_to_i32(&Cell::I64(42)), None);
        assert_eq!(cell_to_i32(&Cell::String("42".to_string())), None);
    }

    #[test]
    fn test_cell_to_i64() {
        assert_eq!(cell_to_i64(&Cell::I64(42)), Some(42));
        assert_eq!(cell_to_i64(&Cell::I64(-42)), Some(-42));
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
        assert_eq!(cell_to_bytes(&Cell::Bytes(test_bytes.clone())), Some(test_bytes));
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
        assert_eq!(cell_to_date32(&Cell::String("2023-05-15".to_string())), None);
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
        
        assert_eq!(cell_to_timestamp(&Cell::Timestamp(test_ts)), Some(expected_micros));
        assert_eq!(cell_to_timestamp(&Cell::Null), None);
        assert_eq!(cell_to_timestamp(&Cell::String("2001-09-09 01:46:40".to_string())), None);
    }

    #[test]
    fn test_cell_to_timestamptz() {
        use chrono::DateTime;
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap();
        let expected_micros = test_ts.timestamp_micros();
        
        assert_eq!(cell_to_timestamptz(&Cell::TimestampTz(test_ts)), Some(expected_micros));
        assert_eq!(cell_to_timestamptz(&Cell::Null), None);
        assert_eq!(cell_to_timestamptz(&Cell::String("2001-09-09T01:46:40Z".to_string())), None);
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
    fn test_cell_to_array_cell() {
        let test_array = ArrayCell::I32(vec![Some(1), Some(2), None]);
        assert_eq!(cell_to_array_cell(&Cell::Array(test_array.clone())), Some(&test_array));
        assert_eq!(cell_to_array_cell(&Cell::Null), None);
        assert_eq!(cell_to_array_cell(&Cell::String("array".to_string())), None);
    }

    #[test]
    fn test_cell_to_string() {
        use chrono::{NaiveDate, NaiveTime};
        use uuid::Uuid;
        
        // Test basic types
        assert_eq!(cell_to_string(&Cell::Null), None);
        assert_eq!(cell_to_string(&Cell::Bool(true)), Some("true".to_string()));
        assert_eq!(cell_to_string(&Cell::Bool(false)), Some("false".to_string()));
        assert_eq!(cell_to_string(&Cell::String("hello".to_string())), Some("hello".to_string()));
        assert_eq!(cell_to_string(&Cell::I16(42)), Some("42".to_string()));
        assert_eq!(cell_to_string(&Cell::I32(-42)), Some("-42".to_string()));
        assert_eq!(cell_to_string(&Cell::U32(42)), Some("42".to_string()));
        assert_eq!(cell_to_string(&Cell::I64(42)), Some("42".to_string()));
        assert_eq!(cell_to_string(&Cell::F32(2.5)), Some("2.5".to_string()));
        assert_eq!(cell_to_string(&Cell::F64(1.234567)), Some("1.234567".to_string()));
        
        // Test temporal types with known formats
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        assert_eq!(cell_to_string(&Cell::Date(test_date)), Some("2023-05-15".to_string()));
        
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(cell_to_string(&Cell::Time(test_time)), Some("12:30:45".to_string()));
        
        // Test UUID
        let test_uuid = Uuid::new_v4();
        assert_eq!(cell_to_string(&Cell::Uuid(test_uuid)), Some(test_uuid.to_string()));
        
        // Test JSON
        let json_val = serde_json::json!({"key": "value"});
        assert_eq!(cell_to_string(&Cell::Json(json_val.clone())), Some(json_val.to_string()));
        
        // Test bytes (Base64 encoded)
        let test_bytes = vec![72, 101, 108, 108, 111]; // "Hello" in ASCII
        let expected_base64 = BASE64_STANDARD.encode(&test_bytes);
        assert_eq!(cell_to_string(&Cell::Bytes(test_bytes)), Some(expected_base64));
        
        // Test array (debug format)
        let test_array = ArrayCell::I32(vec![Some(1), Some(2)]);
        assert!(cell_to_string(&Cell::Array(test_array)).is_some());
    }

    #[test]
    fn test_build_boolean_array() {
        let rows = vec![
            TableRow { values: vec![Cell::Bool(true)] },
            TableRow { values: vec![Cell::Bool(false)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("not bool".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Boolean);
        let bool_array = array_ref.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();

        assert_eq!(bool_array.len(), 4);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));
        assert!(bool_array.is_null(3)); // Non-bool cell becomes null
    }

    #[test]
    fn test_build_i32_array() {
        let rows = vec![
            TableRow { values: vec![Cell::I16(42)] },
            TableRow { values: vec![Cell::I32(-123)] },
            TableRow { values: vec![Cell::U32(456)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("not int".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Int32);
        let int_array = array_ref.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();

        assert_eq!(int_array.len(), 5);
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), -123);
        assert_eq!(int_array.value(2), 456);
        assert!(int_array.is_null(3));
        assert!(int_array.is_null(4)); // Non-int cell becomes null
    }

    #[test]
    fn test_build_i64_array() {
        let rows = vec![
            TableRow { values: vec![Cell::I64(123456789)] },
            TableRow { values: vec![Cell::I64(-987654321)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::I32(42)] }, // Non-I64 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Int64);
        let int_array = array_ref.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();

        assert_eq!(int_array.len(), 4);
        assert_eq!(int_array.value(0), 123456789);
        assert_eq!(int_array.value(1), -987654321);
        assert!(int_array.is_null(2));
        assert!(int_array.is_null(3));
    }

    #[test]
    fn test_build_f32_array() {
        let rows = vec![
            TableRow { values: vec![Cell::F32(2.5)] },
            TableRow { values: vec![Cell::F32(-1.25)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::F64(3.0)] }, // Non-F32 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Float32);
        let float_array = array_ref.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();

        assert_eq!(float_array.len(), 4);
        assert_eq!(float_array.value(0), 2.5);
        assert_eq!(float_array.value(1), -1.25);
        assert!(float_array.is_null(2));
        assert!(float_array.is_null(3));
    }

    #[test]
    fn test_build_f64_array() {
        let rows = vec![
            TableRow { values: vec![Cell::F64(1.23456789)] },
            TableRow { values: vec![Cell::F64(-9.87654321)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::F32(2.5)] }, // Non-F64 becomes null
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Float64);
        let float_array = array_ref.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

        assert_eq!(float_array.len(), 4);
        assert_eq!(float_array.value(0), 1.23456789);
        assert_eq!(float_array.value(1), -9.87654321);
        assert!(float_array.is_null(2));
        assert!(float_array.is_null(3));
    }

    #[test]
    fn test_build_string_array() {
        let rows = vec![
            TableRow { values: vec![Cell::String("hello".to_string())] },
            TableRow { values: vec![Cell::Bool(true)] }, // Converted to string
            TableRow { values: vec![Cell::I32(42)] }, // Converted to string
            TableRow { values: vec![Cell::Null] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Utf8);
        let string_array = array_ref.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();

        assert_eq!(string_array.len(), 4);
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "true");
        assert_eq!(string_array.value(2), "42");
        assert!(string_array.is_null(3));
    }

    #[test]
    fn test_build_binary_array() {
        let test_bytes = vec![1, 2, 3, 4, 5];
        let rows = vec![
            TableRow { values: vec![Cell::Bytes(test_bytes.clone())] },
            TableRow { values: vec![Cell::Bytes(vec![])] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("not bytes".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::LargeBinary);
        let binary_array = array_ref.as_any().downcast_ref::<arrow::array::LargeBinaryArray>().unwrap();

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
            TableRow { values: vec![Cell::Date(test_date)] },
            TableRow { values: vec![Cell::Date(UNIX_EPOCH)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("2023-05-15".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Date32);
        let date_array = array_ref.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();

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
        let expected_micros = test_time.signed_duration_since(MIDNIGHT).num_microseconds().unwrap();
        
        let rows = vec![
            TableRow { values: vec![Cell::Time(test_time)] },
            TableRow { values: vec![Cell::Time(MIDNIGHT)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("12:30:45".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Time64(TimeUnit::Microsecond));
        let time_array = array_ref.as_any().downcast_ref::<arrow::array::Time64MicrosecondArray>().unwrap();

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
            TableRow { values: vec![Cell::Timestamp(test_ts)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("2001-09-09 01:46:40".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Timestamp(TimeUnit::Microsecond, None));
        let ts_array = array_ref.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();

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
            TableRow { values: vec![Cell::TimestampTz(test_ts)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String("2001-09-09T01:46:40Z".to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        let ts_array = array_ref.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();

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
            TableRow { values: vec![Cell::Uuid(test_uuid)] },
            TableRow { values: vec![Cell::Null] },
            TableRow { values: vec![Cell::String(test_uuid.to_string())] },
        ];

        let array_ref = build_array_for_field(&rows, 0, &DataType::FixedSizeBinary(UUID_BYTE_WIDTH));
        let uuid_array = array_ref.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>().unwrap();

        assert_eq!(uuid_array.len(), 3);
        assert_eq!(uuid_array.value(0), expected_bytes);
        assert!(uuid_array.is_null(1));
        assert!(uuid_array.is_null(2));
    }

    #[test]
    fn test_build_list_array_with_i64_elements() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::I64(vec![Some(1234567890), Some(-987654321)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::I64(vec![Some(42), None, Some(100)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Null)],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Int64);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 3);

        // First row: [1234567890, -987654321]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_i64 = first_list.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(first_list_i64.len(), 2);
        assert_eq!(first_list_i64.value(0), 1234567890);
        assert_eq!(first_list_i64.value(1), -987654321);

        // Second row: [42, null, 100]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_i64 = second_list.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(second_list_i64.len(), 3);
        assert_eq!(second_list_i64.value(0), 42);
        assert!(second_list_i64.is_null(1));
        assert_eq!(second_list_i64.value(2), 100);

        // Third row: null array
        assert!(list_array.is_null(2));
    }

    #[test]
    fn test_build_list_array_with_f32_elements() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::F32(vec![Some(1.5), Some(2.25), Some(-3.75)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::F32(vec![Some(0.0), None]))],
            },
            TableRow {
                values: vec![Cell::Null],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Float32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 3);

        // First row: [1.5, 2.25, -3.75]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_f32 = first_list.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
        assert_eq!(first_list_f32.len(), 3);
        assert_eq!(first_list_f32.value(0), 1.5);
        assert_eq!(first_list_f32.value(1), 2.25);
        assert_eq!(first_list_f32.value(2), -3.75);

        // Second row: [0.0, null]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_f32 = second_list.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
        assert_eq!(second_list_f32.len(), 2);
        assert_eq!(second_list_f32.value(0), 0.0);
        assert!(second_list_f32.is_null(1));

        // Third row: null cell
        assert!(list_array.is_null(2));
    }

    #[test]
    fn test_build_list_array_with_f64_elements() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::F64(vec![Some(1.23456789), Some(-9.87654321)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::F64(vec![None, Some(0.0)]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Float64);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 2);

        // First row: [1.23456789, -9.87654321]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_f64 = first_list.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(first_list_f64.len(), 2);
        assert_eq!(first_list_f64.value(0), 1.23456789);
        assert_eq!(first_list_f64.value(1), -9.87654321);

        // Second row: [null, 0.0]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_f64 = second_list.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(second_list_f64.len(), 2);
        assert!(second_list_f64.is_null(0));
        assert_eq!(second_list_f64.value(1), 0.0);
    }

    #[test]
    fn test_build_list_array_with_date32_elements() {
        use chrono::NaiveDate;
        let date1 = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Date(vec![Some(date1), Some(date2)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Date(vec![Some(UNIX_EPOCH), None]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Date32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 2);

        // First row: [date1, date2]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_date = first_list.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
        assert_eq!(first_list_date.len(), 2);
        assert_eq!(first_list_date.value(0), date1.signed_duration_since(UNIX_EPOCH).num_days() as i32);
        assert_eq!(first_list_date.value(1), date2.signed_duration_since(UNIX_EPOCH).num_days() as i32);

        // Second row: [epoch, null]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_date = second_list.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
        assert_eq!(second_list_date.len(), 2);
        assert_eq!(second_list_date.value(0), 0);
        assert!(second_list_date.is_null(1));
    }

    #[test]
    fn test_build_list_array_with_time64_elements() {
        use chrono::NaiveTime;
        let time1 = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let time2 = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Time(vec![Some(time1), Some(time2)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Time(vec![Some(MIDNIGHT), None]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Time64(TimeUnit::Microsecond));
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 2);

        // First row: [time1, time2]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_time = first_list.as_any().downcast_ref::<arrow::array::Time64MicrosecondArray>().unwrap();
        assert_eq!(first_list_time.len(), 2);
        assert_eq!(first_list_time.value(0), time1.signed_duration_since(MIDNIGHT).num_microseconds().unwrap());
        assert_eq!(first_list_time.value(1), time2.signed_duration_since(MIDNIGHT).num_microseconds().unwrap());

        // Second row: [midnight, null]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_time = second_list.as_any().downcast_ref::<arrow::array::Time64MicrosecondArray>().unwrap();
        assert_eq!(second_list_time.len(), 2);
        assert_eq!(second_list_time.value(0), 0);
        assert!(second_list_time.is_null(1));
    }

    #[test]
    fn test_build_list_array_with_timestamp_elements() {
        use chrono::DateTime;
        let ts1 = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let ts2 = DateTime::from_timestamp(1500000000, 0).unwrap().naive_utc();
        
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![Some(ts1), Some(ts2)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Timestamp(vec![None]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Timestamp(TimeUnit::Microsecond, None));
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 2);

        // First row: [ts1, ts2]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_ts = first_list.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        assert_eq!(first_list_ts.len(), 2);
        assert_eq!(first_list_ts.value(0), ts1.and_utc().timestamp_micros());
        assert_eq!(first_list_ts.value(1), ts2.and_utc().timestamp_micros());

        // Second row: [null]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_ts = second_list.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        assert_eq!(second_list_ts.len(), 1);
        assert!(second_list_ts.is_null(0));
    }

    #[test]
    fn test_i32_list_array_with_type_conversions() {
        let rows = vec![
            // Test i16 to i32 conversion
            TableRow {
                values: vec![Cell::Array(ArrayCell::I16(vec![Some(42), Some(-100), None]))],
            },
            // Test u32 to i32 conversion
            TableRow {
                values: vec![Cell::Array(ArrayCell::U32(vec![Some(123), Some(u32::MAX), None]))],
            },
            // Test mixed with direct i32
            TableRow {
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(999)]))],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 3);

        // First row: i16 to i32 [42, -100, null]
        let first_list = list_array.value(0);
        let first_list_i32 = first_list.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(first_list_i32.len(), 3);
        assert_eq!(first_list_i32.value(0), 42);
        assert_eq!(first_list_i32.value(1), -100);
        assert!(first_list_i32.is_null(2));

        // Second row: u32 to i32 [123, -1 (overflow), null]
        let second_list = list_array.value(1);
        let second_list_i32 = second_list.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(second_list_i32.len(), 3);
        assert_eq!(second_list_i32.value(0), 123);
        assert_eq!(second_list_i32.value(1), -1); // u32::MAX as i32
        assert!(second_list_i32.is_null(2));

        // Third row: direct i32 [999]
        let third_list = list_array.value(2);
        let third_list_i32 = third_list.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(third_list_i32.len(), 1);
        assert_eq!(third_list_i32.value(0), 999);
    }

    #[test]
    fn test_list_array_fallback_to_string() {
        // Test boolean array fallback in i32 context
        let boolean_rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false)]))],
            },
        ];

        let array_ref = build_list_array(&boolean_rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        
        // Should fall back to string representation
        let string_array = list_array.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(string_array.len() > 0);

        // Test f64 array fallback in i32 context
        let f64_rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::F64(vec![Some(1.23), Some(4.56)]))],
            },
        ];

        let array_ref = build_list_array(&f64_rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        
        // Should fall back to string representation
        let string_array = list_array.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(string_array.len() > 0);

        // Test string array fallback in boolean context
        let string_rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::String(vec![Some("hello".to_string()), Some("world".to_string())]))],
            },
        ];

        let array_ref = build_list_array(&string_rows, 0, &DataType::Boolean);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        
        // Should fall back to string representation
        let string_array = list_array.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(string_array.len() > 0);
    }

    #[test]
    fn test_list_array_with_non_array_cells() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)]))],
            },
            TableRow {
                values: vec![Cell::String("not an array".to_string())],
            },
            TableRow {
                values: vec![Cell::Null],
            },
            TableRow {
                values: vec![Cell::I32(42)],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 4);

        // First row: valid array [1, 2]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_list_i32 = first_list.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(first_list_i32.len(), 2);

        // Second row: non-array cell becomes null
        assert!(list_array.is_null(1));

        // Third row: null cell becomes null
        assert!(list_array.is_null(2));

        // Fourth row: non-array cell becomes null
        assert!(list_array.is_null(3));
    }

    #[test]
    fn test_list_array_with_null_arrays() {
        let rows = vec![
            TableRow {
                values: vec![Cell::Array(ArrayCell::Null)],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::I32(vec![Some(42)]))],
            },
            TableRow {
                values: vec![Cell::Array(ArrayCell::Null)],
            },
        ];

        let array_ref = build_list_array(&rows, 0, &DataType::Int32);
        let list_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list_array.len(), 3);

        // First row: null array
        assert!(list_array.is_null(0));

        // Second row: valid array [42]
        assert!(!list_array.is_null(1));
        let second_list = list_array.value(1);
        let second_list_i32 = second_list.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(second_list_i32.len(), 1);
        assert_eq!(second_list_i32.value(0), 42);

        // Third row: null array
        assert!(list_array.is_null(2));
    }

    #[test]
    fn test_append_array_cell_as_strings_comprehensive() {
        use chrono::{NaiveDate, NaiveTime, DateTime};
        use uuid::Uuid;
        
        // Test that the append function works with all ArrayCell variants
        // by creating a complete list array and verifying the final results
        
        let mut list_builder = ListBuilder::new(StringBuilder::new());
        
        // Test different array cell types by building complete arrays
        // and checking that they can be processed without panicking
        
        // Boolean array
        let bool_array = ArrayCell::Bool(vec![Some(true), Some(false), None]);
        append_array_cell_as_strings(&mut list_builder, &bool_array);
        list_builder.append(true);
        
        // Integer arrays
        let i16_array = ArrayCell::I16(vec![Some(42), None]);
        append_array_cell_as_strings(&mut list_builder, &i16_array);
        list_builder.append(true);
        
        let i32_array = ArrayCell::I32(vec![Some(123)]);
        append_array_cell_as_strings(&mut list_builder, &i32_array);
        list_builder.append(true);
        
        let i64_array = ArrayCell::I64(vec![Some(1234567890)]);
        append_array_cell_as_strings(&mut list_builder, &i64_array);
        list_builder.append(true);
        
        let u32_array = ArrayCell::U32(vec![Some(789)]);
        append_array_cell_as_strings(&mut list_builder, &u32_array);
        list_builder.append(true);
        
        // Float arrays
        let f32_array = ArrayCell::F32(vec![Some(1.5)]);
        append_array_cell_as_strings(&mut list_builder, &f32_array);
        list_builder.append(true);
        
        let f64_array = ArrayCell::F64(vec![Some(2.5)]);
        append_array_cell_as_strings(&mut list_builder, &f64_array);
        list_builder.append(true);
        
        // String array
        let string_array = ArrayCell::String(vec![Some("hello".to_string())]);
        append_array_cell_as_strings(&mut list_builder, &string_array);
        list_builder.append(true);
        
        // Temporal arrays
        let date_array = ArrayCell::Date(vec![Some(NaiveDate::from_ymd_opt(2023, 5, 15).unwrap())]);
        append_array_cell_as_strings(&mut list_builder, &date_array);
        list_builder.append(true);
        
        let time_array = ArrayCell::Time(vec![Some(NaiveTime::from_hms_opt(12, 30, 45).unwrap())]);
        append_array_cell_as_strings(&mut list_builder, &time_array);
        list_builder.append(true);
        
        let ts_array = ArrayCell::Timestamp(vec![Some(DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc())]);
        append_array_cell_as_strings(&mut list_builder, &ts_array);
        list_builder.append(true);
        
        let ts_tz_array = ArrayCell::TimestampTz(vec![Some(DateTime::from_timestamp(1000000000, 0).unwrap())]);
        append_array_cell_as_strings(&mut list_builder, &ts_tz_array);
        list_builder.append(true);
        
        // Other types
        let uuid_array = ArrayCell::Uuid(vec![Some(Uuid::new_v4())]);
        append_array_cell_as_strings(&mut list_builder, &uuid_array);
        list_builder.append(true);
        
        let json_array = ArrayCell::Json(vec![Some(serde_json::json!({"key": "value"}))]);
        append_array_cell_as_strings(&mut list_builder, &json_array);
        list_builder.append(true);
        
        let bytes_array = ArrayCell::Bytes(vec![Some(vec![72, 101, 108, 108, 111])]);
        append_array_cell_as_strings(&mut list_builder, &bytes_array);
        list_builder.append(true);
        
        // Null array (should not add anything)
        let null_array = ArrayCell::Null;
        append_array_cell_as_strings(&mut list_builder, &null_array);
        list_builder.append(true);
        
        // Build the final array
        let list_array = list_builder.finish();
        
        // We should have one list entry for each append(true) call
        assert_eq!(list_array.len(), 16);
        
        // Verify that we can access the string values in the first list (boolean)
        let first_list = list_array.value(0);
        let string_values = first_list.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(string_values.len(), 3);
        assert_eq!(string_values.value(0), "true");
        assert_eq!(string_values.value(1), "false");
        assert!(string_values.is_null(2));
        
        // Verify the null array case (last one) is empty
        let null_list = list_array.value(15);
        let null_string_values = null_list.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(null_string_values.len(), 0);
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
        let id_array = batch.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(id_array.value(0), 42);
        assert_eq!(id_array.value(1), 100);
        
        let name_array = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(name_array.value(0), "hello");
        assert_eq!(name_array.value(1), "world");
        
        let active_array = batch.column(2).as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
        assert!(active_array.value(0));
        assert!(!active_array.value(1));
    }

    #[test]
    fn test_rows_to_record_batch_with_nulls() {
        use arrow::datatypes::{Field, Schema};
        
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(42),
                    Cell::Null,
                ],
            },
            TableRow {
                values: vec![
                    Cell::Null,
                    Cell::String("test".to_string()),
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();
        
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        
        let id_array = batch.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(id_array.value(0), 42);
        assert!(id_array.is_null(1));
        
        let name_array = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(name_array.is_null(0));
        assert_eq!(name_array.value(1), "test");
    }

    #[test]
    fn test_rows_to_record_batch_temporal_types() {
        use arrow::datatypes::{Field, Schema};
        use chrono::{NaiveDate, NaiveTime, DateTime};
        
        let test_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let test_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let test_ts = DateTime::from_timestamp(1000000000, 0).unwrap().naive_utc();
        let test_ts_tz = DateTime::from_timestamp(1000000000, 0).unwrap();
        
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::Date(test_date),
                    Cell::Time(test_time),
                    Cell::Timestamp(test_ts),
                    Cell::TimestampTz(test_ts_tz),
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("date_col", DataType::Date32, false),
            Field::new("time_col", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new("ts_col", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("ts_tz_col", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();
        
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
        
        let date_array = batch.column(0).as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
        assert_eq!(date_array.value(0), test_date.signed_duration_since(UNIX_EPOCH).num_days() as i32);
        
        let time_array = batch.column(1).as_any().downcast_ref::<arrow::array::Time64MicrosecondArray>().unwrap();
        assert_eq!(time_array.value(0), test_time.signed_duration_since(MIDNIGHT).num_microseconds().unwrap());
        
        let ts_array = batch.column(2).as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        assert_eq!(ts_array.value(0), test_ts.and_utc().timestamp_micros());
        
        let ts_tz_array = batch.column(3).as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        assert_eq!(ts_tz_array.value(0), test_ts_tz.timestamp_micros());
    }

    #[test]
    fn test_rows_to_record_batch_with_lists() {
        use arrow::datatypes::{Field, Schema};
        
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(1),
                    Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), None])),
                    Cell::Array(ArrayCell::String(vec![Some("a".to_string()), Some("b".to_string())])),
                ],
            },
            TableRow {
                values: vec![
                    Cell::I32(2),
                    Cell::Array(ArrayCell::I32(vec![Some(30)])),
                    Cell::Array(ArrayCell::Null),
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("numbers", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), true),
            Field::new("strings", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();
        
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        
        // Verify the list arrays
        let numbers_list = batch.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(numbers_list.len(), 2);
        
        // First row list: [10, 20, null]
        let first_numbers = numbers_list.value(0);
        let first_numbers_i32 = first_numbers.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(first_numbers_i32.len(), 3);
        assert_eq!(first_numbers_i32.value(0), 10);
        assert_eq!(first_numbers_i32.value(1), 20);
        assert!(first_numbers_i32.is_null(2));
        
        // Second row list: [30]
        let second_numbers = numbers_list.value(1);
        let second_numbers_i32 = second_numbers.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(second_numbers_i32.len(), 1);
        assert_eq!(second_numbers_i32.value(0), 30);
        
        // String list
        let strings_list = batch.column(2).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(strings_list.len(), 2);
        
        // First row strings: ["a", "b"]
        let first_strings = strings_list.value(0);
        let first_strings_str = first_strings.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(first_strings_str.len(), 2);
        assert_eq!(first_strings_str.value(0), "a");
        assert_eq!(first_strings_str.value(1), "b");
        
        // Second row strings: null list
        assert!(strings_list.is_null(1));
    }

    #[test]
    fn test_rows_to_record_batch_binary_and_uuid() {
        use arrow::datatypes::{Field, Schema};
        use uuid::Uuid;
        
        let test_bytes = vec![1, 2, 3, 4, 5];
        let test_uuid = Uuid::new_v4();
        
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::Bytes(test_bytes.clone()),
                    Cell::Uuid(test_uuid),
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("data", DataType::LargeBinary, false),
            Field::new("uuid", DataType::FixedSizeBinary(16), false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();
        
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
        
        let bytes_array = batch.column(0).as_any().downcast_ref::<arrow::array::LargeBinaryArray>().unwrap();
        assert_eq!(bytes_array.value(0), test_bytes);
        
        let uuid_array = batch.column(1).as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>().unwrap();
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
                values: vec![
                    Cell::I32(100),
                    Cell::Null,
                ],
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
        
        let id_array = batch.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(id_array.value(0), 42);
        assert_eq!(id_array.value(1), 100);
        
        let metadata_array = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        // JSON should be converted to string representation
        assert_eq!(metadata_array.value(0), r#"{"key":"value","number":123}"#);
        assert!(metadata_array.is_null(1));
    }

    #[test]
    fn test_rows_to_record_batch_type_mismatches() {
        use arrow::datatypes::{Field, Schema};
        
        // Test various type conversions that should work
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(123),                  // Int32 in Int32 field (direct match)
                    Cell::Bool(true),                // Bool in String field (conversion)
                    Cell::F64(456.78),               // F64 in Float64 field (direct match)
                ],
            },
        ];

        let schema = Schema::new(vec![
            Field::new("number", DataType::Int32, false),
            Field::new("bool_as_string", DataType::Utf8, false),
            Field::new("float_val", DataType::Float64, false),
        ]);

        let batch = rows_to_record_batch(&rows, schema).unwrap();
        
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
        
        let number_array = batch.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(number_array.value(0), 123);
        
        let string_array = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(string_array.value(0), "true"); // Bool true converted to string
        
        let float_array = batch.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(float_array.value(0), 456.78); // F64 in Float64 field
    }

    #[test]
    fn test_rows_to_record_batch_unsupported_schema_types() {
        use arrow::datatypes::{Field, Schema};
        
        // Test with schema types that might not be directly supported
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(1),
                    Cell::String("test".to_string()),
                ],
            },
        ];

        // Use schema with more exotic Arrow types
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt8, false), // UInt8 is not directly implemented
            Field::new("name", DataType::LargeUtf8, false), // LargeUtf8 instead of Utf8
        ]);

        // This should return an error for unsupported types rather than panicking
        let result = rows_to_record_batch(&rows, schema);
        
        // For unsupported schema types, the function should return an error
        assert!(result.is_err(), "Expected error for unsupported schema types");
        
        // Verify the error message indicates type mismatch
        let error = result.unwrap_err();
        assert!(error.to_string().contains("column types must match"), 
               "Expected column type mismatch error, got: {}", error);
    }

    #[test]
    fn test_rows_to_record_batch_schema_mismatch_length() {
        use arrow::datatypes::{Field, Schema};
        
        // Test what happens when row has different number of columns than schema
        let rows = vec![
            TableRow {
                values: vec![
                    Cell::I32(1),
                    Cell::String("test".to_string()),
                    Cell::Bool(true), // Extra column not in schema
                ],
            },
        ];

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
            },
            Err(_) => {
                // Error is also acceptable for schema mismatch
            },
        }
    }
}
