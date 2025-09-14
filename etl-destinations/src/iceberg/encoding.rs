use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, ArrowPrimitiveType, BooleanBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
        PrimitiveBuilder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
        Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    },
    error::ArrowError,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{NaiveDate, NaiveTime};
use etl::types::{Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableRow};

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

// TODO: reduce allocations in this method
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
