use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, ArrowPrimitiveType, BooleanBuilder, LargeBinaryBuilder, PrimitiveBuilder,
        RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Schema,
        Time64MicrosecondType, TimeUnit, TimestampMicrosecondType, UInt32Type,
    },
    error::ArrowError,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{NaiveDate, NaiveTime};
use etl::types::{Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableRow};

const UNIX_EPOCH: NaiveDate =
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch is a valid date");

const MIDNIGHT: NaiveTime = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is a valid time");

/// Converts a slice of [`TableRow`]s to an arrow [`RecordBatch`]`.
pub fn rows_to_record_batch(rows: &[TableRow], schema: Schema) -> Result<RecordBatch, ArrowError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (field_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_for_field(rows, field_idx, field.data_type());
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

    Ok(batch)
}

/// Builds an [`ArrayRef`] from the [`TableRow`]s for a field specified by the `field_idx` .
fn build_array_for_field(rows: &[TableRow], field_idx: usize, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => build_boolean_array(rows, field_idx),
        DataType::Int16 => build_primitive_array::<Int16Type, _>(rows, field_idx, cell_to_i16),
        DataType::Int32 => build_primitive_array::<Int32Type, _>(rows, field_idx, cell_to_i32),
        DataType::Int64 => build_primitive_array::<Int64Type, _>(rows, field_idx, cell_to_i64),
        DataType::UInt32 => build_primitive_array::<UInt32Type, _>(rows, field_idx, cell_to_u32),
        DataType::Float32 => build_primitive_array::<Float32Type, _>(rows, field_idx, cell_to_f32),
        DataType::Float64 => build_primitive_array::<Float64Type, _>(rows, field_idx, cell_to_f64),
        DataType::Utf8 => build_string_array(rows, field_idx),
        // DataType::LargeUtf8 => build_string_array(rows, field_idx),
        // DataType::Binary => build_binary_array(rows, field_idx),
        DataType::LargeBinary => build_binary_array(rows, field_idx),
        // DataType::Date32 => build_date32_array(rows, field_idx),
        DataType::Date32 => build_primitive_array::<Date32Type, _>(rows, field_idx, cell_to_date32),
        // DataType::Time64(TimeUnit::Microsecond) => build_time64_array(rows, field_idx),
        DataType::Time64(TimeUnit::Microsecond) => {
            build_primitive_array::<Time64MicrosecondType, _>(rows, field_idx, cell_to_time64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            build_timestamptz_array(rows, field_idx, tz)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_primitive_array::<TimestampMicrosecondType, _>(rows, field_idx, cell_to_timestamp)
        }
        _ => build_string_array(rows, field_idx),
    }
}

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
// impl_array_builder!(build_date32_array, Date32Builder, cell_to_date32);
// impl_array_builder!(build_time64_array, Time64MicrosecondBuilder, cell_to_time64);

fn build_timestamptz_array(rows: &[TableRow], field_idx: usize, tz: &str) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone(tz);

    for row in rows {
        let arrow_value = cell_to_timestamptz(&row.values[field_idx]);
        builder.append_option(arrow_value);
    }

    Arc::new(builder.finish())
}

macro_rules! impl_cell_converter {
    ($fn_name:ident, $return_type:ty, $($pattern:pat => $expr:expr),*) => {
        fn $fn_name(cell: &Cell) -> Option<$return_type> {
            match cell {
                $($pattern => $expr,)*
                _ => None,
            }
        }
    };
}

impl_cell_converter!(
    cell_to_bool, bool,
    Cell::Bool(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_i16, i16,
    Cell::I16(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_i32, i32,
    Cell::I32(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_i64, i64,
    Cell::I64(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_u32, u32,
    Cell::U32(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_f32, f32,
    Cell::F32(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_f64, f64,
    Cell::F64(v) => Some(*v)
);

impl_cell_converter!(
    cell_to_bytes, Vec<u8>,
    Cell::Bytes(v) => Some(v.clone())
);

impl_cell_converter!(
    cell_to_date32, i32,
    Cell::Date(date) => Some(date.signed_duration_since(UNIX_EPOCH).num_days() as i32)
);

impl_cell_converter!(
    cell_to_time64, i64,
    Cell::Time(time) => time.signed_duration_since(MIDNIGHT).num_microseconds()
);

impl_cell_converter!(
    cell_to_timestamp, i64,
    // Cell::Time(time) => time.signed_duration_since(MIDNIGHT).num_microseconds()
    Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros())
);
impl_cell_converter!(
    cell_to_timestamptz, i64,
    // Cell::Time(time) => time.signed_duration_since(MIDNIGHT).num_microseconds()
    // Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros())
    Cell::TimestampTz(ts) => Some(ts.timestamp_micros())
);

// TODO: reduce allocations in this method
/// Converts a Cell to its string representation for Arrow.
pub fn cell_to_string(cell: &Cell) -> Option<String> {
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

// /// Extracts time as microseconds since midnight.
// pub fn cell_to_time64_micros(cell: &Cell) -> Option<i64> {
//     match cell {
//         Cell::Time(time) => {
//             let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
//             Some(time.signed_duration_since(midnight).num_microseconds()?)
//         }
//         _ => None,
//     }
// }

// /// Extracts bytes from Cell.
// pub fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
//     match cell {
//         Cell::Bytes(b) => Some(b.clone()),
//         _ => None,
//     }
// }

// pub fn cell_to_bool(cell: &Cell) -> Option<bool> {
//     match cell {
//         Cell::Bool(b) => Some(*b),
//         _ => None,
//     }
// }

// fn cell_to_i16(cell: &Cell) -> Option<i16> {
//     match cell {
//         Cell::I16(value) => Some(*value),
//         _ => None,
//     }
// }

// fn cell_to_i32(cell: &Cell) -> Option<i32> {
//     match cell {
//         Cell::I32(value) => Some(*value),
//         _ => None,
//     }
// }

// /// Builds an int16 array from cell values.
// fn build_int16_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Int16Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_i16(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds an int32 array from cell values.
// fn build_int32_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Int32Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_i32(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds an int64 array from cell values.
// fn build_int64_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Int64Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_i64(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a uint32 array from cell values.
// fn build_uint32_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = UInt32Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_u32(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a float32 array from cell values.
// fn build_float32_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Float32Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_f32(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a float64 array from cell values.
// fn build_float64_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Float64Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_f64(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a UTF8 array from cell values.
// fn build_utf8_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = StringBuilder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_string(&row.values[field_idx]);
//             builder.append_option(value.as_deref());
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a binary array from cell values.
// fn build_binary_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     // For now, always use LargeBinaryBuilder as it can handle any size
//     let mut builder = LargeBinaryBuilder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_bytes(&row.values[field_idx]);
//             builder.append_option(value.as_deref());
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a date32 array from cell values.
// fn build_date32_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Date32Builder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_date32(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a time64 array from cell values.
// fn build_time64_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     let mut builder = Time64MicrosecondBuilder::new();

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_time64_micros(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Builds a timestamp array from cell values.
// fn build_timestamp_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
//     // Create timestamp array with +00:00 timezone to match schema
//     let mut builder = TimestampMicrosecondBuilder::new().with_timezone("+00:00");

//     for row in rows {
//         if field_idx < row.values.len() {
//             let value = CellToArrowConverter::cell_to_timestamp_micros(&row.values[field_idx]);
//             builder.append_option(value);
//         } else {
//             builder.append_null();
//         }
//     }

//     Arc::new(builder.finish())
// }

// /// Converts Cell values to Arrow array builders.
// pub struct CellToArrowConverter;

// impl CellToArrowConverter {
// /// Extracts boolean value from Cell.
// pub fn cell_to_bool(cell: &Cell) -> Option<bool> {
//     match cell {
//         Cell::Bool(b) => Some(*b),
//         _ => None,
//     }
// }

// /// Extracts i16 value from Cell.
// pub fn cell_to_i16(cell: &Cell) -> Option<i16> {
//     match cell {
//         Cell::I16(i) => Some(*i),
//         _ => None,
//     }
// }

// /// Extracts i32 value from Cell.
// pub fn cell_to_i32(cell: &Cell) -> Option<i32> {
//     match cell {
//         Cell::I32(i) => Some(*i),
//         Cell::I16(i) => Some(*i as i32),
//         _ => None,
//     }
// }

// /// Extracts i64 value from Cell.
// pub fn cell_to_i64(cell: &Cell) -> Option<i64> {
//     match cell {
//         Cell::I64(i) => Some(*i),
//         Cell::I32(i) => Some(*i as i64),
//         Cell::I16(i) => Some(*i as i64),
//         _ => None,
//     }
// }

// /// Extracts f32 value from Cell.
// pub fn cell_to_f32(cell: &Cell) -> Option<f32> {
//     match cell {
//         Cell::F32(f) => Some(*f),
//         _ => None,
//     }
// }

// /// Extracts f64 value from Cell.
// pub fn cell_to_f64(cell: &Cell) -> Option<f64> {
//     match cell {
//         Cell::F64(f) => Some(*f),
//         Cell::F32(f) => Some(*f as f64),
//         _ => None,
//     }
// }

// /// Extracts date as days since epoch.
// pub fn cell_to_date32(cell: &Cell) -> Option<i32> {
//     match cell {
//         Cell::Date(date) => {
//             let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
//             Some(date.signed_duration_since(epoch).num_days() as i32)
//         }
//         _ => None,
//     }
// }

// /// Extracts time as microseconds since midnight.
// pub fn cell_to_time64_micros(cell: &Cell) -> Option<i64> {
//     match cell {
//         Cell::Time(time) => {
//             let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
//             Some(time.signed_duration_since(midnight).num_microseconds()?)
//         }
//         _ => None,
//     }
// }

// /// Extracts timestamp as microseconds since epoch.
// pub fn cell_to_timestamp_micros(cell: &Cell) -> Option<i64> {
//     match cell {
//         Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros()),
//         Cell::TimestampTz(ts) => Some(ts.timestamp_micros()),
//         _ => None,
//     }
// }

// /// Extracts bytes from Cell.
// pub fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
//     match cell {
//         Cell::Bytes(b) => Some(b.clone()),
//         _ => None,
//     }
// }

// /// Extracts u32 value from Cell.
// pub fn cell_to_u32(cell: &Cell) -> Option<u32> {
//     match cell {
//         Cell::U32(u) => Some(*u),
//         Cell::I32(i) if *i >= 0 => Some(*i as u32),
//         _ => None,
//     }
// }
// }
