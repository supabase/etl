use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, ArrowPrimitiveType, BooleanBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
        PrimitiveBuilder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
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

const UUID_BYTE_WIDTH: i32 = 16;

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
        DataType::Int32 => build_primitive_array::<Int32Type, _>(rows, field_idx, cell_to_i32),
        DataType::Int64 => build_primitive_array::<Int64Type, _>(rows, field_idx, cell_to_i64),
        DataType::UInt32 => build_primitive_array::<UInt32Type, _>(rows, field_idx, cell_to_u32),
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

fn build_timestamptz_array(rows: &[TableRow], field_idx: usize, tz: &str) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone(tz);

    for row in rows {
        let arrow_value = cell_to_timestamptz(&row.values[field_idx]);
        builder.append_option(arrow_value);
    }

    Arc::new(builder.finish())
}

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
    cell_to_i32, i32,
    Cell::I16(v) => Some(*v as i32),
    Cell::I32(v) => Some(*v),
    Cell::U32(v) => Some(*v as i32)
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
    Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros())
);

impl_cell_converter!(
    cell_to_timestamptz, i64,
    Cell::TimestampTz(ts) => Some(ts.timestamp_micros())
);

fn cell_to_uuid(cell: &Cell) -> Option<&[u8; UUID_BYTE_WIDTH as usize]> {
    match cell {
        Cell::Uuid(value) => Some(value.as_bytes()),
        _ => None,
    }
}

// TODO: reduce allocations in this method
/// Converts a Cell to its string representation for Arrow.
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
