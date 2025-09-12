use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder, RecordBatch,
        StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt32Builder,
    },
    datatypes::{DataType, Schema, TimeUnit},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{NaiveDate, NaiveTime};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, TableRow},
};

/// Converts a vector of table rows to an Arrow RecordBatch.
pub fn rows_to_record_batch(rows: &[TableRow], schema: &Schema) -> EtlResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Build arrays for each column
    for (field_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_for_field(rows, field_idx, field.data_type())?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationError,
            "Failed to create Arrow RecordBatch",
            e.to_string()
        )
    })?;

    Ok(batch)
}

/// Builds an Arrow array for a specific field from the table rows.
fn build_array_for_field(
    rows: &[TableRow],
    field_idx: usize,
    data_type: &DataType,
) -> EtlResult<ArrayRef> {
    match data_type {
        DataType::Boolean => build_boolean_array(rows, field_idx),
        DataType::Int16 => build_int16_array(rows, field_idx),
        DataType::Int32 => build_int32_array(rows, field_idx),
        DataType::Int64 => build_int64_array(rows, field_idx),
        DataType::UInt32 => build_uint32_array(rows, field_idx),
        DataType::Float32 => build_float32_array(rows, field_idx),
        DataType::Float64 => build_float64_array(rows, field_idx),
        DataType::Utf8 => build_utf8_array(rows, field_idx),
        DataType::LargeUtf8 => build_string_array(rows, field_idx),
        DataType::LargeBinary => build_binary_array(rows, field_idx),
        DataType::Date32 => build_date32_array(rows, field_idx),
        DataType::Time64(TimeUnit::Microsecond) => build_time64_array(rows, field_idx),
        DataType::Timestamp(TimeUnit::Microsecond, _) => build_timestamp_array(rows, field_idx),
        _ => build_string_array(rows, field_idx),
    }
}

/// Builds a boolean array from cell values.
fn build_boolean_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = BooleanBuilder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_bool(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds an int16 array from cell values.
fn build_int16_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Int16Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_i16(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds an int32 array from cell values.
fn build_int32_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Int32Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_i32(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds an int64 array from cell values.
fn build_int64_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Int64Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_i64(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a uint32 array from cell values.
fn build_uint32_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = UInt32Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = match &row.values[field_idx] {
                Cell::U32(u) => Some(*u),
                Cell::I32(i) if *i >= 0 => Some(*i as u32),
                _ => None,
            };
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a float32 array from cell values.
fn build_float32_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Float32Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_f32(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a float64 array from cell values.
fn build_float64_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Float64Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_f64(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a UTF8 array from cell values.
fn build_utf8_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = StringBuilder::new();
    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_string(&row.values[field_idx]);
            builder.append_option(value.as_deref());
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Builds a string array from cell values.
fn build_string_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = LargeStringBuilder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_string(&row.values[field_idx]);
            builder.append_option(value.as_deref());
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a binary array from cell values.
fn build_binary_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = LargeBinaryBuilder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_bytes(&row.values[field_idx]);
            builder.append_option(value.as_deref());
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a date32 array from cell values.
fn build_date32_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Date32Builder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_date32(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a time64 array from cell values.
fn build_time64_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    let mut builder = Time64MicrosecondBuilder::new();

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_time64_micros(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Builds a timestamp array from cell values.
fn build_timestamp_array(rows: &[TableRow], field_idx: usize) -> EtlResult<ArrayRef> {
    // Create timestamp array with +00:00 timezone to match schema
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone("+00:00");

    for row in rows {
        if field_idx < row.values.len() {
            let value = CellToArrowConverter::cell_to_timestamp_micros(&row.values[field_idx]);
            builder.append_option(value);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Converts Cell values to Arrow array builders.
pub struct CellToArrowConverter;

impl CellToArrowConverter {
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
            Cell::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Cell::Time(t) => Some(t.format("%H:%M:%S%.6f").to_string()),
            Cell::Timestamp(ts) => Some(ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            Cell::TimestampTz(ts) => Some(ts.to_rfc3339()),
            Cell::Uuid(u) => Some(u.to_string()),
            Cell::Json(j) => Some(j.to_string()),
            Cell::Bytes(b) => Some(BASE64_STANDARD.encode(b)),
            Cell::Array(arr) => Some(format!("{:?}", arr)), // Simple debug representation
        }
    }

    /// Extracts boolean value from Cell.
    pub fn cell_to_bool(cell: &Cell) -> Option<bool> {
        match cell {
            Cell::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Extracts i16 value from Cell.
    pub fn cell_to_i16(cell: &Cell) -> Option<i16> {
        match cell {
            Cell::I16(i) => Some(*i),
            _ => None,
        }
    }

    /// Extracts i32 value from Cell.
    pub fn cell_to_i32(cell: &Cell) -> Option<i32> {
        match cell {
            Cell::I32(i) => Some(*i),
            Cell::I16(i) => Some(*i as i32),
            _ => None,
        }
    }

    /// Extracts i64 value from Cell.
    pub fn cell_to_i64(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::I64(i) => Some(*i),
            Cell::I32(i) => Some(*i as i64),
            Cell::I16(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Extracts f32 value from Cell.
    pub fn cell_to_f32(cell: &Cell) -> Option<f32> {
        match cell {
            Cell::F32(f) => Some(*f),
            _ => None,
        }
    }

    /// Extracts f64 value from Cell.
    pub fn cell_to_f64(cell: &Cell) -> Option<f64> {
        match cell {
            Cell::F64(f) => Some(*f),
            Cell::F32(f) => Some(*f as f64),
            _ => None,
        }
    }

    /// Extracts date as days since epoch.
    pub fn cell_to_date32(cell: &Cell) -> Option<i32> {
        match cell {
            Cell::Date(date) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                Some(date.signed_duration_since(epoch).num_days() as i32)
            }
            _ => None,
        }
    }

    /// Extracts time as microseconds since midnight.
    pub fn cell_to_time64_micros(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::Time(time) => {
                let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
                Some(time.signed_duration_since(midnight).num_microseconds()?)
            }
            _ => None,
        }
    }

    /// Extracts timestamp as microseconds since epoch.
    pub fn cell_to_timestamp_micros(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::Timestamp(ts) => Some(ts.and_utc().timestamp_micros()),
            Cell::TimestampTz(ts) => Some(ts.timestamp_micros()),
            _ => None,
        }
    }

    /// Extracts bytes from Cell.
    pub fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
        match cell {
            Cell::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}
