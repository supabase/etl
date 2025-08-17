//! Data encoding utilities for converting table rows to Arrow RecordBatch.

use crate::iceberg::schema::{CellToArrowConverter, SchemaMapper};
use etl::types::{Cell, TableRow};
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::{etl_error};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder,
        TimestampMicrosecondBuilder, Time64MicrosecondBuilder, UInt32Builder,
    },
    datatypes::{DataType, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;
use tracing::{debug, warn};

/// Batches table rows based on size constraints to optimize streaming performance.
///
/// Returns slices into the original row data, avoiding expensive cloning operations
/// for better memory efficiency. Respects both row count and byte size limits.
pub fn batch_rows(
    rows: &[TableRow],
    max_batch_size: usize,
    max_batch_size_bytes: usize,
) -> Vec<&[TableRow]> {
    if rows.is_empty() {
        return vec![];
    }

    let mut batches = Vec::new();
    let mut start_idx = 0;
    let mut current_size_bytes = 0;
    let mut current_batch_len = 0;

    for (idx, row) in rows.iter().enumerate() {
        let row_size = estimate_row_size(row);
        
        // Check if adding this row would exceed limits
        if current_batch_len > 0 && 
           (current_batch_len >= max_batch_size || 
            current_size_bytes + row_size > max_batch_size_bytes) {
            // Add current batch slice
            batches.push(&rows[start_idx..start_idx + current_batch_len]);
            start_idx = idx;
            current_size_bytes = 0;
            current_batch_len = 0;
        }

        current_batch_len += 1;
        current_size_bytes += row_size;
    }

    // Add final batch if not empty
    if current_batch_len > 0 {
        batches.push(&rows[start_idx..start_idx + current_batch_len]);
    }

    debug!(
        total_rows = rows.len(),
        num_batches = batches.len(),
        "Batched rows for streaming"
    );

    batches
}

/// Converts a vector of table rows to an Arrow RecordBatch.
pub fn rows_to_record_batch(
    rows: &[TableRow],
    schema: &ArrowSchema,
    _schema_mapper: &SchemaMapper,
) -> EtlResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    debug!(
        row_count = rows.len(),
        column_count = schema.fields().len(),
        "Converting rows to Arrow RecordBatch"
    );

    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Build arrays for each column
    for (field_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_for_field(rows, field_idx, field.data_type())?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| etl_error!(
            ErrorKind::DestinationError,
            "Failed to create Arrow RecordBatch",
            e.to_string()
        ))?;

    debug!(
        rows = batch.num_rows(),
        columns = batch.num_columns(),
        "Successfully created Arrow RecordBatch"
    );

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
        DataType::LargeUtf8 => build_string_array(rows, field_idx),
        DataType::LargeBinary => build_binary_array(rows, field_idx),
        DataType::Date32 => build_date32_array(rows, field_idx),
        DataType::Time64(TimeUnit::Microsecond) => build_time64_array(rows, field_idx),
        DataType::Timestamp(TimeUnit::Microsecond, _) => build_timestamp_array(rows, field_idx),
        _ => {
            warn!(
                data_type = ?data_type,
                "Unsupported data type, converting to string"
            );
            build_string_array(rows, field_idx)
        }
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
    let mut builder = TimestampMicrosecondBuilder::new();

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

/// Estimates the memory size of a TableRow for batching decisions.
pub fn estimate_row_size(row: &TableRow) -> usize {
    let mut size = std::mem::size_of::<TableRow>();
    
    for cell in &row.values {
        size += estimate_cell_size(cell);
    }
    
    size
}

/// Estimates the memory size of a Cell.
fn estimate_cell_size(cell: &Cell) -> usize {
    match cell {
        Cell::Null => 1, // Null has at least some size for the discriminant
        Cell::Bool(_) => std::mem::size_of::<bool>(),
        Cell::String(s) => s.len() + std::mem::size_of::<String>(),
        Cell::I16(_) => std::mem::size_of::<i16>(),
        Cell::I32(_) => std::mem::size_of::<i32>(),
        Cell::U32(_) => std::mem::size_of::<u32>(),
        Cell::I64(_) => std::mem::size_of::<i64>(),
        Cell::F32(_) => std::mem::size_of::<f32>(),
        Cell::F64(_) => std::mem::size_of::<f64>(),
        Cell::Numeric(n) => n.to_string().len() + std::mem::size_of::<String>(),
        Cell::Date(_) => std::mem::size_of::<chrono::NaiveDate>(),
        Cell::Time(_) => std::mem::size_of::<chrono::NaiveTime>(),
        Cell::TimeStamp(_) => std::mem::size_of::<chrono::NaiveDateTime>(),
        Cell::TimeStampTz(_) => std::mem::size_of::<chrono::DateTime<chrono::Utc>>(),
        Cell::Uuid(_) => std::mem::size_of::<uuid::Uuid>(),
        Cell::Json(j) => j.to_string().len() + std::mem::size_of::<serde_json::Value>(),
        Cell::Bytes(b) => b.len() + std::mem::size_of::<Vec<u8>>(),
        Cell::Array(arr) => {
            // Estimate array size (simplified)
            std::mem::size_of::<etl::types::ArrayCell>() + 
            match arr {
                etl::types::ArrayCell::String(vec) => {
                    vec.iter().map(|opt| opt.as_ref().map(|s| s.len()).unwrap_or(0)).sum::<usize>()
                }
                _ => 100, // Conservative estimate for other array types
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use etl::types::{Cell, TableRow};

    fn create_test_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, true),
        ])
    }

    fn create_test_rows() -> Vec<TableRow> {
        vec![
            TableRow {
                values: vec![
                    Cell::I64(1),
                    Cell::String("Alice".to_string()),
                    Cell::Bool(true),
                    Cell::F64(95.5),
                ],
            },
            TableRow {
                values: vec![
                    Cell::I64(2),
                    Cell::String("Bob".to_string()),
                    Cell::Bool(false),
                    Cell::Null,
                ],
            },
        ]
    }

    #[test]
    fn test_rows_to_record_batch() {
        let schema = create_test_schema();
        let rows = create_test_rows();
        let schema_mapper = SchemaMapper::new();

        let batch = rows_to_record_batch(&rows, &schema, &schema_mapper).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
    }

    #[test]
    fn test_empty_rows() {
        let schema = create_test_schema();
        let rows = vec![];
        let schema_mapper = SchemaMapper::new();

        let batch = rows_to_record_batch(&rows, &schema, &schema_mapper).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_estimate_row_size() {
        let row = TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("test".to_string()),
                Cell::Bool(true),
            ],
        };

        let size = estimate_row_size(&row);
        assert!(size > 0);
        // Should include base struct size plus string length
        assert!(size > 4); // At least the string "test"
    }

    #[test]
    fn test_batch_rows() {
        let rows = vec![
            TableRow {
                values: vec![Cell::I64(1)],
            },
            TableRow {
                values: vec![Cell::I64(2)],
            },
            TableRow {
                values: vec![Cell::I64(3)],
            },
        ];

        // Test with max batch size of 2 rows
        let batches = batch_rows(&rows, 2, 1024 * 1024); // 2 rows max, 1MB max
        
        assert_eq!(batches.len(), 2); // 2 rows in first batch, 1 in second
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[1].len(), 1);
    }

    #[test]
    fn test_cell_size_estimation() {
        assert!(estimate_cell_size(&Cell::Null) > 0);
        assert!(estimate_cell_size(&Cell::I64(42)) >= std::mem::size_of::<i64>());
        assert!(estimate_cell_size(&Cell::String("hello".to_string())) >= 5);
        assert!(estimate_cell_size(&Cell::Bool(true)) >= std::mem::size_of::<bool>());
    }

    #[test]
    fn test_build_string_array() {
        let rows = vec![
            TableRow {
                values: vec![Cell::String("hello".to_string())],
            },
            TableRow {
                values: vec![Cell::Null],
            },
        ];

        let array = build_string_array(&rows, 0).unwrap();
        assert_eq!(array.len(), 2);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn test_batch_rows_zero_copy() {
        let rows = vec![
            TableRow {
                values: vec![Cell::I64(1)],
            },
            TableRow {
                values: vec![Cell::I64(2)],
            },
            TableRow {
                values: vec![Cell::I64(3)],
            },
        ];

        // Test with max batch size of 2 rows
        let batches = batch_rows(&rows, 2, 1024 * 1024); // 2 rows max, 1MB max
        
        assert_eq!(batches.len(), 2); // 2 rows in first batch, 1 in second
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[1].len(), 1);
        
        // Check that slices point to original data (zero-copy)
        assert_eq!(batches[0][0].values[0], Cell::I64(1));
        assert_eq!(batches[0][1].values[0], Cell::I64(2));
        assert_eq!(batches[1][0].values[0], Cell::I64(3));
    }
}