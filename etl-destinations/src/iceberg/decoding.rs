use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::TimeUnit,
};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, TableRow},
};

/// Converts a RecordBatch back to a vector of TableRows.
pub fn record_batch_to_table_rows(batch: &RecordBatch) -> EtlResult<Vec<TableRow>> {
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            let cell = arrow_value_to_cell(column, row_idx)?;
            cells.push(cell);
        }

        rows.push(TableRow::new(cells));
    }

    Ok(rows)
}

/// Converts an Arrow array value at a specific index to a Cell.
fn arrow_value_to_cell(array: &ArrayRef, row_idx: usize) -> EtlResult<Cell> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row_idx) {
        return Ok(Cell::Null);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to BooleanArray"
                    )
                })?;
            Ok(Cell::Bool(arr.value(row_idx)))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to downcast to Int16Array"
                )
            })?;
            Ok(Cell::I16(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to downcast to Int32Array"
                )
            })?;
            Ok(Cell::I32(arr.value(row_idx)))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to downcast to Int64Array"
                )
            })?;
            Ok(Cell::I64(arr.value(row_idx)))
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to UInt32Array"
                    )
                })?;
            Ok(Cell::U32(arr.value(row_idx)))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to Float32Array"
                    )
                })?;
            Ok(Cell::F32(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to Float64Array"
                    )
                })?;
            Ok(Cell::F64(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to StringArray"
                    )
                })?;
            Ok(Cell::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to LargeStringArray"
                    )
                })?;
            Ok(Cell::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to LargeBinaryArray"
                    )
                })?;
            Ok(Cell::Bytes(arr.value(row_idx).to_vec()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to BinaryArray"
                    )
                })?;
            Ok(Cell::Bytes(arr.value(row_idx).to_vec()))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to Date32Array"
                    )
                })?;
            let days = arr.value(row_idx);
            // Convert days since Unix epoch (1970-01-01) to NaiveDate
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid epoch date"))?;

            let date = if days >= 0 {
                epoch
                    .checked_add_days(chrono::Days::new(days as u64))
                    .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid date value"))?
            } else {
                epoch
                    .checked_sub_days(chrono::Days::new((-days) as u64))
                    .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid date value"))?
            };
            Ok(Cell::Date(date))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to Time64MicrosecondArray"
                    )
                })?;
            let micros = arr.value(row_idx);
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                (micros / 1_000_000) as u32,
                ((micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid time value"))?;
            Ok(Cell::Time(time))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to TimestampMicrosecondArray"
                    )
                })?;
            let micros = arr.value(row_idx);

            if tz.is_some() {
                // Timezone-aware timestamp
                let dt = chrono::DateTime::from_timestamp_micros(micros).ok_or_else(|| {
                    etl_error!(ErrorKind::DestinationError, "Invalid timestamp value")
                })?;
                Ok(Cell::TimestampTz(dt))
            } else {
                // Naive timestamp
                let dt = chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or_else(|| {
                        etl_error!(ErrorKind::DestinationError, "Invalid timestamp value")
                    })?
                    .naive_utc();
                Ok(Cell::Timestamp(dt))
            }
        }
        DataType::FixedSizeBinary(16) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::DestinationError,
                        "Failed to downcast to FixedSizeBinaryArray"
                    )
                })?;
            let bytes = arr.value(row_idx);

            // Convert 16-byte array to UUID
            let uuid_bytes: [u8; 16] = bytes.try_into().map_err(|_| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Invalid UUID bytes length, expected 16 bytes"
                )
            })?;

            let uuid = uuid::Uuid::from_bytes(uuid_bytes);
            Ok(Cell::Uuid(uuid))
        }
        _ => {
            // For unsupported types, convert to string representation
            Ok(Cell::String(format!("{:?}", array)))
        }
    }
}
