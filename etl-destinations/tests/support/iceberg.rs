#![allow(dead_code)]
#![cfg(feature = "iceberg")]

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::TimeUnit,
};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, TableRow},
};
use futures::StreamExt;

use etl_destinations::iceberg::{IcebergClient, UNIX_EPOCH};

/// Converts a RecordBatch back to a vector of TableRows.
pub fn record_batch_to_table_rows(batch: &RecordBatch) -> EtlResult<Vec<TableRow>> {
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            let cell = arrow_value_to_cell(column, row_idx);
            cells.push(cell);
        }

        rows.push(TableRow::new(cells));
    }

    Ok(rows)
}

/// Converts an Arrow array value at a specific index to a Cell.
fn arrow_value_to_cell(array: &ArrayRef, row_idx: usize) -> Cell {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row_idx) {
        return Cell::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Cell::Bool(arr.value(row_idx))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Cell::I32(arr.value(row_idx))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Cell::I64(arr.value(row_idx))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Cell::U32(arr.value(row_idx))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Cell::F32(arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Cell::F64(arr.value(row_idx))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Cell::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Cell::String(arr.value(row_idx).to_string())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Cell::Bytes(arr.value(row_idx).to_vec())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Cell::Bytes(arr.value(row_idx).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);

            let date = if days >= 0 {
                UNIX_EPOCH
                    .checked_add_days(chrono::Days::new(days as u64))
                    .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid date value"))
                    .unwrap()
            } else {
                UNIX_EPOCH
                    .checked_sub_days(chrono::Days::new((-days) as u64))
                    .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid date value"))
                    .unwrap()
            };
            Cell::Date(date)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                (micros / 1_000_000) as u32,
                ((micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| etl_error!(ErrorKind::DestinationError, "Invalid time value"))
            .unwrap();
            Cell::Time(time)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);

            if tz.is_some() {
                // Timezone-aware timestamp
                let dt = chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or(Cell::Null)
                    .unwrap();
                Cell::TimestampTz(dt)
            } else {
                // Naive timestamp
                let dt = chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or(Cell::Null)
                    .unwrap()
                    .naive_utc();
                Cell::Timestamp(dt)
            }
        }
        DataType::FixedSizeBinary(16) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = arr.value(row_idx);

            // Convert 16-byte array to UUID
            let uuid_bytes: [u8; 16] = bytes.try_into().map_err(|_| Cell::Null).unwrap();

            let uuid = uuid::Uuid::from_bytes(uuid_bytes);
            Cell::Uuid(uuid)
        }
        _ => Cell::String(format!("{array:?}")),
    }
}

/// Read all rows from the destination table.
pub async fn read_all_rows(
    client: &IcebergClient,
    namespace: String,
    table_name: String,
) -> EtlResult<Vec<TableRow>> {
    let table = client
        .load_table_for_test(namespace, table_name)
        .await
        .unwrap();

    let mut table_rows_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();

    let mut all_rows = Vec::new();

    // Iterate over the stream of RecordBatch results
    while let Some(batch_result) = table_rows_stream.next().await {
        let rows = record_batch_to_table_rows(&batch_result.unwrap())?;
        all_rows.extend(rows);
    }

    Ok(all_rows)
}
