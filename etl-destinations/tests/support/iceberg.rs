#![allow(dead_code)]
#![cfg(feature = "iceberg")]

use std::collections::HashMap;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::TimeUnit,
};
use etl::types::{ArrayCell, Cell, TableRow};
use futures::StreamExt;

use etl_destinations::iceberg::{IcebergClient, UNIX_EPOCH};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};

pub const LAKEKEEPER_URL: &str = "http://localhost:8182";
const MINIO_URL: &str = "http://localhost:9010";
const MINIO_USERNAME: &str = "minio-admin";
const MINIO_PASSWORD: &str = "minio-admin-password";

pub fn get_catalog_url() -> String {
    format!("{LAKEKEEPER_URL}/catalog")
}

pub fn create_props() -> HashMap<String, String> {
    let mut props: HashMap<String, String> = HashMap::new();

    props.insert(S3_ACCESS_KEY_ID.to_string(), MINIO_USERNAME.to_string());
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), MINIO_PASSWORD.to_string());
    props.insert(S3_ENDPOINT.to_string(), MINIO_URL.to_string());

    props
}

/// Converts a RecordBatch back to a vector of TableRows.
pub fn record_batch_to_table_rows(batch: &RecordBatch) -> Vec<TableRow> {
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

    rows
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
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Cell::Bytes(arr.value(row_idx).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);

            let date = if days >= 0 {
                UNIX_EPOCH
                    .checked_add_days(chrono::Days::new(days as u64))
                    .unwrap()
            } else {
                UNIX_EPOCH
                    .checked_sub_days(chrono::Days::new(-days as u64))
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
                let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
                Cell::TimestampTz(dt)
            } else {
                // Naive timestamp
                let dt = chrono::DateTime::from_timestamp_micros(micros)
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
            let uuid_bytes: [u8; 16] = bytes.try_into().unwrap();
            let uuid = uuid::Uuid::from_bytes(uuid_bytes);

            Cell::Uuid(uuid)
        }
        DataType::List(field) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list_value = list_array.value(row_idx);

            match field.data_type() {
                DataType::Boolean => {
                    let bool_array = list_value.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let mut values = Vec::with_capacity(bool_array.len());

                    for i in 0..bool_array.len() {
                        if bool_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(bool_array.value(i)));
                        }
                    }

                    Cell::Array(ArrayCell::Bool(values))
                }
                DataType::Int32 => {
                    let int_array = list_value.as_any().downcast_ref::<Int32Array>().unwrap();
                    let mut values = Vec::with_capacity(int_array.len());

                    for i in 0..int_array.len() {
                        if int_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(int_array.value(i)));
                        }
                    }

                    Cell::Array(ArrayCell::I32(values))
                }
                DataType::Int64 => {
                    let int_array = list_value.as_any().downcast_ref::<Int64Array>().unwrap();
                    let mut values = Vec::with_capacity(int_array.len());

                    for i in 0..int_array.len() {
                        if int_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(int_array.value(i)));
                        }
                    }

                    Cell::Array(ArrayCell::I64(values))
                }
                DataType::Float32 => {
                    let float_array = list_value.as_any().downcast_ref::<Float32Array>().unwrap();
                    let mut values = Vec::with_capacity(float_array.len());

                    for i in 0..float_array.len() {
                        if float_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(float_array.value(i)));
                        }
                    }

                    Cell::Array(ArrayCell::F32(values))
                }
                DataType::Float64 => {
                    let float_array = list_value.as_any().downcast_ref::<Float64Array>().unwrap();
                    let mut values = Vec::with_capacity(float_array.len());

                    for i in 0..float_array.len() {
                        if float_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(float_array.value(i)));
                        }
                    }

                    Cell::Array(ArrayCell::F64(values))
                }
                DataType::Utf8 => {
                    let string_array = list_value.as_any().downcast_ref::<StringArray>().unwrap();
                    let mut values = Vec::with_capacity(string_array.len());

                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(string_array.value(i).to_string()));
                        }
                    }

                    Cell::Array(ArrayCell::String(values))
                }
                DataType::LargeBinary => {
                    let binary_array = list_value
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap();
                    let mut values = Vec::with_capacity(binary_array.len());

                    for i in 0..binary_array.len() {
                        if binary_array.is_null(i) {
                            values.push(None);
                        } else {
                            values.push(Some(binary_array.value(i).to_vec()));
                        }
                    }

                    Cell::Array(ArrayCell::Bytes(values))
                }
                DataType::Date32 => {
                    let date_array = list_value.as_any().downcast_ref::<Date32Array>().unwrap();
                    let mut values = Vec::with_capacity(date_array.len());

                    for i in 0..date_array.len() {
                        if date_array.is_null(i) {
                            values.push(None);
                        } else {
                            let days = date_array.value(i);
                            let date = if days >= 0 {
                                UNIX_EPOCH
                                    .checked_add_days(chrono::Days::new(days as u64))
                                    .unwrap()
                            } else {
                                UNIX_EPOCH
                                    .checked_sub_days(chrono::Days::new(-days as u64))
                                    .unwrap()
                            };
                            values.push(Some(date));
                        }
                    }

                    Cell::Array(ArrayCell::Date(values))
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    let time_array = list_value
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap();
                    let mut values = Vec::with_capacity(time_array.len());

                    for i in 0..time_array.len() {
                        if time_array.is_null(i) {
                            values.push(None);
                        } else {
                            let micros = time_array.value(i);
                            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                                (micros / 1_000_000) as u32,
                                ((micros % 1_000_000) * 1000) as u32,
                            )
                            .unwrap();
                            values.push(Some(time));
                        }
                    }

                    Cell::Array(ArrayCell::Time(values))
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    let ts_array = list_value
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();

                    if tz.is_some() {
                        // Timezone-aware timestamps
                        let mut tz_values = Vec::with_capacity(ts_array.len());
                        for i in 0..ts_array.len() {
                            if ts_array.is_null(i) {
                                tz_values.push(None);
                            } else {
                                let micros = ts_array.value(i);
                                let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
                                tz_values.push(Some(dt));
                            }
                        }
                        Cell::Array(ArrayCell::TimestampTz(tz_values))
                    } else {
                        // Naive timestamps
                        let mut values = Vec::with_capacity(ts_array.len());
                        for i in 0..ts_array.len() {
                            if ts_array.is_null(i) {
                                values.push(None);
                            } else {
                                let micros = ts_array.value(i);
                                let dt = chrono::DateTime::from_timestamp_micros(micros)
                                    .unwrap()
                                    .naive_utc();
                                values.push(Some(dt));
                            }
                        }
                        Cell::Array(ArrayCell::Timestamp(values))
                    }
                }
                DataType::FixedSizeBinary(16) => {
                    let uuid_array = list_value
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    let mut values = Vec::with_capacity(uuid_array.len());

                    for i in 0..uuid_array.len() {
                        if uuid_array.is_null(i) {
                            values.push(None);
                        } else {
                            let bytes = uuid_array.value(i);
                            let uuid_bytes: [u8; 16] = bytes.try_into().unwrap();
                            let uuid = uuid::Uuid::from_bytes(uuid_bytes);
                            values.push(Some(uuid));
                        }
                    }

                    Cell::Array(ArrayCell::Uuid(values))
                }
                _ => Cell::String(format!("{array:?}")),
            }
        }
        _ => Cell::String(format!("{array:?}")),
    }
}

/// Read all rows from the destination table.
pub async fn read_all_rows(
    client: &IcebergClient,
    namespace: String,
    table_name: String,
) -> Vec<TableRow> {
    let table = client.load_table(namespace, table_name).await.unwrap();

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
        let rows = record_batch_to_table_rows(&batch_result.unwrap());
        all_rows.extend(rows);
    }

    all_rows
}
