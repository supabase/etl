//! Test support utilities for Iceberg-based integration tests.

use std::collections::{HashMap, HashSet};

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::TimeUnit,
};
use etl::types::{ArrayCell, Cell, TableRow};
use etl_destinations::iceberg::{DeletionVector, IcebergClient, UNIX_EPOCH};
use iceberg::io::FileRead;
use iceberg::puffin::PuffinReader;
use iceberg::spec::DataContentType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
                _ => Cell::String(format!("{array:?}")),
            }
        }
        _ => Cell::String(format!("{array:?}")),
    }
}

/// Read all rows from the destination table, properly applying deletion vectors.
///
/// This function handles DVv2 deletion vectors which iceberg-rust's standard scan
/// doesn't apply. It reads the manifests to find data files and deletion vectors,
/// then reads data files directly (skipping deletion vector files) and filters
/// out deleted rows.
pub async fn read_all_rows(
    client: &IcebergClient,
    namespace: String,
    table_name: String,
) -> Vec<TableRow> {
    let table = client.load_table(namespace, table_name).await.unwrap();

    let Some(current_snapshot) = table.metadata().current_snapshot() else {
        return vec![];
    };

    // Load the manifest list
    let manifest_list = current_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    // Collect data files and deletion vectors from manifests
    // Keep track of data files in order and their deletion vectors
    let mut data_files: Vec<String> = Vec::new();
    let mut deletion_vectors: HashMap<String, HashSet<u64>> = HashMap::new();

    for manifest_entry in manifest_list.entries() {
        let manifest = manifest_entry.load_manifest(table.file_io()).await.unwrap();
        let (manifest_entries, _) = manifest.into_parts();

        for entry in manifest_entries {
            match entry.content_type() {
                DataContentType::Data => {
                    let path = entry.file_path().to_string();
                    if !data_files.contains(&path) {
                        data_files.push(path);
                    }
                }
                DataContentType::PositionDeletes => {
                    // This is a deletion vector file (Puffin format)
                    let puffin_path = entry.file_path();
                    let data_file = entry.data_file();
                    let referenced_data_file = data_file
                        .referenced_data_file()
                        .expect("Deletion vector must reference a data file");

                    // Read the Puffin file and extract the deletion vector
                    if let Ok(deleted_positions) =
                        load_deletion_vector(table.file_io(), puffin_path).await
                    {
                        deletion_vectors
                            .entry(referenced_data_file.clone())
                            .or_default()
                            .extend(deleted_positions);
                    }
                }
                DataContentType::EqualityDeletes => {
                    // Not currently supported
                }
            }
        }
    }

    // Read each data file directly using parquet reader (not the iceberg scan)
    // This avoids the issue where iceberg scan tries to read Puffin files as Parquet
    let mut all_rows = Vec::new();

    for data_file_path in &data_files {
        let deleted_positions = deletion_vectors.get(data_file_path);

        // Read the parquet file bytes using iceberg's FileIO
        let input_file = table.file_io().new_input(data_file_path).unwrap();
        let file_reader = input_file.reader().await.unwrap();
        let file_size = input_file.metadata().await.unwrap().size;
        let bytes = file_reader.read(0..file_size).await.unwrap();

        // Parse parquet file using synchronous reader
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();

        let mut row_idx: u64 = 0;

        for batch_result in parquet_reader {
            let batch = batch_result.unwrap();

            for batch_row_idx in 0..batch.num_rows() {
                let should_skip = deleted_positions
                    .map(|positions| positions.contains(&row_idx))
                    .unwrap_or(false);

                if !should_skip {
                    let mut cells = Vec::with_capacity(batch.num_columns());
                    for col_idx in 0..batch.num_columns() {
                        let cell = arrow_value_to_cell(batch.column(col_idx), batch_row_idx);
                        cells.push(cell);
                    }
                    all_rows.push(TableRow::new(cells));
                }
                row_idx += 1;
            }
        }
    }

    all_rows
}

/// Load deleted row positions from a Puffin deletion vector file.
async fn load_deletion_vector(
    file_io: &iceberg::io::FileIO,
    puffin_path: &str,
) -> Result<Vec<u64>, iceberg::Error> {
    let input_file = file_io.new_input(puffin_path)?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader.file_metadata().await?;

    // Read the first blob (deletion vector)
    if let Some(blob_metadata) = file_metadata.blobs().first() {
        let blob = puffin_reader.blob(blob_metadata).await?;
        let dv = DeletionVector::deserialize(blob)?;
        Ok(dv.deleted_rows())
    } else {
        Ok(vec![])
    }
}
