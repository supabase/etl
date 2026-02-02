//! Reads batches of CDC events from Iceberg changelog tables.

use std::str::FromStr;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::TimeUnit;
use etl::types::{ArrayCell, Cell, TableRow};
use etl_destinations::iceberg::{IcebergClient, UNIX_EPOCH};
use futures::StreamExt;
use tracing::debug;

use crate::error::{MergerError, MergerResult};
use crate::index::PrimaryKey;

/// CDC operation column name in changelog tables.
pub const CDC_OPERATION_COLUMN_NAME: &str = "cdc_operation";
/// Sequence number column name in changelog tables.
pub const SEQUENCE_NUMBER_COLUMN_NAME: &str = "sequence_number";

/// CDC operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// Insert operation.
    Insert,
    /// Update operation.
    Update,
    /// Delete operation.
    Delete,
}

impl FromStr for CdcOperation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            _ => Err(format!("Invalid CDC operation: {}", s)),
        }
    }
}

/// A single changelog entry representing a CDC event.
#[derive(Debug, Clone)]
pub struct ChangelogEntry {
    /// The operation type: INSERT, UPDATE, or DELETE.
    pub operation: CdcOperation,
    /// The sequence number for ordering.
    pub sequence_number: String,
    /// The row data (without CDC columns).
    pub row: TableRow,
    /// Extracted primary key for index operations.
    pub primary_key: PrimaryKey,
}

/// A batch of changelog entries ready for merging.
#[derive(Debug)]
pub struct ChangelogBatch {
    /// The changelog entries in this batch.
    pub entries: Vec<ChangelogEntry>,
    /// Last sequence number in this batch.
    pub last_sequence_number: Option<String>,
}

impl ChangelogBatch {
    /// Creates an empty batch.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            last_sequence_number: None,
        }
    }
}

/// Reads batches of CDC events from Iceberg changelog tables.
pub struct ChangelogReader {
    /// The Iceberg client for loading the table.
    client: IcebergClient,
    /// Namespace of the changelog table.
    namespace: String,
    /// Name of the changelog table.
    table_name: String,
    /// Column index for cdc_operation.
    cdc_operation_col_idx: usize,
    /// Column index for sequence_number.
    sequence_number_col_idx: usize,
    /// Primary key column indices (excluding CDC columns).
    pk_column_indices: Vec<usize>,
    /// Total number of columns in the original table (excluding CDC columns).
    data_column_count: usize,
}

impl ChangelogReader {
    /// Creates a new changelog reader for the given table.
    pub async fn new(
        client: &IcebergClient,
        namespace: &str,
        changelog_table_name: &str,
        pk_column_indices: Vec<usize>,
    ) -> MergerResult<Self> {
        debug!(%namespace, %changelog_table_name, "loading changelog table");
        let table = client
            .load_table(namespace.to_string(), changelog_table_name.to_string())
            .await?;

        let schema = table.metadata().current_schema();
        let fields: Vec<_> = schema.as_struct().fields().iter().collect();

        // Find CDC column indices
        let cdc_operation_col_idx = fields
            .iter()
            .position(|f| f.name == CDC_OPERATION_COLUMN_NAME)
            .ok_or_else(|| MergerError::ColumnNotFound(CDC_OPERATION_COLUMN_NAME.to_string()))?;

        let sequence_number_col_idx = fields
            .iter()
            .position(|f| f.name == SEQUENCE_NUMBER_COLUMN_NAME)
            .ok_or_else(|| MergerError::ColumnNotFound(SEQUENCE_NUMBER_COLUMN_NAME.to_string()))?;

        // Data columns are all columns except the CDC columns
        // CDC columns are always at the end
        let data_column_count = fields.len() - 2;

        Ok(Self {
            client: client.clone(),
            namespace: namespace.to_string(),
            table_name: changelog_table_name.to_string(),
            cdc_operation_col_idx,
            sequence_number_col_idx,
            pk_column_indices,
            data_column_count,
        })
    }

    /// Returns the primary key column indices.
    pub fn pk_column_indices(&self) -> &[usize] {
        &self.pk_column_indices
    }

    /// Returns the number of data columns (excluding CDC columns).
    pub fn data_column_count(&self) -> usize {
        self.data_column_count
    }

    /// Reads a batch of changelog entries starting after the given sequence number.
    ///
    /// Events are returned sorted by sequence_number for correct ordering.
    /// The table is reloaded on each call to get the latest snapshot.
    pub async fn read_batch(
        &self,
        after_sequence_number: Option<&str>,
        batch_size: usize,
    ) -> MergerResult<ChangelogBatch> {
        // Reload the table to get the latest snapshot
        let table = self
            .client
            .load_table(self.namespace.clone(), self.table_name.clone())
            .await?;

        let Some(current_snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(ChangelogBatch::empty());
        };

        let mut stream = table
            .scan()
            .snapshot_id(current_snapshot)
            .select_all()
            .build()?
            .to_arrow()
            .await?;

        let mut all_entries = Vec::new();

        // Read all record batches and convert to entries
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let entries = self.record_batch_to_entries(&batch)?;
            all_entries.extend(entries);
        }

        // Filter by sequence number if provided
        if let Some(after_seq) = after_sequence_number {
            all_entries.retain(|e| e.sequence_number.as_str() > after_seq);
        }

        // Sort by sequence number for correct ordering
        all_entries.sort_by(|a, b| a.sequence_number.cmp(&b.sequence_number));

        // Take up to batch_size entries
        all_entries.truncate(batch_size);

        let last_sequence_number = all_entries.last().map(|e| e.sequence_number.clone());

        Ok(ChangelogBatch {
            entries: all_entries,
            last_sequence_number,
        })
    }

    /// Converts a RecordBatch to changelog entries.
    fn record_batch_to_entries(&self, batch: &RecordBatch) -> MergerResult<Vec<ChangelogEntry>> {
        let mut entries = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            // Extract CDC columns
            let operation_cell =
                arrow_value_to_cell(batch.column(self.cdc_operation_col_idx), row_idx);
            let sequence_cell =
                arrow_value_to_cell(batch.column(self.sequence_number_col_idx), row_idx);

            let operation = match &operation_cell {
                Cell::String(s) => s
                    .parse::<CdcOperation>()
                    .map_err(|_| MergerError::InvalidCdcOperation(s.clone()))?,
                _ => {
                    return Err(MergerError::InvalidCdcOperation(format!(
                        "Expected string, got {:?}",
                        operation_cell
                    )));
                }
            };

            let sequence_number = match sequence_cell {
                Cell::String(s) => s,
                _ => {
                    return Err(MergerError::SequenceNumberParse(format!(
                        "Expected string, got {:?}",
                        sequence_cell
                    )));
                }
            };

            // Extract data columns (excluding CDC columns)
            let mut cells = Vec::with_capacity(self.data_column_count);
            for col_idx in 0..self.data_column_count {
                let cell = arrow_value_to_cell(batch.column(col_idx), row_idx);
                cells.push(cell);
            }

            let row = TableRow::new(cells);
            let primary_key = PrimaryKey::from_row(&row.values, &self.pk_column_indices);

            entries.push(ChangelogEntry {
                operation,
                sequence_number,
                row,
                primary_key,
            });
        }

        Ok(entries)
    }
}

/// Converts an Arrow array value at a specific index to a Cell.
pub fn arrow_value_to_cell(array: &ArrayRef, row_idx: usize) -> Cell {
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
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Cell::I16(arr.value(row_idx))
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
                let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
                Cell::TimestampTz(dt)
            } else {
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
            list_to_array_cell(&list_value, field.data_type())
        }
        _ => Cell::String(format!("{array:?}")),
    }
}

/// Converts an Arrow list to an ArrayCell.
fn list_to_array_cell(list_value: &ArrayRef, element_type: &arrow::datatypes::DataType) -> Cell {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    match element_type {
        DataType::Boolean => {
            let arr = list_value.as_any().downcast_ref::<BooleanArray>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Cell::Array(ArrayCell::Bool(values))
        }
        DataType::Int32 => {
            let arr = list_value.as_any().downcast_ref::<Int32Array>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Cell::Array(ArrayCell::I32(values))
        }
        DataType::Int64 => {
            let arr = list_value.as_any().downcast_ref::<Int64Array>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Cell::Array(ArrayCell::I64(values))
        }
        DataType::Float32 => {
            let arr = list_value.as_any().downcast_ref::<Float32Array>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Cell::Array(ArrayCell::F32(values))
        }
        DataType::Float64 => {
            let arr = list_value.as_any().downcast_ref::<Float64Array>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Cell::Array(ArrayCell::F64(values))
        }
        DataType::Utf8 => {
            let arr = list_value.as_any().downcast_ref::<StringArray>().unwrap();
            let values: Vec<_> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                })
                .collect();
            Cell::Array(ArrayCell::String(values))
        }
        _ => Cell::String(format!("{list_value:?}")),
    }
}
