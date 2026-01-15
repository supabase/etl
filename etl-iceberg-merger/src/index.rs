//! Puffin-based secondary index for changelog deduplication.
//!
//! Implements a secondary index structure stored in Puffin files that maps
//! primary key hashes to file locations for efficient deduplication during merge.
//! Adapted from moonlink's GlobalIndex design but using Iceberg FileIO instead
//! of memory-mapped local files.

use anyhow::Context;
use arrow::array::Array;
use futures::TryStreamExt;
use iceberg::table::Table;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tracing::{debug, info};

/// A secondary index that maps primary key hashes to their locations in Parquet files.
///
/// The index is serialized as Parquet data within Puffin files and stored in Iceberg
/// table metadata. Each index entry contains:
/// - `pk_hash`: Hash of the primary key values
/// - `file_path`: Path to the Parquet file containing the row
/// - `row_offset`: Offset of the row within the file
#[derive(Debug, Clone)]
pub struct PuffinIndex {
    /// Hash bits used for the index (e.g., 64 for full u64 hash).
    #[allow(dead_code)]
    hash_bits: u32,

    /// Total number of rows indexed.
    num_rows: usize,

    /// Map from primary key hash to file location.
    entries: HashMap<u64, FileLocation>,
}

/// Location of a row within a Parquet file.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileLocation {
    /// Path to the Parquet file.
    pub file_path: String,

    /// Row offset within the file.
    pub row_offset: usize,
}

/// Metadata for a Puffin index stored in Iceberg table properties.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PuffinIndexMetadata {
    /// Hash bits used for the index.
    pub hash_bits: u32,

    /// Total number of rows indexed.
    pub num_rows: usize,

    /// Path to the Puffin file containing the index.
    pub puffin_path: String,
}

impl PuffinIndex {
    /// Creates a new empty index.
    pub fn new(hash_bits: u32) -> Self {
        Self {
            hash_bits,
            num_rows: 0,
            entries: HashMap::new(),
        }
    }

    /// Adds an entry to the index.
    pub fn insert(&mut self, pk_hash: u64, file_path: String, row_offset: usize) {
        self.entries.insert(
            pk_hash,
            FileLocation {
                file_path,
                row_offset,
            },
        );
        self.num_rows += 1;
    }

    /// Looks up a primary key hash in the index.
    #[allow(dead_code)]
    pub fn get(&self, pk_hash: u64) -> Option<&FileLocation> {
        self.entries.get(&pk_hash)
    }

    /// Returns the number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the index is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the hash bits used by this index.
    #[allow(dead_code)]
    pub fn hash_bits(&self) -> u32 {
        self.hash_bits
    }

    /// Serializes the index to Parquet format.
    ///
    /// Returns bytes that can be stored in a Puffin file.
    #[allow(dead_code)]
    pub fn to_parquet(&self) -> anyhow::Result<Vec<u8>> {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        debug!(
            num_entries = self.entries.len(),
            "serializing index to parquet"
        );

        let mut pk_hashes = Vec::with_capacity(self.entries.len());
        let mut file_paths = Vec::with_capacity(self.entries.len());
        let mut row_offsets = Vec::with_capacity(self.entries.len());

        for (pk_hash, location) in &self.entries {
            pk_hashes.push(*pk_hash);
            file_paths.push(location.file_path.clone());
            row_offsets.push(location.row_offset as u64);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("pk_hash", DataType::UInt64, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("row_offset", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(pk_hashes)),
                Arc::new(StringArray::from(file_paths)),
                Arc::new(UInt64Array::from(row_offsets)),
            ],
        )
        .context("creating record batch")?;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
            .context("creating arrow writer")?;

        writer.write(&batch).context("writing record batch")?;
        writer.close().context("closing arrow writer")?;

        info!(size_bytes = buffer.len(), "serialized index to parquet");
        Ok(buffer)
    }

    /// Deserializes an index from Parquet format.
    pub fn from_parquet(data: &[u8], hash_bits: u32) -> anyhow::Result<Self> {
        use arrow::array::AsArray;

        debug!(data_size = data.len(), "deserializing index from parquet");

        let bytes = bytes::Bytes::from(data.to_vec());
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).context("creating parquet reader")?;
        let reader = builder.build().context("building parquet reader")?;

        let mut entries = HashMap::new();
        let mut num_rows = 0;

        for batch_result in reader {
            let batch = batch_result.context("reading record batch")?;
            let pk_hashes = batch
                .column(0)
                .as_primitive::<arrow::datatypes::UInt64Type>();
            let file_paths = batch.column(1).as_string::<i32>();
            let row_offsets = batch
                .column(2)
                .as_primitive::<arrow::datatypes::UInt64Type>();

            for i in 0..batch.num_rows() {
                let pk_hash = pk_hashes.value(i);
                let file_path = file_paths.value(i).to_string();
                let row_offset = row_offsets.value(i) as usize;

                entries.insert(
                    pk_hash,
                    FileLocation {
                        file_path,
                        row_offset,
                    },
                );
                num_rows += 1;
            }
        }

        info!(
            num_entries = entries.len(),
            "deserialized index from parquet"
        );

        Ok(Self {
            hash_bits,
            num_rows,
            entries,
        })
    }
}

/// Builder for creating a Puffin index from changelog data.
pub struct IndexBuilder {
    hash_bits: u32,
}

impl IndexBuilder {
    /// Creates a new index builder.
    pub fn new(hash_bits: u32) -> Self {
        Self { hash_bits }
    }

    /// Builds an index by scanning an Iceberg table.
    ///
    /// Scans all records from the table and creates PK hash mappings
    /// to their file locations.
    pub async fn build_from_table(
        &self,
        table: &Table,
        primary_keys: &[String],
    ) -> anyhow::Result<PuffinIndex> {
        info!(
            hash_bits = self.hash_bits,
            pk_count = primary_keys.len(),
            "building index from table"
        );

        let mut index = PuffinIndex::new(self.hash_bits);

        // Check if table has any snapshots.
        let Some(current_snapshot_id) = table.metadata().current_snapshot_id() else {
            debug!("no snapshots in table, returning empty index");
            return Ok(index);
        };

        let scan = table
            .scan()
            .snapshot_id(current_snapshot_id)
            .select_all()
            .build()
            .context("building table scan")?;

        // Get the list of files from the scan plan.
        let file_scan_tasks: Vec<_> = scan
            .plan_files()
            .await
            .context("planning files")?
            .try_collect()
            .await
            .context("collecting file scan tasks")?;

        // Find primary key column indices.
        let schema = table.metadata().current_schema();
        let pk_indices: Vec<usize> = primary_keys
            .iter()
            .filter_map(|pk| {
                schema
                    .as_struct()
                    .fields()
                    .iter()
                    .position(|f| &f.name == pk)
            })
            .collect();

        if pk_indices.len() != primary_keys.len() {
            anyhow::bail!(
                "not all primary key columns found in schema: expected {:?}",
                primary_keys
            );
        }

        // Scan each file and build the index.
        let mut stream = scan.to_arrow().await.context("creating arrow stream")?;
        let mut current_file_idx = 0;
        let mut row_offset_in_file = 0;

        while let Some(batch_result) = stream.try_next().await? {
            let file_path = file_scan_tasks
                .get(current_file_idx)
                .map(|t| t.data_file_path().to_string())
                .unwrap_or_else(|| format!("file_{}", current_file_idx));

            for row_idx in 0..batch_result.num_rows() {
                let pk_hash = compute_pk_hash(&batch_result, &pk_indices, row_idx);
                index.insert(pk_hash, file_path.clone(), row_offset_in_file);
                row_offset_in_file += 1;
            }

            // Check if we've moved to a new file (simplified heuristic).
            if row_offset_in_file > 0 && current_file_idx + 1 < file_scan_tasks.len() {
                current_file_idx += 1;
                row_offset_in_file = 0;
            }
        }

        info!(
            entries = index.len(),
            hash_bits = self.hash_bits,
            "index built successfully"
        );

        Ok(index)
    }

    /// Builds an index by scanning changelog Parquet files directly.
    ///
    /// This method scans the provided Parquet file paths and creates PK hash mappings.
    /// Used for cases where direct file access is preferred over table scanning.
    #[allow(dead_code)]
    pub async fn build_from_changelog(
        &self,
        changelog_files: Vec<String>,
        primary_keys: &[String],
        file_io: &iceberg::io::FileIO,
    ) -> anyhow::Result<PuffinIndex> {
        info!(
            file_count = changelog_files.len(),
            hash_bits = self.hash_bits,
            "building index from changelog files"
        );

        let mut index = PuffinIndex::new(self.hash_bits);

        for file_path in changelog_files {
            debug!(file_path = %file_path, "scanning file for index");

            let input = file_io
                .new_input(&file_path)
                .context("opening changelog file")?;
            let file_bytes = input.read().await.context("reading changelog file")?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes)
                .context("creating parquet reader")?;

            // Get column indices for primary keys.
            let arrow_schema = builder.schema();
            let pk_indices: Vec<usize> = primary_keys
                .iter()
                .filter_map(|pk| arrow_schema.fields().iter().position(|f| f.name() == pk))
                .collect();

            if pk_indices.len() != primary_keys.len() {
                anyhow::bail!(
                    "not all primary key columns found in file {}: expected {:?}",
                    file_path,
                    primary_keys
                );
            }

            let reader = builder.build().context("building parquet reader")?;
            let mut row_offset = 0;

            for batch_result in reader {
                let batch = batch_result.context("reading record batch")?;

                for row_idx in 0..batch.num_rows() {
                    let pk_hash = compute_pk_hash(&batch, &pk_indices, row_idx);
                    index.insert(pk_hash, file_path.clone(), row_offset);
                    row_offset += 1;
                }
            }

            debug!(
                file_path = %file_path,
                rows_indexed = row_offset,
                "file indexed"
            );
        }

        info!(
            entries = index.len(),
            hash_bits = self.hash_bits,
            "index built successfully"
        );

        Ok(index)
    }
}

/// Computes a hash of the primary key columns for a given row.
fn compute_pk_hash(
    batch: &arrow::array::RecordBatch,
    pk_col_indices: &[usize],
    row_idx: usize,
) -> u64 {
    let mut hasher = DefaultHasher::new();

    for &col_idx in pk_col_indices {
        let col = batch.column(col_idx);

        if col.is_null(row_idx) {
            0u8.hash(&mut hasher);
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
        {
            arr.value(row_idx).hash(&mut hasher);
        } else if let Some(arr) = col
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        {
            arr.value(row_idx).hash(&mut hasher);
        } else {
            format!("{:?}", col.slice(row_idx, 1)).hash(&mut hasher);
        }
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_index_new() {
        let index = PuffinIndex::new(64);
        assert_eq!(index.hash_bits(), 64);
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_index_insert_and_get() {
        let mut index = PuffinIndex::new(64);
        index.insert(12345, "file1.parquet".to_string(), 0);
        index.insert(67890, "file2.parquet".to_string(), 42);

        assert_eq!(index.len(), 2);
        assert!(!index.is_empty());

        let loc1 = index.get(12345).unwrap();
        assert_eq!(loc1.file_path, "file1.parquet");
        assert_eq!(loc1.row_offset, 0);

        let loc2 = index.get(67890).unwrap();
        assert_eq!(loc2.file_path, "file2.parquet");
        assert_eq!(loc2.row_offset, 42);

        assert!(index.get(99999).is_none());
    }

    #[test]
    fn test_index_roundtrip() {
        let mut index = PuffinIndex::new(64);
        index.insert(12345, "file1.parquet".to_string(), 0);
        index.insert(67890, "file2.parquet".to_string(), 42);

        let bytes = index.to_parquet().unwrap();
        let loaded = PuffinIndex::from_parquet(&bytes, 64).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get(12345).unwrap().file_path, "file1.parquet");
        assert_eq!(loaded.get(67890).unwrap().row_offset, 42);
    }

    #[test]
    fn test_index_roundtrip_empty() {
        let index = PuffinIndex::new(32);
        let bytes = index.to_parquet().unwrap();
        let loaded = PuffinIndex::from_parquet(&bytes, 32).unwrap();

        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());
        assert_eq!(loaded.hash_bits(), 32);
    }

    #[test]
    fn test_index_roundtrip_large() {
        let mut index = PuffinIndex::new(64);

        for i in 0..10_000 {
            index.insert(i as u64, format!("file_{}.parquet", i % 100), i);
        }

        let bytes = index.to_parquet().unwrap();
        let loaded = PuffinIndex::from_parquet(&bytes, 64).unwrap();

        assert_eq!(loaded.len(), 10_000);

        // Verify a few random entries.
        assert_eq!(loaded.get(0).unwrap().file_path, "file_0.parquet");
        assert_eq!(loaded.get(5000).unwrap().file_path, "file_0.parquet");
        assert_eq!(loaded.get(9999).unwrap().file_path, "file_99.parquet");
    }

    #[test]
    fn test_index_overwrite() {
        let mut index = PuffinIndex::new(64);
        index.insert(12345, "file1.parquet".to_string(), 0);
        index.insert(12345, "file2.parquet".to_string(), 100);

        // The second insert should overwrite the first.
        let loc = index.get(12345).unwrap();
        assert_eq!(loc.file_path, "file2.parquet");
        assert_eq!(loc.row_offset, 100);
    }

    #[test]
    fn test_compute_pk_hash_single_int() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let hash1 = compute_pk_hash(&batch, &[0], 0);
        let hash2 = compute_pk_hash(&batch, &[0], 1);
        let hash3 = compute_pk_hash(&batch, &[0], 0);

        assert_ne!(hash1, hash2);
        assert_eq!(hash1, hash3);
    }

    #[test]
    fn test_compute_pk_hash_composite() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
            ],
        )
        .unwrap();

        let hash_1a = compute_pk_hash(&batch, &[0, 1], 0);
        let hash_1b = compute_pk_hash(&batch, &[0, 1], 1);
        let hash_2a = compute_pk_hash(&batch, &[0, 1], 2);

        // All should differ since the composite key differs.
        assert_ne!(hash_1a, hash_1b);
        assert_ne!(hash_1a, hash_2a);
        assert_ne!(hash_1b, hash_2a);
    }

    #[test]
    fn test_puffin_index_metadata_serialization() {
        let metadata = PuffinIndexMetadata {
            hash_bits: 64,
            num_rows: 1000,
            puffin_path: "s3://bucket/index.puffin".to_string(),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let loaded: PuffinIndexMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.hash_bits, 64);
        assert_eq!(loaded.num_rows, 1000);
        assert_eq!(loaded.puffin_path, "s3://bucket/index.puffin");
    }

    #[test]
    fn test_index_builder_new() {
        let builder = IndexBuilder::new(64);
        assert_eq!(builder.hash_bits, 64);
    }
}
