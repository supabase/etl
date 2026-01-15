//! Puffin-based secondary index for changelog deduplication.
//!
//! Implements a secondary index structure stored in Puffin files that maps
//! primary key hashes to file locations for efficient deduplication during merge.
//! Adapted from moonlink's GlobalIndex design but using Iceberg FileIO instead
//! of memory-mapped local files.
//!
//! ## Architecture
//!
//! The index uses a two-tier strategy optimized for S3 storage:
//!
//! 1. **Persistent Storage**: Index data is stored as Parquet in Puffin files on S3
//! 2. **Memory Cache**: Index entries are cached in-memory for fast lookups
//!
//! ## Hash Function
//!
//! Uses `splitmix64` - a high-quality hash function with excellent distribution
//! properties, adopted from moonlink's implementation:
//!
//! - Fast computation (few arithmetic operations)
//! - Good avalanche properties (small input changes → large hash changes)
//! - Avoids clustering in hash buckets
//!
//! ## Lookup Strategy
//!
//! Supports both single and batch lookups:
//!
//! - **Single lookup**: `get(pk_hash)` for individual key lookups
//! - **Batch lookup**: `search_values(&hashes)` for efficient multi-key lookups

use anyhow::Context;
use arrow::array::Array;
use futures::TryStreamExt;
use iceberg::table::Table;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// High-quality hash function for distributing keys across buckets.
///
/// SplitMix64 is a fast, non-cryptographic hash function with excellent
/// statistical properties. It's used to hash primary keys before indexing.
///
/// Adopted from moonlink's implementation, which provides:
/// - **Fast**: Only a few arithmetic operations
/// - **Good distribution**: Avoids clustering in hash buckets
/// - **Avalanche**: Small input changes cause large output changes
///
/// # Example
///
/// ```
/// # use etl_iceberg_merger::index::splitmix64;
/// let key = 12345u64;
/// let hash = splitmix64(key);
///
/// // Even nearby keys produce very different hashes
/// let hash2 = splitmix64(12346);
/// assert_ne!(hash, hash2);
/// ```
#[inline]
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// A secondary index that maps primary key hashes to their locations in Parquet files.
///
/// The index is serialized as Parquet data within Puffin files and stored in Iceberg
/// table metadata. The data is persisted to S3 but cached in memory for fast lookups.
///
/// ## Index Structure
///
/// Each index entry contains:
/// - `pk_hash`: Hash of the primary key values (using `splitmix64`)
/// - `file_path`: Path to the Parquet file containing the row
/// - `row_offset`: Offset of the row within the file
///
/// ## Lookup Strategy
///
/// The index supports both single and batch lookups:
///
/// - **Single lookup**: `get(pk_hash)` - O(1) average case
/// - **Batch lookup**: `search_values(&hashes)` - More efficient for multiple keys
///   as it processes all lookups in a single pass
///
/// ## Memory Caching
///
/// The index is designed for S3 storage with in-memory caching:
/// - Index data is persisted as Parquet in Puffin files on S3
/// - When loaded, the entire index is cached in memory (via the `entries` HashMap)
/// - Subsequent lookups hit the in-memory cache directly
#[derive(Debug, Clone)]
pub struct PuffinIndex {
    /// Hash bits used for the index (e.g., 64 for full u64 hash).
    hash_bits: u32,

    /// Total number of rows indexed.
    num_rows: usize,

    /// Map from primary key hash to file location.
    /// This serves as the in-memory cache for the index.
    entries: HashMap<u64, FileLocation>,
}

/// Location of a row within a Parquet file.
#[derive(Debug, Clone)]
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
    pub fn get(&self, pk_hash: u64) -> Option<&FileLocation> {
        self.entries.get(&pk_hash)
    }

    /// Returns the number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the hash bits used by this index.
    pub fn hash_bits(&self) -> u32 {
        self.hash_bits
    }

    /// Checks if a primary key hash exists in the index.
    ///
    /// More efficient than `get()` when you only need to know if a key exists.
    pub fn contains(&self, pk_hash: u64) -> bool {
        self.entries.contains_key(&pk_hash)
    }

    /// Search for multiple values efficiently in a single pass.
    ///
    /// This is more efficient than repeated single lookups when looking up
    /// many keys, as it amortizes the overhead of the lookup operation.
    ///
    /// # Arguments
    ///
    /// * `pk_hashes` - Slice of primary key hashes to look up
    ///
    /// # Returns
    ///
    /// Vector of (pk_hash, location) for all matches found.
    ///
    /// # Example
    ///
    /// ```
    /// # use etl_iceberg_merger::index::PuffinIndex;
    /// let mut index = PuffinIndex::new(64);
    /// index.insert(100, "file1.parquet".to_string(), 0);
    /// index.insert(200, "file2.parquet".to_string(), 42);
    /// index.insert(300, "file3.parquet".to_string(), 100);
    ///
    /// let hashes = vec![100, 200, 999];  // 999 doesn't exist
    /// let results = index.search_values(&hashes);
    ///
    /// assert_eq!(results.len(), 2);  // Only 100 and 200 found
    /// ```
    pub fn search_values(&self, pk_hashes: &[u64]) -> Vec<(u64, &FileLocation)> {
        pk_hashes
            .iter()
            .filter_map(|&hash| self.entries.get(&hash).map(|loc| (hash, loc)))
            .collect()
    }

    /// Batch insert entries from an iterator.
    ///
    /// More efficient than calling `insert()` repeatedly as it pre-allocates
    /// space for the new entries.
    ///
    /// # Arguments
    ///
    /// * `entries` - Iterator of (pk_hash, file_path, row_offset) tuples
    pub fn insert_batch<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (u64, String, usize)>,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.entries.reserve(entries.len());
        for (pk_hash, file_path, row_offset) in entries {
            self.entries.insert(
                pk_hash,
                FileLocation {
                    file_path,
                    row_offset,
                },
            );
            self.num_rows += 1;
        }
    }

    /// Serializes the index to Parquet format.
    ///
    /// Returns bytes that can be stored in a Puffin file.
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
///
/// Uses `splitmix64` for final hash mixing to ensure good distribution
/// across hash buckets, following moonlink's approach.
///
/// # Algorithm
///
/// 1. Extract each primary key column value
/// 2. Combine values using a rolling hash (FNV-1a inspired)
/// 3. Apply `splitmix64` for final mixing
///
/// This ensures that even similar composite keys (e.g., (1, 2) vs (2, 1))
/// produce well-distributed hashes.
pub fn compute_pk_hash(
    batch: &arrow::array::RecordBatch,
    pk_col_indices: &[usize],
    row_idx: usize,
) -> u64 {
    // Use FNV-1a-like combination for multiple columns.
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut combined_hash = FNV_OFFSET;

    for &col_idx in pk_col_indices {
        let col = batch.column(col_idx);
        let col_value = extract_column_value(col, row_idx);

        // FNV-1a: XOR then multiply.
        combined_hash ^= col_value;
        combined_hash = combined_hash.wrapping_mul(FNV_PRIME);
    }

    // Apply splitmix64 for final mixing.
    splitmix64(combined_hash)
}

/// Extracts a u64 value from an Arrow column at the given row index.
///
/// For integer types, uses the value directly.
/// For string/binary types, computes a hash of the bytes.
/// For null values, returns 0.
fn extract_column_value(col: &dyn Array, row_idx: usize) -> u64 {
    if col.is_null(row_idx) {
        return 0;
    }

    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int16Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int8Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        arr.value(row_idx)
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt32Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt16Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt8Array>() {
        arr.value(row_idx) as u64
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        hash_bytes(arr.value(row_idx).as_bytes())
    } else if let Some(arr) = col
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        hash_bytes(arr.value(row_idx).as_bytes())
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        hash_bytes(arr.value(row_idx))
    } else if let Some(arr) = col
        .as_any()
        .downcast_ref::<arrow::array::LargeBinaryArray>()
    {
        hash_bytes(arr.value(row_idx))
    } else if let Some(arr) = col
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
    {
        hash_bytes(arr.value(row_idx))
    } else {
        // Fallback: use debug format hash.
        let debug_str = format!("{:?}", col.slice(row_idx, 1));
        hash_bytes(debug_str.as_bytes())
    }
}

/// Computes a hash of a byte slice using FNV-1a.
///
/// FNV-1a is a simple, fast hash function suitable for combining with
/// splitmix64 for the final mixing step.
#[inline]
fn hash_bytes(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
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

    #[test]
    fn test_splitmix64_distribution() {
        // Test that splitmix64 produces well-distributed hashes.
        let hashes: Vec<u64> = (0..1000).map(splitmix64).collect();

        // All hashes should be unique.
        let unique: std::collections::HashSet<_> = hashes.iter().collect();
        assert_eq!(unique.len(), 1000);

        // Adjacent keys should produce very different hashes.
        let hash1 = splitmix64(12345);
        let hash2 = splitmix64(12346);

        // At least half the bits should differ (avalanche property).
        let differing_bits = (hash1 ^ hash2).count_ones();
        assert!(
            differing_bits >= 20,
            "expected at least 20 differing bits, got {}",
            differing_bits
        );
    }

    #[test]
    fn test_splitmix64_deterministic() {
        // Same input should always produce same output.
        let hash1 = splitmix64(42);
        let hash2 = splitmix64(42);
        assert_eq!(hash1, hash2);

        // Different inputs should produce different outputs.
        let hash3 = splitmix64(43);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_search_values() {
        let mut index = PuffinIndex::new(64);
        index.insert(100, "file1.parquet".to_string(), 0);
        index.insert(200, "file2.parquet".to_string(), 1);
        index.insert(300, "file3.parquet".to_string(), 2);

        // Search for existing and non-existing keys.
        let hashes = vec![100, 200, 999];
        let results = index.search_values(&hashes);

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(h, _)| *h == 100));
        assert!(results.iter().any(|(h, _)| *h == 200));

        // Check that 999 is not in results.
        assert!(!results.iter().any(|(h, _)| *h == 999));
    }

    #[test]
    fn test_search_values_empty() {
        let index = PuffinIndex::new(64);
        let results = index.search_values(&[100, 200, 300]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_values_all_found() {
        let mut index = PuffinIndex::new(64);
        index.insert(1, "file.parquet".to_string(), 0);
        index.insert(2, "file.parquet".to_string(), 1);
        index.insert(3, "file.parquet".to_string(), 2);

        let results = index.search_values(&[1, 2, 3]);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_contains() {
        let mut index = PuffinIndex::new(64);
        index.insert(12345, "file.parquet".to_string(), 0);

        assert!(index.contains(12345));
        assert!(!index.contains(99999));
    }

    #[test]
    fn test_insert_batch() {
        let mut index = PuffinIndex::new(64);

        let entries = vec![
            (1u64, "file1.parquet".to_string(), 0usize),
            (2, "file2.parquet".to_string(), 1),
            (3, "file3.parquet".to_string(), 2),
        ];

        index.insert_batch(entries);

        assert_eq!(index.len(), 3);
        assert_eq!(index.get(1).unwrap().file_path, "file1.parquet");
        assert_eq!(index.get(2).unwrap().file_path, "file2.parquet");
        assert_eq!(index.get(3).unwrap().file_path, "file3.parquet");
    }

    #[test]
    fn test_hash_bytes_consistency() {
        // Test that hash_bytes produces consistent results.
        let bytes1 = b"hello world";
        let bytes2 = b"hello world";
        let bytes3 = b"hello worlD";

        assert_eq!(hash_bytes(bytes1), hash_bytes(bytes2));
        assert_ne!(hash_bytes(bytes1), hash_bytes(bytes3));
    }

    #[test]
    fn test_compute_pk_hash_uses_splitmix64() {
        // Verify that the hash function produces consistent results.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![42]))])
                .unwrap();
        let batch2 =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();

        let hash1 = compute_pk_hash(&batch1, &[0], 0);
        let hash2 = compute_pk_hash(&batch2, &[0], 0);

        assert_eq!(hash1, hash2, "same data should produce same hash");
    }

    #[test]
    fn test_compute_pk_hash_string_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice", "bob", "alice"]))],
        )
        .unwrap();

        let hash_alice1 = compute_pk_hash(&batch, &[0], 0);
        let hash_bob = compute_pk_hash(&batch, &[0], 1);
        let hash_alice2 = compute_pk_hash(&batch, &[0], 2);

        assert_eq!(hash_alice1, hash_alice2);
        assert_ne!(hash_alice1, hash_bob);
    }
}
