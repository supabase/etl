//! Puffin-based secondary index for changelog deduplication.
//!
//! Implements a secondary index structure stored in Puffin files that maps
//! primary key hashes to file locations for efficient deduplication during merge.
//! Adapted from moonlink's GlobalIndex design but using Iceberg FileIO instead
//! of memory-mapped local files.

#![allow(dead_code)]

use anyhow::Context;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
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
    hash_bits: u32,

    /// Total number of rows indexed.
    num_rows: usize,

    /// Map from primary key hash to file location.
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

    /// Builds an index by scanning changelog Parquet files.
    ///
    /// This method would scan the changelog files and create PK hash mappings.
    /// The actual implementation depends on how the changelog is structured.
    pub async fn build_from_changelog(
        &self,
        _changelog_files: Vec<String>,
        _primary_keys: &[String],
    ) -> anyhow::Result<PuffinIndex> {
        // Placeholder implementation
        // In a real implementation, this would:
        // 1. Scan each Parquet file in changelog_files
        // 2. For each row, compute hash of primary key columns
        // 3. Store hash -> (file_path, row_offset) mapping
        // 4. Return the populated index

        Ok(PuffinIndex::new(self.hash_bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
