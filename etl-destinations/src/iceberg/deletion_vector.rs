//! Deletion vector support for Apache Iceberg.
//!
//! This module provides functionality to write deletion vectors to Iceberg tables,
//! allowing rows to be marked as deleted without rewriting data files. Deletion
//! vectors use Puffin files with RoaringTreemap bitmaps for efficient storage.
//!
//! The binary format follows the Iceberg spec:
//! ```text
//! | length (4 bytes, big-endian) | magic (4 bytes) | roaring bitmap | crc32c (4 bytes, big-endian) |
//! ```

use std::collections::HashMap;

use iceberg::{
    Catalog,
    io::FileIO,
    puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter},
    spec::{DataContentType, DataFile},
    table::Table,
};
use roaring::RoaringTreemap;

use super::manifest::commit_deletion_vectors_impl;
use super::proxy_structs::{DataFileProxy, PuffinBlobMetadataProxy, get_puffin_metadata_and_close};

/// Magic bytes that identify a deletion vector blob.
/// These bytes appear after the length field in the serialized format.
pub const DELETION_VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Minimum size of a serialized deletion vector blob:
/// 4 bytes (length) + 4 bytes (magic) + 4 bytes (crc32c)
pub const MIN_SERIALIZED_SIZE: usize = 12;

/// Property key for the cardinality (number of deleted rows).
pub const DELETION_VECTOR_CARDINALITY: &str = "cardinality";

/// Property key for the referenced data file path.
pub const DELETION_VECTOR_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// A deletion vector that tracks deleted row positions using a Roaring bitmap.
///
/// The Roaring bitmap provides efficient storage for sparse and dense bitmaps,
/// making it ideal for tracking deleted rows in large data files.
#[derive(Debug)]
pub struct DeletionVector {
    /// Roaring bitmap storing the deleted row indices
    bitmap: RoaringTreemap,
}

impl DeletionVector {
    /// Creates a new empty deletion vector.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    /// Returns the number of deleted rows.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Checks if a row is deleted.
    pub fn is_deleted(&self, row_idx: u64) -> bool {
        self.bitmap.contains(row_idx)
    }

    /// Marks a single row as deleted.
    pub fn delete_row(&mut self, row_idx: u64) {
        self.bitmap.insert(row_idx);
    }

    /// Marks multiple rows as deleted.
    ///
    /// **Important**: For best performance, rows should be in ascending order.
    /// The `append` method is used which requires sorted input.
    pub fn delete_rows(&mut self, rows: Vec<u64>) {
        // append() requires sorted input and is more efficient than individual inserts
        let count = rows.len();
        let appended = self.bitmap.append(rows).expect("Rows must be sorted");
        assert_eq!(
            appended as usize, count,
            "All rows should be appended (check for duplicates or unsorted input)"
        );
    }

    /// Returns all deleted row indices in ascending order.
    pub fn deleted_rows(&self) -> Vec<u64> {
        self.bitmap.iter().collect()
    }

    /// Serializes the deletion vector to a Puffin blob.
    ///
    /// The binary format follows the Iceberg spec:
    /// ```text
    /// | length (4 bytes, big-endian) | magic (4 bytes) | roaring bitmap | crc32c (4 bytes, big-endian) |
    /// ```
    ///
    /// Where:
    /// - `length` = size of (magic + roaring bitmap)
    /// - `magic` = [0xD1, 0xD3, 0x39, 0x64]
    /// - `roaring bitmap` = standard roaring bitmap serialization
    /// - `crc32c` = checksum of (magic + roaring bitmap)
    pub fn serialize(&self, referenced_data_file: &str) -> Blob {
        let serialized_bitmap_size = self.bitmap.serialized_size();
        let combined_length = (DELETION_VECTOR_MAGIC.len() + serialized_bitmap_size) as u32;

        // Total size: length(4) + magic(4) + bitmap(var) + crc(4)
        let total_size = 4 + 4 + serialized_bitmap_size + 4;
        let mut data = Vec::with_capacity(total_size);

        // Write length (big-endian)
        data.extend_from_slice(&combined_length.to_be_bytes());

        // Write magic bytes
        data.extend_from_slice(&DELETION_VECTOR_MAGIC);

        // Write serialized bitmap
        let bitmap_start = data.len();
        data.resize(bitmap_start + serialized_bitmap_size, 0);
        let mut cursor = std::io::Cursor::new(&mut data[bitmap_start..]);
        self.bitmap
            .serialize_into(&mut cursor)
            .expect("Bitmap serialization should not fail");

        // Calculate CRC32C of (magic + bitmap)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data[4..]); // Skip length field
        let crc = hasher.finalize();

        // Write CRC (big-endian)
        data.extend_from_slice(&crc.to_be_bytes());

        // Create blob properties
        let mut properties = HashMap::new();
        properties.insert(
            DELETION_VECTOR_CARDINALITY.to_string(),
            self.cardinality().to_string(),
        );
        properties.insert(
            DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
            referenced_data_file.to_string(),
        );

        // Build the Puffin blob
        // Note: snapshot_id and sequence_number are -1 because they're not known
        // at blob creation time (they're set when the snapshot is committed)
        Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(properties)
            .build()
    }

    /// Deserializes a deletion vector from a Puffin blob.
    pub fn deserialize(blob: Blob) -> iceberg::Result<Self> {
        let data = blob.data();

        // Validate minimum size
        if data.len() < MIN_SERIALIZED_SIZE {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Deletion vector blob too small: {} bytes (minimum {})",
                    data.len(),
                    MIN_SERIALIZED_SIZE
                ),
            ));
        }

        // Validate magic bytes
        let magic = &data[4..8];
        if magic != DELETION_VECTOR_MAGIC {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "Invalid magic bytes in deletion vector blob",
            ));
        }

        // Validate length field
        let combined_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let expected_total = 4 + combined_length + 4; // length + (magic + bitmap) + crc
        if data.len() != expected_total {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Length mismatch: expected {} bytes, got {}",
                    expected_total,
                    data.len()
                ),
            ));
        }

        // Extract bitmap data (between magic and CRC)
        let bitmap_data = &data[8..data.len() - 4];

        // Validate CRC32C
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data[4..data.len() - 4]); // magic + bitmap
        let computed_crc = hasher.finalize();

        let stored_crc = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);

        if computed_crc != stored_crc {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "CRC mismatch: computed {}, stored {}",
                    computed_crc, stored_crc
                ),
            ));
        }

        // Deserialize the bitmap
        let bitmap = RoaringTreemap::deserialize_from(bitmap_data).map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Failed to deserialize roaring bitmap: {}", e),
            )
        })?;

        Ok(Self { bitmap })
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata describing a written deletion vector Puffin file.
///
/// This struct contains all the information needed to reference the deletion
/// vector in a DataFile entry, including the file path and the offset/length
/// of the blob within the Puffin file.
#[derive(Debug, Clone)]
pub struct DeletionVectorDescriptor {
    /// Path to the Puffin file containing the deletion vector.
    pub puffin_path: String,
    /// Total size of the Puffin file in bytes.
    pub file_size: u64,
    /// Byte offset of the deletion vector blob within the Puffin file.
    pub content_offset: i64,
    /// Size of the deletion vector blob in bytes.
    pub content_size_in_bytes: i64,
    /// Number of deleted rows (cardinality).
    pub cardinality: u64,
    /// The blob metadata extracted from PuffinWriter.
    pub blob_metadata: PuffinBlobMetadataProxy,
}

/// Writes a deletion vector to a Puffin file.
///
/// This function creates a DeletionVector from the given row positions,
/// serializes it following the Iceberg spec (magic bytes + RoaringTreemap + CRC32C),
/// writes it to a new Puffin file with Zstd compression, and returns metadata
/// about the written file.
///
/// # Arguments
///
/// * `file_io` - The FileIO instance for writing files.
/// * `table_location` - The base location of the Iceberg table.
/// * `row_positions` - Row positions to mark as deleted (0-indexed, must be sorted).
/// * `referenced_data_file` - Path to the data file these deletions apply to.
///
/// # Returns
///
/// A `DeletionVectorDescriptor` containing metadata about the written Puffin file.
pub async fn write_deletion_vector(
    file_io: &FileIO,
    table_location: &str,
    row_positions: &[u64],
    referenced_data_file: &str,
) -> Result<DeletionVectorDescriptor, iceberg::Error> {
    // Create deletion vector and populate with row positions
    let mut dv = DeletionVector::new();
    dv.delete_rows(row_positions.to_vec());

    // Serialize to Puffin blob with proper format (magic + bitmap + crc)
    let blob = dv.serialize(referenced_data_file);
    let cardinality = dv.cardinality();

    // Generate unique path for the Puffin file
    let puffin_path = format!(
        "{}/data/delete-{}.puffin",
        table_location,
        uuid::Uuid::new_v4()
    );

    let output_file = file_io.new_output(&puffin_path)?;

    // Write the Puffin file with Zstd compression
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;
    writer.add(blob, CompressionCodec::Zstd).await?;

    // Extract blob metadata using the proxy workaround and close the writer
    let blob_metadata_list = get_puffin_metadata_and_close(writer).await?;
    let blob_metadata = blob_metadata_list
        .into_iter()
        .next()
        .ok_or_else(|| iceberg::Error::new(iceberg::ErrorKind::Unexpected, "No blob metadata"))?;

    // Get file size
    let input_file = output_file.to_input_file();
    let file_size = input_file.metadata().await?.size;

    Ok(DeletionVectorDescriptor {
        puffin_path,
        file_size,
        content_offset: blob_metadata.offset as i64,
        content_size_in_bytes: blob_metadata.length as i64,
        cardinality,
        blob_metadata,
    })
}

/// Creates a DataFile entry for a deletion vector.
///
/// This function creates a DataFile that references the deletion vector in
/// a Puffin file. The DataFile uses the proxy struct approach to ensure all
/// deletion vector fields are properly set.
///
/// # Arguments
///
/// * `descriptor` - Metadata about the Puffin file containing the deletion vector.
/// * `referenced_data_file` - Full path to the data file these deletions apply to.
///
/// # Returns
///
/// A `DataFile` entry that can be added to a transaction.
pub fn create_deletion_vector_data_file(
    descriptor: &DeletionVectorDescriptor,
    referenced_data_file: &str,
) -> DataFile {
    DataFileProxy::for_deletion_vector(
        &descriptor.puffin_path,
        &descriptor.blob_metadata,
        referenced_data_file,
    )
    .into_data_file()
}

/// Commits deletion vector files to an Iceberg table.
///
/// This function commits the deletion vectors to the table by directly writing
/// manifest files and creating a new snapshot, bypassing the Transaction API
/// which does not support deletion vectors (position delete files in Puffin format).
///
/// The implementation:
/// 1. Creates a new manifest containing the deletion vector entries.
/// 2. Writes a new manifest list preserving existing manifests.
/// 3. Creates a new snapshot referencing the manifest list.
/// 4. Commits the snapshot to the catalog using `update_table`.
///
/// # Arguments
///
/// * `table` - The Iceberg table to commit deletions to.
/// * `catalog` - The catalog to commit the transaction to.
/// * `delete_files` - The DataFile entries for the deletion vectors.
///
/// # Returns
///
/// The updated table after the commit, or an error if the commit fails.
pub async fn commit_deletion_vectors(
    table: &Table,
    catalog: &dyn Catalog,
    delete_files: Vec<DataFile>,
) -> Result<Table, iceberg::Error> {
    if delete_files.is_empty() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "No deletion vector files to commit",
        ));
    }

    // Verify all files are PositionDeletes.
    for file in &delete_files {
        if file.content_type() != DataContentType::PositionDeletes {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Expected PositionDeletes content type, got {:?}",
                    file.content_type()
                ),
            ));
        }
    }

    // Delegate to the manifest-based implementation.
    commit_deletion_vectors_impl(table, catalog, delete_files).await
}

/// Retrieves all data file paths from an Iceberg table's current snapshot.
///
/// This function scans the table's manifest files to extract all data file
/// paths. This is useful for testing to determine which data files to
/// reference when creating deletion vectors.
///
/// # Arguments
///
/// * `table` - The Iceberg table to scan.
///
/// # Returns
///
/// A vector of data file paths, or an empty vector if the table has no snapshot.
pub async fn get_data_file_paths(table: &Table) -> Result<Vec<String>, iceberg::Error> {
    let Some(current_snapshot) = table.metadata().current_snapshot() else {
        return Ok(vec![]);
    };

    let manifest_list = current_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let mut data_file_paths = Vec::new();

    for manifest_entry in manifest_list.entries() {
        let manifest = manifest_entry.load_manifest(table.file_io()).await?;
        let (manifest_entries, _) = manifest.into_parts();

        for entry in manifest_entries {
            // Only include data files (not delete files)
            if entry.content_type() == DataContentType::Data {
                data_file_paths.push(entry.file_path().to_string());
            }
        }
    }

    Ok(data_file_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::puffin::DELETION_VECTOR_V1;

    #[test]
    fn test_empty_deletion_vector() {
        let dv = DeletionVector::new();
        assert_eq!(dv.cardinality(), 0);
        assert!(dv.deleted_rows().is_empty());
    }

    #[test]
    fn test_delete_rows() {
        let mut dv = DeletionVector::new();
        dv.delete_rows(vec![1, 5, 10, 100, 1000]);

        assert_eq!(dv.cardinality(), 5);
        assert!(dv.is_deleted(1));
        assert!(dv.is_deleted(5));
        assert!(dv.is_deleted(10));
        assert!(dv.is_deleted(100));
        assert!(dv.is_deleted(1000));
        assert!(!dv.is_deleted(0));
        assert!(!dv.is_deleted(2));
        assert!(!dv.is_deleted(999));
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut dv = DeletionVector::new();
        let deleted = vec![0, 1, 5, 42, 100, 999, 10000];
        dv.delete_rows(deleted.clone());

        let blob = dv.serialize("s3://bucket/data/file-001.parquet");

        // Check blob type
        assert_eq!(blob.blob_type(), DELETION_VECTOR_V1);

        // Check properties
        assert_eq!(
            blob.properties().get(DELETION_VECTOR_CARDINALITY),
            Some(&"7".to_string())
        );
        assert_eq!(
            blob.properties().get(DELETION_VECTOR_REFERENCED_DATA_FILE),
            Some(&"s3://bucket/data/file-001.parquet".to_string())
        );

        // Deserialize and verify
        let dv2 = DeletionVector::deserialize(blob).unwrap();
        assert_eq!(dv2.cardinality(), 7);
        assert_eq!(dv2.deleted_rows(), deleted);
    }

    #[test]
    fn test_serialize_empty() {
        let dv = DeletionVector::new();
        let blob = dv.serialize("test://file");

        let dv2 = DeletionVector::deserialize(blob).unwrap();
        assert_eq!(dv2.cardinality(), 0);
    }

    #[test]
    fn test_magic_bytes_present() {
        let mut dv = DeletionVector::new();
        dv.delete_row(42);

        let blob = dv.serialize("test://file");
        let data = blob.data();

        // Check magic bytes are at offset 4
        assert_eq!(&data[4..8], &DELETION_VECTOR_MAGIC);
    }
}
