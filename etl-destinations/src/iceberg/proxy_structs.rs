//! Proxy structs that match the internal memory layout of iceberg-rust types.
//!
//! These are used to work around limitations in the iceberg-rust crate that don't
//! expose certain fields or APIs needed for deletion vector support.
//!
//! **WARNING**: These structs must exactly match the internal layout of their
//! iceberg-rust counterparts. They may break with iceberg crate upgrades.
//! Always verify after updating the iceberg dependency.

use std::collections::{HashMap, HashSet};

use iceberg::puffin::{CompressionCodec, PuffinWriter};
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, Datum, Struct};
use iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Proxy for internal puffin Flag enum.
/// Must match iceberg::puffin::metadata::Flag (which is pub(crate))
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)]
enum FlagProxy {
    FooterPayloadCompressed = 0,
}

/// Proxy for iceberg::puffin::BlobMetadata
///
/// This struct captures the metadata about a blob written to a Puffin file,
/// including the critical `offset` and `length` fields that tell us where
/// the blob lives within the puffin file.
#[derive(Debug, Clone)]
pub struct PuffinBlobMetadataProxy {
    /// Blob type (e.g., "deletion-vector-v1")
    pub r#type: String,

    /// Field IDs this blob applies to
    pub fields: Vec<i32>,

    /// Snapshot ID (-1 if not known at write time)
    pub snapshot_id: i64,

    /// Sequence number (-1 if not known at write time)
    pub sequence_number: i64,

    /// Byte offset of the blob within the puffin file
    pub offset: u64,

    /// Size of the blob in bytes
    pub length: u64,

    /// Compression codec used for this blob
    pub compression_codec: CompressionCodec,

    /// Blob properties (e.g., "cardinality", "referenced-data-file")
    pub properties: HashMap<String, String>,
}

/// Proxy for iceberg::puffin::PuffinWriter internal structure.
///
/// Used to extract `written_blobs_metadata` which is not exposed publicly.
#[allow(dead_code)]
struct PuffinWriterProxy {
    writer: Box<dyn iceberg::io::FileWrite>,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<PuffinBlobMetadataProxy>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: HashSet<FlagProxy>,
}

/// Extract blob metadata from a PuffinWriter and close it.
///
/// This is the key workaround - iceberg-rust's PuffinWriter doesn't expose
/// the blob metadata (offset/length) after writing, but we need it to create
/// proper manifest entries for deletion vectors.
///
/// # Safety
///
/// This uses `std::mem::transmute` to reinterpret the PuffinWriter's memory
/// as our proxy struct. This is unsafe and depends on the struct layouts
/// matching exactly.
pub async fn get_puffin_metadata_and_close(
    puffin_writer: PuffinWriter,
) -> iceberg::Result<Vec<PuffinBlobMetadataProxy>> {
    // Transmute to access internal fields
    let puffin_writer_proxy =
        unsafe { std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer) };

    // Clone the metadata we need
    let puffin_metadata = puffin_writer_proxy.written_blobs_metadata.clone();

    // Transmute back so we can properly close the writer
    let puffin_writer =
        unsafe { std::mem::transmute::<PuffinWriterProxy, PuffinWriter>(puffin_writer_proxy) };

    // Close the writer to flush and finalize the puffin file
    puffin_writer.close().await?;

    Ok(puffin_metadata)
}

/// Proxy for iceberg::spec::DataFile
///
/// The iceberg-rust DataFile builder doesn't expose all fields needed for
/// deletion vectors, specifically:
/// - `referenced_data_file`: Which data file this deletion vector applies to
/// - `content_offset`: Byte offset of the blob in the puffin file
/// - `content_size_in_bytes`: Size of the blob
///
/// We create a proxy struct and transmute it to DataFile.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct DataFileProxy {
    /// Type of content: Data, EqualityDeletes, or PositionDeletes
    pub content: DataContentType,

    /// Full URI for the file (puffin file path for deletion vectors)
    pub file_path: String,

    /// File format: Parquet, Avro, ORC, or Puffin
    pub file_format: DataFileFormat,

    /// Partition data tuple
    pub partition: Struct,

    /// Number of records (cardinality for deletion vectors)
    pub record_count: u64,

    /// Total file size in bytes
    pub file_size_in_bytes: u64,

    /// Map from column id to size on disk
    pub column_sizes: HashMap<i32, u64>,

    /// Map from column id to value count
    pub value_counts: HashMap<i32, u64>,

    /// Map from column id to null value count
    pub null_value_counts: HashMap<i32, u64>,

    /// Map from column id to NaN value count
    pub nan_value_counts: HashMap<i32, u64>,

    /// Map from column id to lower bound
    pub lower_bounds: HashMap<i32, Datum>,

    /// Map from column id to upper bound
    pub upper_bounds: HashMap<i32, Datum>,

    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,

    /// Split offsets (e.g., row group offsets in Parquet)
    pub split_offsets: Option<Vec<i64>>,

    /// Field ids for equality in equality delete files
    pub equality_ids: Option<Vec<i32>>,

    /// Sort order ID
    pub sort_order_id: Option<i32>,

    /// First row ID (for row-level operations)
    pub first_row_id: Option<i64>,

    /// Partition spec ID
    pub partition_spec_id: i32,

    /// **DELETION VECTOR FIELD**: Path to the data file this DV references
    pub referenced_data_file: Option<String>,

    /// **DELETION VECTOR FIELD**: Byte offset of the blob in the puffin file
    pub content_offset: Option<i64>,

    /// **DELETION VECTOR FIELD**: Size of the blob in bytes
    pub content_size_in_bytes: Option<i64>,
}

impl DataFileProxy {
    /// Convert this proxy to an actual iceberg DataFile.
    ///
    /// # Safety
    ///
    /// This uses `std::mem::transmute` which requires the struct layouts
    /// to match exactly.
    pub fn into_data_file(self) -> DataFile {
        unsafe { std::mem::transmute::<DataFileProxy, DataFile>(self) }
    }

    /// Create a DataFileProxy for a deletion vector puffin blob.
    pub fn for_deletion_vector(
        puffin_file_path: &str,
        blob_metadata: &PuffinBlobMetadataProxy,
        referenced_data_file: &str,
    ) -> Self {
        let cardinality: u64 = blob_metadata
            .properties
            .get(super::deletion_vector::DELETION_VECTOR_CARDINALITY)
            .expect("Deletion vector blob must have cardinality property")
            .parse()
            .expect("Cardinality must be a valid u64");

        DataFileProxy {
            content: DataContentType::PositionDeletes,
            file_path: puffin_file_path.to_string(),
            file_format: DataFileFormat::Puffin,
            partition: Struct::empty(),
            record_count: cardinality,
            file_size_in_bytes: 0, // Not used for puffin blobs
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: Some(referenced_data_file.to_string()),
            content_offset: Some(blob_metadata.offset as i64),
            content_size_in_bytes: Some(blob_metadata.length as i64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_proxy_size() {
        // Sanity check - if this fails, the struct layout has changed
        // and we need to update our proxy
        println!(
            "DataFileProxy size: {} bytes",
            std::mem::size_of::<DataFileProxy>()
        );
        println!("DataFile size: {} bytes", std::mem::size_of::<DataFile>());

        // These should be equal for transmute to work safely
        assert_eq!(
            std::mem::size_of::<DataFileProxy>(),
            std::mem::size_of::<DataFile>(),
            "DataFileProxy and DataFile must have the same size!"
        );
    }

    #[test]
    fn test_table_commit_proxy_size() {
        // Sanity check - if this fails, the struct layout has changed
        // and we need to update our proxy
        println!(
            "TableCommitProxy size: {} bytes",
            std::mem::size_of::<TableCommitProxy>()
        );
        println!(
            "TableCommit size: {} bytes",
            std::mem::size_of::<TableCommit>()
        );

        // These should be equal for transmute to work safely
        assert_eq!(
            std::mem::size_of::<TableCommitProxy>(),
            std::mem::size_of::<TableCommit>(),
            "TableCommitProxy and TableCommit must have the same size!"
        );
    }
}

/// Proxy for iceberg::TableCommit.
///
/// The iceberg-rust `TableCommit` struct has a private builder (the `build()` method
/// is marked `pub(crate)`), which means we cannot construct it from outside the crate.
/// This is intentional - iceberg-rust wants users to go through the Transaction API.
///
/// However, the Transaction API does not support deletion vectors. We need to bypass
/// it and construct `TableCommit` directly to commit deletion vector snapshots.
///
/// The struct fields must match the exact order and types of `TableCommit`:
/// - ident: TableIdent
/// - requirements: Vec<TableRequirement>
/// - updates: Vec<TableUpdate>
#[derive(Debug)]
pub struct TableCommitProxy {
    /// The table identifier.
    pub ident: TableIdent,
    /// The requirements that must be met for the commit to succeed.
    pub requirements: Vec<TableRequirement>,
    /// The updates to apply to the table.
    pub updates: Vec<TableUpdate>,
}

impl TableCommitProxy {
    /// Creates a new `TableCommitProxy` with the given identifier, updates, and requirements.
    pub fn new(
        ident: TableIdent,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> Self {
        Self {
            ident,
            requirements,
            updates,
        }
    }

    /// Convert this proxy to an actual iceberg `TableCommit`.
    ///
    /// # Safety
    ///
    /// This uses `std::mem::transmute` which requires the struct layouts
    /// to match exactly.
    pub fn into_table_commit(self) -> TableCommit {
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(self) }
    }
}
