//! Snapshot file encoding primitives shared by destinations.

pub mod avro;

use etl::{data::TableRow, error::EtlResult};

/// Snapshot file format for destination initial-copy files.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum SnapshotFormat {
    /// Avro object container files.
    #[default]
    Avro,
    /// Parquet files.
    Parquet,
}

impl SnapshotFormat {
    /// Returns the file extension used in object names.
    pub fn file_extension(self) -> &'static str {
        match self {
            Self::Avro => "avro",
            Self::Parquet => "parquet",
        }
    }

    /// Returns the upload content type used for staged files.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Avro => "application/avro",
            Self::Parquet => "application/vnd.apache.parquet",
        }
    }
}

/// Upload body produced by a snapshot file encoder.
#[derive(Clone)]
pub enum UploadBody {
    /// In-memory bytes for small files, tests, and mocks.
    Bytes(Vec<u8>),
    /// Local file path for bounded temp-file based uploads.
    File(std::path::PathBuf),
}

/// One batch of snapshot rows passed to a file encoder.
pub struct SnapshotBatch {
    /// Rows in source table column order.
    pub rows: Vec<TableRow>,
}

/// Completed snapshot file ready to stage in object storage.
#[derive(Clone)]
pub struct CompletedSnapshotFile {
    /// Snapshot file format.
    pub format: SnapshotFormat,
    /// Number of rows encoded in the file.
    pub row_count: u64,
    /// Estimated or exact file size in bytes.
    pub size_bytes: u64,
    /// Encoded data body.
    pub body: UploadBody,
}

/// Encodes source snapshot rows into load-job compatible files.
pub trait SnapshotFileEncoder {
    /// Writes one batch of rows to the encoder.
    fn write_batch(&mut self, batch: SnapshotBatch) -> EtlResult<()>;

    /// Returns the current estimated encoded byte count.
    fn estimated_bytes(&self) -> usize;

    /// Finishes the file and returns the uploadable artifact.
    fn finish(self) -> EtlResult<CompletedSnapshotFile>;
}
