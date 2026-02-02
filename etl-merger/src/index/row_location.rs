//! Row location tracking for the secondary index.

/// Identifies the physical location of a row in the mirror table.
///
/// This is used by the secondary index to quickly locate rows for
/// UPDATE and DELETE operations without scanning the entire table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLocation {
    /// The data file path containing this row.
    pub data_file_path: String,

    /// The row index within the data file (0-based).
    pub row_index: u64,

    /// The sequence number when this row was written.
    /// Used for ordering and conflict resolution.
    pub sequence_number: String,
}

impl RowLocation {
    /// Creates a new row location.
    pub fn new(data_file_path: String, row_index: u64, sequence_number: String) -> Self {
        Self {
            data_file_path,
            row_index,
            sequence_number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_location_creation() {
        let loc = RowLocation::new(
            "s3://bucket/data/file-001.parquet".to_string(),
            42,
            "0000000000000010/0000000000000005".to_string(),
        );

        assert_eq!(loc.data_file_path, "s3://bucket/data/file-001.parquet");
        assert_eq!(loc.row_index, 42);
        assert_eq!(loc.sequence_number, "0000000000000010/0000000000000005");
    }

    #[test]
    fn test_row_location_equality() {
        let loc1 = RowLocation::new("file1.parquet".to_string(), 0, "seq1".to_string());
        let loc2 = RowLocation::new("file1.parquet".to_string(), 0, "seq1".to_string());
        let loc3 = RowLocation::new("file1.parquet".to_string(), 1, "seq1".to_string());

        assert_eq!(loc1, loc2);
        assert_ne!(loc1, loc3);
    }
}
