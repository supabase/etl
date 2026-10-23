//! Shared table-selection policy for DuckLake data-file rewrites.

/// Matches DuckLake's default `rewrite_delete_threshold`.
const REWRITE_DELETED_ROW_RATIO_THRESHOLD: f64 = 0.95;

/// Storage pressure that warrants rewriting a table's data files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataFileRewriteReason {
    /// The table exceeds the configured active data-file count.
    ActiveDataFiles,
    /// More than 95% of the rows in active data files have been deleted.
    DeletedRowRatio,
}

impl DataFileRewriteReason {
    /// Selects a table by file count or deletion pressure, excluding tables
    /// without files.
    pub fn for_table(
        active_data_files: i64,
        deleted_row_ratio: f64,
        min_active_data_files: i64,
    ) -> Option<Self> {
        if active_data_files <= 0 {
            None
        } else if active_data_files > min_active_data_files {
            Some(Self::ActiveDataFiles)
        } else if deleted_row_ratio > REWRITE_DELETED_ROW_RATIO_THRESHOLD {
            Some(Self::DeletedRowRatio)
        } else {
            None
        }
    }

    /// Returns the stable reason label used by maintenance logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ActiveDataFiles => "active_data_files_threshold",
            Self::DeletedRowRatio => "deleted_row_ratio_threshold",
        }
    }
}
