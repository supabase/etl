//! Merge progress tracking for resumability.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Tracks merge progress for resumability.
///
/// The sequence_number format is "{commit_lsn:016x}/{start_lsn:016x}"
/// which allows lexicographic ordering of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeProgress {
    /// Table identifier (namespace.table_name).
    pub table_id: String,

    /// Last successfully processed sequence number.
    pub last_sequence_number: Option<String>,

    /// Total events processed in this session.
    pub events_processed: u64,

    /// Last merge timestamp.
    pub last_merge_time: DateTime<Utc>,
}

impl MergeProgress {
    /// Creates a new progress tracker for a table.
    pub fn new(table_id: String) -> Self {
        Self {
            table_id,
            last_sequence_number: None,
            events_processed: 0,
            last_merge_time: Utc::now(),
        }
    }

    /// Creates a new progress tracker starting from a checkpoint.
    pub fn from_checkpoint(table_id: String, checkpoint: String) -> Self {
        Self {
            table_id,
            last_sequence_number: Some(checkpoint),
            events_processed: 0,
            last_merge_time: Utc::now(),
        }
    }

    /// Updates progress after successfully merging a batch.
    pub fn update(&mut self, last_sequence_number: String, batch_size: u64) {
        self.last_sequence_number = Some(last_sequence_number);
        self.events_processed += batch_size;
        self.last_merge_time = Utc::now();
    }

    /// Checks if a sequence number is newer than the last processed.
    pub fn is_newer_than(&self, sequence_number: &str) -> bool {
        match &self.last_sequence_number {
            Some(last) => sequence_number > last.as_str(),
            None => true, // No progress yet, everything is new
        }
    }
}

impl Default for MergeProgress {
    fn default() -> Self {
        Self::new("unknown".to_string())
    }
}
