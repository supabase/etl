/// Placeholder module for MySQL replication state.
///
/// This module provides compatibility with the PostgreSQL implementation
/// but adapts the concepts for MySQL's binlog-based replication.

pub struct ReplicationState {
    /// The current binlog file name.
    pub binlog_file: String,
    /// The current position in the binlog file.
    pub binlog_position: u64,
}

impl ReplicationState {
    pub fn new(binlog_file: String, binlog_position: u64) -> Self {
        Self {
            binlog_file,
            binlog_position,
        }
    }
}
