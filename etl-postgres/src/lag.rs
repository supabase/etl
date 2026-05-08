use std::collections::BTreeMap;

use crate::types::TableId;

/// Lag metrics associated with a logical replication slot.
#[derive(Debug, Clone)]
pub struct SlotLagMetrics {
    /// Bytes between the current WAL LSN and the slot restart LSN.
    pub restart_lsn_bytes: i64,
    /// Bytes between the current WAL LSN and the confirmed flush LSN.
    pub confirmed_flush_lsn_bytes: i64,
    /// Bytes that can be retained safely for the slot.
    pub safe_wal_size_bytes: i64,
    /// Replication write lag in milliseconds.
    pub write_lag_ms: Option<i64>,
    /// Replication flush lag in milliseconds.
    pub flush_lag_ms: Option<i64>,
}

/// Lag metrics for pipeline apply and table sync workers.
#[derive(Debug, Clone, Default)]
pub struct PipelineLagMetrics {
    /// Lag metrics for the apply worker slot.
    pub apply: Option<SlotLagMetrics>,
    /// Lag metrics for table sync worker slots by table ID.
    pub table_sync: BTreeMap<TableId, SlotLagMetrics>,
}
