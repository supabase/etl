use thiserror::Error;
use tokio_postgres::types::Oid;

use crate::types::TableId;

/// Maximum length for a Postgres replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Prefix for apply-worker replication slots.
pub const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";

/// Prefix for table-sync replication slots.
pub const TABLE_SYNC_WORKER_PREFIX: &str = "supabase_etl_table_sync";

/// Error type for slot operations.
#[derive(Debug, Error)]
pub enum EtlReplicationSlotError {
    /// Slot name exceeds Postgres identifier length.
    #[error("Invalid slot name length: {0}")]
    InvalidSlotNameLength(String),

    /// Slot name does not match an ETL slot pattern.
    #[error("Invalid slot name: {0}")]
    InvalidSlotName(String),
}

/// Parsed representation of a replication slot name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlReplicationSlot {
    /// Apply worker slot for a pipeline.
    Apply { pipeline_id: u64 },
    /// Table sync worker slot for a pipeline and table.
    TableSync { pipeline_id: u64, table_id: TableId },
}

impl EtlReplicationSlot {
    /// Creates a new [`EtlReplicationSlot`] for the apply worker.
    #[must_use]
    pub fn for_apply_worker(pipeline_id: u64) -> Self {
        Self::Apply { pipeline_id }
    }

    /// Creates a new [`EtlReplicationSlot`] for the table sync worker.
    #[must_use]
    pub fn for_table_sync_worker(pipeline_id: u64, table_id: TableId) -> Self {
        Self::TableSync { pipeline_id, table_id }
    }

    /// Returns the apply-worker slot prefix for a pipeline.
    pub fn apply_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{APPLY_WORKER_PREFIX}_{pipeline_id}");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }

    /// Returns the table-sync slot prefix for a pipeline.
    pub fn table_sync_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{TABLE_SYNC_WORKER_PREFIX}_{pipeline_id}_");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }
}

impl TryFrom<&str> for EtlReplicationSlot {
    type Error = EtlReplicationSlotError;

    fn try_from(slot_name: &str) -> Result<Self, Self::Error> {
        if let Some(rest) = slot_name.strip_prefix(APPLY_WORKER_PREFIX) {
            let rest = rest
                .strip_prefix('_')
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let pipeline_id: u64 = rest
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            return Ok(EtlReplicationSlot::for_apply_worker(pipeline_id));
        }

        if let Some(rest) = slot_name.strip_prefix(TABLE_SYNC_WORKER_PREFIX) {
            let rest = rest
                .strip_prefix('_')
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let mut parts = rest.rsplitn(2, '_');
            let table_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let pipeline_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            let pipeline_id: u64 = pipeline_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;
            let table_oid: Oid = table_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidSlotName(slot_name.into()))?;

            return Ok(EtlReplicationSlot::for_table_sync_worker(
                pipeline_id,
                TableId::new(table_oid),
            ));
        }

        Err(EtlReplicationSlotError::InvalidSlotName(slot_name.into()))
    }
}

impl TryFrom<EtlReplicationSlot> for String {
    type Error = EtlReplicationSlotError;

    fn try_from(slot: EtlReplicationSlot) -> Result<Self, Self::Error> {
        let slot_name = match slot {
            EtlReplicationSlot::Apply { pipeline_id } => {
                format!("{APPLY_WORKER_PREFIX}_{pipeline_id}")
            }
            EtlReplicationSlot::TableSync { pipeline_id, table_id } => {
                format!("{TABLE_SYNC_WORKER_PREFIX}_{pipeline_id}_{}", table_id.into_inner())
            }
        };

        if slot_name.len() > MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(slot_name));
        }

        Ok(slot_name)
    }
}
