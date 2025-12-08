use sqlx::MySqlPool;
use thiserror::Error;

use crate::types::TableId;

/// Maximum length for a MySQL replication identifier in bytes.
const MAX_IDENTIFIER_LENGTH: usize = 64;

/// Prefixes for different types of replication identifiers.
pub const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";
pub const TABLE_SYNC_WORKER_PREFIX: &str = "supabase_etl_table_sync";

/// Error type for slot operations.
#[derive(Debug, Error)]
pub enum EtlReplicationSlotError {
    #[error("Invalid identifier length: {0}")]
    InvalidIdentifierLength(String),

    #[error("Invalid identifier name: {0}")]
    InvalidIdentifierName(String),
}

/// Parsed representation of a replication identifier name.
///
/// Note: MySQL doesn't have replication slots like PostgreSQL, but we maintain
/// similar naming conventions for consistency and future CDC integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlReplicationSlot {
    /// Apply worker identifier for a pipeline.
    Apply { pipeline_id: u64 },
    /// Table sync worker identifier for a pipeline and table.
    TableSync { pipeline_id: u64, table_id: TableId },
}

impl EtlReplicationSlot {
    /// Creates a new [`EtlReplicationSlot`] for the apply worker.
    pub fn for_apply_worker(pipeline_id: u64) -> Self {
        Self::Apply { pipeline_id }
    }

    /// Creates a new [`EtlReplicationSlot`] for the table sync worker.
    pub fn for_table_sync_worker(pipeline_id: u64, table_id: TableId) -> Self {
        Self::TableSync {
            pipeline_id,
            table_id,
        }
    }

    /// Returns the prefix of apply sync identifier for a pipeline.
    pub fn apply_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{APPLY_WORKER_PREFIX}_{pipeline_id}");

        if prefix.len() >= MAX_IDENTIFIER_LENGTH {
            return Err(EtlReplicationSlotError::InvalidIdentifierLength(prefix));
        }

        Ok(prefix)
    }

    /// Returns the prefix of table sync identifiers for a pipeline.
    pub fn table_sync_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{TABLE_SYNC_WORKER_PREFIX}_{pipeline_id}_");

        if prefix.len() >= MAX_IDENTIFIER_LENGTH {
            return Err(EtlReplicationSlotError::InvalidIdentifierLength(prefix));
        }

        Ok(prefix)
    }
}

impl TryFrom<&str> for EtlReplicationSlot {
    type Error = EtlReplicationSlotError;

    fn try_from(identifier: &str) -> Result<Self, Self::Error> {
        if let Some(rest) = identifier.strip_prefix(APPLY_WORKER_PREFIX) {
            let rest = rest
                .strip_prefix('_')
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;
            let pipeline_id: u64 = rest
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;

            return Ok(EtlReplicationSlot::for_apply_worker(pipeline_id));
        }

        if let Some(rest) = identifier.strip_prefix(TABLE_SYNC_WORKER_PREFIX) {
            let rest = rest
                .strip_prefix('_')
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;
            let mut parts = rest.rsplitn(2, '_');
            let table_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;
            let pipeline_id_str = parts
                .next()
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;

            let pipeline_id: u64 = pipeline_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;
            let table_oid: u32 = table_id_str
                .parse()
                .ok()
                .ok_or_else(|| EtlReplicationSlotError::InvalidIdentifierName(identifier.into()))?;

            return Ok(EtlReplicationSlot::for_table_sync_worker(
                pipeline_id,
                TableId::new(table_oid),
            ));
        }

        Err(EtlReplicationSlotError::InvalidIdentifierName(
            identifier.into(),
        ))
    }
}

impl TryFrom<EtlReplicationSlot> for String {
    type Error = EtlReplicationSlotError;

    fn try_from(slot: EtlReplicationSlot) -> Result<Self, Self::Error> {
        let identifier = match slot {
            EtlReplicationSlot::Apply { pipeline_id } => {
                format!("{APPLY_WORKER_PREFIX}_{pipeline_id}")
            }
            EtlReplicationSlot::TableSync {
                pipeline_id,
                table_id,
            } => {
                format!(
                    "{TABLE_SYNC_WORKER_PREFIX}_{pipeline_id}_{}",
                    table_id.into_inner()
                )
            }
        };

        if identifier.len() > MAX_IDENTIFIER_LENGTH {
            return Err(EtlReplicationSlotError::InvalidIdentifierLength(
                identifier,
            ));
        }

        Ok(identifier)
    }
}

/// Placeholder for MySQL CDC connection management.
///
/// Note: MySQL doesn't have built-in logical replication slots like PostgreSQL.
/// This function is provided for API compatibility but doesn't perform actual operations.
pub async fn delete_pipeline_replication_slots(
    _pool: &MySqlPool,
    _pipeline_id: u64,
    _table_ids: &[TableId],
) -> sqlx::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_worker_identifier_name() {
        let pipeline_id = 1;
        let result: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
            .try_into()
            .unwrap();

        assert!(result.starts_with(APPLY_WORKER_PREFIX));
        assert!(result.len() <= MAX_IDENTIFIER_LENGTH);
        assert_eq!(result, "supabase_etl_apply_1");
    }

    #[test]
    fn test_table_sync_worker_identifier_name() {
        let pipeline_id = 1;
        let table_id = TableId::new(12345);
        let result: String = EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id)
            .try_into()
            .unwrap();

        assert!(result.starts_with(TABLE_SYNC_WORKER_PREFIX));
        assert!(result.len() <= MAX_IDENTIFIER_LENGTH);
        assert_eq!(result, "supabase_etl_table_sync_1_12345");
    }

    #[test]
    fn test_parse_apply_worker_identifier() {
        let identifier = "supabase_etl_apply_42";
        let slot: EtlReplicationSlot = identifier.try_into().unwrap();

        assert_eq!(slot, EtlReplicationSlot::for_apply_worker(42));
    }

    #[test]
    fn test_parse_table_sync_worker_identifier() {
        let identifier = "supabase_etl_table_sync_42_67890";
        let slot: EtlReplicationSlot = identifier.try_into().unwrap();

        assert_eq!(
            slot,
            EtlReplicationSlot::for_table_sync_worker(42, TableId::new(67890))
        );
    }
}
