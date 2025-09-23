use sqlx::PgPool;
use thiserror::Error;
use tokio_postgres::types::Oid;

use crate::types::TableId;

/// Maximum length for a Postgres replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Prefixes for different types of replication slots
pub const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";
pub const TABLE_SYNC_WORKER_PREFIX: &str = "supabase_etl_table_sync";

/// Error type for slot operations.
#[derive(Debug, Error)]
pub enum EtlReplicationSlotError {
    #[error("Invalid slot name length: {0}")]
    InvalidSlotNameLength(String),

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
    pub fn for_apply_worker(pipeline_id: u64) -> Self {
        Self::Apply { pipeline_id }
    }

    pub fn for_table_sync_worker(pipeline_id: u64, table_id: TableId) -> Self {
        Self::TableSync {
            pipeline_id,
            table_id,
        }
    }

    /// Generates a replication slot name for the given pipeline ID and worker type.
    pub fn try_to_string(&self) -> Result<String, EtlReplicationSlotError> {
        let slot_name = match self {
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

        if slot_name.len() > MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(slot_name));
        }

        Ok(slot_name)
    }

    /// Parses a replication slot name into its logical components.
    pub fn try_from_string(slot_name: &str) -> Result<EtlReplicationSlot, EtlReplicationSlotError> {
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

    /// Returns the prefix of apply sync slot for a pipeline.
    pub fn apply_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{APPLY_WORKER_PREFIX}_{pipeline_id}");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }

    /// Returns the prefix of table sync slots for a pipeline.
    pub fn table_sync_prefix(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
        let prefix = format!("{TABLE_SYNC_WORKER_PREFIX}_{pipeline_id}_");

        if prefix.len() >= MAX_SLOT_NAME_LENGTH {
            return Err(EtlReplicationSlotError::InvalidSlotNameLength(prefix));
        }

        Ok(prefix)
    }
}

/// Deletes all replication slots for a given pipeline.
///
/// This function deletes both the apply worker slot and all table sync worker slots
/// for the tables associated with the pipeline.
///
/// If the slot name can't be computed, this function will silently skip the deletion of the slot.
pub async fn delete_pipeline_replication_slots(
    pool: &PgPool,
    pipeline_id: u64,
    table_ids: &[TableId],
) -> sqlx::Result<()> {
    // Collect all slot names that need to be deleted
    let mut slot_names = Vec::with_capacity(table_ids.len() + 1);

    // Add apply worker slot
    let slot_name = EtlReplicationSlot::for_apply_worker(pipeline_id);
    if let Ok(apply_slot_name) = slot_name.try_to_string() {
        slot_names.push(apply_slot_name.clone());
    };

    // Add table sync worker slots
    for table_id in table_ids {
        let slot_name = EtlReplicationSlot::for_table_sync_worker(pipeline_id, *table_id);
        if let Ok(table_sync_slot_name) = slot_name.try_to_string() {
            slot_names.push(table_sync_slot_name);
        };
    }

    // Delete only active slots
    let query = String::from(
        r#"
        select pg_drop_replication_slot(r.slot_name)
        from pg_replication_slots r
        where r.slot_name = any($1) and r.active = false;
        "#,
    );
    sqlx::query(&query).bind(slot_names).execute(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_worker_slot_name() {
        let pipeline_id = 1;
        let result = EtlReplicationSlot::for_apply_worker(pipeline_id)
            .try_to_string()
            .unwrap();

        assert!(result.starts_with(APPLY_WORKER_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_apply_1");
    }

    #[test]
    fn test_table_sync_slot_name() {
        let pipeline_id = 1;
        let result = EtlReplicationSlot::for_table_sync_worker(pipeline_id, TableId::new(123))
            .try_to_string()
            .unwrap();

        assert!(result.starts_with(TABLE_SYNC_WORKER_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_table_sync_1_123");
    }

    #[test]
    fn test_slot_name_length_validation() {
        // Test that normal slot names are within limits
        // Max u64
        let pipeline_id = 9223372036854775807_u64;
        // Max u32
        let result =
            EtlReplicationSlot::for_table_sync_worker(pipeline_id, TableId::new(4294967295))
                .try_to_string();
        assert!(result.is_ok());

        let slot_name = result.unwrap();
        assert!(slot_name.len() <= MAX_SLOT_NAME_LENGTH);

        // The longest possible slot name with current prefixes should still be valid
        assert_eq!(
            slot_name,
            "supabase_etl_table_sync_9223372036854775807_4294967295"
        );
        assert!(slot_name.len() <= MAX_SLOT_NAME_LENGTH);
    }

    #[test]
    fn test_apply_sync_slot_prefix() {
        let prefix = EtlReplicationSlot::apply_prefix(42).unwrap();
        assert_eq!(prefix, "supabase_etl_apply_42");
    }

    #[test]
    fn test_table_sync_slot_prefix() {
        let prefix = EtlReplicationSlot::table_sync_prefix(42).unwrap();
        assert_eq!(prefix, "supabase_etl_table_sync_42_");
    }

    #[test]
    fn test_parse_apply_slot() {
        let parsed = EtlReplicationSlot::try_from_string("supabase_etl_apply_13").unwrap();
        assert_eq!(parsed, EtlReplicationSlot::Apply { pipeline_id: 13 });
    }

    #[test]
    fn test_parse_table_sync_slot() {
        let parsed =
            EtlReplicationSlot::try_from_string("supabase_etl_table_sync_7_12345").unwrap();
        assert_eq!(
            parsed,
            EtlReplicationSlot::TableSync {
                pipeline_id: 7,
                table_id: TableId::new(12345_u32),
            }
        );
    }

    #[test]
    fn test_parse_invalid_slot() {
        assert!(EtlReplicationSlot::try_from_string("unknown_slot").is_err());
        assert!(EtlReplicationSlot::try_from_string("supabase_etl_apply_").is_err());
        assert!(EtlReplicationSlot::try_from_string("supabase_etl_table_sync_abc").is_err());
    }
}
