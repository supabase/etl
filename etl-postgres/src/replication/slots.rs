use sqlx::PgPool;

use crate::replication::worker::WorkerType;
use crate::schema::TableId;

/// Maximum length for a Postgres replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Prefixes for different types of replication slots
const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";
const TABLE_SYNC_PREFIX: &str = "supabase_etl_table_sync";

/// Error type for slot operations.
#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    #[error("Invalid slot name length: {0}")]
    InvalidSlotNameLength(String),
}

/// Generates a replication slot name for the given pipeline ID and worker type.
pub fn get_slot_name(pipeline_id: u64, worker_type: WorkerType) -> Result<String, SlotError> {
    let slot_name = match worker_type {
        WorkerType::Apply => {
            format!("{APPLY_WORKER_PREFIX}_{pipeline_id}")
        }
        WorkerType::TableSync { table_id } => {
            format!(
                "{TABLE_SYNC_PREFIX}_{pipeline_id}_{}",
                table_id.into_inner()
            )
        }
    };

    if slot_name.len() > MAX_SLOT_NAME_LENGTH {
        return Err(SlotError::InvalidSlotNameLength(slot_name));
    }

    Ok(slot_name)
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
) -> Result<(), sqlx::Error> {
    // Collect all slot names that need to be deleted
    let mut slot_names = Vec::with_capacity(table_ids.len() + 1);

    // Add apply worker slot
    if let Ok(apply_slot_name) = get_slot_name(pipeline_id, WorkerType::Apply) {
        slot_names.push(apply_slot_name.clone());
    };

    // Add table sync worker slots
    for table_id in table_ids {
        if let Ok(table_sync_slot_name) = get_slot_name(
            pipeline_id,
            WorkerType::TableSync {
                table_id: *table_id,
            },
        ) {
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
        let result = get_slot_name(pipeline_id, WorkerType::Apply).unwrap();
        assert!(result.starts_with(APPLY_WORKER_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_apply_1");
    }

    #[test]
    fn test_table_sync_slot_name() {
        let pipeline_id = 1;
        let result = get_slot_name(
            pipeline_id,
            WorkerType::TableSync {
                table_id: TableId::new(123),
            },
        )
        .unwrap();
        assert!(result.starts_with(TABLE_SYNC_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
        assert_eq!(result, "supabase_etl_table_sync_1_123");
    }

    #[test]
    fn test_slot_name_length_validation() {
        // Test that normal slot names are within limits
        let pipeline_id = 9223372036854775807_u64; // Max u64
        let result = get_slot_name(
            pipeline_id,
            WorkerType::TableSync {
                table_id: TableId::new(4294967295), // Max u32
            },
        );
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
}
