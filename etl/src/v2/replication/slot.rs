use thiserror::Error;

use crate::v2::pipeline::PipelineId;
use crate::v2::workers::base::WorkerType;

/// Maximum length for a PostgreSQL replication slot name in bytes.
const MAX_SLOT_NAME_LENGTH: usize = 63;

/// Prefixes for different types of replication slots
const APPLY_WORKER_PREFIX: &str = "supabase_etl_apply";
const TABLE_SYNC_PREFIX: &str = "supabase_etl_table_sync";

/// Error types that can occur when working with replication slots
#[derive(Debug, Error)]
pub enum SlotError {
    #[error("Replication slot name exceeds maximum length of {MAX_SLOT_NAME_LENGTH} characters: name must be shorter")]
    NameTooLong,
}

/// Generates a replication slot name.
pub fn get_slot_name(
    pipeline_id: PipelineId,
    worker_type: WorkerType,
) -> Result<String, SlotError> {
    let slot_name = match worker_type {
        WorkerType::Apply => {
            format!("{}_{}", APPLY_WORKER_PREFIX, pipeline_id,)
        }
        WorkerType::TableSync { table_id } => {
            format!("{}_{}_{}", TABLE_SYNC_PREFIX, pipeline_id, table_id)
        }
    };

    if slot_name.len() > MAX_SLOT_NAME_LENGTH {
        return Err(SlotError::NameTooLong);
    }

    Ok(slot_name)
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
    }

    #[test]
    fn test_table_sync_slot_name() {
        let pipeline_id = 1;
        let result = get_slot_name(pipeline_id, WorkerType::TableSync { table_id: 123 }).unwrap();
        assert!(result.starts_with(TABLE_SYNC_PREFIX));
        assert!(result.len() <= MAX_SLOT_NAME_LENGTH);
    }
}
