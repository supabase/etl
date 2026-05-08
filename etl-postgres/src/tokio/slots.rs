use std::time::Duration;

use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    slots::{EtlReplicationSlot, EtlReplicationSlotError},
    tokio::{PgSourceClient, PgSourceError},
};

/// Errors from replication slot source operations.
#[derive(Debug, Error)]
pub enum ReplicationSlotOperationError {
    /// A source database operation failed.
    #[error("Source database operation failed: {0}")]
    Source(#[source] Box<PgSourceError>),

    /// A slot name could not be derived or parsed.
    #[error(transparent)]
    Slot(#[from] EtlReplicationSlotError),
}

impl From<PgSourceError> for ReplicationSlotOperationError {
    fn from(error: PgSourceError) -> Self {
        Self::Source(Box::new(error))
    }
}

/// Creates the pattern to match on the table sync worker replication slot and
/// escapes it to make sure it's parsed correctly by Postgres.
fn table_sync_like_pattern(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
    let prefix = EtlReplicationSlot::table_sync_prefix(pipeline_id)?;
    Ok(format!("{}%", prefix.replace('_', r"\_")))
}

/// Deletes ETL-managed replication slots for a pipeline.
pub async fn delete_pipeline_replication_slots(
    client: &PgSourceClient,
    pipeline_id: i64,
) -> Result<(), ReplicationSlotOperationError> {
    let pipeline_id = pipeline_id as u64;
    let apply_slot_name = String::try_from(EtlReplicationSlot::for_apply_worker(pipeline_id))?;
    let table_sync_pattern = table_sync_like_pattern(pipeline_id)?;

    const MAX_RETRIES: u32 = 3;
    const INITIAL_BACKOFF_MS: u64 = 100;

    for attempt in 0..MAX_RETRIES {
        let terminate_result = client
            .execute(
                r#"
                select pg_terminate_backend(r.active_pid)
                from pg_replication_slots r
                where (r.slot_name = $1 or r.slot_name like $2 escape '\')
                  and r.active = true
                  and r.active_pid is not null
                "#,
                &[&apply_slot_name, &table_sync_pattern],
            )
            .await;
        if let Err(error) = terminate_result {
            warn!(%pipeline_id, %error, "could not terminate backend(s) using replication slot(s) that have to be deleted");
        }

        if attempt == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let drop_result = client
            .execute(
                r#"
                select pg_drop_replication_slot(r.slot_name)
                from pg_replication_slots r
                where r.slot_name = $1 or r.slot_name like $2 escape '\'
                "#,
                &[&apply_slot_name, &table_sync_pattern],
            )
            .await;

        match drop_result {
            Ok(_) => return Ok(()),
            Err(error) if attempt == MAX_RETRIES - 1 => return Err(error.into()),
            Err(error) => {
                warn!(%pipeline_id, %error, "could not drop replication slot(s) for pipeline");
                debug!(%pipeline_id, "waiting for backoff before retrying to drop replication slot(s)");
                let backoff_ms = INITIAL_BACKOFF_MS * 2_u64.pow(attempt);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    Ok(())
}
