use std::time::Duration;

use tracing::{debug, warn};

use crate::{
    slots::{EtlReplicationSlot, EtlReplicationSlotError},
    tokio::{PgSourceClient, PgSourceError},
};

/// Builds a SQL `LIKE` pattern for all table-sync slots in a pipeline.
///
/// The pattern escapes literal `_` characters in the slot prefix so `LIKE`
/// does not treat them as single-character wildcards. Use with
/// `LIKE ... ESCAPE '\'`.
fn table_sync_like_pattern(pipeline_id: u64) -> Result<String, EtlReplicationSlotError> {
    let prefix = EtlReplicationSlot::table_sync_prefix(pipeline_id)?;
    Ok(format!("{}%", prefix.replace('_', r"\_")))
}

/// Deletes ETL-managed replication slots for a pipeline.
///
/// Deletes both the apply worker slot and all table sync worker slots matching
/// the ETL naming convention for the pipeline. Active walsender processes are
/// terminated first, then slot drops are retried with exponential backoff.
pub async fn delete_pipeline_replication_slots(
    client: &PgSourceClient,
    pipeline_id: i64,
) -> Result<(), PgSourceError> {
    let pipeline_id = pipeline_id as u64;
    let Ok(apply_slot_name) = String::try_from(EtlReplicationSlot::for_apply_worker(pipeline_id))
    else {
        warn!(%pipeline_id, "could not derive apply-worker replication slot name during cleanup");
        return Ok(());
    };
    let Ok(table_sync_pattern) = table_sync_like_pattern(pipeline_id) else {
        warn!(%pipeline_id, "could not derive table-sync replication slot prefix during cleanup");
        return Ok(());
    };

    const MAX_RETRIES: u32 = 3;
    const INITIAL_BACKOFF_MS: u64 = 100;

    for attempt in 0..MAX_RETRIES {
        // Phase 1: terminate active walsender processes for the ETL-managed
        // slots associated with this pipeline ID. We use both the exact apply
        // slot name and the table-sync prefix so cleanup can still succeed even
        // if ETL metadata was already removed.
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

        // Brief delay to allow walsender processes to terminate.
        if attempt == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Phase 2: drop all matching replication slots, whether still active
        // or already inactive. `pg_drop_replication_slot` signals walsenders to
        // terminate if they are still active.
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
            Err(error) => {
                warn!(%pipeline_id, %error, "could not drop replication slot(s) for pipeline");
                if attempt == MAX_RETRIES - 1 {
                    return Err(error);
                }

                debug!(%pipeline_id, "waiting for backoff before retrying to drop replication slot(s)");
                // Exponential backoff: 100ms, 200ms, 400ms.
                let backoff_ms = INITIAL_BACKOFF_MS * 2_u64.pow(attempt);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    Ok(())
}
