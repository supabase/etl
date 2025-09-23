use sqlx::{FromRow, PgPool};
use std::collections::BTreeMap;

use crate::replication::slots::EtlReplicationSlot;
use crate::types::TableId;

/// Lag metrics associated with a logical replication slot.
#[derive(Debug)]
pub struct SlotLagMetrics {
    /// The number of bytes between the current WAL LSN and the slot restart LSN.
    pub restart_lsn_bytes: i64,
    /// The number of bytes between the current WAL LSN and the confirmed flush LSN.
    pub confirmed_flush_lsn_bytes: i64,
    /// How many bytes of WAL are still safe to build up before the limit of the slot is reached.
    pub safe_wal_size_bytes: i64,
    /// Write lag in milliseconds relative to the primary.
    pub write_lag_ms: Option<i64>,
    /// Flush lag in milliseconds relative to the primary.
    pub flush_lag_ms: Option<i64>,
}

/// Lag metrics for pipeline apply and table sync workers.
#[derive(Debug, Default)]
pub struct PipelineLagMetrics {
    /// Lag metrics for the apply worker slot.
    pub apply: Option<SlotLagMetrics>,
    /// Lag metrics keyed by table OID for table sync worker slots.
    pub table_sync: BTreeMap<TableId, SlotLagMetrics>,
}

/// Database row returned by the replication slot lag query.
#[derive(Debug, FromRow)]
struct SlotLagRow {
    slot_name: String,
    restart_lsn_bytes: i64,
    confirmed_flush_lsn_bytes: i64,
    safe_wal_size_bytes: i64,
    write_lag_ms: Option<i64>,
    flush_lag_ms: Option<i64>,
}

/// Fetches replication lag metrics for the given pipeline by inspecting logical replication slots.
///
/// Returns aggregated lag metrics for the apply worker slot and each table sync slot associated
/// with the pipeline. Slots that are not currently active in `pg_stat_replication` still report
/// their WAL metrics, while the write and flush lag values remain `None`.
pub async fn get_pipeline_lag_metrics(
    pool: &PgPool,
    pipeline_id: u64,
) -> sqlx::Result<PipelineLagMetrics> {
    let Ok(apply_prefix) = EtlReplicationSlot::apply_prefix(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };
    let Ok(table_sync_prefix) = EtlReplicationSlot::table_sync_prefix(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };

    let rows: Vec<SlotLagRow> = sqlx::query_as(
        r#"
        select
            s.slot_name,
            coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn), 0)::bigint as restart_lsn_bytes,
            coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn), 0)::bigint as confirmed_flush_lsn_bytes,
            coalesce(s.safe_wal_size, 0) as safe_wal_size_bytes,
            round(extract(epoch from r.write_lag) * 1000)::bigint as write_lag_ms,
            round(extract(epoch from r.flush_lag) * 1000)::bigint as flush_lag_ms
        from pg_replication_slots as s
        left outer join pg_stat_replication as r on s.active_pid = r.pid
        where s.slot_type = 'logical'
          and (s.slot_name = $1 or s.slot_name like $2)
        "#,
    )
    .bind(apply_prefix)
    .bind(format!("{table_sync_prefix}%"))
    .fetch_all(pool)
    .await?;

    let mut metrics = PipelineLagMetrics::default();

    for row in rows {
        let slot_lag_metrics = SlotLagMetrics {
            restart_lsn_bytes: row.restart_lsn_bytes,
            confirmed_flush_lsn_bytes: row.confirmed_flush_lsn_bytes,
            safe_wal_size_bytes: row.safe_wal_size_bytes,
            write_lag_ms: row.write_lag_ms,
            flush_lag_ms: row.flush_lag_ms,
        };

        match EtlReplicationSlot::try_from_string(&row.slot_name) {
            Ok(EtlReplicationSlot::Apply {
                pipeline_id: slot_pipeline_id,
            }) if slot_pipeline_id == pipeline_id => {
                metrics.apply = Some(slot_lag_metrics);
            }
            Ok(EtlReplicationSlot::TableSync {
                pipeline_id: slot_pipeline_id,
                table_id,
            }) if slot_pipeline_id == pipeline_id => {
                metrics.table_sync.insert(table_id, slot_lag_metrics);
            }
            _ => {}
        }
    }

    Ok(metrics)
}
