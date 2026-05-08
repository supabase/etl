use crate::{
    lag::{PipelineLagMetrics, SlotLagMetrics},
    slots::EtlReplicationSlot,
    tokio::{PgSourceError, PgSourceTransaction},
};

/// Reads pipeline lag metrics.
pub async fn pipeline_lag_metrics(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
) -> Result<PipelineLagMetrics, PgSourceError> {
    let pipeline_id = pipeline_id as u64;
    let Ok(apply_prefix) = EtlReplicationSlot::apply_prefix(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };
    let Ok(table_sync_prefix) = EtlReplicationSlot::table_sync_prefix(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };
    let table_sync_pattern = format!("{table_sync_prefix}%");

    let rows = txn
        .query(
            r#"
            select
                s.slot_name,
                coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn), 0)::bigint as restart_lsn_bytes,
                coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn), 0)::bigint as confirmed_flush_lsn_bytes,
                coalesce(s.safe_wal_size, 0)::bigint as safe_wal_size_bytes,
                round(extract(epoch from r.write_lag) * 1000)::bigint as write_lag_ms,
                round(extract(epoch from r.flush_lag) * 1000)::bigint as flush_lag_ms
            from pg_replication_slots as s
            left outer join pg_stat_replication as r on s.active_pid = r.pid
            where s.slot_type = 'logical'
              and (s.slot_name = $1 or s.slot_name like $2)
            "#,
            &[&apply_prefix, &table_sync_pattern],
        )
        .await?;

    let mut metrics = PipelineLagMetrics::default();
    for row in rows {
        let slot_lag_metrics = SlotLagMetrics {
            restart_lsn_bytes: row.get("restart_lsn_bytes"),
            confirmed_flush_lsn_bytes: row.get("confirmed_flush_lsn_bytes"),
            safe_wal_size_bytes: row.get("safe_wal_size_bytes"),
            write_lag_ms: row.get("write_lag_ms"),
            flush_lag_ms: row.get("flush_lag_ms"),
        };

        match EtlReplicationSlot::try_from(row.get::<_, String>("slot_name").as_str()) {
            Ok(EtlReplicationSlot::Apply { pipeline_id: slot_pipeline_id })
                if slot_pipeline_id == pipeline_id =>
            {
                metrics.apply = Some(slot_lag_metrics);
            }
            Ok(EtlReplicationSlot::TableSync { pipeline_id: slot_pipeline_id, table_id })
                if slot_pipeline_id == pipeline_id =>
            {
                metrics.table_sync.insert(table_id, slot_lag_metrics);
            }
            _ => {}
        }
    }

    Ok(metrics)
}
