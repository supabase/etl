//! Replication lag queries for Postgres logical replication slots.

use std::collections::BTreeMap;

use sqlx::{FromRow, PgExecutor};

use crate::{
    schema::TableId,
    slots::{EtlReplicationSlot, table_sync_like_pattern},
};

/// Lag metrics associated with a logical replication slot.
#[derive(Debug)]
pub struct SlotLagMetrics {
    /// Whether the slot currently has an active replication connection.
    pub active: bool,
    /// The WAL availability status reported by Postgres for the slot.
    pub wal_status: Option<SlotWalStatus>,
    /// The number of bytes between the current WAL LSN and the slot restart
    /// LSN.
    pub restart_lsn_bytes: i64,
    /// The number of bytes between the current WAL LSN and the confirmed flush
    /// LSN.
    pub confirmed_flush_lsn_bytes: i64,
    /// How many bytes of WAL are still safe to build up before the limit of the
    /// slot is reached.
    pub safe_wal_size_bytes: Option<i64>,
    /// Write lag in milliseconds relative to the primary.
    pub write_lag_ms: Option<i64>,
    /// Flush lag in milliseconds relative to the primary.
    pub flush_lag_ms: Option<i64>,
    /// Milliseconds elapsed since the walsender last received client feedback.
    pub reply_time_lag_ms: Option<i64>,
}

/// WAL availability status reported by Postgres for a replication slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotWalStatus {
    /// Required WAL is still within normal retention.
    Reserved,
    /// Required WAL exceeds `max_wal_size`, but is still retained.
    Extended,
    /// Required WAL is no longer fully reserved and can be removed at the next
    /// checkpoint.
    Unreserved,
    /// Required WAL has been removed and the slot is no longer usable.
    Lost,
    /// A WAL status value not known by this ETL version.
    Unknown,
}

impl From<String> for SlotWalStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "reserved" => Self::Reserved,
            "extended" => Self::Extended,
            "unreserved" => Self::Unreserved,
            "lost" => Self::Lost,
            _ => Self::Unknown,
        }
    }
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
    active: bool,
    wal_status: Option<String>,
    restart_lsn_bytes: i64,
    confirmed_flush_lsn_bytes: i64,
    safe_wal_size_bytes: Option<i64>,
    write_lag_ms: Option<i64>,
    flush_lag_ms: Option<i64>,
    reply_time_lag_ms: Option<i64>,
}

/// Fetches replication lag metrics for the given pipeline by inspecting logical
/// replication slots.
///
/// Returns aggregated lag metrics for the apply worker slot and each table sync
/// slot associated with the pipeline. Slots that are not currently active in
/// `pg_stat_replication` still report their slot WAL metrics, while the
/// walsender feedback metrics remain `None`.
pub async fn get_pipeline_lag_metrics<'c, E>(
    executor: E,
    pipeline_id: u64,
) -> sqlx::Result<PipelineLagMetrics>
where
    E: PgExecutor<'c>,
{
    let Ok(apply_prefix) = EtlReplicationSlot::apply_prefix(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };
    let Ok(table_sync_pattern) = table_sync_like_pattern(pipeline_id) else {
        return Ok(PipelineLagMetrics::default());
    };

    let rows: Vec<SlotLagRow> = sqlx::query_as(
        r#"
        with current_wal as (
            select pg_current_wal_lsn() as lsn
        )
        select
            s.slot_name,
            s.active,
            s.wal_status,
            coalesce(pg_wal_lsn_diff(current_wal.lsn, s.restart_lsn), 0)::bigint as restart_lsn_bytes,
            coalesce(pg_wal_lsn_diff(current_wal.lsn, s.confirmed_flush_lsn), 0)::bigint as confirmed_flush_lsn_bytes,
            s.safe_wal_size as safe_wal_size_bytes,
            round(extract(epoch from r.write_lag) * 1000)::bigint as write_lag_ms,
            round(extract(epoch from r.flush_lag) * 1000)::bigint as flush_lag_ms,
            round(extract(epoch from clock_timestamp() - r.reply_time) * 1000)::bigint as reply_time_lag_ms
        from pg_replication_slots as s
        cross join current_wal
        left outer join pg_stat_replication as r on s.active_pid = r.pid
        where s.slot_type = 'logical'
          and s.database = current_database()
          and (s.slot_name = $1 or s.slot_name like $2 escape '\')
        "#,
    )
    .bind(apply_prefix)
    .bind(table_sync_pattern)
    .fetch_all(executor)
    .await?;

    let mut metrics = PipelineLagMetrics::default();

    for row in rows {
        let slot_lag_metrics = SlotLagMetrics {
            active: row.active,
            wal_status: row.wal_status.map(Into::into),
            restart_lsn_bytes: row.restart_lsn_bytes,
            confirmed_flush_lsn_bytes: row.confirmed_flush_lsn_bytes,
            safe_wal_size_bytes: row.safe_wal_size_bytes,
            write_lag_ms: row.write_lag_ms,
            flush_lag_ms: row.flush_lag_ms,
            reply_time_lag_ms: row.reply_time_lag_ms,
        };

        match EtlReplicationSlot::try_from(row.slot_name.as_str()) {
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
