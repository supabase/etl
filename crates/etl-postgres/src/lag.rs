//! WAL positions and replication-slot backlog measurements.
//!
//! Distances are bytes in the cluster-wide WAL address space, including WAL
//! outside the publication and padding. They are not payload sizes, row counts,
//! elapsed time, or estimates of catchup duration.

use std::collections::BTreeMap;

use sqlx::{FromRow, PgExecutor};

use crate::{
    schema::TableId,
    slots::{EtlReplicationSlot, table_sync_like_pattern},
};

/// Source positions sampled once for all slots in a pipeline.
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceWalPositions {
    /// Flushed WAL on a primary, or replayed WAL on a standby.
    decoding_lsn: Option<u64>,
    /// Written WAL on a primary; unavailable on a standby.
    write_lsn: Option<u64>,
}

impl SourceWalPositions {
    /// Returns the end of WAL available to logical decoding.
    ///
    /// A standby may not yet have replayed any WAL, in which case this is
    /// unavailable. Open transactions can still be buffered by PostgreSQL.
    pub fn decoding_lsn(&self) -> Option<u64> {
        self.decoding_lsn
    }

    /// Returns the primary's WAL write position, including unflushed WAL.
    pub fn write_lsn(&self) -> Option<u64> {
        self.write_lsn
    }
}

/// Backlog and acknowledgment measurements for one logical replication slot.
#[derive(Debug)]
pub struct SlotLagMetrics {
    /// Whether the slot has an active replication connection.
    pub active: bool,
    /// Availability of the slot's required WAL; missing status is unknown.
    pub wal_status: Option<SlotWalStatus>,
    /// Primary WAL write position minus the oldest WAL required by the slot.
    ///
    /// Unavailable on a standby, for a lost slot, or without a restart LSN.
    /// Retention overlaps between slots; do not sum their distances.
    pub wal_retention_bytes: Option<i64>,
    /// Decoding head minus PostgreSQL's consumer-confirmed position.
    ///
    /// This can trail ETL's local safe progress until feedback is processed.
    /// Missing positions and lost slots remain unavailable, not zero.
    pub acknowledged_wal_lag_bytes: Option<i64>,
    /// Bytes of remaining slot retention headroom reported by PostgreSQL.
    ///
    /// Missing means either unlimited retention or unavailable headroom;
    /// interpret it together with [`Self::wal_status`].
    pub safe_wal_size_bytes: Option<i64>,
    /// Recent write acknowledgment latency reported by PostgreSQL, in
    /// milliseconds.
    ///
    /// This is not backlog age and can be unavailable while idle or inactive.
    pub write_lag_ms: Option<i64>,
    /// Recent flush acknowledgment latency reported by PostgreSQL, in
    /// milliseconds.
    ///
    /// This is not destination query visibility or an estimate of catchup time.
    pub flush_lag_ms: Option<i64>,
    /// Source clock minus the client's last feedback timestamp, in
    /// milliseconds.
    ///
    /// Clock skew can make this negative; it does not measure backlog age.
    pub reply_time_lag_ms: Option<i64>,
}

/// WAL availability reported by PostgreSQL for a replication slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotWalStatus {
    /// Required WAL is within normal retention.
    Reserved,
    /// Required WAL exceeds `max_wal_size` but remains retained.
    Extended,
    /// Required WAL can be removed at the next checkpoint.
    Unreserved,
    /// The slot is no longer usable.
    Lost,
    /// A status not understood by this ETL version.
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

/// One source sample and the pipeline's apply and table-sync slot measurements.
#[derive(Debug, Default)]
pub struct PipelineLagMetrics {
    /// Source positions, also present when the pipeline has no slots.
    pub source_wal: SourceWalPositions,
    /// Apply slot measurements, if the slot exists.
    pub apply: Option<SlotLagMetrics>,
    /// Table-sync slot measurements keyed by source table OID.
    pub table_sync: BTreeMap<TableId, SlotLagMetrics>,
}

/// Row from the source sample's left join against the pipeline's slots.
#[derive(Debug, FromRow)]
struct SlotLagRow {
    /// Source decoding head serialized by PostgreSQL.
    decoding_lsn: Option<String>,
    /// Primary write head serialized by PostgreSQL.
    write_lsn: Option<String>,
    /// Missing when the pipeline has no slots.
    slot_name: Option<String>,
    /// Missing when the pipeline has no slots.
    active: Option<bool>,
    /// WAL availability reported by the slot.
    wal_status: Option<String>,
    /// Primary retention distance, when known.
    wal_retention_bytes: Option<i64>,
    /// Consumer acknowledgment distance, when known.
    acknowledged_wal_lag_bytes: Option<i64>,
    /// Remaining retention headroom, when bounded and known.
    safe_wal_size_bytes: Option<i64>,
    /// Recent PostgreSQL write acknowledgment latency.
    write_lag_ms: Option<i64>,
    /// Recent PostgreSQL flush acknowledgment latency.
    flush_lag_ms: Option<i64>,
    /// Source clock minus the client-supplied feedback timestamp.
    reply_time_lag_ms: Option<i64>,
}

/// Parses an optional server-supplied WAL address without losing its range.
fn parse_lsn(value: Option<&str>) -> sqlx::Result<Option<u64>> {
    value
        .map(|value| {
            let (high, low) = value.split_once('/').ok_or_else(|| {
                sqlx::Error::Decode("Missing separator in source WAL position".into())
            })?;
            let high = u32::from_str_radix(high, 16)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let low = u32::from_str_radix(low, 16)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            Ok((u64::from(high) << 32) | u64::from(low))
        })
        .transpose()
}

/// Samples one decoding head and all logical slots belonging to a pipeline.
///
/// PostgreSQL supplies flushed WAL on a primary and replayed WAL on a standby.
/// A materialized source sample gives all slots the same reference position.
/// Slot state can change concurrently, so negative backlog and retention
/// distances are clamped to zero while missing positions remain unknown.
/// Retention headroom preserves PostgreSQL's signed value. Lost slots never
/// report usable lag. Inactive slots retain their backlog and status.
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
        with source as materialized (
            select
                case
                    when pg_is_in_recovery() then pg_last_wal_replay_lsn()
                    else pg_current_wal_flush_lsn()
                end as decoding_lsn,
                case
                    when not pg_is_in_recovery() then pg_current_wal_lsn()
                end as write_lsn
        )
        select
            source.decoding_lsn::text,
            source.write_lsn::text,
            s.slot_name,
            s.active,
            s.wal_status,
            case
                when s.wal_status in ('reserved', 'extended', 'unreserved')
                    and s.restart_lsn is not null
                    and source.write_lsn is not null
                then greatest(pg_wal_lsn_diff(source.write_lsn, s.restart_lsn), 0)::bigint
            end as wal_retention_bytes,
            case
                when s.wal_status in ('reserved', 'extended', 'unreserved')
                    and s.confirmed_flush_lsn is not null
                    and source.decoding_lsn is not null
                then greatest(pg_wal_lsn_diff(source.decoding_lsn, s.confirmed_flush_lsn), 0)::bigint
            end as acknowledged_wal_lag_bytes,
            s.safe_wal_size as safe_wal_size_bytes,
            round(extract(epoch from r.write_lag) * 1000)::bigint as write_lag_ms,
            round(extract(epoch from r.flush_lag) * 1000)::bigint as flush_lag_ms,
            round(extract(epoch from clock_timestamp() - r.reply_time) * 1000)::bigint as reply_time_lag_ms
        from source
        left join pg_replication_slots as s
          on s.slot_type = 'logical'
         and s.database = current_database()
         and (s.slot_name = $1 or s.slot_name like $2 escape '\')
        left join pg_stat_replication as r on s.active_pid = r.pid
        "#,
    )
    .bind(apply_prefix)
    .bind(table_sync_pattern)
    .fetch_all(executor)
    .await?;

    let mut metrics = PipelineLagMetrics::default();
    for row in rows {
        metrics.source_wal = SourceWalPositions {
            decoding_lsn: parse_lsn(row.decoding_lsn.as_deref())?,
            write_lsn: parse_lsn(row.write_lsn.as_deref())?,
        };

        let Some(slot_name) = row.slot_name else {
            continue;
        };

        let slot = SlotLagMetrics {
            active: row.active.unwrap_or(false),
            wal_status: row.wal_status.map(Into::into),
            wal_retention_bytes: row.wal_retention_bytes,
            acknowledged_wal_lag_bytes: row.acknowledged_wal_lag_bytes,
            safe_wal_size_bytes: row.safe_wal_size_bytes,
            write_lag_ms: row.write_lag_ms,
            flush_lag_ms: row.flush_lag_ms,
            reply_time_lag_ms: row.reply_time_lag_ms,
        };

        match EtlReplicationSlot::try_from(slot_name.as_str()) {
            Ok(EtlReplicationSlot::Apply { pipeline_id: id }) if id == pipeline_id => {
                metrics.apply = Some(slot);
            }
            Ok(EtlReplicationSlot::TableSync { pipeline_id: id, table_id })
                if id == pipeline_id =>
            {
                metrics.table_sync.insert(table_id, slot);
            }
            _ => {}
        }
    }

    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use crate::lag::parse_lsn;

    /// Server addresses preserve the full unsigned WAL range.
    #[test]
    fn source_lsn_parsing_preserves_range_and_missing_positions() {
        assert_eq!(parse_lsn(None).unwrap(), None);
        assert_eq!(parse_lsn(Some("0/0")).unwrap(), Some(0));
        assert_eq!(parse_lsn(Some("1/0")).unwrap(), Some(1_u64 << 32));
        assert_eq!(parse_lsn(Some("FFFFFFFF/FFFFFFFF")).unwrap(), Some(u64::MAX));
    }

    /// Malformed addresses return errors rather than panicking or truncating.
    #[test]
    fn source_lsn_parsing_rejects_malformed_addresses() {
        for value in
            ["", "0", "/", "0/", "/0", "0/0/0", "100000000/0", "0/100000000", "é/0", "0/💥", "-1/0"]
        {
            assert!(parse_lsn(Some(value)).is_err());
        }
    }
}
