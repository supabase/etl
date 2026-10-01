//! SQL accessors for persisted replication checkpoints.
//!
//! The physical `etl.replication_progress.flush_lsn` names are retained for
//! storage compatibility. At the API boundary, the stored LSN is a checkpoint:
//! the durable replay frontier selected by the replication worker.

use std::str::FromStr;

use sqlx::{PgExecutor, postgres::types::Oid as SqlxTableId};
use tokio_postgres::types::PgLsn;

use crate::schema::TableId;

/// Parses a `pg_lsn` string returned by SQLx.
fn parse_lsn(lsn: &str) -> sqlx::Result<PgLsn> {
    PgLsn::from_str(lsn).map_err(|_| {
        sqlx::Error::Protocol(format!(
            "Invalid pg_lsn value returned from etl.replication_progress: {lsn}."
        ))
    })
}

/// Loads all persisted checkpoints for a pipeline in one query.
///
/// The worker/table constraint identifies workers by table ID. Separate
/// branches match the partial indexes for null and non-null table IDs.
pub async fn load_replication_checkpoints<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<Vec<(Option<TableId>, PgLsn)>>
where
    E: PgExecutor<'c>,
{
    let rows: Vec<(Option<SqlxTableId>, String)> = sqlx::query_as(
        r#"
        select table_id, flush_lsn::text
        from etl.replication_progress
        where pipeline_id = $1 and table_id is null
        union all
        select table_id, flush_lsn::text
        from etl.replication_progress
        where pipeline_id = $1 and table_id is not null
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|(table_id, lsn)| Ok((table_id.map(|id| TableId::new(id.0)), parse_lsn(&lsn)?)))
        .collect()
}

/// Monotonically persists a replication checkpoint for a pipeline worker.
///
/// A stale or duplicated checkpoint cannot move the stored replay frontier
/// backward. PostgreSQL applies the maximum and returns the stored LSN in one
/// atomic statement, so callers can cache the returned value after success.
pub async fn upsert_replication_checkpoint<'c, E>(
    executor: E,
    pipeline_id: i64,
    worker_type: &'static str,
    table_id: Option<TableId>,
    checkpoint_lsn: PgLsn,
) -> sqlx::Result<PgLsn>
where
    E: PgExecutor<'c>,
{
    let checkpoint_lsn = checkpoint_lsn.to_string();
    let persisted_checkpoint_lsn: String = if let Some(table_id) = table_id {
        sqlx::query_scalar(
            r#"
            insert into etl.replication_progress (pipeline_id, worker_type, table_id, flush_lsn)
            values ($1, $2::etl.replication_worker_type, $3, $4::pg_lsn)
            on conflict (pipeline_id, worker_type, table_id) where table_id is not null
            do update set
                flush_lsn = greatest(excluded.flush_lsn, etl.replication_progress.flush_lsn),
                updated_at = case
                    when excluded.flush_lsn > etl.replication_progress.flush_lsn
                        then now()
                    else etl.replication_progress.updated_at
                end
            returning flush_lsn::text
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .bind(SqlxTableId(table_id.into_inner()))
        .bind(checkpoint_lsn)
        .fetch_one(executor)
        .await?
    } else {
        sqlx::query_scalar(
            r#"
            insert into etl.replication_progress (pipeline_id, worker_type, flush_lsn)
            values ($1, $2::etl.replication_worker_type, $3::pg_lsn)
            on conflict (pipeline_id, worker_type) where table_id is null
            do update set
                flush_lsn = greatest(excluded.flush_lsn, etl.replication_progress.flush_lsn),
                updated_at = case
                    when excluded.flush_lsn > etl.replication_progress.flush_lsn
                        then now()
                    else etl.replication_progress.updated_at
                end
            returning flush_lsn::text
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .bind(checkpoint_lsn)
        .fetch_one(executor)
        .await?
    };

    parse_lsn(&persisted_checkpoint_lsn)
}

/// Deletes the persisted replication checkpoint for a pipeline worker.
pub async fn delete_replication_checkpoint<'c, E>(
    executor: E,
    pipeline_id: i64,
    worker_type: &'static str,
    table_id: Option<TableId>,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    let result = if let Some(table_id) = table_id {
        sqlx::query(
            r#"
            delete from etl.replication_progress
            where pipeline_id = $1
              and worker_type = $2::etl.replication_worker_type
              and table_id = $3
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .bind(SqlxTableId(table_id.into_inner()))
        .execute(executor)
        .await?
    } else {
        sqlx::query(
            r#"
            delete from etl.replication_progress
            where pipeline_id = $1
              and worker_type = $2::etl.replication_worker_type
              and table_id is null
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .execute(executor)
        .await?
    };

    Ok(result.rows_affected())
}

/// Deletes the persisted checkpoint for a specific table-sync worker.
pub async fn delete_replication_checkpoint_for_table<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    delete_replication_checkpoint(executor, pipeline_id, "table_sync", Some(table_id)).await
}
