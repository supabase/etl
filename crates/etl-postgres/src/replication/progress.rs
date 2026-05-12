use std::str::FromStr;

use sqlx::{PgExecutor, postgres::types::Oid as SqlxTableId};
use tokio_postgres::types::PgLsn;

use crate::types::TableId;

/// Converts a [`TableId`] into the matching SQLx OID wrapper.
fn table_id_to_sqlx(table_id: TableId) -> SqlxTableId {
    SqlxTableId(table_id.into_inner())
}

/// Parses a `pg_lsn` string returned by SQLx.
fn parse_lsn(lsn: &str) -> sqlx::Result<PgLsn> {
    PgLsn::from_str(lsn).map_err(|_| {
        sqlx::Error::Protocol(format!(
            "Invalid pg_lsn value returned from etl.replication_progress: {lsn}."
        ))
    })
}

/// Fetches durable replication progress for a pipeline worker.
pub async fn get_replication_progress<'c, E>(
    executor: E,
    pipeline_id: i64,
    worker_type: &'static str,
    table_id: Option<TableId>,
) -> sqlx::Result<Option<PgLsn>>
where
    E: PgExecutor<'c>,
{
    let flush_lsn: Option<String> = if let Some(table_id) = table_id {
        sqlx::query_scalar(
            r#"
            select flush_lsn::text
            from etl.replication_progress
            where pipeline_id = $1
              and worker_type = $2::etl.replication_worker_type
              and table_id = $3
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .bind(table_id_to_sqlx(table_id))
        .fetch_optional(executor)
        .await?
    } else {
        sqlx::query_scalar(
            r#"
            select flush_lsn::text
            from etl.replication_progress
            where pipeline_id = $1
              and worker_type = $2::etl.replication_worker_type
              and table_id is null
            "#,
        )
        .bind(pipeline_id)
        .bind(worker_type)
        .fetch_optional(executor)
        .await?
    };

    flush_lsn.as_deref().map(parse_lsn).transpose()
}

/// Upserts durable replication progress for a pipeline worker.
///
/// The update is monotonic: a stale or duplicated flush LSN cannot move stored
/// progress backward.
pub async fn upsert_replication_progress<'c, E>(
    executor: E,
    pipeline_id: i64,
    worker_type: &'static str,
    table_id: Option<TableId>,
    flush_lsn: PgLsn,
) -> sqlx::Result<PgLsn>
where
    E: PgExecutor<'c>,
{
    let flush_lsn = flush_lsn.to_string();
    let stored_lsn: String = if let Some(table_id) = table_id {
        sqlx::query_scalar(
            r#"
            insert into etl.replication_progress (pipeline_id, worker_type, table_id, flush_lsn)
            values ($1, $2::etl.replication_worker_type, $3, $4::pg_lsn)
            on conflict (pipeline_id, worker_type, table_id) where table_id is not null
            do update set
                flush_lsn = case
                    when excluded.flush_lsn > etl.replication_progress.flush_lsn
                        then excluded.flush_lsn
                    else etl.replication_progress.flush_lsn
                end,
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
        .bind(table_id_to_sqlx(table_id))
        .bind(flush_lsn)
        .fetch_one(executor)
        .await?
    } else {
        sqlx::query_scalar(
            r#"
            insert into etl.replication_progress (pipeline_id, worker_type, flush_lsn)
            values ($1, $2::etl.replication_worker_type, $3::pg_lsn)
            on conflict (pipeline_id, worker_type) where table_id is null
            do update set
                flush_lsn = case
                    when excluded.flush_lsn > etl.replication_progress.flush_lsn
                        then excluded.flush_lsn
                    else etl.replication_progress.flush_lsn
                end,
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
        .bind(flush_lsn)
        .fetch_one(executor)
        .await?
    };

    parse_lsn(&stored_lsn)
}

/// Deletes durable replication progress for a pipeline worker.
pub async fn delete_replication_progress<'c, E>(
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
        .bind(table_id_to_sqlx(table_id))
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

/// Deletes durable replication progress for a specific table-sync worker.
pub async fn delete_replication_progress_for_table<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    delete_replication_progress(executor, pipeline_id, "table_sync", Some(table_id)).await
}
