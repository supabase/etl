use sqlx::{postgres::types::Oid as SqlxTableId, prelude::FromRow, Type};

#[cfg(feature = "sqlx")]
use config::shared::PgConnectionConfig;
#[cfg(feature = "sqlx")]
use sqlx::PgPool;
#[cfg(feature = "sqlx")]
use crate::schema::TableId;

/// Table replication state as stored in the database
#[derive(Debug, Clone, Copy, Type, PartialEq)]
#[sqlx(type_name = "etl.table_state", rename_all = "snake_case")]
pub enum TableState {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone,
    Ready,
    Skipped,
}

/// A row from the etl.replication_state table
#[derive(Debug, FromRow)]
pub struct ReplicationStateRow {
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableState,
    pub sync_done_lsn: Option<String>,
}


/// Fetch replication state rows for a specific pipeline from the source database
#[cfg(feature = "sqlx")]
pub async fn get_replication_state_rows(
    pool: &PgPool,
    pipeline_id: i64,
) -> sqlx::Result<Vec<ReplicationStateRow>> {
    let states = sqlx::query_as::<_, ReplicationStateRow>(
        r#"
        select pipeline_id, table_id, state, sync_done_lsn
        from etl.replication_state
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    Ok(states)
}


/// Update replication state for a specific table in the source database
#[cfg(feature = "sqlx")]
pub async fn update_replication_state(
    source_config: &PgConnectionConfig,
    pipeline_id: u64,
    table_id: TableId,
    state: TableState,
    sync_done_lsn: Option<String>,
) -> sqlx::Result<()> {
    // We connect to source database each time we update because we assume that
    // these updates will be infrequent. It has some overhead to establish a
    // connection but it's better than holding a connection open for long periods
    // when there's little activity on it.
    let pool = crate::replication::connect_to_source_database(source_config).await?;
    sqlx::query(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, sync_done_lsn)
        values ($1, $2, $3, $4)
        on conflict (pipeline_id, table_id)
        do update set state = $3, sync_done_lsn = $4
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id))
    .bind(state)
    .bind(sync_done_lsn)
    .execute(&pool)
    .await?;

    Ok(())
}

/// Update replication state using an existing connection pool
#[cfg(feature = "sqlx")]
pub async fn update_replication_state_with_pool(
    pool: &PgPool,
    pipeline_id: u64,
    table_id: TableId,
    state: TableState,
    sync_done_lsn: Option<String>,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, sync_done_lsn)
        values ($1, $2, $3, $4)
        on conflict (pipeline_id, table_id)
        do update set state = $3, sync_done_lsn = $4
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id))
    .bind(state)
    .bind(sync_done_lsn)
    .execute(pool)
    .await?;

    Ok(())
}