use sqlx::{PgPool, Type, postgres::types::Oid as SqlxTableId, prelude::FromRow};

use crate::schema::TableId;

/// Table replication state as stored in the database
#[derive(Debug, Clone, Copy, Type, PartialEq)]
#[sqlx(type_name = "etl.table_state", rename_all = "snake_case")]
pub enum TableReplicationState {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone,
    Ready,
    Skipped,
}

/// A row from the etl.replication_state table
#[derive(Debug, FromRow)]
pub struct TableReplicationStateRow {
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableReplicationState,
    pub sync_done_lsn: Option<String>,
}

/// Fetch replication state rows for a specific pipeline from the source database
#[cfg(feature = "sqlx")]
pub async fn get_table_replication_state_rows(
    pool: &PgPool,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableReplicationStateRow>> {
    let states = sqlx::query_as::<_, TableReplicationStateRow>(
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

/// Update replication state using an existing connection pool
pub async fn update_replication_state(
    pool: &PgPool,
    pipeline_id: u64,
    table_id: TableId,
    state: TableReplicationState,
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
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(state)
    .bind(sync_done_lsn)
    .execute(pool)
    .await?;

    Ok(())
}
