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
    Errored,
}

/// A row from the etl.replication_state table
#[derive(Debug, FromRow)]
pub struct TableReplicationStateRow {
    pub id: i64,
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableReplicationState,
    pub metadata: Option<serde_json::Value>,
    pub prev: Option<i64>,
    pub is_current: bool,
}

/// Fetch replication state rows for a specific pipeline from the source database
#[cfg(feature = "sqlx")]
pub async fn get_table_replication_state_rows(
    pool: &PgPool,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableReplicationStateRow>> {
    let states = sqlx::query_as::<_, TableReplicationStateRow>(
        r#"
        select id, pipeline_id, table_id, state, metadata, prev, is_current
        from etl.replication_state
        where pipeline_id = $1 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    Ok(states)
}

/// Update replication state using transactional approach with history chaining
pub async fn update_replication_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationState,
    metadata: serde_json::Value,
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;

    // Get the current row's id (if any)
    let current_id: Option<i64> = sqlx::query_scalar(
        r#"
        select id from etl.replication_state 
        where pipeline_id = $1 and table_id = $2 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(&mut *tx)
    .await?;

    // Set current row to not current
    if let Some(prev_id) = current_id {
        sqlx::query(
            r#"
            update etl.replication_state 
            set is_current = false 
            where id = $1
            "#,
        )
        .bind(prev_id)
        .execute(&mut *tx)
        .await?;
    }

    // Insert new row as current, linking to previous
    sqlx::query(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3, $4, $5, true)
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(state)
    .bind(metadata)
    .bind(current_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Rollback to the previous state for a table
pub async fn rollback_replication_state(
    pool: &PgPool,
    pipeline_id: u64,
    table_id: TableId,
) -> sqlx::Result<Option<TableReplicationStateRow>> {
    let mut tx = pool.begin().await?;

    // Get current row and its prev id
    let current_row: Option<(i64, Option<i64>)> = sqlx::query_as(
        r#"
        select id, prev from etl.replication_state 
        where pipeline_id = $1 and table_id = $2 and is_current = true
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(&mut *tx)
    .await?;

    if let Some((current_id, prev_id)) = current_row {
        if let Some(prev_id) = prev_id {
            // Set current row to not current
            sqlx::query(
                r#"
                update etl.replication_state 
                set is_current = false 
                where id = $1
                "#,
            )
            .bind(current_id)
            .execute(&mut *tx)
            .await?;

            // Set previous row to current
            sqlx::query(
                r#"
                update etl.replication_state 
                set is_current = true 
                where id = $1
                "#,
            )
            .bind(prev_id)
            .execute(&mut *tx)
            .await?;

            // Fetch the restored row
            let restored_row = sqlx::query_as::<_, TableReplicationStateRow>(
                r#"
                select id, pipeline_id, table_id, state, metadata, prev, is_current
                from etl.replication_state 
                where id = $1
                "#,
            )
            .bind(prev_id)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;

            return Ok(Some(restored_row));
        }
    }

    tx.rollback().await?;

    Ok(None)
}
