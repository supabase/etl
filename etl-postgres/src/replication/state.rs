use sqlx::{PgExecutor, Type, postgres::types::Oid as SqlxTableId, prelude::FromRow};

use crate::types::TableId;

/// Database enum type for table replication states.
#[derive(Debug, Clone, Copy, Type, PartialEq)]
#[sqlx(type_name = "etl.table_state", rename_all = "snake_case")]
pub enum TableReplicationStateType {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone,
    Ready,
    Errored,
}

/// Database row representation of table replication state.
#[derive(Debug, FromRow)]
pub struct TableReplicationStateRow {
    pub id: i64,
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableReplicationStateType,
    pub metadata: Option<serde_json::Value>,
    pub prev: Option<i64>,
    pub is_current: bool,
}

impl TableReplicationStateRow {
    /// Returns the state type without deserializing metadata.
    pub fn state_type(&self) -> TableReplicationStateType {
        self.state
    }
}

/// Fetches current replication state rows for a pipeline.
///
/// Retrieves the current replication state records of all tables of a pipeline.
pub async fn get_table_replication_state_rows<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableReplicationStateRow>>
where
    E: PgExecutor<'c>,
{
    let states: Vec<TableReplicationStateRow> = sqlx::query_as(
        r#"
        select id, pipeline_id, table_id, state, metadata, prev, is_current
        from etl.replication_state
        where pipeline_id = $1 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(executor)
    .await?;

    Ok(states)
}

/// Updates replication state using raw database types.
///
/// Performs the database update with pre-serialized state data and proper history chaining.
pub async fn update_replication_state_raw<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationStateType,
    metadata: serde_json::Value,
) -> sqlx::Result<()>
where
    E: PgExecutor<'c>,
{
    // Mark the old row as not current and insert the new one in a single CTE.
    // The INSERT references mark_old to ensure the UPDATE completes first.
    sqlx::query(
        r#"
        with mark_old as (
            update etl.replication_state
            set is_current = false, updated_at = now()
            where pipeline_id = $1 and table_id = $2 and is_current = true
            returning id
        )
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3, $4, (select id from mark_old), true)
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(state)
    .bind(metadata)
    .execute(executor)
    .await?;

    Ok(())
}

/// Rolls back table state to the previous entry in the history chain.
///
/// Restores the previous state by updating the current flags in the database,
/// returning the restored state if successful.
pub async fn rollback_replication_state(
    conn: &mut sqlx::PgConnection,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<Option<TableReplicationStateRow>> {
    // Get current row and its prev id
    let current_row: Option<(i64, Option<i64>)> = sqlx::query_as(
        r#"
        select id, prev from etl.replication_state
        where pipeline_id = $1 and table_id = $2 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(&mut *conn)
    .await?;

    if let Some((current_id, Some(prev_id))) = current_row {
        // Delete the row we are rolling back from to avoid buildup. Technically, we could keep the
        // previous row for tracking purposes, but especially during timed retries, we might end up
        // with an infinite growth of the database.
        sqlx::query(
            r#"
            delete from etl.replication_state
            where id = $1
            "#,
        )
        .bind(current_id)
        .execute(&mut *conn)
        .await?;

        // Set previous row to current
        sqlx::query(
            r#"
            update etl.replication_state
            set is_current = true, updated_at = now()
            where id = $1
            "#,
        )
        .bind(prev_id)
        .execute(&mut *conn)
        .await?;

        // Fetch the restored row
        let restored_row: TableReplicationStateRow = sqlx::query_as(
            r#"
            select id, pipeline_id, table_id, state, metadata, prev, is_current
            from etl.replication_state
            where id = $1
            "#,
        )
        .bind(prev_id)
        .fetch_one(&mut *conn)
        .await?;

        return Ok(Some(restored_row));
    }

    Ok(None)
}

/// Resets table replication state to initial state.
///
/// Removes all existing state entries for the table (including history) and creates a new
/// Init entry, effectively restarting replication from scratch.
/// Table mappings and schemas are preserved for use on restart.
pub async fn reset_replication_state(
    conn: &mut sqlx::PgConnection,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<TableReplicationStateRow> {
    // Delete all existing entries for this pipeline and table
    sqlx::query(
        r#"
        delete from etl.replication_state
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(&mut *conn)
    .await?;

    // Insert a new `Init` state entry and return it
    let metadata = serde_json::json!({"type": "init"});
    let row: TableReplicationStateRow = sqlx::query_as(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3, $4, null, true)
        returning id, pipeline_id, table_id, state, metadata, prev, is_current
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(TableReplicationStateType::Init)
    .bind(metadata)
    .fetch_one(&mut *conn)
    .await?;

    Ok(row)
}

/// Deletes all replication state entries for a pipeline.
///
/// Removes all replication state records including historical entries
/// for the specified pipeline. Used during pipeline cleanup.
pub async fn delete_replication_state_for_all_tables<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.replication_state
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes all replication state entries for a specific table in a pipeline.
pub async fn delete_replication_state_for_table<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.replication_state
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Gets all table IDs that have replication state for a given pipeline.
///
/// Returns a vector of table IDs that are currently being replicated for the specified pipeline.
/// This is useful for operations that need to act on all tables in a pipeline, such as
/// cleaning up replication slots.
pub async fn get_pipeline_table_ids<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableId>>
where
    E: PgExecutor<'c>,
{
    let ids: Vec<SqlxTableId> = sqlx::query_scalar(
        r#"
        select distinct table_id
        from etl.replication_state
        where pipeline_id = $1 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(executor)
    .await?;

    Ok(ids.into_iter().map(|oid| TableId::new(oid.0)).collect())
}
