use crate::{
    store::{TableReplicationStateRow, TableReplicationStateType},
    tokio::{PgSourceError, PgSourceTransaction},
    types::TableId,
};

/// Builds a table replication state row from a database row.
fn replication_state_row(
    row: tokio_postgres::Row,
) -> Result<TableReplicationStateRow, PgSourceError> {
    let state: String = row.get("state");
    Ok(TableReplicationStateRow {
        id: row.get("id"),
        pipeline_id: row.get("pipeline_id"),
        table_id: row.get("table_id"),
        state: TableReplicationStateType::try_from(state.as_str())?,
        metadata: row.get("metadata"),
        prev: row.get("prev"),
        is_current: row.get("is_current"),
    })
}

/// Reads current table replication states for a pipeline.
///
/// Retrieves the current replication state records for all tables in the
/// pipeline.
pub async fn table_replication_state_rows(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
) -> Result<Vec<TableReplicationStateRow>, PgSourceError> {
    let rows = txn
        .query(
            r#"
            select id, pipeline_id, table_id, state::text as state, metadata, prev, is_current
            from etl.replication_state
            where pipeline_id = $1 and is_current = true
            "#,
            &[&pipeline_id],
        )
        .await?;

    rows.into_iter().map(replication_state_row).collect()
}

/// Updates replication state using raw database types.
///
/// Performs the database update with pre-serialized state data and proper
/// history chaining.
pub async fn update_replication_state_raw(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationStateType,
    metadata: serde_json::Value,
) -> Result<(), PgSourceError> {
    let state: &'static str = state.into();
    // Mark the old row as not current and insert the new one in a single CTE.
    // The INSERT references mark_old to ensure the UPDATE completes first.
    txn.execute(
        r#"
        with mark_old as (
            update etl.replication_state
            set is_current = false, updated_at = now()
            where pipeline_id = $1 and table_id = $2 and is_current = true
            returning id
        )
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3::etl.table_state, $4, (select id from mark_old), true)
        "#,
        &[&pipeline_id, &table_id, &state, &metadata],
    )
    .await?;

    Ok(())
}

/// Rolls back one table to its previous replication state.
///
/// Restores the previous state by updating the current flags in the database,
/// returning the restored state if successful.
pub async fn rollback_replication_state(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<Option<TableReplicationStateRow>, PgSourceError> {
    let current_row = txn
        .query_opt(
            r#"
            select id, prev from etl.replication_state
            where pipeline_id = $1 and table_id = $2 and is_current = true
            "#,
            &[&pipeline_id, &table_id],
        )
        .await?;

    let Some(row) = current_row else {
        return Ok(None);
    };
    let current_id: i64 = row.get("id");
    let prev_id: Option<i64> = row.get("prev");
    let Some(prev_id) = prev_id else {
        return Ok(None);
    };

    // Delete the row we are rolling back from to avoid buildup. Technically, we
    // could keep the previous row for tracking purposes, but especially during
    // timed retries, we might end up with unbounded database growth.
    txn.execute("delete from etl.replication_state where id = $1", &[&current_id]).await?;

    // Set previous row to current.
    txn.execute(
        "update etl.replication_state set is_current = true, updated_at = now() where id = $1",
        &[&prev_id],
    )
    .await?;

    // Fetch the restored row.
    let row = txn
        .query_one(
            r#"
            select id, pipeline_id, table_id, state::text as state, metadata, prev, is_current
            from etl.replication_state
            where id = $1
            "#,
            &[&prev_id],
        )
        .await?;

    Ok(Some(replication_state_row(row)?))
}

/// Resets one table to initial replication state.
///
/// Removes all existing state entries for the table, including history, and
/// creates a new `Init` entry. Destination metadata and schemas are preserved
/// for use on restart.
pub async fn reset_replication_state(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<TableReplicationStateRow, PgSourceError> {
    // Delete all existing entries for this pipeline and table.
    txn.execute(
        "delete from etl.replication_state where pipeline_id = $1 and table_id = $2",
        &[&pipeline_id, &table_id],
    )
    .await?;

    // Insert a new `Init` state entry and return it.
    let metadata = serde_json::json!({"type": "init"});
    let state: &'static str = TableReplicationStateType::Init.into();
    let row = txn
        .query_one(
            r#"
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
            values ($1, $2, $3::etl.table_state, $4, null, true)
            returning id, pipeline_id, table_id, state::text as state, metadata, prev, is_current
            "#,
            &[&pipeline_id, &table_id, &state, &metadata],
        )
        .await?;

    replication_state_row(row)
}

/// Deletes all replication state entries for a pipeline.
///
/// Removes current and historical replication state rows.
pub async fn delete_replication_state_for_all_tables(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute("delete from etl.replication_state where pipeline_id = $1", &[&pipeline_id])
        .await?)
}

/// Deletes replication state entries for one table.
///
/// Removes current and historical replication state rows for the table.
pub async fn delete_replication_state(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute(
            "delete from etl.replication_state where pipeline_id = $1 and table_id = $2",
            &[&pipeline_id, &table_id],
        )
        .await?)
}
