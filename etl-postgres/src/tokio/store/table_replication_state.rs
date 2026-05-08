use tokio_postgres::Transaction;

use crate::{
    store::{TableReplicationStateRow, TableReplicationStateType},
    tokio::PgSourceError,
    types::TableId,
};

fn state_type_from_str(state: &str) -> Result<TableReplicationStateType, PgSourceError> {
    match state {
        "init" => Ok(TableReplicationStateType::Init),
        "data_sync" => Ok(TableReplicationStateType::DataSync),
        "finished_copy" => Ok(TableReplicationStateType::FinishedCopy),
        "sync_done" => Ok(TableReplicationStateType::SyncDone),
        "ready" => Ok(TableReplicationStateType::Ready),
        "errored" => Ok(TableReplicationStateType::Errored),
        _ => Err(PgSourceError::InvalidData(format!("Unknown table replication state '{state}'"))),
    }
}

fn replication_state_row(
    row: tokio_postgres::Row,
) -> Result<TableReplicationStateRow, PgSourceError> {
    let state: String = row.get("state");
    Ok(TableReplicationStateRow {
        id: row.get("id"),
        pipeline_id: row.get("pipeline_id"),
        table_id: row.get("table_id"),
        state: state_type_from_str(&state)?,
        metadata: row.get("metadata"),
        prev: row.get("prev"),
        is_current: row.get("is_current"),
    })
}

/// Reads current table replication states for a pipeline.
pub async fn table_replication_state_rows(
    txn: &Transaction<'_>,
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
pub async fn update_replication_state_raw(
    txn: &Transaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationStateType,
    metadata: serde_json::Value,
) -> Result<(), PgSourceError> {
    let state = state.as_str();
    txn.execute(
        r#"
        with mark_old as (
            update etl.replication_state
            set is_current = false, updated_at = now()
            where pipeline_id = $1 and table_id = $2 and is_current = true
            returning id
        )
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3::text::etl.table_state, $4, (select id from mark_old), true)
        "#,
        &[&pipeline_id, &table_id, &state, &metadata],
    )
    .await?;

    Ok(())
}

/// Rolls back one table to its previous replication state.
pub async fn rollback_replication_state(
    txn: &Transaction<'_>,
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

    txn.execute("delete from etl.replication_state where id = $1", &[&current_id]).await?;
    txn.execute(
        "update etl.replication_state set is_current = true, updated_at = now() where id = $1",
        &[&prev_id],
    )
    .await?;

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
pub async fn reset_replication_state(
    txn: &Transaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<TableReplicationStateRow, PgSourceError> {
    txn.execute(
        "delete from etl.replication_state where pipeline_id = $1 and table_id = $2",
        &[&pipeline_id, &table_id],
    )
    .await?;

    let metadata = serde_json::json!({"type": "init"});
    let state = TableReplicationStateType::Init.as_str();
    let row = txn
        .query_one(
            r#"
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
            values ($1, $2, $3::text::etl.table_state, $4, null, true)
            returning id, pipeline_id, table_id, state::text as state, metadata, prev, is_current
            "#,
            &[&pipeline_id, &table_id, &state, &metadata],
        )
        .await?;

    replication_state_row(row)
}

/// Deletes all replication state entries for a pipeline.
pub async fn delete_replication_state_for_all_tables(
    txn: &Transaction<'_>,
    pipeline_id: i64,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute("delete from etl.replication_state where pipeline_id = $1", &[&pipeline_id])
        .await?)
}

/// Deletes replication state entries for one table.
pub async fn delete_replication_state(
    txn: &Transaction<'_>,
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
