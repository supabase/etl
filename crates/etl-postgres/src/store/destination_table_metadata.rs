//! SQL accessors for destination table metadata stored in Postgres.

use std::collections::HashMap;

use sqlx::{
    PgExecutor, Row, Type,
    postgres::{PgRow, types::Oid as SqlxTableId},
};

use crate::schema::{SnapshotId, TableId};

/// Database enum type for destination table schema status.
///
/// Maps to the `etl.destination_table_schema_status` PostgreSQL enum type.
#[derive(Debug, Clone, Copy, Type, PartialEq, Eq)]
#[sqlx(type_name = "etl.destination_table_schema_status", rename_all = "snake_case")]
pub enum StoredDestinationTableSchemaStatus {
    /// The destination table is being created for the first time.
    Creating,
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

/// Decodes destination table metadata from a database row.
fn decode_metadata_row(row: PgRow) -> Result<StoredDestinationTableMetadataRow, sqlx::Error> {
    let table_id: SqlxTableId = row.get("table_id");
    let snapshot_id_str: String = row.get("snapshot_id");
    let previous_snapshot_id_str: Option<String> = row.get("previous_snapshot_id");
    let snapshot_id =
        snapshot_id_str.parse::<SnapshotId>().map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let previous_snapshot_id = previous_snapshot_id_str
        .map(|s| s.parse::<SnapshotId>())
        .transpose()
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    Ok(StoredDestinationTableMetadataRow {
        table_id: TableId::new(table_id.0),
        destination_table_id: row.get("destination_table_id"),
        snapshot_id,
        replication_mask: row.get("replication_mask"),
        previous_snapshot_id,
        previous_replication_mask: row.get("previous_replication_mask"),
        schema_status: row.get("schema_status"),
    })
}

/// Database row representation of destination table metadata.
#[derive(Debug, Clone)]
pub struct StoredDestinationTableMetadataRow {
    /// Source table identifier.
    pub table_id: TableId,
    /// Destination-specific table identifier.
    pub destination_table_id: String,
    /// Current schema snapshot applied at the destination.
    pub snapshot_id: SnapshotId,
    /// Column replication mask stored as raw bytes.
    pub replication_mask: Vec<u8>,
    /// The schema version before the current change. None for initial schemas.
    pub previous_snapshot_id: Option<SnapshotId>,
    /// The column replication mask before the current change.
    pub previous_replication_mask: Option<Vec<u8>>,
    /// Current destination schema application status.
    pub schema_status: StoredDestinationTableSchemaStatus,
}

/// Stores destination table metadata in the database.
///
/// Inserts or updates the complete metadata for a table at a destination. Uses
/// upsert semantics: if a row exists for (pipeline_id, table_id), all fields
/// are updated.
#[expect(clippy::too_many_arguments)]
pub async fn store_destination_table_metadata<'c, E>(
    executor: E,
    pipeline_id: &str,
    table_id: TableId,
    destination_table_id: &str,
    snapshot_id: SnapshotId,
    replication_mask: &[u8],
    previous_snapshot_id: Option<SnapshotId>,
    previous_replication_mask: Option<&[u8]>,
    schema_status: StoredDestinationTableSchemaStatus,
) -> Result<(), sqlx::Error>
where
    E: PgExecutor<'c>,
{
    sqlx::query(
        r#"
        insert into etl.destination_tables_metadata
            (pipeline_id_v2, table_id, destination_table_id, snapshot_id,
             replication_mask, previous_snapshot_id, previous_replication_mask, schema_status)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        on conflict (pipeline_id_v2, table_id)
        do update set
            destination_table_id = excluded.destination_table_id,
            snapshot_id = excluded.snapshot_id,
            replication_mask = excluded.replication_mask,
            previous_snapshot_id = excluded.previous_snapshot_id,
            previous_replication_mask = excluded.previous_replication_mask,
            schema_status = excluded.schema_status,
            updated_at = now()
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .bind(snapshot_id.to_string())
    .bind(replication_mask)
    .bind(previous_snapshot_id.map(|snapshot_id| snapshot_id.to_string()))
    .bind(previous_replication_mask)
    .bind(schema_status)
    .execute(executor)
    .await?;

    Ok(())
}

/// Loads all destination table metadata for a pipeline.
///
/// Returns a map from table_id to the complete metadata row.
pub async fn load_destination_tables_metadata<'c, E>(
    executor: E,
    pipeline_id: &str,
) -> Result<HashMap<TableId, StoredDestinationTableMetadataRow>, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let rows = sqlx::query(
        r#"
        select table_id, destination_table_id, snapshot_id,
               replication_mask, previous_snapshot_id, previous_replication_mask, schema_status
        from etl.destination_tables_metadata
        where pipeline_id_v2 = $1
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(executor)
    .await?;

    let mut metadata = HashMap::new();
    for row in rows {
        let row = decode_metadata_row(row)?;
        metadata.insert(row.table_id, row);
    }

    Ok(metadata)
}

/// Gets destination table metadata for a single table.
pub async fn get_destination_table_metadata<'c, E>(
    executor: E,
    pipeline_id: &str,
    table_id: TableId,
) -> Result<Option<StoredDestinationTableMetadataRow>, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let row = sqlx::query(
        r#"
        select table_id, destination_table_id, snapshot_id,
               replication_mask, previous_snapshot_id, previous_replication_mask, schema_status
        from etl.destination_tables_metadata
        where pipeline_id_v2 = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(executor)
    .await?;

    row.map(decode_metadata_row).transpose()
}

/// Deletes all destination table metadata for a pipeline.
///
/// Used during pipeline cleanup.
pub async fn delete_destination_tables_metadata_for_all_tables<'c, E>(
    executor: E,
    pipeline_id: &str,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_tables_metadata
        where pipeline_id_v2 = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes destination table metadata for a single table.
pub async fn delete_destination_table_metadata<'c, E>(
    executor: E,
    pipeline_id: &str,
    table_id: TableId,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_tables_metadata
        where pipeline_id_v2 = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}
