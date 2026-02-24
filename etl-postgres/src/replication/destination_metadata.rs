use sqlx::postgres::types::Oid as SqlxTableId;
use sqlx::{PgExecutor, PgPool, Row, Type};
use std::collections::HashMap;

use crate::types::{SnapshotId, TableId};

/// Database enum type for destination table schema status.
///
/// Maps to the `etl.destination_table_schema_status` PostgreSQL enum type.
#[derive(Debug, Clone, Copy, Type, PartialEq, Eq)]
#[sqlx(
    type_name = "etl.destination_table_schema_status",
    rename_all = "snake_case"
)]
pub enum DestinationTableSchemaStatus {
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

/// Parses snapshot IDs from PG LSN strings.
///
/// Converts a required snapshot_id string and optional previous_snapshot_id string
/// into their corresponding [`SnapshotId`] values.
fn parse_snapshot_ids(
    snapshot_id_str: &str,
    previous_snapshot_id_str: Option<String>,
) -> Result<(SnapshotId, Option<SnapshotId>), sqlx::Error> {
    let snapshot_id = SnapshotId::from_pg_lsn_string(snapshot_id_str)
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let previous_snapshot_id = previous_snapshot_id_str
        .map(|s| SnapshotId::from_pg_lsn_string(&s))
        .transpose()
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    Ok((snapshot_id, previous_snapshot_id))
}

/// Database row representation of destination table metadata.
#[derive(Debug, Clone)]
pub struct DestinationTableMetadataRow {
    pub table_id: TableId,
    pub destination_table_id: String,
    pub snapshot_id: SnapshotId,
    /// The schema version before the current change. None for initial schemas.
    pub previous_snapshot_id: Option<SnapshotId>,
    pub schema_status: DestinationTableSchemaStatus,
    pub replication_mask: Vec<u8>,
}

/// Stores destination table metadata in the database.
///
/// Inserts or updates the complete metadata for a table at a destination.
/// Uses upsert semantics: if a row exists for (pipeline_id, table_id),
/// all fields are updated.
#[allow(clippy::too_many_arguments)]
pub async fn store_destination_table_metadata(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
    snapshot_id: SnapshotId,
    previous_snapshot_id: Option<SnapshotId>,
    schema_status: DestinationTableSchemaStatus,
    replication_mask: &[u8],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into etl.destination_tables_metadata
            (pipeline_id, table_id, destination_table_id, snapshot_id,
             previous_snapshot_id, schema_status, replication_mask)
        values ($1, $2, $3, $4::pg_lsn, $5::pg_lsn, $6, $7)
        on conflict (pipeline_id, table_id)
        do update set
            destination_table_id = excluded.destination_table_id,
            snapshot_id = excluded.snapshot_id,
            previous_snapshot_id = excluded.previous_snapshot_id,
            schema_status = excluded.schema_status,
            replication_mask = excluded.replication_mask,
            updated_at = now()
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .bind(snapshot_id.to_pg_lsn_string())
    .bind(previous_snapshot_id.map(|s| s.to_pg_lsn_string()))
    .bind(schema_status)
    .bind(replication_mask)
    .execute(pool)
    .await?;

    Ok(())
}

/// Loads all destination table metadata for a pipeline.
///
/// Returns a map from table_id to the complete metadata row.
pub async fn load_destination_tables_metadata(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<HashMap<TableId, DestinationTableMetadataRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        select table_id, destination_table_id, snapshot_id::text as snapshot_id,
               previous_snapshot_id::text as previous_snapshot_id, schema_status, replication_mask
        from etl.destination_tables_metadata
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    let mut metadata = HashMap::new();
    for row in rows {
        let table_id: SqlxTableId = row.get("table_id");
        let table_id = TableId::new(table_id.0);
        let snapshot_id_str: String = row.get("snapshot_id");
        let previous_snapshot_id_str: Option<String> = row.get("previous_snapshot_id");
        let (snapshot_id, previous_snapshot_id) =
            parse_snapshot_ids(&snapshot_id_str, previous_snapshot_id_str)?;

        metadata.insert(
            table_id,
            DestinationTableMetadataRow {
                table_id,
                destination_table_id: row.get("destination_table_id"),
                snapshot_id,
                previous_snapshot_id,
                schema_status: row.get("schema_status"),
                replication_mask: row.get("replication_mask"),
            },
        );
    }

    Ok(metadata)
}

/// Gets destination table metadata for a single table.
pub async fn get_destination_table_metadata(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<Option<DestinationTableMetadataRow>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select table_id, destination_table_id, snapshot_id::text as snapshot_id,
               previous_snapshot_id::text as previous_snapshot_id, schema_status, replication_mask
        from etl.destination_tables_metadata
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => {
            let table_id: SqlxTableId = r.get("table_id");
            let snapshot_id_str: String = r.get("snapshot_id");
            let previous_snapshot_id_str: Option<String> = r.get("previous_snapshot_id");
            let (snapshot_id, previous_snapshot_id) =
                parse_snapshot_ids(&snapshot_id_str, previous_snapshot_id_str)?;

            Ok(Some(DestinationTableMetadataRow {
                table_id: TableId::new(table_id.0),
                destination_table_id: r.get("destination_table_id"),
                snapshot_id,
                previous_snapshot_id,
                schema_status: r.get("schema_status"),
                replication_mask: r.get("replication_mask"),
            }))
        }
        None => Ok(None),
    }
}

/// Deletes all destination table metadata for a pipeline.
///
/// Used during pipeline cleanup.
pub async fn delete_destination_tables_metadata_for_all_tables<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_tables_metadata
        where pipeline_id = $1
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
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_tables_metadata
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}
