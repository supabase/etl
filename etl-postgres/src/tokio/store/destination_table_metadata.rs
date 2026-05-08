use std::collections::HashMap;

use crate::{
    store::{DestinationTableMetadataRow, DestinationTableSchemaStatus},
    tokio::{PgSourceClient, PgSourceError, PgSourceTransaction, store::parse_snapshot_id},
    types::{SnapshotId, TableId},
};

/// Stores destination table metadata in the source metadata tables.
///
/// Uses upsert semantics: if a row already exists for `(pipeline_id,
/// table_id)`, all metadata fields are replaced with the new values.
#[allow(clippy::too_many_arguments)]
pub async fn store_destination_table_metadata(
    source_client: &PgSourceClient,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
    snapshot_id: SnapshotId,
    previous_snapshot_id: Option<SnapshotId>,
    schema_status: DestinationTableSchemaStatus,
    replication_mask: &[u8],
) -> Result<(), PgSourceError> {
    let snapshot_id = snapshot_id.to_pg_lsn_string();
    let previous_snapshot_id = previous_snapshot_id.map(SnapshotId::to_pg_lsn_string);
    let schema_status: &'static str = schema_status.into();

    source_client
        .execute(
            r#"
            insert into etl.destination_tables_metadata
                (pipeline_id, table_id, destination_table_id, snapshot_id,
                 previous_snapshot_id, schema_status, replication_mask)
            values ($1, $2, $3, $4::pg_lsn, $5::pg_lsn, $6::etl.destination_table_schema_status, $7)
            on conflict (pipeline_id, table_id)
            do update set
                destination_table_id = excluded.destination_table_id,
                snapshot_id = excluded.snapshot_id,
                previous_snapshot_id = excluded.previous_snapshot_id,
                schema_status = excluded.schema_status,
                replication_mask = excluded.replication_mask,
                updated_at = now()
            "#,
            &[
                &pipeline_id,
                &table_id,
                &destination_table_id,
                &snapshot_id,
                &previous_snapshot_id,
                &schema_status,
                &replication_mask,
            ],
        )
        .await?;

    Ok(())
}

/// Loads destination table metadata from source metadata tables.
///
/// Returns metadata keyed by source table ID.
pub async fn load_destination_tables_metadata(
    source_client: &PgSourceClient,
    pipeline_id: i64,
) -> Result<HashMap<TableId, DestinationTableMetadataRow>, PgSourceError> {
    let rows = source_client
        .query(
            r#"
            select table_id, destination_table_id, snapshot_id::text as snapshot_id,
                   previous_snapshot_id::text as previous_snapshot_id,
                   schema_status::text as schema_status, replication_mask
            from etl.destination_tables_metadata
            where pipeline_id = $1
            "#,
            &[&pipeline_id],
        )
        .await?;

    let mut metadata = HashMap::with_capacity(rows.len());
    for row in rows {
        let table_id: TableId = row.get("table_id");
        let snapshot_id = parse_snapshot_id(row.get::<_, String>("snapshot_id").as_str())?;
        let previous_snapshot_id = row
            .get::<_, Option<String>>("previous_snapshot_id")
            .map(|snapshot_id| parse_snapshot_id(&snapshot_id))
            .transpose()?;

        metadata.insert(
            table_id,
            DestinationTableMetadataRow {
                table_id,
                destination_table_id: row.get("destination_table_id"),
                snapshot_id,
                previous_snapshot_id,
                schema_status: DestinationTableSchemaStatus::try_from(
                    row.get::<_, String>("schema_status").as_str(),
                )?,
                replication_mask: row.get("replication_mask"),
            },
        );
    }

    Ok(metadata)
}

/// Deletes all destination table metadata for a pipeline.
pub async fn delete_destination_tables_metadata_for_all_tables(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute(
            "delete from etl.destination_tables_metadata where pipeline_id = $1",
            &[&pipeline_id],
        )
        .await?)
}

/// Deletes destination table metadata for one table.
pub async fn delete_destination_table_metadata(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute(
            "delete from etl.destination_tables_metadata where pipeline_id = $1 and table_id = $2",
            &[&pipeline_id, &table_id],
        )
        .await?)
}
