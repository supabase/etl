use std::collections::HashMap;

use tokio_postgres::{Row, types::PgLsn};

use crate::{
    store::type_mappings::{postgres_type_to_string, string_to_postgres_type},
    tokio::{PgSourceClient, PgSourceError, PgSourceTransaction},
    types::{ColumnSchema, SnapshotId, TableId, TableName, TableSchema},
};

fn parse_snapshot_id(value: &str) -> Result<SnapshotId, PgSourceError> {
    SnapshotId::from_pg_lsn_string(value).map_err(|err| {
        PgSourceError::InvalidData(format!("Snapshot ID deserialization failed: {err}"))
    })
}

fn parse_column_schema(row: &Row) -> ColumnSchema {
    ColumnSchema {
        name: row.get("column_name"),
        typ: string_to_postgres_type(row.get::<_, String>("column_type").as_str()),
        modifier: row.get("type_modifier"),
        nullable: row.get("nullable"),
        ordinal_position: row.get("ordinal_position"),
        primary_key_ordinal_position: row.get("primary_key_ordinal_position"),
    }
}

/// Stores a table schema in source metadata tables.
pub async fn store_table_schema(
    source_client: &mut PgSourceClient,
    pipeline_id: i64,
    table_schema: &TableSchema,
) -> Result<(), PgSourceError> {
    let tx = source_client.begin().await?;
    let snapshot_id = table_schema.snapshot_id.to_pg_lsn_string();
    let table_schema_id: i64 = tx
        .query_one(
            r#"
            insert into etl.table_schemas (pipeline_id, table_id, schema_name, table_name, snapshot_id)
            values ($1, $2, $3, $4, $5::text::pg_lsn)
            on conflict (pipeline_id, table_id, snapshot_id)
            do update set
                schema_name = excluded.schema_name,
                table_name = excluded.table_name,
                updated_at = now()
            returning id
            "#,
            &[
                &pipeline_id,
                &table_schema.id,
                &table_schema.name.schema,
                &table_schema.name.name,
                &snapshot_id,
            ],
        )
        .await?
        .get(0);

    tx.execute("delete from etl.table_columns where table_schema_id = $1", &[&table_schema_id])
        .await?;

    for column_schema in &table_schema.column_schemas {
        tx.execute(
            r#"
            insert into etl.table_columns
            (table_schema_id, column_name, column_type, type_modifier, nullable,
             ordinal_position, primary_key_ordinal_position)
            values ($1, $2, $3, $4, $5, $6, $7)
            "#,
            &[
                &table_schema_id,
                &column_schema.name,
                &postgres_type_to_string(&column_schema.typ),
                &column_schema.modifier,
                &column_schema.nullable,
                &column_schema.ordinal_position,
                &column_schema.primary_key_ordinal_position,
            ],
        )
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Loads all current table schemas for a pipeline.
pub async fn load_table_schemas(
    source_client: &PgSourceClient,
    pipeline_id: i64,
) -> Result<Vec<TableSchema>, PgSourceError> {
    load_table_schemas_at_snapshot(source_client, pipeline_id, SnapshotId::max()).await
}

/// Loads one table schema at or before a snapshot.
pub async fn load_table_schema_at_snapshot(
    source_client: &PgSourceClient,
    pipeline_id: i64,
    table_id: TableId,
    snapshot_id: SnapshotId,
) -> Result<Option<TableSchema>, PgSourceError> {
    let snapshot_id = snapshot_id.to_pg_lsn_string();
    let rows = source_client
        .query(
            r#"
            select
                ts.table_id,
                ts.schema_name,
                ts.table_name,
                ts.snapshot_id::text as snapshot_id,
                tc.column_name,
                tc.column_type,
                tc.type_modifier,
                tc.nullable,
                tc.ordinal_position,
                tc.primary_key_ordinal_position
            from etl.table_schemas ts
            inner join etl.table_columns tc on ts.id = tc.table_schema_id
            where ts.id = (
                select id from etl.table_schemas
                where pipeline_id = $1 and table_id = $2 and snapshot_id <= $3::text::pg_lsn
                order by snapshot_id desc
                limit 1
            )
            order by tc.ordinal_position
            "#,
            &[&pipeline_id, &table_id, &snapshot_id],
        )
        .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    Ok(Some(table_schema_from_rows(rows)?))
}

/// Loads all table schemas at or before a snapshot.
pub async fn load_table_schemas_at_snapshot(
    source_client: &PgSourceClient,
    pipeline_id: i64,
    snapshot_id: SnapshotId,
) -> Result<Vec<TableSchema>, PgSourceError> {
    let snapshot_id = snapshot_id.to_pg_lsn_string();
    let rows = source_client
        .query(
            r#"
            with latest_schemas as (
                select distinct on (ts.table_id)
                    ts.id,
                    ts.table_id,
                    ts.schema_name,
                    ts.table_name,
                    ts.snapshot_id
                from etl.table_schemas ts
                where ts.pipeline_id = $1
                  and ts.snapshot_id <= $2::text::pg_lsn
                order by ts.table_id, ts.snapshot_id desc
            )
            select
                ls.table_id,
                ls.schema_name,
                ls.table_name,
                ls.snapshot_id::text as snapshot_id,
                tc.column_name,
                tc.column_type,
                tc.type_modifier,
                tc.nullable,
                tc.ordinal_position,
                tc.primary_key_ordinal_position
            from latest_schemas ls
            inner join etl.table_columns tc on ls.id = tc.table_schema_id
            order by ls.table_id, tc.ordinal_position
            "#,
            &[&pipeline_id, &snapshot_id],
        )
        .await?;

    let mut table_schemas = HashMap::new();
    for row in rows {
        let table_id: TableId = row.get("table_id");
        let schema_name: String = row.get("schema_name");
        let table_name: String = row.get("table_name");
        let snapshot_id = parse_snapshot_id(row.get::<_, String>("snapshot_id").as_str())?;

        let entry = table_schemas.entry(table_id).or_insert_with(|| {
            TableSchema::with_snapshot_id(
                table_id,
                TableName::new(schema_name, table_name),
                vec![],
                snapshot_id,
            )
        });

        entry.add_column_schema(parse_column_schema(&row));
    }

    Ok(table_schemas.into_values().collect())
}

fn table_schema_from_rows(rows: Vec<Row>) -> Result<TableSchema, PgSourceError> {
    let first_row = rows.first().expect("table schema rows must be non-empty");
    let table_id: TableId = first_row.get("table_id");
    let schema_name: String = first_row.get("schema_name");
    let table_name: String = first_row.get("table_name");
    let snapshot_id = parse_snapshot_id(first_row.get::<_, String>("snapshot_id").as_str())?;

    let mut table_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new(schema_name, table_name),
        vec![],
        snapshot_id,
    );

    for row in rows {
        table_schema.add_column_schema(parse_column_schema(&row));
    }

    Ok(table_schema)
}

/// Deletes obsolete table schema versions from source metadata tables.
pub async fn delete_obsolete_table_schema_versions(
    source_client: &PgSourceClient,
    pipeline_id: i64,
    retention_lsns: &HashMap<TableId, PgLsn>,
) -> Result<u64, PgSourceError> {
    if retention_lsns.is_empty() {
        return Ok(0);
    }

    let mut table_ids = Vec::with_capacity(retention_lsns.len());
    let mut retention_lsn_values = Vec::with_capacity(retention_lsns.len());
    for (table_id, retention_lsn) in retention_lsns {
        table_ids.push(*table_id);
        retention_lsn_values.push(retention_lsn.to_string());
    }

    source_client
        .execute(
            r#"
            with cleanup_retentions as (
                select table_id, retention_lsn::pg_lsn as retention_lsn
                from unnest($2::oid[], $3::text[])
                    as cleanup(table_id, retention_lsn)
            ),
            retained_snapshots as (
                select ts.table_id, max(ts.snapshot_id) as retained_snapshot_id
                from etl.table_schemas ts
                join cleanup_retentions cleanup
                  on cleanup.table_id = ts.table_id
                where ts.pipeline_id = $1
                  and ts.snapshot_id <= cleanup.retention_lsn
                group by ts.table_id
            )
            delete from etl.table_schemas ts
            using retained_snapshots rs
            where ts.pipeline_id = $1
              and ts.table_id = rs.table_id
              and ts.snapshot_id < rs.retained_snapshot_id
            "#,
            &[&pipeline_id, &table_ids, &retention_lsn_values],
        )
        .await
}

/// Deletes all table schemas for a pipeline.
pub async fn delete_table_schemas_for_all_tables(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
) -> Result<u64, PgSourceError> {
    Ok(txn.execute("delete from etl.table_schemas where pipeline_id = $1", &[&pipeline_id]).await?)
}

/// Deletes table schemas for one table.
pub async fn delete_table_schemas(
    txn: &PgSourceTransaction<'_>,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, PgSourceError> {
    Ok(txn
        .execute(
            "delete from etl.table_schemas where pipeline_id = $1 and table_id = $2",
            &[&pipeline_id, &table_id],
        )
        .await?)
}
