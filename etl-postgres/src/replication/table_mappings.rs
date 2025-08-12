use sqlx::{PgExecutor, PgPool, Row, postgres::types::Oid as SqlxTableId};
use std::collections::HashMap;

use crate::schema::TableId;

/// Stores a table mapping in the database.
///
/// Inserts or updates a mapping between source table ID and destination table ID
/// for the specified pipeline.
pub async fn store_table_mapping(
    pool: &PgPool,
    pipeline_id: i64,
    source_table_id: &TableId,
    destination_table_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO etl.table_mappings (pipeline_id, source_table_id, destination_table_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (pipeline_id, source_table_id)
        DO UPDATE SET 
            destination_table_id = EXCLUDED.destination_table_id,
            updated_at = now()
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(source_table_id.into_inner()))
    .bind(destination_table_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Loads all table mappings for a pipeline from the database.
///
/// Retrieves all source table ID to destination table ID mappings for the specified pipeline.
pub async fn load_table_mappings(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<HashMap<TableId, String>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT source_table_id, destination_table_id
        FROM etl.table_mappings
        WHERE pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    let mut mappings = HashMap::new();
    for row in rows {
        let source_table_id: SqlxTableId = row.get("source_table_id");
        let destination_table_id: String = row.get("destination_table_id");

        mappings.insert(TableId::new(source_table_id.0), destination_table_id);
    }

    Ok(mappings)
}

/// Deletes all table mappings for a pipeline from the database.
///
/// Removes all table mapping records for the specified pipeline.
/// Used during pipeline cleanup.
pub async fn delete_pipeline_table_mappings<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        DELETE FROM etl.table_mappings
        WHERE pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}
