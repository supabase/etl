//! SQL accessors for durable destination write stream state.

use sqlx::{PgExecutor, PgPool, Row, postgres::types::Oid as SqlxTableId};

use crate::schema::TableId;

/// Database row representation of destination write stream state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDestinationWriteStreamRow {
    /// Source table identifier.
    pub table_id: TableId,
    /// Destination-specific physical table identifier.
    pub destination_table_id: String,
    /// Fully-qualified BigQuery Storage Write API stream name.
    pub stream_name: String,
    /// Next contiguous row offset to append.
    pub next_offset: i64,
    /// Last append-only sequence number durably appended to this stream.
    pub last_sequence_number: Option<String>,
}

/// Gets write stream state for a physical destination table.
pub async fn get_destination_write_stream(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
) -> Result<Option<StoredDestinationWriteStreamRow>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select table_id, destination_table_id, stream_name, next_offset, last_sequence_number
        from etl.destination_write_streams
        where pipeline_id = $1 and table_id = $2 and destination_table_id = $3
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| {
        let table_id: SqlxTableId = row.get("table_id");
        StoredDestinationWriteStreamRow {
            table_id: TableId::new(table_id.0),
            destination_table_id: row.get("destination_table_id"),
            stream_name: row.get("stream_name"),
            next_offset: row.get("next_offset"),
            last_sequence_number: row.get("last_sequence_number"),
        }
    }))
}

/// Stores write stream state for a physical destination table.
pub async fn store_destination_write_stream(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
    stream_name: &str,
    next_offset: i64,
    last_sequence_number: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into etl.destination_write_streams
            (pipeline_id, table_id, destination_table_id, stream_name, next_offset, last_sequence_number)
        values ($1, $2, $3, $4, $5, $6)
        on conflict (pipeline_id, table_id, destination_table_id)
        do update set
            stream_name = excluded.stream_name,
            next_offset = excluded.next_offset,
            last_sequence_number = excluded.last_sequence_number,
            updated_at = now()
        where etl.destination_write_streams.stream_name = excluded.stream_name
            and etl.destination_write_streams.next_offset <= excluded.next_offset
            and (
                etl.destination_write_streams.last_sequence_number is null
                or excluded.last_sequence_number >= etl.destination_write_streams.last_sequence_number
            )
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .bind(stream_name)
    .bind(next_offset)
    .bind(last_sequence_number)
    .execute(pool)
    .await?;

    Ok(())
}

/// Replaces write stream state when a destination rotates streams.
pub async fn replace_destination_write_stream(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
    expected_stream_name: &str,
    stream_name: &str,
    next_offset: i64,
    last_sequence_number: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        insert into etl.destination_write_streams
            (pipeline_id, table_id, destination_table_id, stream_name, next_offset, last_sequence_number)
        values ($1, $2, $3, $5, $6, $7)
        on conflict (pipeline_id, table_id, destination_table_id)
        do update set
            stream_name = excluded.stream_name,
            next_offset = excluded.next_offset,
            last_sequence_number = excluded.last_sequence_number,
            updated_at = now()
        where etl.destination_write_streams.stream_name = $4
            and (
                etl.destination_write_streams.last_sequence_number is null
                or excluded.last_sequence_number >= etl.destination_write_streams.last_sequence_number
            )
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .bind(expected_stream_name)
    .bind(stream_name)
    .bind(next_offset)
    .bind(last_sequence_number)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes write stream state for a single source table.
pub async fn delete_destination_write_streams_for_table<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_write_streams
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes write stream state for one physical destination table.
pub async fn delete_destination_write_stream<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
    destination_table_id: &str,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_write_streams
        where pipeline_id = $1
          and table_id = $2
          and destination_table_id = $3
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(destination_table_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes all write stream state for a pipeline.
pub async fn delete_destination_write_streams_for_all_tables<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.destination_write_streams
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}
