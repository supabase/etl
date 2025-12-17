use sqlx::postgres::PgRow;
use sqlx::postgres::types::Oid as SqlxTableId;
use sqlx::{PgExecutor, PgPool, Row, Type};
use std::collections::HashMap;
use tokio_postgres::types::Type as PgType;

use crate::types::{ColumnSchema, SnapshotId, TableId, TableName, TableSchema};

macro_rules! define_type_mappings {
    (
        $(
            $pg_type:ident => $string_name:literal
        ),* $(,)?
    ) => {
        /// Converts a Postgres type name string to a [`PgType`].
        ///
        /// Maps string representations to their corresponding Postgres types,
        /// handling common types and falling back to `TEXT` for unknown types.
        pub fn string_to_postgres_type(type_str: &str) -> PgType {
            match type_str {
                $(
                    $string_name => PgType::$pg_type,
                )*
                _ => PgType::TEXT, // Fallback for unknown types
            }
        }

        /// Converts a Postgres [`PgType`] to its string representation.
        ///
        /// Maps Postgres types to string equivalents for database storage,
        /// handling common types with fallback for unknown types.
        pub fn postgres_type_to_string(pg_type: &PgType) -> String {
            match *pg_type {
                $(
                    PgType::$pg_type => $string_name.to_string(),
                )*
                _ => format!("UNKNOWN({})", pg_type.name()),
            }
        }

        #[cfg(test)]
        pub fn get_test_type_mappings() -> Vec<(PgType, &'static str)> {
            vec![
                $(
                    (PgType::$pg_type, $string_name),
                )*
            ]
        }
    };
}

define_type_mappings! {
    // Basic types
    BOOL => "BOOL",
    CHAR => "CHAR",
    INT2 => "INT2",
    INT4 => "INT4",
    INT8 => "INT8",
    FLOAT4 => "FLOAT4",
    FLOAT8 => "FLOAT8",
    TEXT => "TEXT",
    VARCHAR => "VARCHAR",
    TIMESTAMP => "TIMESTAMP",
    TIMESTAMPTZ => "TIMESTAMPTZ",
    DATE => "DATE",
    TIME => "TIME",
    TIMETZ => "TIMETZ",
    BYTEA => "BYTEA",
    UUID => "UUID",
    JSON => "JSON",
    JSONB => "JSONB",

    // Character types
    NAME => "NAME",
    BPCHAR => "BPCHAR",

    // Numeric types
    NUMERIC => "NUMERIC",
    MONEY => "MONEY",

    // Date/time types
    INTERVAL => "INTERVAL",

    // Network types
    INET => "INET",
    CIDR => "CIDR",
    MACADDR => "MACADDR",
    MACADDR8 => "MACADDR8",

    // Bit string types
    BIT => "BIT",
    VARBIT => "VARBIT",

    // Geometric types
    POINT => "POINT",
    LSEG => "LSEG",
    PATH => "PATH",
    BOX => "BOX",
    POLYGON => "POLYGON",
    LINE => "LINE",
    CIRCLE => "CIRCLE",

    // Other common types
    OID => "OID",
    XML => "XML",

    // Text search types
    TS_VECTOR => "TSVECTOR",
    TSQUERY => "TSQUERY",

    // Array types
    BOOL_ARRAY => "BOOL_ARRAY",
    INT2_ARRAY => "INT2_ARRAY",
    INT4_ARRAY => "INT4_ARRAY",
    INT8_ARRAY => "INT8_ARRAY",
    FLOAT4_ARRAY => "FLOAT4_ARRAY",
    FLOAT8_ARRAY => "FLOAT8_ARRAY",
    TEXT_ARRAY => "TEXT_ARRAY",
    VARCHAR_ARRAY => "VARCHAR_ARRAY",
    TIMESTAMP_ARRAY => "TIMESTAMP_ARRAY",
    TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ_ARRAY",
    DATE_ARRAY => "DATE_ARRAY",
    UUID_ARRAY => "UUID_ARRAY",
    JSON_ARRAY => "JSON_ARRAY",
    JSONB_ARRAY => "JSONB_ARRAY",
    NUMERIC_ARRAY => "NUMERIC_ARRAY",

    // Range types
    INT4_RANGE => "INT4_RANGE",
    INT8_RANGE => "INT8_RANGE",
    NUM_RANGE => "NUM_RANGE",
    TS_RANGE => "TS_RANGE",
    TSTZ_RANGE => "TSTZ_RANGE",
    DATE_RANGE => "DATE_RANGE"
}

/// Stores a table schema in the database with a specific snapshot ID.
///
/// Upserts table schema and replaces all column information in schema storage tables
/// using a transaction to ensure atomicity. If a schema version already exists for
/// the same (pipeline_id, table_id, snapshot_id), columns are deleted and re-inserted.
pub async fn store_table_schema(
    pool: &PgPool,
    pipeline_id: i64,
    table_schema: &TableSchema,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Upsert table schema version
    let table_schema_id: i64 = sqlx::query(
        r#"
        insert into etl.table_schemas (pipeline_id, table_id, schema_name, table_name, snapshot_id)
        values ($1, $2, $3, $4, $5::pg_lsn)
        on conflict (pipeline_id, table_id, snapshot_id)
        do update set schema_name = excluded.schema_name, table_name = excluded.table_name
        returning id
        "#,
    )
    .bind(pipeline_id)
    .bind(table_schema.id.into_inner() as i64)
    .bind(&table_schema.name.schema)
    .bind(&table_schema.name.name)
    .bind(table_schema.snapshot_id.to_pg_lsn_string())
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    // Delete existing columns for this table schema to handle schema changes
    sqlx::query("delete from etl.table_columns where table_schema_id = $1")
        .bind(table_schema_id)
        .execute(&mut *tx)
        .await?;

    // Insert all columns
    for column_schema in table_schema.column_schemas.iter() {
        sqlx::query(
            r#"
            insert into etl.table_columns
            (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key,
             column_order, primary_key_ordinal_position)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(table_schema_id)
        .bind(&column_schema.name)
        .bind(postgres_type_to_string(&column_schema.typ))
        .bind(column_schema.modifier)
        .bind(column_schema.nullable)
        .bind(column_schema.primary_key())
        .bind(column_schema.ordinal_position)
        .bind(column_schema.primary_key_ordinal_position)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    Ok(())
}

/// Loads all table schemas for a pipeline from the database at the latest snapshot.
///
/// Retrieves table schemas and columns from schema storage tables,
/// reconstructing complete [`TableSchema`] objects. This is equivalent to
/// calling [`load_table_schemas_at_snapshot`] with the maximum LSN value.
pub async fn load_table_schemas(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<Vec<TableSchema>, sqlx::Error> {
    load_table_schemas_at_snapshot(pool, pipeline_id, SnapshotId::max()).await
}

/// Loads a single table schema with the largest snapshot_id <= the requested snapshot.
///
/// Returns `None` if no schema version exists for the table at or before the given snapshot.
pub async fn load_table_schema_at_snapshot(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    snapshot_id: SnapshotId,
) -> Result<Option<TableSchema>, sqlx::Error> {
    let rows = sqlx::query(
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
            tc.primary_key,
            tc.column_order,
            tc.primary_key_ordinal_position
        from etl.table_schemas ts
        inner join etl.table_columns tc on ts.id = tc.table_schema_id
        where ts.id = (
            select id from etl.table_schemas
            where pipeline_id = $1 and table_id = $2 and snapshot_id <= $3::pg_lsn
            order by snapshot_id desc
            limit 1
        )
        order by tc.column_order
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(snapshot_id.to_pg_lsn_string())
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let first_row = &rows[0];
    let table_oid: SqlxTableId = first_row.get("table_id");
    let table_id = TableId::new(table_oid.0);
    let schema_name: String = first_row.get("schema_name");
    let table_name: String = first_row.get("table_name");
    let snapshot_id_str: String = first_row.get("snapshot_id");
    let snapshot_id = SnapshotId::from_pg_lsn_string(&snapshot_id_str)
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    let mut table_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new(schema_name, table_name),
        vec![],
        snapshot_id,
    );

    for row in rows {
        table_schema.add_column_schema(parse_column_schema(&row));
    }

    Ok(Some(table_schema))
}

/// Loads all table schemas for a pipeline at a specific snapshot point.
///
/// For each table, retrieves the schema version with the largest snapshot_id
/// that is <= the requested snapshot_id. Tables without any schema version
/// at or before the snapshot are excluded from the result.
pub async fn load_table_schemas_at_snapshot(
    pool: &PgPool,
    pipeline_id: i64,
    snapshot_id: SnapshotId,
) -> Result<Vec<TableSchema>, sqlx::Error> {
    // Use DISTINCT ON to efficiently find the latest schema version for each table.
    // PostgreSQL optimizes DISTINCT ON with ORDER BY using index scans when possible.
    let rows = sqlx::query(
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
              and ts.snapshot_id <= $2::pg_lsn
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
            tc.primary_key,
            tc.column_order,
            tc.primary_key_ordinal_position
        from latest_schemas ls
        inner join etl.table_columns tc on ls.id = tc.table_schema_id
        order by ls.table_id, tc.column_order
        "#,
    )
    .bind(pipeline_id)
    .bind(snapshot_id.to_pg_lsn_string())
    .fetch_all(pool)
    .await?;

    let mut table_schemas = HashMap::new();

    for row in rows {
        let table_oid: SqlxTableId = row.get("table_id");
        let table_id = TableId::new(table_oid.0);
        let schema_name: String = row.get("schema_name");
        let table_name: String = row.get("table_name");
        let snapshot_id_str: String = row.get("snapshot_id");
        let row_snapshot_id = SnapshotId::from_pg_lsn_string(&snapshot_id_str)
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

        let entry = table_schemas.entry(table_id).or_insert_with(|| {
            TableSchema::with_snapshot_id(
                table_id,
                TableName::new(schema_name, table_name),
                vec![],
                row_snapshot_id,
            )
        });

        entry.add_column_schema(parse_column_schema(&row));
    }

    Ok(table_schemas.into_values().collect())
}

/// Deletes all table schemas for a pipeline from the database.
///
/// Removes all table schema records and associated columns for the specified
/// pipeline, using CASCADE delete for automatic cleanup of related column records.
pub async fn delete_table_schemas_for_all_tables<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.table_schemas
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Deletes the table schema for a specific table in a pipeline.
///
/// Also cascades to `etl.table_columns` via FK.
pub async fn delete_table_schema_for_table<'c, E>(
    executor: E,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.table_schemas
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Builds a [`ColumnSchema`] from a database row.
///
/// Assumes all required fields are present in the row after appropriate joins.
fn parse_column_schema(row: &PgRow) -> ColumnSchema {
    let column_name: String = row.get("column_name");
    let column_type: String = row.get("column_type");
    let type_modifier: i32 = row.get("type_modifier");
    let ordinal_position: i32 = row.get("column_order");
    let primary_key_ordinal_position: Option<i32> = row.get("primary_key_ordinal_position");
    let nullable: bool = row.get("nullable");

    ColumnSchema::new(
        column_name,
        string_to_postgres_type(&column_type),
        type_modifier,
        ordinal_position,
        primary_key_ordinal_position,
        nullable,
    )
}

/// Database enum type for destination schema phase.
///
/// Maps to the `etl.destination_schema_state` PostgreSQL enum type.
/// This is internal to the postgres store and should not be exposed.
#[derive(Debug, Clone, Copy, Type, PartialEq, Eq)]
#[sqlx(type_name = "etl.destination_schema_state", rename_all = "snake_case")]
pub enum DestinationSchemaPhase {
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

/// Database row representation of destination schema state.
///
/// Contains all fields from the `destination_schema_states` table joined with `table_schemas`.
/// This is internal to the postgres store and should not be exposed.
#[derive(Debug, Clone)]
pub struct DestinationSchemaStateRow {
    pub table_id: TableId,
    pub phase: DestinationSchemaPhase,
    pub snapshot_id: SnapshotId,
    pub replication_mask: Vec<u8>,
}

/// Stores a destination schema state in the database.
///
/// Inserts or updates the destination schema state for the specified table schema.
/// The `table_schema_id` is looked up from the `table_schemas` table using
/// (pipeline_id, table_id, snapshot_id) to maintain referential integrity.
pub async fn store_destination_schema_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    phase: DestinationSchemaPhase,
    snapshot_id: SnapshotId,
    replication_mask: &[u8],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO etl.destination_schema_states
            (table_schema_id, state_type, replication_mask)
        VALUES (
            (SELECT id FROM etl.table_schemas WHERE pipeline_id = $1 AND table_id = $2 AND snapshot_id = $3::pg_lsn),
            $4,
            $5
        )
        ON CONFLICT (table_schema_id)
        DO UPDATE SET
            state_type = EXCLUDED.state_type,
            replication_mask = EXCLUDED.replication_mask,
            updated_at = NOW()
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(snapshot_id.to_pg_lsn_string())
    .bind(phase)
    .bind(replication_mask)
    .execute(pool)
    .await?;

    Ok(())
}

/// Loads all destination schema states for a pipeline from the database.
///
/// Retrieves all destination schema state records for the specified pipeline by
/// joining with table_schemas to get pipeline_id, table_id, and snapshot_id.
pub async fn load_destination_schema_states(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<HashMap<TableId, DestinationSchemaStateRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT ts.table_id, dss.state_type, ts.snapshot_id::text as snapshot_id, dss.replication_mask
        FROM etl.destination_schema_states dss
        JOIN etl.table_schemas ts ON ts.id = dss.table_schema_id
        WHERE ts.pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    let mut states = HashMap::new();
    for row in rows {
        let table_id: SqlxTableId = row.get("table_id");
        let phase: DestinationSchemaPhase = row.get("state_type");
        let snapshot_id_str: String = row.get("snapshot_id");
        let snapshot_id = SnapshotId::from_pg_lsn_string(&snapshot_id_str);
        let replication_mask: Vec<u8> = row.get("replication_mask");

        states.insert(
            TableId::new(table_id.0),
            DestinationSchemaStateRow {
                table_id: TableId::new(table_id.0),
                phase,
                snapshot_id,
                replication_mask,
            },
        );
    }

    Ok(states)
}

/// Gets a single destination schema state for a specific table.
///
/// Joins with table_schemas to look up by pipeline_id and table_id.
pub async fn get_destination_schema_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
) -> Result<Option<DestinationSchemaStateRow>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT ts.table_id, dss.state_type, ts.snapshot_id::text as snapshot_id, dss.replication_mask
        FROM etl.destination_schema_states dss
        JOIN etl.table_schemas ts ON ts.id = dss.table_schema_id
        WHERE ts.pipeline_id = $1 AND ts.table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        let table_id: SqlxTableId = r.get("table_id");
        let snapshot_id_str: String = r.get("snapshot_id");
        DestinationSchemaStateRow {
            table_id: TableId::new(table_id.0),
            phase: r.get("state_type"),
            snapshot_id: SnapshotId::from_pg_lsn_string(&snapshot_id_str),
            replication_mask: r.get("replication_mask"),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Type;

    #[test]
    fn test_type_string_conversion() {
        let test_types = get_test_type_mappings();

        // Test type to string conversion
        for (pg_type, expected_string) in &test_types {
            let result = postgres_type_to_string(pg_type);
            assert_eq!(result, *expected_string, "Failed for type: {pg_type:?}");
        }

        // Test string to type conversion
        for (expected_type, type_string) in &test_types {
            let result = string_to_postgres_type(type_string);
            assert_eq!(result, *expected_type, "Failed for string: {type_string}");
        }

        // Test roundtrip conversion
        for (original_type, _) in &test_types {
            let type_string = postgres_type_to_string(original_type);
            let converted_back = string_to_postgres_type(&type_string);
            assert_eq!(
                converted_back, *original_type,
                "Roundtrip failed for: {original_type:?}"
            );
        }

        // Test unknown type fallback
        assert_eq!(string_to_postgres_type("UNKNOWN_TYPE"), Type::TEXT);

        // Test array and range syntax specifically
        assert_eq!(string_to_postgres_type("INT4_ARRAY"), Type::INT4_ARRAY);
        assert_eq!(string_to_postgres_type("TEXT_ARRAY"), Type::TEXT_ARRAY);
        assert_eq!(postgres_type_to_string(&Type::BOOL_ARRAY), "BOOL_ARRAY");
        assert_eq!(postgres_type_to_string(&Type::UUID_ARRAY), "UUID_ARRAY");
        assert_eq!(string_to_postgres_type("INT4_RANGE"), Type::INT4_RANGE);
        assert_eq!(postgres_type_to_string(&Type::DATE_RANGE), "DATE_RANGE");
    }
}
