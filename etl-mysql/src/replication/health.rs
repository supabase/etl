use sqlx::MySqlExecutor;

/// Fully-qualified table names required by ETL.
pub const ETL_TABLE_NAMES: [&str; 4] = [
    "etl.replication_state",
    "etl.table_mappings",
    "etl.table_schemas",
    "etl.table_columns",
];

/// Returns true if all required ETL tables exist in the source database.
///
/// Checks presence of the following relations:
/// - etl.replication_state
/// - etl.table_mappings
/// - etl.table_schemas
/// - etl.table_columns
pub async fn etl_tables_present<'c, E>(executor: E) -> Result<bool, sqlx::Error>
where
    E: MySqlExecutor<'c>,
{
    // Check if all required tables exist with a single query
    let count: i64 = sqlx::query_scalar(
        "
        SELECT COUNT(DISTINCT CONCAT(table_schema, '.', table_name))
        FROM information_schema.tables
        WHERE CONCAT(table_schema, '.', table_name) IN (?, ?, ?, ?)
        ",
    )
    .bind(ETL_TABLE_NAMES[0])
    .bind(ETL_TABLE_NAMES[1])
    .bind(ETL_TABLE_NAMES[2])
    .bind(ETL_TABLE_NAMES[3])
    .fetch_one(executor)
    .await?;

    Ok(count == ETL_TABLE_NAMES.len() as i64)
}
