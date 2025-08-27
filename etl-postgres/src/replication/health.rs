use sqlx::PgExecutor;

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
    E: PgExecutor<'c>,
{
    // Perform a single query checking all required relations via unnest
    let table_names: Vec<String> = ETL_TABLE_NAMES.iter().map(|s| s.to_string()).collect();
    let present: bool = sqlx::query_scalar(
        r#"
        select coalesce(bool_and(to_regclass(t) is not null), false)
        from unnest($1::text[]) t
        "#,
    )
    .bind(table_names)
    .fetch_one(executor)
    .await?;

    Ok(present)
}
