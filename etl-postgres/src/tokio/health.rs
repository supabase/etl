use crate::tokio::{PgSourceError, PgSourceTransaction};

/// Fully-qualified table names required by ETL.
pub const ETL_TABLE_NAMES: [&str; 4] = [
    "etl.replication_state",
    "etl.destination_tables_metadata",
    "etl.table_schemas",
    "etl.table_columns",
];

/// Returns whether all ETL metadata tables are present.
pub async fn etl_tables_present(txn: &PgSourceTransaction<'_>) -> Result<bool, PgSourceError> {
    let table_names = ETL_TABLE_NAMES.iter().map(ToString::to_string).collect::<Vec<_>>();

    Ok(txn
        .query_one(
            r#"
            select coalesce(bool_and(to_regclass(t) is not null), false)
            from unnest($1::text[]) t
            "#,
            &[&table_names],
        )
        .await?
        .get(0))
}
