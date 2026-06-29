//! Health checks for ETL-owned Postgres metadata tables.

use sqlx::PgExecutor;

use crate::store::catalog::{ETL_CORE_STATE_TABLES, EtlTable};

/// Returns true if all required ETL tables exist in the source database.
///
/// Checks presence of the following relations:
/// - etl.replication_state
/// - etl.destination_tables_metadata
/// - etl.table_schemas
/// - etl.table_columns
pub async fn etl_tables_present<'c, E>(executor: E) -> Result<bool, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    // Perform a single query checking all required relations via unnest
    let table_names: Vec<String> =
        ETL_CORE_STATE_TABLES.iter().map(EtlTable::qualified_name).collect();
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
