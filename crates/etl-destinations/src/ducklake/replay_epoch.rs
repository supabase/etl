use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use pg_escape::quote_identifier;
use rand::Rng;
use sqlx::{AssertSqlSafe, PgPool};

use crate::ducklake::DuckLakeTableName;

/// Replay epoch assigned to rows written before epoch tracking existed.
pub(super) const LEGACY_REPLAY_EPOCH: &str = "__legacy__";

/// Metadata catalog table storing the current replay epoch per DuckLake table.
const REPLAY_EPOCHS_TABLE: &str = "__etl_replay_epochs";

/// Returns the quoted metadata table name for replay epochs.
fn replay_epochs_table_name(metadata_schema: &str) -> String {
    format!("{}.{}", quote_identifier(metadata_schema), quote_identifier(REPLAY_EPOCHS_TABLE))
}

/// Generates a new opaque replay epoch identifier.
fn new_replay_epoch() -> String {
    format!("{:032x}", rand::rng().random::<u128>())
}

/// Ensures the Postgres-backed replay epoch table exists in the DuckLake metadata schema.
pub(super) async fn ensure_replay_epoch_table_exists(
    pool: &PgPool,
    metadata_schema: &str,
) -> EtlResult<()> {
    let table_name = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {table_name} (
             table_name TEXT PRIMARY KEY,
             replay_epoch TEXT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
           );"#
    );

    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake replay epoch table creation failed",
            source: source
        )
    })?;

    Ok(())
}

/// Reads the current replay epoch for a table.
pub(super) async fn read_table_replay_epoch(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
) -> EtlResult<String> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!("SELECT replay_epoch FROM {epochs_table} WHERE table_name = $1;");
    let table_id = table_name.id();

    let replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .fetch_optional(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch lookup failed",
                format!("table={table_id}"),
                source: source
            )
        })?
        .unwrap_or_else(|| LEGACY_REPLAY_EPOCH.to_owned());

    Ok(replay_epoch)
}

/// Rotates the replay epoch for a table and returns the new value.
pub(super) async fn rotate_table_replay_epoch(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
) -> EtlResult<String> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"INSERT INTO {epochs_table} (table_name, replay_epoch, updated_at)
           VALUES ($1, $2, now())
           ON CONFLICT (table_name)
           DO UPDATE SET replay_epoch = excluded.replay_epoch, updated_at = excluded.updated_at;"#
    );
    let table_id = table_name.id();
    let replay_epoch = new_replay_epoch();

    sqlx::query(AssertSqlSafe(sql))
        .bind(&table_id)
        .bind(&replay_epoch)
        .execute(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch rotation failed",
                format!("table={table_id}"),
                source: source
            )
        })?;

    Ok(replay_epoch)
}
