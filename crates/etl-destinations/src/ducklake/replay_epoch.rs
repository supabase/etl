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

/// Metadata catalog table storing replay epoch transitions per DuckLake table.
const REPLAY_EPOCHS_TABLE: &str = "__etl_replay_epochs";

/// Returns the quoted metadata table name for replay epochs.
fn replay_epochs_table_name(metadata_schema: &str) -> String {
    format!("{}.{}", quote_identifier(metadata_schema), quote_identifier(REPLAY_EPOCHS_TABLE))
}

/// Generates a new opaque replay epoch identifier.
fn new_replay_epoch() -> String {
    format!("{:032x}", rand::rng().random::<u128>())
}

/// Ensures the Postgres-backed replay epoch table exists in the DuckLake
/// metadata schema.
pub(super) async fn ensure_replay_epoch_table_exists(
    pool: &PgPool,
    metadata_schema: &str,
) -> EtlResult<()> {
    let table_name = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"create table if not exists {table_name} (
             table_name text primary key,
             replay_epoch text not null,
             pending_replay_epoch text,
             updated_at timestamptz not null default now()
           );"#
    );

    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake replay epoch table creation failed",
            source: source
        )
    })?;

    // Older catalogs only have the committed replay epoch. Adding the pending
    // column here upgrades them before any reset can start a recoverable epoch
    // transition.
    let sql =
        format!("alter table {table_name} add column if not exists pending_replay_epoch text;");
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake pending replay epoch column creation failed",
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
    let sql = format!("select replay_epoch from {epochs_table} where table_name = $1;");
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

/// Starts or resumes a replay epoch transition and returns its pending value.
pub(super) async fn begin_table_replay_epoch_transition(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
) -> EtlResult<String> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"insert into {epochs_table} as epochs
             (table_name, replay_epoch, pending_replay_epoch, updated_at)
           values ($1, $2, $3, now())
           on conflict (table_name)
           do update set
             pending_replay_epoch = coalesce(
               epochs.pending_replay_epoch,
               excluded.pending_replay_epoch
             ),
             updated_at = now()
           returning pending_replay_epoch;"#
    );
    let table_id = table_name.id();
    let new_pending_replay_epoch = new_replay_epoch();

    let pending_replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .bind(LEGACY_REPLAY_EPOCH)
        .bind(&new_pending_replay_epoch)
        .fetch_one(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch transition failed to begin",
                format!("table={table_id}"),
                source: source
            )
        })?;

    Ok(pending_replay_epoch)
}

/// Promotes a pending replay epoch after its DuckLake reset commits.
pub(super) async fn complete_table_replay_epoch_transition(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
    pending_replay_epoch: &str,
) -> EtlResult<()> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"update {epochs_table}
           set replay_epoch = $2, pending_replay_epoch = null, updated_at = now()
           where table_name = $1
             and (
               pending_replay_epoch = $2
               or (pending_replay_epoch is null and replay_epoch = $2)
             )
           returning replay_epoch;"#
    );
    let table_id = table_name.id();

    let completed_replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .bind(pending_replay_epoch)
        .fetch_optional(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch transition failed to complete",
                format!("table={table_id}"),
                source: source
            )
        })?;

    if completed_replay_epoch.is_none() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake replay epoch transition is inconsistent",
            format!("table={table_id}")
        ));
    }

    Ok(())
}
