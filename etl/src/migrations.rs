use etl_config::shared::{ETL_MIGRATION_OPTIONS, IntoConnectOptions, PgConnectionConfig};
use sqlx::{Executor, postgres::PgPoolOptions};
use tracing::info;

/// Runs database migrations on the source database `etl` schema.
///
/// Creates a connection pool to the source database, sets up the `etl` schema,
/// and applies all pending migrations. The migrations are run in the `etl` schema
/// to avoid cluttering the public schema with migration metadata tables created by `sqlx`.
pub async fn apply_etl_migrations(
    connection_config: &PgConnectionConfig,
) -> Result<(), sqlx::Error> {
    let options = connection_config.with_db(Some(&ETL_MIGRATION_OPTIONS));

    let pool = PgPoolOptions::new()
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create the `etl` schema if it doesn't exist.
                conn.execute("create schema if not exists etl;").await?;

                // Set the `search_path` to `etl` so that the `_sqlx_migrations`
                // metadata table is created inside that schema instead of the public
                // schema.
                conn.execute("set search_path = 'etl';").await?;

                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    info!("applying etl migrations before starting pipeline");

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    info!("etl migrations successfully applied");

    Ok(())
}
