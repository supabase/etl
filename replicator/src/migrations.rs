use config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tracing::{debug, info};

const NUM_POOL_CONNECTIONS: u32 = 1;

/// This function runs migrations on the source database when the source database acts
/// as a state store.
pub async fn migrate_state_store(
    connection_config: &PgConnectionConfig,
) -> Result<(), sqlx::Error> {
    debug!("connecting to postgres for state store migrations");
    let options: PgConnectOptions = connection_config.with_db();

    let pool = PgPoolOptions::new()
        .max_connections(NUM_POOL_CONNECTIONS)
        .min_connections(NUM_POOL_CONNECTIONS)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create the etl schema if it doesn't exist
                conn.execute("create schema if not exists etl;").await?;
                // We set the search_path to etl so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'etl';").await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;
    debug!("postgres connection established for migrations");

    debug!("running state store migrations");
    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;
    info!("state store migrations completed successfully");

    debug!("closing migration connection pool");

    Ok(())
}
