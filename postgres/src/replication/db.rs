use config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{PgPool, postgres::PgPoolOptions};

/// Connect to the source database with a minimal connection pool
#[cfg(feature = "replication")]
pub async fn connect_to_source_database(
    config: &PgConnectionConfig,
    min_connections: u32,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    let options = config.with_db();

    let pool = PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect_with(options)
        .await?;

    Ok(pool)
}
