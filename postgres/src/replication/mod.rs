pub mod state;

pub use state::*;

#[cfg(feature = "sqlx")]
use config::shared::{IntoConnectOptions, PgConnectionConfig};
#[cfg(feature = "sqlx")]
use sqlx::{PgPool, postgres::{PgConnectOptions, PgPoolOptions}};

/// Connect to the source database with a minimal connection pool
#[cfg(feature = "sqlx")]
pub async fn connect_to_source_database(source_config: &PgConnectionConfig) -> Result<PgPool, sqlx::Error> {
    let options: PgConnectOptions = source_config.with_db();
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .connect_with(options)
        .await?;
    
    Ok(pool)
}