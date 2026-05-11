use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use pg_escape::{quote_identifier, quote_literal};
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool};

/// Creates a new Postgres database and returns a connection pool.
///
/// Connects to Postgres server, creates a new database, and returns a
/// [`PgPool`] connected to the newly created database.
///
/// # Panics
/// Panics if connection or database creation fails.
pub async fn create_pg_database(config: &PgConnectionConfig) -> PgPool {
    // Create the database via a single connection.
    let mut connection = PgConnection::connect_with(&config.without_db(None))
        .await
        .expect("Failed to connect to Postgres");
    let create_database_query = format!("create database {};", quote_identifier(&config.name));
    sqlx::query(AssertSqlSafe(create_database_query))
        .execute(&mut connection)
        .await
        .expect("Failed to create database");

    // Create a connection pool to the database.
    PgPool::connect_with(config.with_db(None)).await.expect("Failed to connect to Postgres")
}

/// Drops a Postgres database and terminates all connections.
///
/// Connects to Postgres server, forcefully terminates active connections
/// to the target database, and drops it if it exists. Used for test cleanup.
///
/// This function will not panic on errors - it logs them and continues.
/// This ensures test cleanup doesn't fail when databases are already gone
/// or connections can't be established.
pub async fn drop_pg_database(config: &PgConnectionConfig) {
    // Connect to the default database.
    let mut connection = match PgConnection::connect_with(&config.without_db(None)).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("warning: failed to connect to Postgres for cleanup: {e}");
            return;
        }
    };

    // Forcefully terminate any remaining connections to the database.
    let terminate_connections_query = format!(
        r#"
            select pg_terminate_backend(pg_stat_activity.pid)
            from pg_stat_activity
            where pg_stat_activity.datname = {}
            and pid <> pg_backend_pid();"#,
        quote_literal(&config.name)
    );
    if let Err(e) =
        sqlx::query(AssertSqlSafe(terminate_connections_query)).execute(&mut connection).await
    {
        eprintln!("warning: failed to terminate connections for database {}: {}", config.name, e);
    }

    // Drop the database.
    let drop_database_query =
        format!("drop database if exists {};", quote_identifier(&config.name));
    if let Err(e) = sqlx::query(AssertSqlSafe(drop_database_query)).execute(&mut connection).await {
        eprintln!("warning: failed to drop database {}: {}", config.name, e);
    }
}
