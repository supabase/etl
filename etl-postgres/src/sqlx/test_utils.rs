use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{Connection, Executor, PgConnection, PgPool};

/// Creates a new PostgreSQL database and returns a connection pool.
///
/// Connects to PostgreSQL server, creates a new database, and returns a [`PgPool`]
/// connected to the newly created database.
///
/// # Panics
/// Panics if connection or database creation fails.
pub async fn create_pg_database(config: &PgConnectionConfig) -> PgPool {
    // Create the database via a single connection.
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres");
    connection
        .execute(&*format!(r#"create database "{}";"#, config.name))
        .await
        .expect("Failed to create database");

    // Create a connection pool to the database.
    PgPool::connect_with(config.with_db())
        .await
        .expect("Failed to connect to Postgres")
}

/// Drops a PostgreSQL database and terminates all connections.
///
/// Connects to PostgreSQL server, forcefully terminates active connections
/// to the target database, and drops it if it exists. Used for test cleanup.
///
/// # Panics
/// Panics if any database operation fails.
pub async fn drop_pg_database(config: &PgConnectionConfig) {
    // Connect to the default database.
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres");

    // Forcefully terminate any remaining connections to the database.
    connection
        .execute(&*format!(
            r#"
            select pg_terminate_backend(pg_stat_activity.pid)
            from pg_stat_activity
            where pg_stat_activity.datname = '{}'
            and pid <> pg_backend_pid();"#,
            config.name
        ))
        .await
        .expect("Failed to terminate database connections");

    // Drop the database.
    connection
        .execute(&*format!(r#"drop database if exists "{}";"#, config.name))
        .await
        .expect("Failed to destroy database");
}
