use sqlx::{Connection, Executor, MySqlConnection, MySqlPool};

use crate::replication::db::MySqlConnectionConfig;

/// Creates a new MySQL database and returns a connection pool.
///
/// Connects to MySQL server, creates a new database, and returns a [`MySqlPool`]
/// connected to the newly created database.
///
/// # Panics
/// Panics if connection or database creation fails.
pub async fn create_mysql_database(config: &MySqlConnectionConfig) -> MySqlPool {
    let connection_url = format!(
        "mysql://{}:{}@{}:{}",
        config.username,
        config.password.as_ref().unwrap_or(&String::new()),
        config.host,
        config.port
    );

    let mut connection = MySqlConnection::connect(&connection_url)
        .await
        .expect("Failed to connect to MySQL");

    connection
        .execute(&*format!("CREATE DATABASE `{}`", config.name))
        .await
        .expect("Failed to create database");

    let db_url = format!("{}/{}", connection_url, config.name);
    MySqlPool::connect(&db_url)
        .await
        .expect("Failed to connect to MySQL database")
}

/// Drops a MySQL database and terminates all connections.
///
/// Connects to MySQL server and drops the target database if it exists.
/// Used for test cleanup.
///
/// # Panics
/// Panics if any database operation fails.
pub async fn drop_mysql_database(config: &MySqlConnectionConfig) {
    let connection_url = format!(
        "mysql://{}:{}@{}:{}",
        config.username,
        config.password.as_ref().unwrap_or(&String::new()),
        config.host,
        config.port
    );

    let mut connection = MySqlConnection::connect(&connection_url)
        .await
        .expect("Failed to connect to MySQL");

    connection
        .execute(&*format!("DROP DATABASE IF EXISTS `{}`", config.name))
        .await
        .expect("Failed to destroy database");
}
