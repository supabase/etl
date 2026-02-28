use std::num::NonZeroI32;

use tokio::runtime::Handle;

use crate::replication::db::MySqlConnectionConfig;
use crate::replication::extract_server_version;
use crate::types::{ColumnSchema, TableId, TableName};

/// Table modification operations for ALTER TABLE statements.
pub enum TableModification<'a> {
    /// Add a new column with specified name and data type.
    AddColumn { name: &'a str, data_type: &'a str },
    /// Drop an existing column by name.
    DropColumn { name: &'a str },
    /// Alter an existing column with the specified alteration.
    AlterColumn {
        name: &'a str,
        alteration: &'a str,
    },
}

/// MySQL database wrapper for testing operations.
///
/// Provides a unified interface for database operations with automatic cleanup functionality.
pub struct MySqlDatabase {
    pub config: MySqlConnectionConfig,
    pub pool: Option<sqlx::MySqlPool>,
    server_version: Option<NonZeroI32>,
    destroy_on_drop: bool,
}

impl MySqlDatabase {
    pub fn server_version(&self) -> Option<NonZeroI32> {
        self.server_version
    }

    /// Creates a new table with the given name and column definitions.
    ///
    /// Optionally adds a primary key column named `id` of type `BIGINT AUTO_INCREMENT`.
    /// Returns a computed table ID based on the table name.
    pub async fn create_table(
        &self,
        table_name: TableName,
        add_pk_col: bool,
        columns: &[(&str, &str)],
    ) -> Result<TableId, sqlx::Error> {
        let columns_str = columns
            .iter()
            .map(|(name, typ)| format!("`{name}` {typ}"))
            .collect::<Vec<_>>()
            .join(", ");

        let pk_col = if add_pk_col {
            "`id` BIGINT AUTO_INCREMENT PRIMARY KEY, "
        } else {
            ""
        };

        let create_table_query = format!(
            "CREATE TABLE {} ({pk_col}{columns_str})",
            table_name.as_quoted_identifier(),
        );

        sqlx::query(&create_table_query)
            .execute(self.pool.as_ref().unwrap())
            .await?;

        use crate::replication::schema::compute_table_id;
        let table_id = compute_table_id(&table_name);

        Ok(table_id)
    }

    /// Modifies an existing table using ALTER TABLE operations.
    ///
    /// Applies the specified modifications (add/drop/alter columns) to the table.
    pub async fn alter_table(
        &self,
        table_name: TableName,
        modifications: &[TableModification<'_>],
    ) -> Result<(), sqlx::Error> {
        for modification in modifications {
            let alter_clause = match modification {
                TableModification::AddColumn { name, data_type } => {
                    format!("ADD COLUMN `{name}` {data_type}")
                }
                TableModification::DropColumn { name } => {
                    format!("DROP COLUMN `{name}`")
                }
                TableModification::AlterColumn { name, alteration } => {
                    format!("MODIFY COLUMN `{name}` {alteration}")
                }
            };

            let alter_table_query = format!(
                "ALTER TABLE {} {}",
                table_name.as_quoted_identifier(),
                alter_clause
            );

            sqlx::query(&alter_table_query)
                .execute(self.pool.as_ref().unwrap())
                .await?;
        }

        Ok(())
    }

    /// Inserts a single row of values into the specified table.
    ///
    /// Takes column names and generates appropriate parameterized placeholders
    /// for the INSERT statement.
    pub async fn insert_values<T>(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[T],
    ) -> Result<u64, sqlx::Error>
    where
        T: Send + Sync,
        for<'q> &'q T: sqlx::Encode<'q, sqlx::MySql> + sqlx::Type<sqlx::MySql>,
    {
        let columns_str = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders: Vec<String> = (0..values.len()).map(|_| "?".to_string()).collect();
        let placeholders_str = placeholders.join(", ");

        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name.as_quoted_identifier(),
            columns_str,
            placeholders_str
        );

        let mut query = sqlx::query(&insert_query);
        for value in values {
            query = query.bind(value);
        }

        let result = query.execute(self.pool.as_ref().unwrap()).await?;

        Ok(result.rows_affected())
    }

    /// Updates all rows in the table with new values.
    ///
    /// Sets the specified columns to new values across all rows in the table.
    /// Returns the number of rows affected.
    pub async fn update_values<T>(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[T],
    ) -> Result<u64, sqlx::Error>
    where
        T: Send + Sync,
        for<'q> &'q T: sqlx::Encode<'q, sqlx::MySql> + sqlx::Type<sqlx::MySql>,
    {
        let set_clauses: Vec<String> = columns
            .iter()
            .map(|col| format!("`{col}` = ?"))
            .collect();
        let set_clause = set_clauses.join(", ");

        let update_query = format!(
            "UPDATE {} SET {}",
            table_name.as_quoted_identifier(),
            set_clause
        );

        let mut query = sqlx::query(&update_query);
        for value in values {
            query = query.bind(value);
        }

        let result = query.execute(self.pool.as_ref().unwrap()).await?;

        Ok(result.rows_affected())
    }

    /// Deletes rows from the table based on column conditions.
    ///
    /// Constructs a DELETE statement with WHERE clause using the provided
    /// column names, expressions, and logical operator.
    pub async fn delete_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        expressions: &[&str],
        operator: &str,
    ) -> Result<u64, sqlx::Error> {
        let delete_clauses: Vec<String> = columns
            .iter()
            .zip(expressions.iter())
            .map(|(col, val)| format!("`{col}` = {val}"))
            .collect();
        let delete_clauses = delete_clauses.join(operator);

        let delete_query = format!(
            "DELETE FROM {} WHERE {}",
            table_name.as_quoted_identifier(),
            delete_clauses
        );

        let result = sqlx::query(&delete_query)
            .execute(self.pool.as_ref().unwrap())
            .await?;

        Ok(result.rows_affected())
    }

    /// Queries values from a single column with optional WHERE clause.
    ///
    /// Returns all values from the specified column, optionally filtered
    /// by the provided WHERE condition.
    pub async fn query_table<T>(
        &self,
        table_name: &TableName,
        column: &str,
        where_clause: Option<&str>,
    ) -> Result<Vec<T>, sqlx::Error>
    where
        T: Send + Unpin,
        for<'r> T: sqlx::Decode<'r, sqlx::MySql> + sqlx::Type<sqlx::MySql>,
    {
        let where_str = where_clause.map_or(String::new(), |w| format!("WHERE {w}"));
        let query = format!(
            "SELECT `{}` FROM {} {}",
            column,
            table_name.as_quoted_identifier(),
            where_str
        );

        let rows: Vec<(T,)> = sqlx::query_as(&query)
            .fetch_all(self.pool.as_ref().unwrap())
            .await?;

        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    /// Truncates all data from the specified table.
    ///
    /// Removes all rows from the table while preserving the table structure.
    pub async fn truncate_table(&self, table_name: TableName) -> Result<(), sqlx::Error> {
        let query = format!("TRUNCATE TABLE {}", table_name.as_quoted_identifier());

        sqlx::query(&query)
            .execute(self.pool.as_ref().unwrap())
            .await?;

        Ok(())
    }

    /// Executes arbitrary SQL on the database.
    pub async fn run_sql(&self, sql: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(sql)
            .execute(self.pool.as_ref().unwrap())
            .await?;

        Ok(result.rows_affected())
    }

    /// Creates a new test database with automatic cleanup.
    ///
    /// Creates a new MySQL database and establishes a connection pool.
    /// The database will be dropped automatically when this instance is dropped.
    pub async fn new(config: MySqlConnectionConfig) -> Self {
        let (pool, server_version) = create_mysql_database(&config).await;

        Self {
            config,
            pool: Some(pool),
            server_version,
            destroy_on_drop: true,
        }
    }

    /// Creates a duplicate connection to the same database.
    ///
    /// Establishes an additional connection pool to the existing database
    /// without creating a new database.
    pub async fn duplicate(&self) -> Self {
        let config = self.config.clone();
        let (pool, server_version) = connect_to_mysql_database(&config).await;

        Self {
            config,
            pool: Some(pool),
            server_version,
            destroy_on_drop: false,
        }
    }
}

impl Drop for MySqlDatabase {
    fn drop(&mut self) {
        if self.destroy_on_drop {
            tokio::task::block_in_place(move || {
                Handle::current().block_on(async move { drop_mysql_database(&self.config).await });
            });
        }
    }
}

/// Returns the default ID column schema for test tables.
///
/// Creates a [`ColumnSchema`] for a non-nullable, primary key column named "id"
/// of type `BIGINT` that is added by default to tables created by [`MySqlDatabase`].
pub fn id_column_schema() -> ColumnSchema {
    ColumnSchema {
        name: "id".to_string(),
        typ: "BIGINT".to_string(),
        modifier: -1,
        nullable: false,
        primary: true,
    }
}

/// Creates a new MySQL database and returns a connected pool.
///
/// Establishes connection to MySQL server, creates a new database,
/// and returns a [`MySqlPool`] connected to the newly created database.
///
/// # Panics
/// Panics if connection or database creation fails.
pub async fn create_mysql_database(
    config: &MySqlConnectionConfig,
) -> (sqlx::MySqlPool, Option<NonZeroI32>) {
    let connection_url = format!(
        "mysql://{}:{}@{}:{}",
        config.username,
        config.password.as_ref().unwrap_or(&String::new()),
        config.host,
        config.port
    );

    let pool = sqlx::MySqlPool::connect(&connection_url)
        .await
        .expect("Failed to connect to MySQL");

    sqlx::query(&format!("CREATE DATABASE `{}`", config.name))
        .execute(&pool)
        .await
        .expect("Failed to create database");

    let (db_pool, server_version) = connect_to_mysql_database(config).await;

    (db_pool, server_version)
}

/// Connects to an existing MySQL database.
///
/// Establishes a connection pool to the database specified in the configuration.
/// Assumes the database already exists.
pub async fn connect_to_mysql_database(
    config: &MySqlConnectionConfig,
) -> (sqlx::MySqlPool, Option<NonZeroI32>) {
    let connection_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        config.username,
        config.password.as_ref().unwrap_or(&String::new()),
        config.host,
        config.port,
        config.name
    );

    let pool = sqlx::MySqlPool::connect(&connection_url)
        .await
        .expect("Failed to connect to MySQL database");

    let version_row: (String,) = sqlx::query_as("SELECT VERSION()")
        .fetch_one(&pool)
        .await
        .expect("Failed to get server version");

    let server_version = extract_server_version(version_row.0);

    (pool, server_version)
}

/// Drops a MySQL database and cleans up all resources.
///
/// Drops the database and removes all resources. Used for thorough cleanup
/// of test databases.
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

    let pool = sqlx::MySqlPool::connect(&connection_url)
        .await
        .expect("Failed to connect to MySQL");

    sqlx::query(&format!("DROP DATABASE IF EXISTS `{}`", config.name))
        .execute(&pool)
        .await
        .expect("Failed to destroy database");
}
