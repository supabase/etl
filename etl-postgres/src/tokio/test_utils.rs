use std::num::NonZeroI32;

use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use tokio::runtime::Handle;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, GenericClient, NoTls, Transaction};
use tracing::info;

use crate::replication::extract_server_version;
use crate::types::{ColumnSchema, TableId, TableName};

/// Table modification operations for ALTER TABLE statements.
pub enum TableModification<'a> {
    /// Add a new column with specified name and data type.
    AddColumn {
        name: &'a str,
        data_type: &'a str,
    },
    /// Drop an existing column by name.
    DropColumn {
        name: &'a str,
    },
    /// Alter an existing column with the specified alteration.
    AlterColumn {
        name: &'a str,
        alteration: &'a str,
    },
    ReplicaIdentity {
        value: &'a str,
    },
}

/// Postgres database wrapper for testing operations.
///
/// Provides a unified interface for database operations across different client types
/// with automatic cleanup functionality.
pub struct PgDatabase<G> {
    pub config: PgConnectionConfig,
    pub client: Option<G>,
    server_version: Option<NonZeroI32>,
    destroy_on_drop: bool,
}

impl<G: GenericClient> PgDatabase<G> {
    pub fn server_version(&self) -> Option<NonZeroI32> {
        self.server_version
    }

    /// Creates a Postgres publication for the specified tables.
    ///
    /// Sets up logical replication by creating a publication that includes
    /// the given tables for change data capture.
    pub async fn create_publication(
        &self,
        publication_name: &str,
        table_names: &[TableName],
    ) -> Result<(), tokio_postgres::Error> {
        let table_names = table_names
            .iter()
            .map(TableName::as_quoted_identifier)
            .collect::<Vec<_>>();

        let create_publication_query = format!(
            "create publication {} for table {}",
            publication_name,
            table_names.join(", ")
        );
        self.client
            .as_ref()
            .unwrap()
            .execute(&create_publication_query, &[])
            .await?;

        Ok(())
    }

    pub async fn create_publication_for_all(
        &self,
        publication_name: &str,
        schema: Option<&str>,
    ) -> Result<(), tokio_postgres::Error> {
        let client = self.client.as_ref().unwrap();

        if let Some(server_version) = self.server_version
            && server_version.get() >= 150000
        {
            // PostgreSQL 15+ supports FOR ALL TABLES IN SCHEMA syntax
            let create_publication_query = match schema {
                Some(schema_name) => format!(
                    "create publication {publication_name} for tables in schema {schema_name}"
                ),
                None => format!("create publication {publication_name} for all tables"),
            };

            client.execute(&create_publication_query, &[]).await?;
        } else {
            // PostgreSQL 14 and earlier: create publication and add tables individually
            match schema {
                Some(schema_name) => {
                    let create_pub_query = format!("create publication {publication_name}");
                    client.execute(&create_pub_query, &[]).await?;

                    let tables_query = format!(
                        "select schemaname, tablename from pg_tables where schemaname = '{schema_name}'"
                    );
                    let rows = client.query(&tables_query, &[]).await?;

                    for row in rows {
                        let schema: String = row.get(0);
                        let table: String = row.get(1);
                        let add_table_query = format!(
                            "alter publication {publication_name} add table {schema}.{table}"
                        );
                        client.execute(&add_table_query, &[]).await?;
                    }
                }
                None => {
                    let create_publication_query =
                        format!("create publication {publication_name} for all tables");
                    client.execute(&create_publication_query, &[]).await?;
                }
            }
        }

        Ok(())
    }

    /// Creates a new table with the given name and column definitions.
    ///
    /// Optionally adds a primary key column named `id` of type `bigserial`.
    /// Returns the Postgres OID of the created table.
    pub async fn create_table(
        &self,
        table_name: TableName,
        add_pk_col: bool,
        columns: &[(&str, &str)], // (column_name, column_type)
    ) -> Result<TableId, tokio_postgres::Error> {
        let columns_str = columns
            .iter()
            .map(|(name, typ)| format!("{name} {typ}"))
            .collect::<Vec<_>>()
            .join(", ");

        let pk_col = if add_pk_col {
            "id bigserial primary key, "
        } else {
            ""
        };

        let create_table_query = format!(
            "create table {} ({pk_col}{columns_str})",
            table_name.as_quoted_identifier(),
        );
        self.client
            .as_ref()
            .unwrap()
            .execute(&create_table_query, &[])
            .await?;

        // Get the OID of the newly created table
        let row = self
            .client
            .as_ref()
            .unwrap()
            .query_one(
                "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace \
            where n.nspname = $1 and c.relname = $2",
                &[&table_name.schema, &table_name.name],
            )
            .await?;

        let table_id: TableId = row.get(0);

        Ok(table_id)
    }

    /// Modifies an existing table using ALTER TABLE operations.
    ///
    /// Applies the specified modifications (add/drop/alter columns) to the table
    /// in a single ALTER TABLE statement.
    pub async fn alter_table(
        &self,
        table_name: TableName,
        modifications: &[TableModification<'_>],
    ) -> Result<(), tokio_postgres::Error> {
        let modifications_str = modifications
            .iter()
            .map(|modification| match modification {
                TableModification::AddColumn { name, data_type } => {
                    format!("add column {name} {data_type}")
                }
                TableModification::DropColumn { name } => {
                    format!("drop column {name}")
                }
                TableModification::AlterColumn { name, alteration } => {
                    format!("alter column {name} {alteration}")
                }
                TableModification::ReplicaIdentity { value } => {
                    format!("replica identity {value}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let alter_table_query = format!(
            "alter table {} {}",
            table_name.as_quoted_identifier(),
            modifications_str
        );
        self.client
            .as_ref()
            .unwrap()
            .execute(&alter_table_query, &[])
            .await?;

        Ok(())
    }

    /// Inserts a single row of values into the specified table.
    ///
    /// Takes column names and corresponding values, generating appropriate
    /// parameterized placeholders for the INSERT statement.
    pub async fn insert_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let columns_str = columns.join(", ");
        let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${i}")).collect();
        let placeholders_str = placeholders.join(", ");

        let insert_query = format!(
            "insert into {} ({}) values ({})",
            table_name.as_quoted_identifier(),
            columns_str,
            placeholders_str
        );

        self.client
            .as_ref()
            .unwrap()
            .execute(&insert_query, values)
            .await
    }

    /// Inserts multiple rows using Postgres's `generate_series` function.
    ///
    /// Generates and inserts rows with values created by `generate_series`
    /// for efficient bulk data insertion during testing.
    pub async fn insert_generate_series(
        &self,
        table_name: TableName,
        columns: &[&str],
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<u64, tokio_postgres::Error> {
        let columns_str = columns.join(", ");
        let values = (1..=columns.len())
            .map(|_| format!("generate_series({start}, {end}, {step})"))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_query = format!(
            "insert into {} ({columns_str}) values ({values})",
            table_name.as_quoted_identifier(),
        );

        self.client
            .as_ref()
            .unwrap()
            .execute(&insert_query, &[])
            .await
    }

    /// Updates all rows in the table with new values.
    ///
    /// Sets the specified columns to new values across all rows in the table.
    /// Returns the number of rows affected.
    pub async fn update_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let set_clauses: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{col} = ${}", i + 1))
            .collect();
        let set_clause = set_clauses.join(", ");

        let update_query = format!(
            "update {} set {}",
            table_name.as_quoted_identifier(),
            set_clause
        );

        self.client
            .as_ref()
            .unwrap()
            .execute(&update_query, values)
            .await
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
    ) -> Result<u64, tokio_postgres::Error> {
        let delete_clauses: Vec<String> = columns
            .iter()
            .zip(expressions.iter())
            .map(|(col, val)| format!("{col} = {val}"))
            .collect();
        let delete_clauses = delete_clauses.join(operator);

        let delete_query = format!(
            "delete from {} where {}",
            table_name.as_quoted_identifier(),
            delete_clauses
        );

        self.client
            .as_ref()
            .unwrap()
            .execute(&delete_query, &[])
            .await
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
    ) -> Result<Vec<T>, tokio_postgres::Error>
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        let where_str = where_clause.map_or(String::new(), |w| format!("where {w}"));
        let query = format!(
            "select {} from {} {}",
            column,
            table_name.as_quoted_identifier(),
            where_str
        );

        let rows = self.client.as_ref().unwrap().query(&query, &[]).await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Truncates all data from the specified table.
    ///
    /// Removes all rows from the table while preserving the table structure.
    pub async fn truncate_table(&self, table_name: TableName) -> Result<(), tokio_postgres::Error> {
        let query = format!("truncate table {}", table_name.as_quoted_identifier(),);

        self.client.as_ref().unwrap().execute(&query, &[]).await?;

        Ok(())
    }

    /// Checks whether a Postgres replication slot exists.
    ///
    /// Queries the `pg_replication_slots` system catalog to determine
    /// if a replication slot with the given name exists.
    pub async fn replication_slot_exists(
        &self,
        slot_name: &str,
    ) -> Result<bool, tokio_postgres::Error> {
        let query = "select exists(select 1 from pg_replication_slots where slot_name = $1)";
        let row = self
            .client
            .as_ref()
            .unwrap()
            .query_one(query, &[&slot_name])
            .await?;

        Ok(row.get(0))
    }

    /// Executes arbitrary SQL on the database.
    pub async fn run_sql(&self, sql: &str) -> Result<u64, tokio_postgres::Error> {
        self.client.as_ref().unwrap().execute(sql, &[]).await
    }
}

impl PgDatabase<Client> {
    /// Creates a new test database with automatic cleanup.
    ///
    /// Creates a new Postgres database and establishes a client connection.
    /// The database will be dropped automatically when this instance is dropped.
    pub async fn new(config: PgConnectionConfig) -> Self {
        let client = create_pg_database(&config).await;

        Self {
            config,
            client: Some(client.0),
            server_version: client.1,
            destroy_on_drop: true,
        }
    }

    /// Creates a duplicate connection to the same database.
    ///
    /// Establishes an additional client connection to the existing database
    /// without creating a new database.
    pub async fn duplicate(&self) -> Self {
        let config = self.config.clone();
        // This connects to the database assuming it already exists since this is meant to be
        // a duplicate connection.
        let client = connect_to_pg_database(&config).await;

        Self {
            config,
            client: Some(client.0),
            server_version: client.1,
            destroy_on_drop: true,
        }
    }

    /// Begins a new database transaction.
    ///
    /// Returns a [`PgDatabase`] wrapping a [`Transaction`] for executing queries
    /// within a transaction context. The transaction must be committed or rolled back.
    pub async fn begin_transaction(&mut self) -> PgDatabase<Transaction<'_>> {
        let transaction = self.client.as_mut().unwrap().transaction().await.unwrap();

        PgDatabase {
            config: self.config.clone(),
            client: Some(transaction),
            server_version: self.server_version,
            destroy_on_drop: false,
        }
    }
}

impl PgDatabase<Transaction<'_>> {
    /// Commits the current transaction.
    ///
    /// Finalizes all changes made within the transaction and releases the transaction.
    pub async fn commit_transaction(mut self) {
        if let Some(client) = self.client.take() {
            client.commit().await.unwrap();
        }
    }
}

impl<G> Drop for PgDatabase<G> {
    fn drop(&mut self) {
        if self.destroy_on_drop {
            // To use `block_in_place,` we need a multithreaded runtime since when a blocking
            // task is issued, the runtime will offload existing tasks to another worker.
            tokio::task::block_in_place(move || {
                Handle::current().block_on(async move { drop_pg_database(&self.config).await });
            });
        }
    }
}

/// Returns the default ID column schema for test tables.
///
/// Creates a [`ColumnSchema`] for a non-nullable, primary key column named "id"
/// of type `INT8` that is added by default to tables created by [`PgDatabase`].
pub fn id_column_schema() -> ColumnSchema {
    ColumnSchema {
        name: "id".to_string(),
        typ: Type::INT8,
        modifier: -1,
        nullable: false,
        primary: true,
    }
}

/// Creates a new Postgres database and returns a connected client.
///
/// Establishes connection to Postgres server, creates a new database,
/// and returns a [`Client`] connected to the newly created database.
///
/// # Panics
/// Panics if connection or database creation fails.
pub async fn create_pg_database(config: &PgConnectionConfig) -> (Client, Option<NonZeroI32>) {
    // Create the database via a single connection
    let (client, connection) = {
        let config: tokio_postgres::Config = config.without_db();
        config
            .connect(NoTls)
            .await
            .expect("Failed to connect to Postgres")
    };

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            info!("connection error: {e}");
        }
    });

    // Create the database
    client
        .execute(&*format!(r#"create database "{}";"#, config.name), &[])
        .await
        .expect("Failed to create database");

    // Connects to the actual Postgres database
    let (client, server_version) = connect_to_pg_database(config).await;

    (client, server_version)
}

/// Connects to an existing Postgres database.
///
/// Establishes a client connection to the database specified in the configuration.
/// Assumes the database already exists.
pub async fn connect_to_pg_database(config: &PgConnectionConfig) -> (Client, Option<NonZeroI32>) {
    // Create a new client connected to the created database
    let (client, connection) = {
        let config: tokio_postgres::Config = config.with_db();
        config
            .connect(NoTls)
            .await
            .expect("Failed to connect to Postgres")
    };
    let server_version = connection
        .parameter("server_version")
        .and_then(extract_server_version);

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            info!("connection error: {e}");
        }
    });

    (client, server_version)
}

/// Drops a Postgres database and cleans up all resources.
///
/// Terminates all active connections, drops replication slots, and removes
/// the database. Used for thorough cleanup of test databases.
///
/// # Panics
/// Panics if any database operation fails.
pub async fn drop_pg_database(config: &PgConnectionConfig) {
    // Connect to the default database
    let (client, connection) = {
        let config: tokio_postgres::Config = config.without_db();
        config
            .connect(NoTls)
            .await
            .expect("Failed to connect to Postgres")
    };

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            info!("connection error: {e}");
        }
    });

    // Forcefully terminate any remaining connections to the database
    client
        .execute(
            &format!(
                r#"
                select pg_terminate_backend(pg_stat_activity.pid)
                from pg_stat_activity
                where pg_stat_activity.datname = '{}'
                and pid <> pg_backend_pid();"#,
                config.name
            ),
            &[],
        )
        .await
        .expect("Failed to terminate database connections");

    // Drop any replication slots on this database
    client
        .execute(
            &format!(
                r#"
                select pg_drop_replication_slot(slot_name)
                from pg_replication_slots
                where database = '{}';"#,
                config.name
            ),
            &[],
        )
        .await
        .expect("Failed to drop test replication slots");

    // Drop the database
    client
        .execute(
            &format!(r#"drop database if exists "{}";"#, config.name),
            &[],
        )
        .await
        .expect("Failed to destroy database");
}
