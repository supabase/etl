use crate::schema::{ColumnSchema, TableId, TableName};
use crate::tokio::config::PgConnectionConfig;
use tokio::runtime::Handle;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient, NoTls, Transaction};

pub enum TableModification<'a> {
    AddColumn { name: &'a str, data_type: &'a str },
    DropColumn { name: &'a str },
    AlterColumn { name: &'a str, alteration: &'a str },
}

pub struct PgDatabase<G> {
    pub config: PgConnectionConfig,
    pub client: Option<G>,
    destroy_on_drop: bool,
}

impl<G: GenericClient> PgDatabase<G> {
    /// Creates a new publication for the specified tables.
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

    /// Creates a new table with the specified name and columns.
    pub async fn create_table(
        &self,
        table_name: TableName,
        columns: &[(&str, &str)], // (column_name, column_type)
    ) -> Result<TableId, tokio_postgres::Error> {
        let columns_str = columns
            .iter()
            .map(|(name, typ)| format!("{} {}", name, typ))
            .collect::<Vec<_>>()
            .join(", ");

        let create_table_query = format!(
            "create table {} (id bigserial primary key, {})",
            table_name.as_quoted_identifier(),
            columns_str
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

    /// Modifies a table by adding, dropping, or altering columns.
    pub async fn alter_table(
        &self,
        table_name: TableName,
        modifications: &[TableModification<'_>],
    ) -> Result<(), tokio_postgres::Error> {
        let modifications_str = modifications
            .iter()
            .map(|modification| match modification {
                TableModification::AddColumn { name, data_type } => {
                    format!("add column {} {}", name, data_type)
                }
                TableModification::DropColumn { name } => {
                    format!("drop column {}", name)
                }
                TableModification::AlterColumn { name, alteration } => {
                    format!("alter column {} {}", name, alteration)
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

    /// Inserts values into the specified table.
    pub async fn insert_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let columns_str = columns.join(", ");
        let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${}", i)).collect();
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

    /// Generates values using PostgreSQL's `generate_series` function
    /// and inserts them into the specified table.
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
            .map(|_| format!("generate_series({}, {}, {})", start, end, step))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_query = format!(
            "insert into {} ({columns_str}) values ({values})",
            table_name.as_quoted_identifier(),
        );

        self.client
            .as_ref()
            .unwrap()
            .execute(&dbg!(insert_query), &[])
            .await
    }

    /// Updates all rows in the specified table with the given values.
    pub async fn update_values(
        &self,
        table_name: TableName,
        columns: &[&str],
        values: &[&str],
    ) -> Result<u64, tokio_postgres::Error> {
        let set_clauses: Vec<String> = columns
            .iter()
            .zip(values.iter())
            .map(|(col, val)| format!("{} = {}", col, val))
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
            .execute(&update_query, &[])
            .await
    }

    /// Queries rows from a single column of a table.
    pub async fn query_table<T>(
        &self,
        table_name: &TableName,
        column: &str,
        where_clause: Option<&str>,
    ) -> Result<Vec<T>, tokio_postgres::Error>
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        let where_str = where_clause.map_or(String::new(), |w| format!(" where {}", w));
        let query = format!(
            "select {} from {}{}",
            column,
            table_name.as_quoted_identifier(),
            where_str
        );

        let rows = self.client.as_ref().unwrap().query(&query, &[]).await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }
}

impl PgDatabase<Client> {
    pub async fn new(config: PgConnectionConfig) -> Self {
        let client = create_pg_database(&config).await;

        Self {
            config,
            client: Some(client),
            destroy_on_drop: true,
        }
    }

    /// Begins a new transaction.
    ///
    /// Returns a `Transaction` object that can be used to execute queries within the transaction.
    /// The transaction must be committed or rolled back before it is dropped.
    pub async fn begin_transaction(&mut self) -> PgDatabase<Transaction<'_>> {
        let transaction = self.client.as_mut().unwrap().transaction().await.unwrap();

        PgDatabase {
            config: self.config.clone(),
            client: Some(transaction),
            destroy_on_drop: false,
        }
    }
}

impl PgDatabase<Transaction<'_>> {
    pub async fn commit_transaction(&mut self) {
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

/// Returns a [`ColumnSchema`] representing a non-nullable, primary key column
/// named "id" of type `INT8` which is added by default to all tables created within
/// [`PgDatabase`].
pub fn id_column_schema() -> ColumnSchema {
    ColumnSchema {
        name: "id".to_string(),
        typ: Type::INT8,
        modifier: -1,
        nullable: false,
        primary: true,
    }
}

/// Creates a new PostgreSQL database and returns a client connected to it.
///
/// Establishes a connection to the PostgreSQL server using the provided options,
/// creates a new database, and returns a [`Client`] connected to the new database.
/// Panics if the connection fails or if database creation fails.
pub async fn create_pg_database(config: &PgConnectionConfig) -> Client {
    // Create the database via a single connection
    let (client, connection) = config
        .without_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create the database
    client
        .execute(&*format!(r#"create database "{}";"#, config.name), &[])
        .await
        .expect("Failed to create database");

    // Create a new client connected to the created database
    let (client, connection) = config
        .with_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

/// Drops a PostgreSQL database and cleans up all connections.
///
/// Connects to the PostgreSQL server, forcefully terminates all active connections
/// to the target database, and drops the database if it exists. Useful for cleaning
/// up test databases. Takes a reference to [`PgConnectionConfig`] specifying the database
/// to drop. Panics if any operation fails.
pub async fn drop_pg_database(config: &PgConnectionConfig) {
    // Connect to the default database
    let (client, connection) = config
        .without_db()
        .connect(NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection on a new task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
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
