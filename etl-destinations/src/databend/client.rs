use databend_driver::Client as DatabendDriverClient;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{ColumnSchema, TableRow};
use std::sync::Arc;
use tracing::{debug, info};

use crate::databend::encoding::encode_table_row;
use crate::databend::validation::validate_column_schemas;

/// Trace identifier for ETL operations in Databend client.
const ETL_TRACE_ID: &str = "ETL DatabendClient";

/// Databend DSN (Data Source Name).
pub type DatabendDsn = String;

/// Databend database identifier.
pub type DatabendDatabase = String;

/// Databend table identifier.
pub type DatabendTableId = String;

/// Client for interacting with Databend.
///
/// Provides methods for table management, data insertion, and query execution
/// against Databend databases with authentication and error handling.
#[derive(Clone)]
pub struct DatabendClient {
    pub(crate) dsn: DatabendDsn,
    pub(crate) database: DatabendDatabase,
    pub(crate) client: Arc<DatabendDriverClient>,
}

impl DatabendClient {
    /// Creates a new [`DatabendClient`] from a DSN connection string.
    ///
    /// The DSN should be in the format:
    /// `databend://<user>:<password>@<host>:<port>/<database>?<params>`
    ///
    /// # Examples
    ///
    /// ```text
    /// databend://root:password@localhost:8000/my_database
    /// databend+http://user:pass@host:8000/db?warehouse=wh
    /// ```
    pub async fn new(dsn: DatabendDsn, database: DatabendDatabase) -> EtlResult<DatabendClient> {
        let client = DatabendDriverClient::new(dsn.clone());
        let conn = client.get_conn().await.map_err(databend_error_to_etl_error)?;

        // Test the connection
        conn.version().await.map_err(databend_error_to_etl_error)?;

        info!("successfully connected to databend at database: {}", database);

        Ok(DatabendClient {
            dsn,
            database,
            client: Arc::new(client),
        })
    }

    /// Returns the fully qualified Databend table name.
    ///
    /// Formats the table name as `database.table_id`.
    pub fn full_table_name(&self, table_id: &DatabendTableId) -> String {
        format!("`{}`.`{}`", self.database, table_id)
    }

    /// Creates a table in Databend if it doesn't already exist.
    ///
    /// Returns `true` if the table was created, `false` if it already existed.
    pub async fn create_table_if_missing(
        &self,
        table_id: &DatabendTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<bool> {
        if self.table_exists(table_id).await? {
            return Ok(false);
        }

        self.create_table(table_id, column_schemas).await?;

        Ok(true)
    }

    /// Creates a new table in the Databend database.
    ///
    /// Builds and executes a CREATE TABLE statement with the provided column schemas.
    pub async fn create_table(
        &self,
        table_id: &DatabendTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<()> {
        validate_column_schemas(column_schemas)?;

        let full_table_name = self.full_table_name(table_id);
        let columns_spec = Self::create_columns_spec(column_schemas);

        info!("creating table {} in Databend", full_table_name);

        let query = format!("CREATE TABLE {} {}", full_table_name, columns_spec);

        self.execute(&query).await?;

        Ok(())
    }

    /// Creates a table or replaces it if it exists.
    ///
    /// Uses CREATE OR REPLACE TABLE to efficiently handle table recreation.
    /// Returns `true` if the table previously existed and was replaced.
    pub async fn create_or_replace_table(
        &self,
        table_id: &DatabendTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<bool> {
        validate_column_schemas(column_schemas)?;

        let table_existed = self.table_exists(table_id).await?;
        let full_table_name = self.full_table_name(table_id);
        let columns_spec = Self::create_columns_spec(column_schemas);

        info!(
            "creating or replacing table {} in Databend (existed: {})",
            full_table_name, table_existed
        );

        let query = format!("CREATE OR REPLACE TABLE {} {}", full_table_name, columns_spec);

        self.execute(&query).await?;

        Ok(table_existed)
    }

    /// Truncates all data from a Databend table.
    ///
    /// Executes a TRUNCATE TABLE statement to remove all rows while preserving the table structure.
    pub async fn truncate_table(&self, table_id: &DatabendTableId) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id);

        info!("truncating table {} in Databend", full_table_name);

        let query = format!("TRUNCATE TABLE {}", full_table_name);

        self.execute(&query).await?;

        Ok(())
    }

    /// Drops a table from Databend.
    ///
    /// Executes a DROP TABLE IF EXISTS statement to remove the table and all its data.
    pub async fn drop_table(&self, table_id: &DatabendTableId) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id);

        info!("dropping table {} from Databend", full_table_name);

        let query = format!("DROP TABLE IF EXISTS {}", full_table_name);

        self.execute(&query).await?;

        Ok(())
    }

    /// Checks whether a table exists in the Databend database.
    ///
    /// Returns `true` if the table exists, `false` otherwise.
    pub async fn table_exists(&self, table_id: &DatabendTableId) -> EtlResult<bool> {
        let query = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}' LIMIT 1",
            self.database, table_id
        );

        let conn = self.client.get_conn().await.map_err(databend_error_to_etl_error)?;
        let rows = conn
            .query_iter(&query)
            .await
            .map_err(databend_error_to_etl_error)?;

        let has_rows = rows.count().await > 0;
        Ok(has_rows)
    }

    /// Inserts a batch of rows into a Databend table.
    ///
    /// Uses INSERT INTO VALUES syntax for batch insertion.
    pub async fn insert_rows(
        &self,
        table_id: &DatabendTableId,
        column_schemas: &[ColumnSchema],
        table_rows: Vec<TableRow>,
    ) -> EtlResult<u64> {
        if table_rows.is_empty() {
            return Ok(0);
        }

        let full_table_name = self.full_table_name(table_id);

        // Encode rows into VALUES clause
        let values_clauses: Result<Vec<String>, EtlError> = table_rows
            .iter()
            .map(|row| encode_table_row(row, column_schemas))
            .collect();

        let values_clauses = values_clauses?;
        let values_str = values_clauses.join(", ");

        let query = format!("INSERT INTO {} VALUES {}", full_table_name, values_str);

        debug!("inserting {} rows into {}", table_rows.len(), full_table_name);

        self.execute(&query).await?;

        Ok(table_rows.len() as u64)
    }

    /// Executes a query that doesn't return results.
    async fn execute(&self, query: &str) -> EtlResult<()> {
        let conn = self.client.get_conn().await.map_err(databend_error_to_etl_error)?;
        conn.exec(query).await.map_err(databend_error_to_etl_error)?;
        Ok(())
    }

    /// Creates the column specification for CREATE TABLE statements.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let column_definitions: Vec<String> = column_schemas
            .iter()
            .map(|col| {
                let databend_type = postgres_type_to_databend_type(&col.typ);
                let nullable = if col.optional { "" } else { " NOT NULL" };
                format!("`{}` {}{}", col.name, databend_type, nullable)
            })
            .collect();

        format!("({})", column_definitions.join(", "))
    }
}

/// Converts a Databend error to an [`EtlError`].
fn databend_error_to_etl_error(err: databend_driver::Error) -> EtlError {
    etl_error!(
        ErrorKind::DestinationWriteFailed,
        "Databend operation failed",
        err.to_string()
    )
}

/// Converts a Postgres type to the corresponding Databend type.
fn postgres_type_to_databend_type(pg_type: &etl::types::Type) -> &'static str {
    use etl::types::Type;

    match pg_type {
        // Boolean
        Type::Bool => "BOOLEAN",

        // Integer types
        Type::Int2 => "SMALLINT",
        Type::Int4 => "INT",
        Type::Int8 => "BIGINT",

        // Floating point types
        Type::Float4 => "FLOAT",
        Type::Float8 => "DOUBLE",

        // Numeric/Decimal
        Type::Numeric => "DECIMAL(38, 18)",

        // Text types
        Type::Text | Type::Varchar | Type::Bpchar | Type::Name => "STRING",

        // Binary data
        Type::Bytea => "BINARY",

        // Date and time types
        Type::Date => "DATE",
        Type::Time => "STRING", // Databend doesn't have TIME type, use STRING
        Type::Timestamp => "TIMESTAMP",
        Type::Timestamptz => "TIMESTAMP",

        // UUID
        Type::Uuid => "STRING", // Store UUID as STRING

        // JSON types
        Type::Json | Type::Jsonb => "VARIANT",

        // OID
        Type::Oid => "BIGINT",

        // Array types - map to ARRAY<type>
        Type::BoolArray => "ARRAY(BOOLEAN)",
        Type::Int2Array => "ARRAY(SMALLINT)",
        Type::Int4Array => "ARRAY(INT)",
        Type::Int8Array => "ARRAY(BIGINT)",
        Type::Float4Array => "ARRAY(FLOAT)",
        Type::Float8Array => "ARRAY(DOUBLE)",
        Type::NumericArray => "ARRAY(DECIMAL(38, 18))",
        Type::TextArray | Type::VarcharArray | Type::BpcharArray => "ARRAY(STRING)",
        Type::ByteaArray => "ARRAY(BINARY)",
        Type::DateArray => "ARRAY(DATE)",
        Type::TimeArray => "ARRAY(STRING)",
        Type::TimestampArray => "ARRAY(TIMESTAMP)",
        Type::TimestamptzArray => "ARRAY(TIMESTAMP)",
        Type::UuidArray => "ARRAY(STRING)",
        Type::JsonArray | Type::JsonbArray => "ARRAY(VARIANT)",
        Type::OidArray => "ARRAY(BIGINT)",

        // Other types - default to STRING for safety
        _ => "STRING",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::Type;

    #[test]
    fn test_postgres_type_to_databend_type() {
        assert_eq!(postgres_type_to_databend_type(&Type::Bool), "BOOLEAN");
        assert_eq!(postgres_type_to_databend_type(&Type::Int4), "INT");
        assert_eq!(postgres_type_to_databend_type(&Type::Int8), "BIGINT");
        assert_eq!(postgres_type_to_databend_type(&Type::Float8), "DOUBLE");
        assert_eq!(postgres_type_to_databend_type(&Type::Text), "STRING");
        assert_eq!(postgres_type_to_databend_type(&Type::Timestamp), "TIMESTAMP");
        assert_eq!(postgres_type_to_databend_type(&Type::Json), "VARIANT");
        assert_eq!(postgres_type_to_databend_type(&Type::Int4Array), "ARRAY(INT)");
        assert_eq!(postgres_type_to_databend_type(&Type::TextArray), "ARRAY(STRING)");
    }

    #[test]
    fn test_create_columns_spec() {
        let column_schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::Text,
                optional: true,
            },
            ColumnSchema {
                name: "created_at".to_string(),
                typ: Type::Timestamp,
                optional: false,
            },
        ];

        let spec = DatabendClient::create_columns_spec(&column_schemas);
        assert_eq!(
            spec,
            "(`id` INT NOT NULL, `name` STRING, `created_at` TIMESTAMP NOT NULL)"
        );
    }

    #[test]
    fn test_full_table_name() {
        // Create a mock client for testing
        let dsn = "databend://localhost:8000/test_db".to_string();
        let database = "test_db".to_string();
        let driver_client = DatabendDriverClient::new(dsn.clone());

        let client = DatabendClient {
            dsn,
            database,
            client: Arc::new(driver_client),
        };

        assert_eq!(
            client.full_table_name("users"),
            "`test_db`.`users`"
        );
    }
}
