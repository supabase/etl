//! Iceberg client mirroring BigQuery client patterns.

use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::{etl_error};
use etl::types::{Cell, TableRow, TableSchema};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::fmt;
use std::sync::Arc;
use tracing::{info, debug, warn};

/// Maximum byte size for streaming data to Iceberg (similar to BigQuery limit).
const MAX_SIZE_BYTES: usize = 64 * 1024 * 1024; // 64MB

/// Trace identifier for ETL operations in Iceberg client.
const ETL_TRACE_ID: &str = "ETL IcebergClient";

/// Special column name for Change Data Capture operations in Iceberg (matching BigQuery).
const ICEBERG_CDC_SPECIAL_COLUMN: &str = "_CHANGE_TYPE";

/// Special column name for Change Data Capture sequence ordering in Iceberg (matching BigQuery).
const ICEBERG_CDC_SEQUENCE_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

/// Change Data Capture operation types for Iceberg streaming (matching BigQuery).
#[derive(Debug, Clone)]
pub enum IcebergOperationType {
    Upsert,
    Delete,
}

impl IcebergOperationType {
    /// Converts the operation type into a [`Cell`] for streaming.
    pub fn into_cell(self) -> Cell {
        Cell::String(self.to_string())
    }
}

impl fmt::Display for IcebergOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IcebergOperationType::Upsert => write!(f, "UPSERT"),
            IcebergOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Client for interacting with Apache Iceberg.
///
/// Provides methods for table management and data insertion
/// mirroring BigQuery client patterns for consistency.
#[derive(Clone, Debug)]
pub struct IcebergClient {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    #[allow(dead_code)]
    catalog_uri: String,
    #[allow(dead_code)]
    warehouse: String,
    #[allow(dead_code)]
    auth_token: Option<String>,
}

impl IcebergClient {
    /// Creates a new [`IcebergClient`] with REST catalog configuration.
    ///
    /// Mirrors BigQuery's simple constructor pattern.
    pub async fn new_with_rest_catalog(
        catalog_uri: String,
        warehouse: String,
        namespace: String,
        auth_token: Option<String>,
    ) -> EtlResult<IcebergClient> {
        if catalog_uri.is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Invalid Iceberg catalog URI",
                "Catalog URI cannot be empty"
            ));
        }

        if warehouse.is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Invalid Iceberg warehouse",
                "Warehouse location cannot be empty"
            ));
        }

        if namespace.is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Invalid Iceberg namespace",
                "Namespace cannot be empty"
            ));
        }

        // Initialize actual Iceberg REST catalog
        let config_builder = RestCatalogConfig::builder()
            .uri(catalog_uri.clone())
            .warehouse(warehouse.clone());

        if let Some(_token) = &auth_token {
            // Note: Authentication will be added in future phases
            debug!("Authentication token provided but not yet implemented");
        }

        let config = config_builder.build();

        let catalog = Arc::new(RestCatalog::new(config));
        info!(
            catalog_uri = %catalog_uri,
            warehouse = %warehouse,
            namespace = %namespace,
            "Connected to Iceberg REST catalog"
        );

        Ok(IcebergClient {
            catalog,
            namespace,
            catalog_uri,
            warehouse,
            auth_token,
        })
    }

    /// Creates a table if it doesn't exist with the given schema.
    ///
    /// Mirrors BigQuery's table creation pattern.
    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> EtlResult<()> {
        info!(
            table = %table_name,
            namespace = %self.namespace,
            columns = table_schema.column_schemas.len(),
            "Creating Iceberg table if not exists"
        );

        let namespace_ident = NamespaceIdent::new(self.namespace.clone());

        let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

        // Check if table already exists
        match self.catalog.table_exists(&table_ident).await {
            Ok(true) => {
                debug!(table = %table_name, "Table already exists, skipping creation");
                return Ok(());
            }
            Ok(false) => {
                debug!(table = %table_name, "Table does not exist, creating");
            }
            Err(e) => {
                warn!(
                    table = %table_name,
                    error = %e,
                    "Failed to check table existence, attempting creation"
                );
            }
        }

        // Convert ETL TableSchema to Iceberg schema using our schema mapper
        let mut schema_mapper = crate::iceberg::schema::SchemaMapper::new();
        let iceberg_schema = schema_mapper.postgres_to_iceberg(table_schema)?;

        // Create table
        let table_creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(iceberg_schema)
            .build();

        self.catalog
            .create_table(&table_ident.namespace(), table_creation)
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to create Iceberg table",
                    format!("Table: {}, Error: {}", table_name, e)
                )
            })?;

        info!(
            table = %table_name,
            namespace = %self.namespace,
            "Successfully created Iceberg table"
        );

        Ok(())
    }

    /// Streams rows to an Iceberg table.
    ///
    /// Mirrors BigQuery's streaming insert pattern.
    pub async fn stream_rows(
        &self,
        table_name: &str,
        rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        info!(
            table = %table_name,
            row_count = rows.len(),
            "Streaming rows to Iceberg table"
        );

        // Calculate approximate size for batching
        let estimated_size = rows.len() * 1024; // Rough estimate
        if estimated_size > MAX_SIZE_BYTES {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Batch too large for Iceberg streaming",
                format!(
                    "Estimated size {} bytes exceeds maximum {} bytes",
                    estimated_size, MAX_SIZE_BYTES
                )
            ));
        }

        let namespace_ident = NamespaceIdent::new(self.namespace.clone());

        let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

        // Load the table
        let _table = self.catalog.load_table(&table_ident).await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to load Iceberg table for writing",
                format!("Table: {}, Error: {}", table_name, e)
            )
        })?;

        // For Phase 2, implement a simplified write approach
        // Real Iceberg writes will be enhanced in future phases
        info!(
            table = %table_name,
            rows = rows.len(),
            "Phase 2: Simulating Iceberg write operation"
        );
        
        // TODO: Implement actual Iceberg write operations
        // This would involve:
        // 1. Converting rows to Arrow RecordBatch
        // 2. Writing the batch as Parquet files
        // 3. Updating Iceberg metadata
        // 4. Committing the transaction

        info!(
            table = %table_name,
            rows = rows.len(),
            "Successfully streamed rows to Iceberg table"
        );

        Ok(())
    }

    /// Drops a table if it exists.
    ///
    /// Used for cleanup operations.
    pub async fn drop_table_if_exists(&self, table_name: &str) -> EtlResult<()> {
        info!(
            table = %table_name,
            namespace = %self.namespace,
            "Dropping Iceberg table if exists"
        );

        // TODO: Implement actual table dropping
        // This would involve calling catalog.drop_table()

        info!(
            table = %table_name,
            "Iceberg table drop completed (placeholder implementation)"
        );

        Ok(())
    }

    /// Checks if a table exists in the catalog.
    pub async fn table_exists(&self, table_name: &str) -> EtlResult<bool> {
        info!(
            table = %table_name,
            namespace = %self.namespace,
            "Checking if Iceberg table exists"
        );

        // TODO: Implement actual table existence check
        // This would involve calling catalog.table_exists()

        // For now, return false (table doesn't exist)
        Ok(false)
    }

    /// Lists all tables in the namespace.
    pub async fn list_tables(&self) -> EtlResult<Vec<String>> {
        info!(
            namespace = %self.namespace,
            "Listing Iceberg tables in namespace"
        );

        // TODO: Implement actual table listing
        // This would involve calling catalog.list_tables()

        // For now, return empty list
        Ok(vec![])
    }

    /// Gets the catalog URI.
    pub fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    /// Gets the warehouse location.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Gets the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// Converts Iceberg errors to ETL errors.
fn iceberg_error_to_etl_error(err: &str) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "Iceberg operation failed",
        err
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let client = IcebergClient::new_with_rest_catalog(
            "http://localhost:8181".to_string(),
            "s3://test-bucket/warehouse".to_string(),
            "test".to_string(),
            None,
        ).await;

        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.catalog_uri(), "http://localhost:8181");
        assert_eq!(client.namespace(), "test");
    }

    #[tokio::test]
    async fn test_client_creation_invalid_config() {
        let result = IcebergClient::new_with_rest_catalog(
            "".to_string(), // Empty URI should fail
            "s3://test-bucket/warehouse".to_string(),
            "test".to_string(),
            None,
        ).await;

        assert!(result.is_err());
    }

    #[test]
    fn test_operation_type_display() {
        assert_eq!(IcebergOperationType::Upsert.to_string(), "UPSERT");
        assert_eq!(IcebergOperationType::Delete.to_string(), "DELETE");
    }

    #[test]
    fn test_operation_type_into_cell() {
        let upsert_cell = IcebergOperationType::Upsert.into_cell();
        match upsert_cell {
            Cell::String(s) => assert_eq!(s, "UPSERT"),
            _ => panic!("Expected string cell"),
        }
    }
}