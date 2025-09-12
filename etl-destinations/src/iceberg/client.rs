use std::{collections::HashMap, sync::Arc};

use etl::{
    error::EtlResult,
    types::{TableRow, TableSchema},
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

use crate::iceberg::{
    encoding::rows_to_record_batch, error::iceberg_error_to_etl_error,
    schema::postgres_to_iceberg_schema,
};

/// Client for connecting to Iceberg data lakes.
#[derive(Debug, Clone)]
pub struct IcebergClient {
    catalog: Arc<dyn Catalog>,
}

impl IcebergClient {
    /// Creates a new [IcebergClient] from a REST catalog URI and a warehouse name.
    pub fn new_with_rest_catalog(catalog_uri: String, warehouse_name: String) -> Self {
        let catalog_config = RestCatalogConfig::builder()
            .uri(catalog_uri)
            .warehouse(warehouse_name)
            .build();
        let catalog = RestCatalog::new(catalog_config);
        IcebergClient {
            catalog: Arc::new(catalog),
        }
    }

    /// Creates a namespace if it doesn't exist.
    pub async fn create_namespace_if_missing(&self, namespace: &str) -> Result<(), iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?;
        }

        Ok(())
    }

    /// Returns true if the `namespace` exists, false otherwise.
    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool, iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.namespace_exists(&namespace_ident).await
    }

    /// Creates a table if it doesn't exits.
    pub async fn create_table_if_missing(
        &self,
        namespace: &str,
        table_name: String,
        table_schema: &TableSchema,
    ) -> Result<(), iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());
        if !self.catalog.table_exists(&table_ident).await? {
            let iceberg_schema = postgres_to_iceberg_schema(table_schema)?;
            let creation = TableCreation::builder()
                .name(table_name)
                .schema(iceberg_schema)
                .build();
            self.catalog
                .create_table(&namespace_ident, creation)
                .await?;
        }
        Ok(())
    }

    /// Returns true if the table exists, false otherwise.
    pub async fn table_exists(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<bool, iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.table_exists(&table_ident).await
    }

    /// Drops a table
    pub async fn drop_table(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<(), iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.drop_table(&table_ident).await
    }

    /// Drops a namespace
    pub async fn drop_namespace(&self, namespace: &str) -> Result<(), iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.drop_namespace(&namespace_ident).await
    }

    /// Send rows to the destination
    pub async fn stream_rows(
        &self,
        namespace: String,
        table_name: String,
        table_rows: &[TableRow],
    ) -> EtlResult<()> {
        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);

        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(iceberg_error_to_etl_error)?;
        let table_metadata = table.metadata();
        let iceberg_schema = table_metadata.current_schema();

        // Convert the actual Iceberg schema to Arrow schema using iceberg-rust's built-in converter
        // This preserves field IDs properly for transaction-based writes
        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(iceberg_error_to_etl_error)?;
        let _record_batch = rows_to_record_batch(table_rows, &arrow_schema)?;

        Ok(())
    }
}
