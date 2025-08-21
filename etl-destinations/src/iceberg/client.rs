use std::{collections::HashMap, sync::Arc};

use iceberg::{Catalog, NamespaceIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tracing::info;

#[derive(Clone)]
pub struct IcebergClient {
    catalog: Arc<dyn Catalog>,
}

impl IcebergClient {
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

    /// Creates a namespace if it doesn't exist. `namespace` must be a dot separated hierarchical namespace.
    pub async fn create_namespace_if_missing(&self, namespace: &str) -> Result<(), iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?;
        }

        Ok(())
    }

    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool, iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.namespace_exists(&namespace_ident).await
    }
}

#[cfg(test)]
mod tests {
    use etl_telemetry::tracing::init_test_tracing;

    use super::*;

    #[tokio::test]
    async fn test_create_namespace() {
        init_test_tracing();

        let client = IcebergClient::new_with_rest_catalog(
            "http://localhost:8182/catalog".to_string(),
            "dev-and-test-warehouse".to_string(),
        );

        let namespace = "test-namespace";

        // namespace doesn't exist yet
        assert!(!client.namespace_exists(namespace).await.unwrap());

        client.create_namespace_if_missing(namespace).await.unwrap();

        // namespace should exist now
        assert!(client.namespace_exists(namespace).await.unwrap());

        // trying to create an existing namespace is a no-op
        client.create_namespace_if_missing(namespace).await.unwrap();

        // namespace still exists
        assert!(client.namespace_exists(namespace).await.unwrap());
    }
}
