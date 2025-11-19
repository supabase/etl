use crate::error::ValidationError;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use std::collections::HashMap;

pub(super) async fn validate_rest(
    catalog_uri: &str,
    warehouse_name: &str,
    properties: &HashMap<String, String>,
) -> Result<(), ValidationError> {
    let mut props = properties.clone();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse_name.to_string(),
    );

    let builder = RestCatalogBuilder::default();
    let catalog = builder.load("RestCatalog", props).await.map_err(|e| {
        ValidationError::ConnectionFailed(format!("Failed to create Iceberg REST catalog: {}", e))
    })?;

    catalog.list_namespaces(None).await.map_err(|e| {
        ValidationError::ConnectionFailed(format!(
            "Failed to list namespaces in Iceberg catalog: {}",
            e
        ))
    })?;

    Ok(())
}

pub(super) async fn validate_supabase(
    project_ref: &str,
    warehouse_name: &str,
    catalog_token: &str,
    s3_access_key_id: &str,
    s3_secret_access_key: &str,
    s3_region: &str,
) -> Result<(), ValidationError> {
    let catalog_uri = format!("https://{}.supabase.co/storage/v1/s3", project_ref);

    let mut properties = HashMap::new();
    properties.insert("s3.access-key-id".to_string(), s3_access_key_id.to_string());
    properties.insert(
        "s3.secret-access-key".to_string(),
        s3_secret_access_key.to_string(),
    );
    properties.insert("s3.region".to_string(), s3_region.to_string());
    properties.insert("token".to_string(), catalog_token.to_string());

    validate_rest(&catalog_uri, warehouse_name, &properties).await
}
