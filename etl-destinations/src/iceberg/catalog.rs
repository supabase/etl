use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
    spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec},
    table::Table,
};
use iceberg_catalog_rest::RestCatalog;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

/// A REST catalog which fixes some incompatibilities in Supabase catalog REST endpoints.
/// For endpoints which are compatible with the standard, it uses the inner [`RestCatalog`].
/// For endpoints with non-standard behaviour it uses its own HTTP client.
#[derive(Debug)]
pub(crate) struct SupabaseCatalog {
    inner: RestCatalog,
    client: SupabaseClient,
}

impl SupabaseCatalog {
    pub fn new(inner: RestCatalog, client: SupabaseClient) -> Self {
        SupabaseCatalog { inner, client }
    }
}

#[async_trait]
impl Catalog for SupabaseCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // self.client.list_tables(namespace).await
        self.inner.list_tables(namespace).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.inner.table_exists(identifier).await
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.inner.drop_table(identifier).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        self.inner.load_table(identifier).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.client.create_table(namespace, creation).await
        // self.inner.create_table(namespace, creation).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }
}

#[derive(Debug)]
pub(crate) struct SupabaseClient {
    client: Client,
    base_uri: String,
    warehouse: String,
    auth_token: String,
}

impl SupabaseClient {
    pub fn new(base_uri: String, warehouse: String, auth_token: String) -> Self {
        let client = Client::new();
        Self {
            client,
            base_uri,
            warehouse,
            auth_token,
        }
    }

    fn tables_url(&self, namespace_path: &str) -> String {
        let base = self.base_uri.trim_end_matches('/');
        format!(
            "{base}/v1/{}/namespaces/{namespace_path}/tables",
            self.warehouse
        )
    }

    pub async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let supabase_request = SupabaseCreateTableRequest {
            name: creation.name.clone(),
            location: creation.location.clone(),
            schema: creation.schema.clone(),
            spec: creation.partition_spec.clone(),
            sort_order: creation.sort_order.clone(),
            stage_create: Some(false),
            properties: if creation.properties.is_empty() {
                None
            } else {
                Some(creation.properties.clone())
            },
        };

        let namespace_path = namespace.as_ref().join(".");
        // let url = self.build_url(&format!("namespaces/{}/tables", namespace_path))?;
        let url = self.tables_url(&namespace_path);

        let body = serde_json::to_value(supabase_request).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON serialization failed: {}", e),
            )
        })?;

        let response = self
            .send_request(reqwest::Method::POST, &url, Some(body))
            .await?;

        let create_response: SupabaseCreateTableResponse = response.json().await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON deserialization failed: {}", e),
            )
        })?;

        // Convert Supabase response back to Iceberg Table
        self.convert_to_table(namespace, &creation.name, create_response)
            .await
    }

    /// Send authenticated request to Supabase
    async fn send_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Response> {
        let mut request = self
            .client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .header("Content-Type", "application/json");

        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = request.send().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, format!("HTTP request failed: {}", e))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("HTTP {} error: {}", status, error_text),
            ));
        }

        Ok(response)
    }

    /// Convert Supabase table response to Iceberg Table object
    async fn convert_to_table(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
        response: SupabaseCreateTableResponse,
    ) -> Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), name.to_string());

        // Create Table object with the returned metadata
        // The metadata location and content are provided by Supabase
        Table::builder()
            .metadata_location(response.metadata_location)
            .identifier(table_ident)
            .metadata(response.metadata)
            .build()
    }
}

/// Supabase-specific create table request format.
/// All optional properties must be omitted if missing instead of sending a null value.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct SupabaseCreateTableRequest {
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,

    schema: Schema,

    /// This is supposed to be partition-spec according to the standard, but Supabase
    /// catalog REST endpoint expects spec
    #[serde(skip_serializing_if = "Option::is_none")]
    spec: Option<UnboundPartitionSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    sort_order: Option<SortOrder>,

    #[serde(skip_serializing_if = "Option::is_none")]
    stage_create: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SupabaseCreateTableResponse {
    metadata_location: String,
    metadata: TableMetadata,
}
