use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
    io::FileIO,
    spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec},
    table::Table,
};
use iceberg_catalog_rest::RestCatalog;
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

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
        self.inner.list_tables(namespace).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.inner.table_exists(identifier).await
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.client.drop_table(identifier).await
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
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn register_table(
        &self,
        identifier: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner
            .register_table(identifier, metadata_location)
            .await
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

    fn table_url(&self, namespace: &str, table: &str) -> String {
        let base = self.base_uri.trim_end_matches('/');
        format!(
            "{base}/v1/{}/namespaces/{namespace}/tables/{table}",
            self.warehouse
        )
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
        let supabase_request = CreateTableRequest {
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
        let url = self.tables_url(&namespace_path);

        let body = serde_json::to_value(supabase_request).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON serialization failed: {e}"),
            )
        })?;

        let http_response = self
            .send_request(reqwest::Method::POST, &url, Some(body), None)
            .await?;

        let response = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResponse>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Tried to create a table under a namespace that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "The table already exists",
                ));
            }
            _ => return Err(deserialize_unexpected_catalog_error(http_response).await),
        };

        let metadata_location = response.metadata_location.as_ref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            "Metadata location missing in `create_table` response!",
        ))?;

        let config = response.config.unwrap_or_default().into_iter().collect();

        let file_io = self
            .load_file_io(Some(metadata_location), Some(config))
            .await?;

        let table_ident = TableIdent::new(namespace.clone(), creation.name.to_string());

        let table_builder = Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(response.metadata);

        if let Some(metadata_location) = response.metadata_location {
            table_builder.metadata_location(metadata_location).build()
        } else {
            table_builder.build()
        }
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let namespace_path = table.namespace().as_ref().join(".");
        let url = self.table_url(&namespace_path, table.name());

        let http_response = self
            .send_request(
                reqwest::Method::DELETE,
                &url,
                None,
                Some(&[("purgeRequested", "true")]), // S3 tables doesn't support dropping tables with purging
            )
            .await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a table that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(http_response).await),
        }
    }

    async fn load_file_io(
        &self,
        metadata_location: Option<&str>,
        extra_config: Option<HashMap<String, String>>,
    ) -> Result<FileIO> {
        let mut props = HashMap::new();
        if let Some(config) = extra_config {
            props.extend(config);
        }

        let file_io = match metadata_location {
            Some(url) => FileIO::from_path(url)?.with_props(props).build()?,
            None => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Unable to load file io, metadata location is not set!",
                ))?;
            }
        };

        Ok(file_io)
    }

    /// Send authenticated request to Supabase
    async fn send_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<serde_json::Value>,
        query: Option<&[(&str, &str)]>,
    ) -> Result<Response> {
        let mut request = self
            .client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .header("Content-Type", "application/json");

        if let Some(body) = body {
            request = request.json(&body);
        }

        if let Some(query) = query {
            request = request.query(query)
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("HTTP {status} error: {error_text}"),
            ));
        }

        Ok(response)
    }
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(response: Response) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context("headers", format!("{:?}", response.headers()));

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    if bytes.is_empty() {
        return err;
    }
    err.with_context("json", String::from_utf8_lossy(&bytes))
}

/// Supabase-specific create table request format.
/// All optional properties must be omitted if missing instead of sending a null value.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateTableRequest {
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
struct LoadTableResponse {
    metadata_location: Option<String>,
    metadata: TableMetadata,
    config: Option<HashMap<String, String>>,
}
