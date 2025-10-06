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

/// A REST catalog implementation that handles Supabase-specific API incompatibilities.
///
/// This catalog acts as a compatibility layer between the standard Iceberg catalog interface
/// and Supabase's REST API endpoints. For standard-compliant endpoints, it delegates to
/// the inner [`RestCatalog`]. For endpoints with non-standard behavior, it uses a custom
/// HTTP client ([`SupabaseClient`]) to ensure proper communication with Supabase services.
///
/// The catalog maintains both a standard REST catalog instance and a Supabase-specific
/// client to provide seamless operation across different endpoint types.
#[derive(Debug)]
pub(crate) struct SupabaseCatalog {
    /// Standard REST catalog for compatible endpoints.
    inner: RestCatalog,
    /// Supabase-specific HTTP client for non-standard endpoints.
    client: SupabaseClient,
}

impl SupabaseCatalog {
    /// Creates a new Supabase catalog instance.
    ///
    /// Combines a standard REST catalog with a Supabase-specific client to handle
    /// both compatible and incompatible API endpoints.
    pub fn new(inner: RestCatalog, client: SupabaseClient) -> Self {
        SupabaseCatalog { inner, client }
    }
}

/// Implementation of the [`Catalog`] trait for Supabase.
///
/// Routes operations to either the standard REST catalog or the Supabase-specific
/// client based on endpoint compatibility. Table creation and deletion use the
/// custom client, while other operations delegate to the standard implementation.
#[async_trait]
impl Catalog for SupabaseCatalog {
    /// Lists all namespaces under the specified parent namespace.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    /// Creates a new namespace with the specified properties.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    /// Retrieves metadata for the specified namespace.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    /// Checks whether the specified namespace exists in the catalog.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    /// Updates properties for the specified namespace.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    /// Removes the specified namespace from the catalog.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    /// Lists all tables within the specified namespace.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    /// Checks whether the specified table exists in the catalog.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.inner.table_exists(identifier).await
    }

    /// Removes the specified table from the catalog.
    ///
    /// Uses the Supabase-specific client due to non-standard endpoint behavior.
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.client.drop_table(identifier).await
    }

    /// Renames a table from the source identifier to the destination identifier.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    /// Loads an existing table by its identifier.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        self.inner.load_table(identifier).await
    }

    /// Creates a new table in the specified namespace.
    ///
    /// Uses the Supabase-specific client due to non-standard request format requirements.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.client.create_table(namespace, creation).await
    }

    /// Updates an existing table with the specified commit operations.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }

    /// Registers an existing table with the catalog using its metadata location.
    ///
    /// Delegates to the standard REST catalog as this endpoint is compatible.
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

/// HTTP client for Supabase-specific catalog operations.
///
/// Handles authentication and request formatting for Supabase catalog REST endpoints
/// that deviate from the standard Iceberg catalog API. Maintains connection details
/// and provides methods for table creation and deletion operations.
#[derive(Debug)]
pub(crate) struct SupabaseClient {
    /// HTTP client for making requests.
    client: Client,
    /// Base URI for the Supabase catalog API.
    base_uri: String,
    /// Warehouse identifier for scoping operations.
    warehouse: String,
    /// Bearer token for authentication.
    auth_token: String,
}

impl SupabaseClient {
    /// Creates a new Supabase catalog client.
    ///
    /// Initializes an HTTP client with the provided connection details for
    /// communicating with Supabase catalog REST endpoints.
    pub fn new(base_uri: String, warehouse: String, auth_token: String) -> Self {
        let client = Client::new();
        Self {
            client,
            base_uri,
            warehouse,
            auth_token,
        }
    }

    /// Constructs the API URL for a specific table.
    ///
    /// Builds the complete URL path for table-specific operations using the
    /// warehouse identifier, namespace, and table name.
    fn table_url(&self, namespace: &str, table: &str) -> String {
        let base = self.base_uri.trim_end_matches('/');
        format!(
            "{base}/v1/{}/namespaces/{namespace}/tables/{table}",
            self.warehouse
        )
    }

    /// Constructs the API URL for namespace table operations.
    ///
    /// Builds the complete URL path for operations on tables within a specific
    /// namespace, such as listing or creating tables.
    fn tables_url(&self, namespace_path: &str) -> String {
        let base = self.base_uri.trim_end_matches('/');
        format!(
            "{base}/v1/{}/namespaces/{namespace_path}/tables",
            self.warehouse
        )
    }

    /// Creates a new table using Supabase-specific API format.
    ///
    /// Converts the standard [`TableCreation`] request to Supabase's expected format
    /// and handles the non-standard response structure. Returns a fully configured
    /// [`Table`] instance with metadata and file I/O capabilities.
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

    /// Removes a table from the catalog with data purging.
    ///
    /// Sends a DELETE request with `purgeRequested=true` parameter as S3 tables
    /// in Supabase require purging during deletion. Returns an error if the
    /// table does not exist.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let namespace_path = table.namespace().as_ref().join(".");
        let url = self.table_url(&namespace_path, table.name());

        let http_response = self
            .send_request(
                reqwest::Method::DELETE,
                &url,
                None,
                Some(&[("purgeRequested", "true")]), // S3 tables doesn't support dropping tables without purging
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

    /// Creates a configured file I/O instance for table operations.
    ///
    /// Builds a [`FileIO`] instance from the metadata location and additional
    /// configuration properties. The file I/O instance is used for reading
    /// and writing table data and metadata files.
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

    /// Sends an authenticated HTTP request to the Supabase catalog API.
    ///
    /// Configures the request with bearer token authentication and JSON content type.
    /// Includes optional request body and query parameters. Returns an error for
    /// non-successful HTTP status codes.
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

/// Deserializes a catalog HTTP response into the specified type.
///
/// Reads the response body bytes and attempts to parse them as JSON into the
/// target type. Provides detailed error context including the raw response
/// content if deserialization fails.
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

/// Converts an unexpected HTTP response into a detailed error.
///
/// Extracts status code, headers, and response body to create a comprehensive
/// error message for debugging failed catalog operations. Handles empty response
/// bodies gracefully.
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

/// Request structure for Supabase table creation endpoint.
///
/// Represents the JSON payload expected by Supabase's create table API.
/// Optional fields are omitted from serialization when `None` to match
/// Supabase's requirement for absent properties rather than null values.
/// Note that the partition specification field is named `spec` instead
/// of the standard `partition-spec`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateTableRequest {
    /// Name of the table to create.
    name: String,

    /// Optional location for table data storage.
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,

    /// Table schema definition.
    schema: Schema,

    /// Partition specification for the table.
    ///
    /// Named `spec` instead of the standard `partition-spec` to match
    /// Supabase's API expectations.
    #[serde(skip_serializing_if = "Option::is_none")]
    spec: Option<UnboundPartitionSpec>,

    /// Sort order specification for the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_order: Option<SortOrder>,

    /// Whether to stage the table creation operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    stage_create: Option<bool>,

    /// Additional table properties as key-value pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>,
}

/// Response structure from Supabase table operations.
///
/// Contains the table metadata and configuration returned by Supabase
/// catalog endpoints for table creation and loading operations.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    /// Location of the table's metadata file.
    metadata_location: Option<String>,
    /// Complete table metadata specification.
    metadata: TableMetadata,
    /// Additional configuration properties for the table.
    config: Option<HashMap<String, String>>,
}
