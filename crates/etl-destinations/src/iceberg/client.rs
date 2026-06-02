use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use arrow::{
    array::{RecordBatch, new_null_array},
    datatypes::{Field, Schema as ArrowSchema},
};
use etl::{
    error::EtlResult,
    etl_error,
    types::{ColumnSchema, SchemaDiff, TableRow},
};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{
        DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile,
        ManifestListWriter, ManifestWriter, ManifestWriterBuilder, NestedFieldRef, Operation,
        PrimitiveType, Schema as IcebergSchema, SchemaRef, Snapshot, SnapshotReference,
        SnapshotRetention, SnapshotSummaryCollector, Summary, TableProperties, Type as IcebergType,
    },
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
        },
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use iceberg_catalog_rest::{
    CommitTableRequest, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use parquet::{
    arrow::PARQUET_FIELD_ID_META_KEY, basic::Compression, file::properties::WriterProperties,
};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{
    iceberg::{
        catalog::{SupabaseCatalog, SupabaseClient, storage_factory_for_catalog_props},
        encoding::rows_to_record_batch,
        error::{arrow_error_to_etl_error, iceberg_error_to_etl_error},
        schema::postgres_to_iceberg_schema,
    },
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
};

/// Authentication token key for catalog configuration.
const CATALOG_TOKEN: &str = "token";
/// Iceberg REST client version header value mirrored from iceberg-catalog-rest.
const ICEBERG_REST_CLIENT_VERSION: &str = "0.14.1";
/// Iceberg REST catalog API path version.
const REST_API_ROOT: &str = "v1";
/// Metadata directory under an Iceberg table location.
const METADATA_DIRECTORY: &str = "metadata";
/// Default number of row-delta commit retries.
const ROW_DELTA_COMMIT_RETRIES: u32 = 10;
/// Default number of schema commit retries.
const SCHEMA_COMMIT_RETRIES: u32 = 10;
/// Table property controlling equality-delete target file size.
const WRITE_DELETE_TARGET_FILE_SIZE_BYTES: &str = "write.delete.target-file-size-bytes";
/// Iceberg's default equality-delete target file size.
const WRITE_DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 64 * 1024 * 1024;

/// Client for managing Apache Iceberg data lake operations.
///
/// This client provides a high-level interface for interacting with Iceberg
/// catalogs, supporting operations such as namespace and table management, data
/// insertion, and schema operations. It abstracts the underlying catalog
/// implementation and provides a unified interface for both REST catalogs and
/// Supabase-specific configurations.
///
/// The client maintains a thread-safe reference to the catalog implementation,
/// allowing for concurrent operations across multiple threads.
#[derive(Debug, Clone)]
pub struct IcebergClient {
    /// The underlying Iceberg catalog implementation.
    catalog: Arc<dyn Catalog>,
    /// REST commit helper used for table commits not exposed by
    /// iceberg-rust's public transaction API yet.
    rest_commit: RestCommitClient,
}

impl IcebergClient {
    /// Creates a new [`IcebergClient`] using a REST catalog configuration.
    ///
    /// This constructor initializes a client that connects to an Iceberg
    /// catalog through the REST catalog protocol. The REST catalog provides
    /// a standardized HTTP-based interface for catalog operations.
    pub async fn new_with_rest_catalog(
        catalog_uri: String,
        warehouse_name: String,
        mut props: HashMap<String, String>,
    ) -> Result<Self, iceberg::Error> {
        props.insert(REST_CATALOG_PROP_URI.to_owned(), catalog_uri);
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse_name);

        let builder = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory_for_catalog_props(&props)?);
        let catalog = builder.load("RestCatalog", props.clone()).await?;
        let rest_commit = RestCommitClient::from_props(props);

        Ok(IcebergClient { catalog: Arc::new(catalog), rest_commit })
    }

    /// Creates a new [`IcebergClient`] configured for Supabase storage
    /// integration.
    ///
    /// This constructor creates a client specifically configured to work with
    /// Supabase's storage service, automatically setting up the necessary
    /// S3-compatible endpoints and authentication parameters. The client
    /// uses Supabase's REST catalog implementation with additional custom
    /// behavior for Supabase-specific operations.
    ///
    /// The method automatically constructs the catalog URI and S3 endpoint from
    /// the provided project reference and domain, simplifying the
    /// configuration process for Supabase users.
    pub async fn new_with_supabase_catalog(
        project_ref: &str,
        supabase_domain: &str,
        catalog_token: String,
        warehouse_name: String,
        s3_access_key_id: String,
        s3_secret_access_key: String,
        s3_region: String,
    ) -> Result<Self, iceberg::Error> {
        let base_uri = format!("https://{project_ref}.storage.{supabase_domain}/storage");
        let catalog_uri = format!("{base_uri}/v1/iceberg");
        let s3_endpoint = format!("{base_uri}/v1/s3");

        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(CATALOG_TOKEN.to_owned(), catalog_token.clone());
        props.insert(S3_ACCESS_KEY_ID.to_owned(), s3_access_key_id);
        props.insert(S3_SECRET_ACCESS_KEY.to_owned(), s3_secret_access_key);
        props.insert(S3_ENDPOINT.to_owned(), s3_endpoint);
        props.insert(S3_REGION.to_owned(), s3_region);
        props.insert(REST_CATALOG_PROP_URI.to_owned(), catalog_uri.clone());
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse_name.clone());

        let builder = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory_for_catalog_props(&props)?);
        let inner = builder.load("SupabaseCatalog", props.clone()).await?;
        let client = SupabaseClient::new(catalog_uri, warehouse_name, catalog_token);
        let catalog = SupabaseCatalog::new(inner, client);
        let rest_commit = RestCommitClient::from_props(props);

        Ok(IcebergClient { catalog: Arc::new(catalog), rest_commit })
    }

    /// Creates a namespace if it does not already exist.
    ///
    /// This method performs an idempotent namespace creation operation. It
    /// first checks if the namespace exists and only creates it if it's
    /// missing. The namespace string can use dot notation for hierarchical
    /// namespaces (e.g., "warehouse.schema").
    pub async fn create_namespace_if_missing(&self, namespace: &str) -> Result<(), iceberg::Error> {
        debug!(%namespace, "creating namespace");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog.create_namespace(&namespace_ident, HashMap::new()).await?;
        }

        Ok(())
    }

    /// Checks whether a namespace exists in the catalog.
    ///
    /// This method queries the catalog to determine if the specified namespace
    /// is present. The namespace string supports dot notation for hierarchical
    /// namespaces.
    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool, iceberg::Error> {
        debug!(%namespace, "checking if namespace exists");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.namespace_exists(&namespace_ident).await
    }

    /// Validates that the catalog is accessible by listing namespaces.
    ///
    /// This method attempts to list namespaces in the catalog as a connectivity
    /// check, similar to how BigQuery's `dataset_exists` verifies access.
    /// This is the cheapest way to verify the connection to the Iceberg
    /// catalog works correctly. Returns `Ok(())` if the catalog is
    /// accessible, or an error if connectivity fails.
    pub async fn validate_connectivity(&self) -> Result<(), iceberg::Error> {
        debug!("validating iceberg catalog connectivity");
        // Try to list namespaces as a connectivity check; this is the cheapest
        // operation that actually polls data from the catalog.
        self.catalog.list_namespaces(None).await?;
        Ok(())
    }

    /// Creates a table if it does not already exist.
    ///
    /// This method performs an idempotent table creation operation. It checks
    /// if the table exists within the specified namespace and creates it if
    /// missing. The table schema is derived from the provided column
    /// schemas, which are automatically converted from PostgreSQL types to
    /// Iceberg schema format.
    ///
    /// The created table includes default commit properties for retry behavior
    /// and transaction management.
    pub async fn create_table_if_missing(
        &self,
        namespace: &str,
        table_name: String,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), iceberg::Error> {
        debug!(%table_name, %namespace, "creating table if missing");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());

        if !self.catalog.table_exists(&table_ident).await? {
            let iceberg_schema = postgres_to_iceberg_schema(column_schemas)?;
            let creation = TableCreation::builder()
                .name(table_name)
                .schema(iceberg_schema)
                .format_version(FormatVersion::V2)
                .properties(Self::get_table_properties())
                .build();
            self.catalog.create_table(&namespace_ident, creation).await?;
        }

        Ok(())
    }

    /// Evolves an existing table to match the provided source columns.
    ///
    /// The desired schema is converted with stable source-derived field IDs.
    /// If the current Iceberg schema already matches, no catalog commit is
    /// issued. Otherwise the method commits a new schema and sets it as current
    /// using the REST catalog update path.
    pub async fn evolve_table_schema(
        &self,
        namespace: &str,
        table_name: String,
        current_column_schemas: &[ColumnSchema],
        desired_column_schemas: &[ColumnSchema],
        diff: &SchemaDiff,
    ) -> Result<(), iceberg::Error> {
        debug!(%table_name, %namespace, "evolving iceberg table schema");

        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        let current_schema = postgres_to_iceberg_schema(current_column_schemas)?;
        let desired_schema = postgres_to_iceberg_schema(desired_column_schemas)?;
        validate_postgres_schema_evolution(&current_schema, &desired_schema, diff)?;

        retry_with_backoff(
            RetryPolicy {
                max_retries: SCHEMA_COMMIT_RETRIES,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(1),
            },
            |error: &iceberg::Error| {
                if error.retryable() { RetryDecision::Retry } else { RetryDecision::Stop }
            },
            |delay| delay,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying iceberg schema commit"
                );
            },
            || {
                let catalog = Arc::clone(&self.catalog);
                let rest_commit = self.rest_commit.clone();
                let table_ident = table_ident.clone();
                let current_schema = current_schema.clone();
                let desired_schema = desired_schema.clone();

                async move {
                    let table = catalog.load_table(&table_ident).await?;
                    let Some(commit) =
                        build_schema_update_commit(&table, &current_schema, desired_schema)?
                    else {
                        return Ok(());
                    };

                    rest_commit.commit_table(&table_ident, commit).await
                }
            },
        )
        .await
        .map_err(|failure| failure.last_error)
    }

    /// Returns the field IDs in the table's current top-level Iceberg schema.
    pub(super) async fn current_schema_field_ids(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<HashSet<i32>, iceberg::Error> {
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        let table = self.catalog.load_table(&table_ident).await?;

        Ok(table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect())
    }

    /// Generates default table properties for commit behavior and retry
    /// configuration.
    ///
    /// This method returns a set of table properties that configure the commit
    /// behavior for Iceberg tables, including retry timeouts, retry counts, and
    /// total retry time limits. These properties help ensure reliable commits
    /// during etl operations.
    fn get_table_properties() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_owned(),
            "100".to_owned(),
        );
        props.insert(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_owned(),
            "10000".to_owned(),
        );
        props.insert(TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_owned(), "10".to_owned());
        props.insert(
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS.to_owned(),
            "1800000".to_owned(),
        );
        props.insert(TableProperties::PROPERTY_FORMAT_VERSION.to_owned(), "2".to_owned());
        props
    }

    /// Checks whether a table exists within the specified namespace.
    ///
    /// This method queries the catalog to determine if the specified table
    /// exists within the given namespace.
    pub async fn table_exists(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<bool, iceberg::Error> {
        debug!(%table_name, %namespace, "checking if table exists");

        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.table_exists(&table_ident).await
    }

    /// Removes a table from the specified namespace if it exists.
    ///
    /// This method checks if the table exists before attempting to drop it,
    /// providing idempotent behavior. Returns `Ok(true)` if a table was
    /// dropped, `Ok(false)` if the table did not exist.
    pub async fn drop_table_if_exists(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<bool, iceberg::Error> {
        debug!(%table_name, %namespace, "dropping table if exists");

        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);

        if !self.catalog.table_exists(&table_ident).await? {
            return Ok(false);
        }

        self.catalog.drop_table(&table_ident).await?;

        Ok(true)
    }

    /// Removes a namespace from the catalog.
    ///
    /// This method permanently deletes the namespace from the catalog. The
    /// namespace must be empty (contain no tables) before it can be
    /// dropped.
    pub async fn drop_namespace(&self, namespace: &str) -> Result<(), iceberg::Error> {
        debug!(%namespace, "dropping namespace");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.drop_namespace(&namespace_ident).await
    }

    /// Loads a table from the catalog.
    ///
    /// This method retrieves the table metadata and returns an
    /// [`iceberg::table::Table`]
    /// instance that can be used for further operations such as reading
    /// data, writing data, or inspecting the table schema and properties.
    pub async fn load_table(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<iceberg::table::Table, iceberg::Error> {
        debug!(%table_name, %namespace, "loading table");
        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.load_table(&table_ident).await
    }

    /// Inserts rows into a table within the specified namespace.
    ///
    /// This method performs a batch insert operation, converting the provided
    /// table rows into Arrow RecordBatch format and writing them to the Iceberg
    /// table using Parquet format. The operation uses a fast append strategy
    /// without duplicate checking for optimal performance. Returns the total
    /// size in bytes of the written data files.
    pub async fn insert_rows(
        &self,
        namespace: String,
        table_name: String,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<u64> {
        if table_rows.is_empty() {
            return Ok(0);
        }

        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);

        let table =
            self.catalog.load_table(&table_ident).await.map_err(iceberg_error_to_etl_error)?;
        let table_metadata = table.metadata();
        let iceberg_schema = table_metadata.current_schema();

        // Convert the actual Iceberg schema to Arrow schema using iceberg-rust's
        // built-in converter. This preserves field IDs properly for
        // transaction-based writes.
        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(iceberg_error_to_etl_error)?;
        let record_batch =
            rows_to_record_batch(&table_rows, arrow_schema).map_err(arrow_error_to_etl_error)?;

        self.write_record_batch(&table, record_batch).await.map_err(iceberg_error_to_etl_error)
    }

    /// Applies a CDC row-delta batch to an Iceberg table.
    ///
    /// Data rows are appended as Parquet data files. Delete rows are written as
    /// Iceberg v2 equality-delete files using the table identifier fields.
    /// When delete files are present, the method commits data and deletes in
    /// one snapshot through the REST catalog commit endpoint.
    pub async fn apply_row_delta(
        &self,
        namespace: String,
        table_name: String,
        data_rows: Vec<TableRow>,
        delete_rows: Vec<TableRow>,
    ) -> EtlResult<u64> {
        if data_rows.is_empty() && delete_rows.is_empty() {
            return Ok(0);
        }

        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);
        let table =
            self.catalog.load_table(&table_ident).await.map_err(iceberg_error_to_etl_error)?;

        let mut bytes_sent = 0;
        let data_files = if data_rows.is_empty() {
            Vec::new()
        } else {
            let record_batch = self.rows_to_table_record_batch(&table, &data_rows)?;
            let data_files =
                self.write_data_files(&table, record_batch).await.map_err(|error| {
                    tracing::error!(error = %error, "failed to write iceberg data files");
                    iceberg_error_to_etl_error(error)
                })?;
            bytes_sent += total_file_size(&data_files);
            data_files
        };

        if delete_rows.is_empty() {
            self.commit_append(&table, data_files).await.map_err(iceberg_error_to_etl_error)?;
            return Ok(bytes_sent);
        }

        let record_batch = self.rows_to_equality_delete_record_batch(&table, &delete_rows)?;
        let delete_files =
            self.write_equality_delete_files(&table, record_batch).await.map_err(|error| {
                tracing::error!(error = %error, "failed to write iceberg equality-delete files");
                iceberg_error_to_etl_error(error)
            })?;
        bytes_sent += total_file_size(&delete_files);

        self.commit_row_delta(table_ident, data_files, delete_files).await.map_err(|error| {
            tracing::error!(error = %error, "failed to commit iceberg row delta");
            iceberg_error_to_etl_error(error)
        })?;

        Ok(bytes_sent)
    }

    /// Converts rows to a [`RecordBatch`] using the current Iceberg table
    /// schema.
    fn rows_to_table_record_batch(
        &self,
        table: &iceberg::table::Table,
        table_rows: &[TableRow],
    ) -> EtlResult<RecordBatch> {
        let table_metadata = table.metadata();
        let iceberg_schema = table_metadata.current_schema();

        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(iceberg_error_to_etl_error)?;
        let record_batch =
            rows_to_record_batch(table_rows, arrow_schema).map_err(arrow_error_to_etl_error)?;

        Ok(record_batch)
    }

    /// Converts dense equality-key rows to the table-shaped batch expected by
    /// iceberg-rust's equality-delete writer projector.
    fn rows_to_equality_delete_record_batch(
        &self,
        table: &iceberg::table::Table,
        table_rows: &[TableRow],
    ) -> EtlResult<RecordBatch> {
        let table_metadata = table.metadata();
        let iceberg_schema = table_metadata.current_schema();
        let equality_ids = identifier_field_ids_in_schema_order(iceberg_schema);

        if equality_ids.is_empty() {
            return Err(etl_error!(
                etl::error::ErrorKind::InvalidState,
                "Iceberg equality deletes require identifier fields"
            ));
        }

        for table_row in table_rows {
            if table_row.values().len() != equality_ids.len() {
                return Err(etl_error!(
                    etl::error::ErrorKind::InvalidState,
                    "Iceberg equality-delete row width does not match identifier fields",
                    format!(
                        "Delete row has {} values for {} identifier fields.",
                        table_row.values().len(),
                        equality_ids.len()
                    )
                ));
            }
        }

        let config = EqualityDeleteWriterConfig::new(equality_ids, Arc::clone(iceberg_schema))
            .map_err(iceberg_error_to_etl_error)?;
        let projected_arrow_schema = config.projected_arrow_schema_ref().as_ref().clone();
        let projected_batch = rows_to_record_batch(table_rows, projected_arrow_schema)
            .map_err(arrow_error_to_etl_error)?;
        let full_arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(iceberg_error_to_etl_error)?;
        let field_positions = arrow_field_positions_by_id(&full_arrow_schema)?;
        let mut columns = full_arrow_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), table_rows.len()))
            .collect::<Vec<_>>();

        for (projected_index, projected_field) in
            config.projected_arrow_schema_ref().fields().iter().enumerate()
        {
            let field_id = arrow_field_id(projected_field)?;
            let Some(&full_index) = field_positions.get(&field_id) else {
                return Err(etl_error!(
                    etl::error::ErrorKind::InvalidState,
                    "Iceberg equality-delete field is missing from table schema",
                    format!("Field id {field_id} is not present in the current table schema.")
                ));
            };

            columns[full_index] = Arc::clone(projected_batch.column(projected_index));
        }

        RecordBatch::try_new(Arc::new(full_arrow_schema), columns).map_err(arrow_error_to_etl_error)
    }

    /// Writes a RecordBatch to an Iceberg table using Parquet format.
    ///
    /// This method handles the low-level details of writing Arrow RecordBatch
    /// data to an Iceberg table. It creates the necessary writers, applies
    /// the data, and commits the transaction to make the data visible in
    /// the table. Returns the total number of bytes of the written data
    /// files.
    async fn write_record_batch(
        &self,
        table: &iceberg::table::Table,
        record_batch: RecordBatch,
    ) -> Result<u64, iceberg::Error> {
        let data_files = self.write_data_files(table, record_batch).await?;
        let bytes_sent = total_file_size(&data_files);

        self.commit_append(table, data_files).await?;

        Ok(bytes_sent)
    }

    /// Writes a [`RecordBatch`] to data files without committing them.
    async fn write_data_files(
        &self,
        table: &iceberg::table::Table,
        record_batch: RecordBatch,
    ) -> Result<Vec<DataFile>, iceberg::Error> {
        let writer_props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
        let target_file_size = table.metadata().table_properties()?.write_target_file_size_bytes;

        let rolling_writer_builder = parquet_rolling_writer_builder(
            table,
            writer_props,
            target_file_size,
            "data",
            Arc::clone(table.metadata().current_schema()),
        )?;

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let mut data_file_writer = data_file_writer_builder.build(None).await?;

        data_file_writer.write(record_batch).await?;

        data_file_writer.close().await
    }

    /// Writes equality-delete files for the table identifier fields.
    async fn write_equality_delete_files(
        &self,
        table: &iceberg::table::Table,
        record_batch: RecordBatch,
    ) -> Result<Vec<DataFile>, iceberg::Error> {
        let equality_ids = identifier_field_ids_in_schema_order(table.metadata().current_schema());

        if equality_ids.is_empty() {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::PreconditionFailed,
                "Iceberg equality deletes require identifier fields",
            ));
        }

        let config = EqualityDeleteWriterConfig::new(
            equality_ids,
            Arc::clone(table.metadata().current_schema()),
        )?;
        let delete_schema =
            Arc::new(iceberg::arrow::arrow_schema_to_schema(config.projected_arrow_schema_ref())?);
        let writer_props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
        let target_file_size = table_delete_target_file_size(table)?;
        let rolling_writer_builder = parquet_rolling_writer_builder(
            table,
            writer_props,
            target_file_size,
            "delete",
            delete_schema,
        )?;
        let equality_delete_writer_builder =
            EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config);

        let mut delete_writer = equality_delete_writer_builder.build(None).await?;
        delete_writer.write(record_batch).await?;
        delete_writer.close().await
    }

    /// Commits data files as a fast append transaction.
    ///
    /// Duplicate file checks are disabled because this destination writes new
    /// unique file names for every batch and avoids the extra metadata scan.
    async fn commit_append(
        &self,
        table: &iceberg::table::Table,
        data_files: Vec<DataFile>,
    ) -> Result<(), iceberg::Error> {
        if data_files.is_empty() {
            return Ok(());
        }

        let transaction = Transaction::new(table);
        let append_action =
            transaction.fast_append().with_check_duplicate(false).add_data_files(data_files);

        let updated_transaction = append_action.apply(transaction)?;

        let _updated_table = updated_transaction.commit(&*self.catalog).await?;

        Ok(())
    }

    /// Commits data and equality-delete files in a single row-delta snapshot.
    async fn commit_row_delta(
        &self,
        table_ident: TableIdent,
        data_files: Vec<DataFile>,
        delete_files: Vec<DataFile>,
    ) -> Result<(), iceberg::Error> {
        retry_with_backoff(
            RetryPolicy {
                max_retries: ROW_DELTA_COMMIT_RETRIES,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(1),
            },
            |error: &iceberg::Error| {
                if error.retryable() { RetryDecision::Retry } else { RetryDecision::Stop }
            },
            |delay| delay,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying iceberg row-delta commit"
                );
            },
            || {
                let catalog = Arc::clone(&self.catalog);
                let rest_commit = self.rest_commit.clone();
                let table_ident = table_ident.clone();
                let data_files = data_files.clone();
                let delete_files = delete_files.clone();

                async move {
                    let table = catalog.load_table(&table_ident).await?;
                    let commit = build_row_delta_commit(&table, data_files, delete_files).await?;
                    rest_commit.commit_table(&table_ident, commit).await
                }
            },
        )
        .await
        .map_err(|failure| failure.last_error)
    }
}

/// Request body needed for a REST table metadata commit.
struct RestTableCommit {
    /// Requirements that must still hold at commit time.
    requirements: Vec<TableRequirement>,
    /// Table metadata updates to apply.
    updates: Vec<TableUpdate>,
}

/// REST commit helper for table updates that iceberg-rust can represent
/// in metadata but does not yet expose as public transaction actions.
#[derive(Debug, Clone)]
struct RestCommitClient {
    /// REST catalog base URI.
    catalog_uri: String,
    /// Optional warehouse query value.
    warehouse: Option<String>,
    /// Catalog properties used for auth headers and runtime config.
    props: HashMap<String, String>,
    /// HTTP client.
    client: Client,
}

/// Runtime catalog config returned by the REST catalog.
#[derive(Debug, Deserialize)]
struct CatalogConfigResponse {
    /// Server-side property overrides.
    overrides: HashMap<String, String>,
    /// Server-side property defaults.
    defaults: HashMap<String, String>,
}

/// Resolved REST catalog config.
#[derive(Debug)]
struct ResolvedRestConfig {
    /// Effective catalog URI.
    catalog_uri: String,
    /// Effective REST catalog properties.
    props: HashMap<String, String>,
}

impl ResolvedRestConfig {
    /// Builds the REST table endpoint for a table.
    fn table_endpoint(&self, table: &TableIdent) -> Result<String, iceberg::Error> {
        let mut url = Url::parse(self.catalog_uri.trim_end_matches('/')).map_err(|error| {
            iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "Invalid Iceberg REST catalog URI")
                .with_context("uri", self.catalog_uri.clone())
                .with_source(error)
        })?;

        {
            let mut path = url.path_segments_mut().map_err(|()| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "Iceberg REST catalog URI cannot be used as a base URL",
                )
                .with_context("uri", self.catalog_uri.clone())
            })?;

            path.push(REST_API_ROOT);
            if let Some(prefix) = self.props.get("prefix") {
                for segment in
                    prefix.trim_matches('/').split('/').filter(|segment| !segment.is_empty())
                {
                    path.push(segment);
                }
            }
            path.extend(["namespaces", &table.namespace.to_url_string(), "tables", &table.name]);
        }

        Ok(url.into())
    }
}

/// OAuth token response from REST catalogs.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    /// Access token.
    access_token: String,
}

impl RestCommitClient {
    /// Creates a new [`RestCommitClient`] from REST catalog properties.
    fn from_props(mut props: HashMap<String, String>) -> Self {
        let catalog_uri = props.remove(REST_CATALOG_PROP_URI).unwrap_or_default();
        let warehouse = props.remove(REST_CATALOG_PROP_WAREHOUSE);

        RestCommitClient { catalog_uri, warehouse, props, client: Client::new() }
    }

    /// Commits table updates through the Iceberg REST API.
    async fn commit_table(
        &self,
        table_ident: &TableIdent,
        commit: RestTableCommit,
    ) -> Result<(), iceberg::Error> {
        let config = self.load_config().await?;
        let url = config.table_endpoint(table_ident)?;
        let body = CommitTableRequest {
            identifier: Some(table_ident.clone()),
            requirements: commit.requirements,
            updates: commit.updates,
        };

        let response = self
            .request_with_props(Method::POST, url, &config.props)
            .await?
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                iceberg::Error::new(iceberg::ErrorKind::Unexpected, "Iceberg REST commit failed")
                    .with_source(error)
            })?;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => Err(iceberg::Error::new(
                iceberg::ErrorKind::CatalogCommitConflicts,
                "Catalog commit conflict while applying Iceberg table update",
            )
            .with_retryable(true)),
            StatusCode::NOT_FOUND => Err(iceberg::Error::new(
                iceberg::ErrorKind::TableNotFound,
                "Tried to update an Iceberg table that does not exist",
            )),
            status => Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Received unexpected response from Iceberg REST commit",
            )
            .with_context("status", status.to_string())),
        }
    }

    /// Loads runtime REST catalog config and merges it with user config.
    async fn load_config(&self) -> Result<ResolvedRestConfig, iceberg::Error> {
        let mut request =
            self.request_with_props(Method::GET, self.config_endpoint(), &self.props).await?;
        if let Some(warehouse) = &self.warehouse {
            request = request.query(&[("warehouse", warehouse)]);
        }

        let response = request.send().await.map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Iceberg REST config request failed",
            )
            .with_source(error)
        })?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Received unexpected response from Iceberg REST config",
            )
            .with_context("status", status.to_string()));
        }

        let config = response.json::<CatalogConfigResponse>().await.map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to parse Iceberg REST config",
            )
            .with_source(error)
        })?;

        let mut props = config.defaults;
        props.extend(self.props.clone());
        props.extend(config.overrides);

        let catalog_uri =
            props.get(REST_CATALOG_PROP_URI).cloned().unwrap_or_else(|| self.catalog_uri.clone());

        Ok(ResolvedRestConfig { catalog_uri, props })
    }

    /// Creates an authenticated request builder from catalog properties.
    async fn request_with_props(
        &self,
        method: Method,
        url: String,
        props: &HashMap<String, String>,
    ) -> Result<reqwest::RequestBuilder, iceberg::Error> {
        let mut headers = self.headers(props)?;

        if props.get("rest.sigv4-enabled").is_some_and(|value| value == "true") {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Iceberg REST SigV4 authentication is not yet implemented by this destination",
            ));
        }

        if let Some(token) = self.bearer_token(props).await? {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        "Invalid Iceberg REST bearer token",
                    )
                    .with_source(error)
                })?,
            );
        }

        Ok(self.client.request(method, url).headers(headers))
    }

    /// Returns the REST config endpoint.
    fn config_endpoint(&self) -> String {
        [self.catalog_uri.trim_end_matches('/'), REST_API_ROOT, "config"].join("/")
    }

    /// Builds default and user-provided REST headers.
    fn headers(&self, props: &HashMap<String, String>) -> Result<HeaderMap, iceberg::Error> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            HeaderName::from_static("x-client-version"),
            HeaderValue::from_static(ICEBERG_REST_CLIENT_VERSION),
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("etl-iceberg-writer"));

        for (key, value) in props
            .iter()
            .filter_map(|(key, value)| key.strip_prefix("header.").map(|key| (key, value)))
        {
            headers.insert(
                HeaderName::from_bytes(key.as_bytes()).map_err(|error| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        "Invalid Iceberg REST header name",
                    )
                    .with_source(error)
                })?,
                HeaderValue::from_str(value).map_err(|error| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        "Invalid Iceberg REST header value",
                    )
                    .with_source(error)
                })?,
            );
        }

        Ok(headers)
    }

    /// Returns a bearer token, exchanging OAuth credentials when necessary.
    async fn bearer_token(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<Option<String>, iceberg::Error> {
        if let Some(token) = props.get(CATALOG_TOKEN) {
            return Ok(Some(token.clone()));
        }

        let Some(credential) = props.get("credential") else {
            return Ok(None);
        };

        let (client_id, client_secret) = match credential.split_once(':') {
            Some((client_id, client_secret)) => (Some(client_id), client_secret),
            None => (None, credential.as_str()),
        };

        let mut params = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("client_secret", client_secret);
        params.insert("scope", props.get("scope").map_or("catalog", String::as_str));
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }

        let token_endpoint = props.get("oauth2-server-uri").cloned().unwrap_or_else(|| {
            [self.catalog_uri.trim_end_matches('/'), REST_API_ROOT, "oauth", "tokens"].join("/")
        });

        let response = self
            .client
            .post(token_endpoint)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .form(&params)
            .send()
            .await
            .map_err(|error| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Iceberg REST OAuth request failed",
                )
                .with_source(error)
            })?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Received unexpected response from Iceberg REST OAuth endpoint",
            )
            .with_context("status", status.to_string()));
        }

        let token = response.json::<TokenResponse>().await.map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to parse Iceberg REST OAuth response",
            )
            .with_source(error)
        })?;

        Ok(Some(token.access_token))
    }
}

/// Builds an Iceberg schema update commit when the desired schema differs.
fn build_schema_update_commit(
    table: &iceberg::table::Table,
    expected_current_schema: &IcebergSchema,
    desired_schema: IcebergSchema,
) -> Result<Option<RestTableCommit>, iceberg::Error> {
    let actual_current_schema = table.metadata().current_schema().as_ref();

    if schema_matches(actual_current_schema, &desired_schema) {
        return Ok(None);
    }

    if !schema_matches(actual_current_schema, expected_current_schema) {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Iceberg table schema does not match applied ETL metadata",
        )
        .with_context("table", table.identifier().to_string())
        .with_context("schema_id", table.metadata().current_schema_id().to_string()));
    }

    let updates = vec![
        TableUpdate::AddSchema { schema: desired_schema },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
    ];
    let requirements = vec![
        TableRequirement::UuidMatch { uuid: table.metadata().uuid() },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table.metadata().current_schema_id(),
        },
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: table.metadata().last_column_id(),
        },
    ];

    Ok(Some(RestTableCommit { requirements, updates }))
}

/// Validates that an ETL schema diff is safe for Iceberg.
fn validate_postgres_schema_evolution(
    current_schema: &IcebergSchema,
    desired_schema: &IcebergSchema,
    diff: &SchemaDiff,
) -> Result<(), iceberg::Error> {
    for column in &diff.columns_to_add {
        if !column.nullable {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Iceberg schema evolution cannot add a required field without defaults",
            )
            .with_context("field", column.name.clone()));
        }
    }

    validate_schema_evolution(current_schema, desired_schema)
}

/// Returns whether two schemas are equivalent for writer purposes.
fn schema_matches(current_schema: &IcebergSchema, desired_schema: &IcebergSchema) -> bool {
    current_schema.as_struct() == desired_schema.as_struct()
        && identifier_field_ids(current_schema) == identifier_field_ids(desired_schema)
}

/// Validates that an Iceberg schema change is safe for the destination.
fn validate_schema_evolution(
    current_schema: &IcebergSchema,
    desired_schema: &IcebergSchema,
) -> Result<(), iceberg::Error> {
    let current_identifier_ids = identifier_field_ids(current_schema);
    let desired_identifier_ids = identifier_field_ids(desired_schema);
    if current_identifier_ids != desired_identifier_ids {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Iceberg schema evolution cannot change identifier fields",
        ));
    }

    for desired_field in desired_schema.as_struct().fields() {
        let Some(current_field) = current_schema.field_by_id(desired_field.id) else {
            if desired_field.required {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    "Iceberg schema evolution cannot add a required field without defaults",
                )
                .with_context("field", desired_field.name.clone()));
            }

            continue;
        };

        validate_field_evolution(current_field, desired_field)?;
    }

    Ok(())
}

/// Returns a schema's identifier field IDs.
fn identifier_field_ids(schema: &IcebergSchema) -> HashSet<i32> {
    schema.identifier_field_ids().collect()
}

/// Returns identifier field IDs in top-level schema order.
fn identifier_field_ids_in_schema_order(schema: &IcebergSchema) -> Vec<i32> {
    let identifier_ids = identifier_field_ids(schema);

    schema
        .as_struct()
        .fields()
        .iter()
        .filter_map(|field| identifier_ids.contains(&field.id).then_some(field.id))
        .collect()
}

/// Returns top-level Arrow field positions keyed by Iceberg field ID.
fn arrow_field_positions_by_id(schema: &ArrowSchema) -> EtlResult<HashMap<i32, usize>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| arrow_field_id(field).map(|field_id| (field_id, index)))
        .collect()
}

/// Returns the Iceberg field ID stored in Arrow field metadata.
fn arrow_field_id(field: &Field) -> EtlResult<i32> {
    let Some(field_id) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Err(etl_error!(
            etl::error::ErrorKind::InvalidState,
            "Iceberg Arrow field is missing field ID metadata",
            format!("Field '{}' has no Iceberg field ID metadata.", field.name())
        ));
    };

    field_id.parse().map_err(|error| {
        etl_error!(
            etl::error::ErrorKind::InvalidState,
            "Iceberg Arrow field ID metadata is invalid",
            format!("Field '{}' has invalid field ID metadata.", field.name()),
            source: error
        )
    })
}

/// Validates evolution for a field that exists in both schemas.
fn validate_field_evolution(
    current_field: &NestedFieldRef,
    desired_field: &NestedFieldRef,
) -> Result<(), iceberg::Error> {
    if !current_field.required && desired_field.required {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Iceberg schema evolution cannot make an optional field required",
        )
        .with_context("field", desired_field.name.clone()));
    }

    if !is_supported_type_evolution(&current_field.field_type, &desired_field.field_type) {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Iceberg schema evolution does not support this type change",
        )
        .with_context("field", desired_field.name.clone())
        .with_context("field_id", desired_field.id.to_string()));
    }

    Ok(())
}

/// Returns whether an Iceberg type change is spec-compatible for this writer.
fn is_supported_type_evolution(current_type: &IcebergType, desired_type: &IcebergType) -> bool {
    if current_type == desired_type {
        return true;
    }

    match (current_type, desired_type) {
        (
            IcebergType::Primitive(PrimitiveType::Int),
            IcebergType::Primitive(PrimitiveType::Long),
        )
        | (
            IcebergType::Primitive(PrimitiveType::Float),
            IcebergType::Primitive(PrimitiveType::Double),
        ) => true,
        (IcebergType::List(current_list), IcebergType::List(desired_list)) => {
            current_list.element_field.id == desired_list.element_field.id
                && (current_list.element_field.required || !desired_list.element_field.required)
                && is_supported_type_evolution(
                    &current_list.element_field.field_type,
                    &desired_list.element_field.field_type,
                )
        }
        _ => false,
    }
}

/// Builds and writes a row-delta Iceberg metadata commit.
async fn build_row_delta_commit(
    table: &iceberg::table::Table,
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
) -> Result<RestTableCommit, iceberg::Error> {
    if data_files.is_empty() && delete_files.is_empty() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "Cannot commit an empty Iceberg row delta",
        ));
    }

    let operation = if data_files.is_empty() { Operation::Delete } else { Operation::Overwrite };
    let summary = build_snapshot_summary(table, operation, &data_files, &delete_files);
    let snapshot_id = generate_snapshot_id(table);
    let commit_uuid = uuid::Uuid::new_v4();
    let mut manifest_counter = 0_u64;
    let mut manifests = existing_manifests(table).await?;

    if !data_files.is_empty() {
        let manifest = write_manifest(
            table,
            snapshot_id,
            commit_uuid,
            &mut manifest_counter,
            ManifestContentType::Data,
            data_files,
        )
        .await?;
        manifests.push(manifest);
    }

    if !delete_files.is_empty() {
        let manifest = write_manifest(
            table,
            snapshot_id,
            commit_uuid,
            &mut manifest_counter,
            ManifestContentType::Deletes,
            delete_files,
        )
        .await?;
        manifests.push(manifest);
    }

    let manifest_list_path =
        write_manifest_list(table, snapshot_id, commit_uuid, manifests).await?;
    let snapshot = build_snapshot(table, snapshot_id, manifest_list_path, summary);

    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_owned(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];
    let requirements = vec![
        TableRequirement::UuidMatch { uuid: table.metadata().uuid() },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_owned(),
            snapshot_id: table.metadata().current_snapshot_id(),
        },
    ];

    Ok(RestTableCommit { requirements, updates })
}

/// Returns the currently live manifests for the table.
async fn existing_manifests(
    table: &iceberg::table::Table,
) -> Result<Vec<ManifestFile>, iceberg::Error> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };

    let manifest_list = snapshot.load_manifest_list(table.file_io(), table.metadata()).await?;
    let manifests = manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.has_added_files() || manifest.has_existing_files())
        .cloned()
        .collect();

    Ok(manifests)
}

/// Writes a manifest file containing added data or delete files.
async fn write_manifest(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    commit_uuid: uuid::Uuid,
    manifest_counter: &mut u64,
    content: ManifestContentType,
    files: Vec<DataFile>,
) -> Result<ManifestFile, iceberg::Error> {
    let mut writer =
        new_manifest_writer(table, snapshot_id, commit_uuid, manifest_counter, content)?;
    for file in files {
        writer.add_file(file, -1)?;
    }
    writer.write_manifest_file().await
}

/// Creates a manifest writer for the table format version.
fn new_manifest_writer(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    commit_uuid: uuid::Uuid,
    manifest_counter: &mut u64,
    content: ManifestContentType,
) -> Result<ManifestWriter, iceberg::Error> {
    let manifest_path = format!(
        "{}/{}/{}-m{}.{}",
        table.metadata().location(),
        METADATA_DIRECTORY,
        commit_uuid,
        *manifest_counter,
        DataFileFormat::Avro,
    );
    *manifest_counter += 1;

    let output_file = table.file_io().new_output(manifest_path)?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(snapshot_id),
        None,
        Arc::clone(table.metadata().current_schema()),
        table.metadata().default_partition_spec().as_ref().clone(),
    );

    match (table.metadata().format_version(), content) {
        (FormatVersion::V1, _) => Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Iceberg row-level deletes require table format version 2 or newer",
        )),
        (FormatVersion::V2, ManifestContentType::Data) => Ok(builder.build_v2_data()),
        (FormatVersion::V2, ManifestContentType::Deletes) => Ok(builder.build_v2_deletes()),
        (FormatVersion::V3, ManifestContentType::Data) => Ok(builder.build_v3_data()),
        (FormatVersion::V3, ManifestContentType::Deletes) => Ok(builder.build_v3_deletes()),
    }
}

/// Writes the manifest list for a new snapshot.
async fn write_manifest_list(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    commit_uuid: uuid::Uuid,
    manifests: Vec<ManifestFile>,
) -> Result<String, iceberg::Error> {
    let manifest_list_path = format!(
        "{}/{}/snap-{}-0-{}.{}",
        table.metadata().location(),
        METADATA_DIRECTORY,
        snapshot_id,
        commit_uuid,
        DataFileFormat::Avro,
    );

    let next_sequence_number = table.metadata().next_sequence_number();
    let mut writer = match table.metadata().format_version() {
        FormatVersion::V1 => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Iceberg row-level deletes require table format version 2 or newer",
            ));
        }
        FormatVersion::V2 => ManifestListWriter::v2(
            table.file_io().new_output(manifest_list_path.clone())?,
            snapshot_id,
            table.metadata().current_snapshot_id(),
            next_sequence_number,
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            table.file_io().new_output(manifest_list_path.clone())?,
            snapshot_id,
            table.metadata().current_snapshot_id(),
            next_sequence_number,
            Some(table.metadata().next_row_id()),
        ),
    };

    writer.add_manifests(manifests.into_iter())?;
    writer.close().await?;

    Ok(manifest_list_path)
}

/// Builds a snapshot summary for a row-delta commit.
fn build_snapshot_summary(
    table: &iceberg::table::Table,
    operation: Operation,
    data_files: &[DataFile],
    delete_files: &[DataFile],
) -> Summary {
    let mut collector = SnapshotSummaryCollector::default();
    let schema = Arc::clone(table.metadata().current_schema());
    let partition_spec = Arc::clone(table.metadata().default_partition_spec());

    for file in data_files.iter().chain(delete_files) {
        collector.add_file(file, Arc::clone(&schema), Arc::clone(&partition_spec));
    }

    Summary { operation, additional_properties: collector.build() }
}

/// Builds the snapshot metadata for a row-delta commit.
fn build_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    manifest_list_path: String,
    summary: Summary,
) -> Snapshot {
    Snapshot::builder()
        .with_manifest_list(manifest_list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(table.metadata().next_sequence_number())
        .with_summary(summary)
        .with_schema_id(table.metadata().current_schema_id())
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .build()
}

/// Generates a snapshot ID that is not already used by the table.
fn generate_snapshot_id(table: &iceberg::table::Table) -> i64 {
    loop {
        let (lhs, rhs) = uuid::Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        let snapshot_id = if snapshot_id < 0 { -snapshot_id } else { snapshot_id };

        if table.metadata().snapshot_by_id(snapshot_id).is_none() {
            return snapshot_id;
        }
    }
}

/// Creates a rolling Parquet writer builder for data or delete files.
fn parquet_rolling_writer_builder(
    table: &iceberg::table::Table,
    writer_props: WriterProperties,
    target_file_size: usize,
    prefix: &str,
    schema: SchemaRef,
) -> Result<
    RollingFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    iceberg::Error,
> {
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        prefix.to_owned(),
        Some(uuid::Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, schema);

    Ok(RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    ))
}

/// Returns the target equality-delete file size for a table.
fn table_delete_target_file_size(table: &iceberg::table::Table) -> Result<usize, iceberg::Error> {
    let Some(value) = table.metadata().properties().get(WRITE_DELETE_TARGET_FILE_SIZE_BYTES) else {
        return Ok(WRITE_DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    };

    value.parse().map_err(|error| {
        iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Invalid Iceberg delete target file size table property",
        )
        .with_context("property", WRITE_DELETE_TARGET_FILE_SIZE_BYTES)
        .with_context("value", value.clone())
        .with_source(error)
    })
}

/// Returns the total file size for a list of Iceberg data files.
fn total_file_size(files: &[DataFile]) -> u64 {
    files.iter().map(DataFile::file_size_in_bytes).sum()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::{NamespaceIdent, TableIdent};

    use crate::iceberg::client::ResolvedRestConfig;

    #[test]
    fn rest_table_endpoint_encodes_table_name_path_segment() {
        let config = ResolvedRestConfig {
            catalog_uri: "http://localhost:8181/catalog/".to_owned(),
            props: HashMap::new(),
        };
        let table = TableIdent::new(
            NamespaceIdent::new("public".to_owned()),
            "quoted/table ? name".to_owned(),
        );

        let endpoint = config.table_endpoint(&table).unwrap();

        assert_eq!(
            endpoint,
            "http://localhost:8181/catalog/v1/namespaces/public/tables/quoted%2Ftable%20%3F%20name"
        );
    }

    #[test]
    fn rest_table_endpoint_encodes_namespace_and_prefix_path_segments() {
        let config = ResolvedRestConfig {
            catalog_uri: "http://localhost:8181/catalog".to_owned(),
            props: HashMap::from([("prefix".to_owned(), "/warehouse/main/".to_owned())]),
        };
        let table = TableIdent::new(
            NamespaceIdent::from_strs(["tenant one", "public"]).unwrap(),
            "café".to_owned(),
        );

        let endpoint = config.table_endpoint(&table).unwrap();

        assert_eq!(
            endpoint,
            "http://localhost:8181/catalog/v1/warehouse/main/namespaces/tenant%20one%1Fpublic/tables/caf%C3%A9"
        );
    }
}
