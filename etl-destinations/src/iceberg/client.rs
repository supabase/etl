use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use etl::{
    error::EtlResult,
    types::{ColumnSchema, TableRow},
};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{
        PROPERTY_COMMIT_MAX_RETRY_WAIT_MS, PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
        PROPERTY_COMMIT_NUM_RETRIES, PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use parquet::{basic::Compression, file::properties::WriterProperties};
use tracing::debug;

use crate::iceberg::{
    catalog::{SupabaseCatalog, SupabaseClient},
    encoding::rows_to_record_batch,
    error::{arrow_error_to_etl_error, iceberg_error_to_etl_error},
    schema::postgres_to_iceberg_schema,
};

/// Authentication token key for catalog configuration.
const CATALOG_TOKEN: &str = "token";

/// Client for managing Apache Iceberg data lake operations.
///
/// This client provides a high-level interface for interacting with Iceberg catalogs,
/// supporting operations such as namespace and table management, data insertion, and
/// schema operations. It abstracts the underlying catalog implementation and provides
/// a unified interface for both REST catalogs and Supabase-specific configurations.
///
/// The client maintains a thread-safe reference to the catalog implementation,
/// allowing for concurrent operations across multiple threads.
#[derive(Debug, Clone)]
pub struct IcebergClient {
    /// The underlying Iceberg catalog implementation.
    catalog: Arc<dyn Catalog>,
}

impl IcebergClient {
    /// Creates a new [`IcebergClient`] using a REST catalog configuration.
    ///
    /// This constructor initializes a client that connects to an Iceberg catalog
    /// through the REST catalog protocol. The REST catalog provides a standardized
    /// HTTP-based interface for catalog operations.
    pub async fn new_with_rest_catalog(
        catalog_uri: String,
        warehouse_name: String,
        mut props: HashMap<String, String>,
    ) -> Result<Self, iceberg::Error> {
        props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri);
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_name);

        let builder = RestCatalogBuilder::default();
        let catalog = builder.load("RestCatalog", props).await?;

        Ok(IcebergClient {
            catalog: Arc::new(catalog),
        })
    }

    /// Creates a new [`IcebergClient`] configured for Supabase storage integration.
    ///
    /// This constructor creates a client specifically configured to work with Supabase's
    /// storage service, automatically setting up the necessary S3-compatible endpoints
    /// and authentication parameters. The client uses Supabase's REST catalog implementation
    /// with additional custom behavior for Supabase-specific operations.
    ///
    /// The method automatically constructs the catalog URI and S3 endpoint from the
    /// provided project reference and domain, simplifying the configuration process
    /// for Supabase users.
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
        props.insert(CATALOG_TOKEN.to_string(), catalog_token.clone());
        props.insert(S3_ACCESS_KEY_ID.to_string(), s3_access_key_id);
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), s3_secret_access_key);
        props.insert(S3_ENDPOINT.to_string(), s3_endpoint);
        props.insert(S3_REGION.to_string(), s3_region);
        props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.clone());
        props.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse_name.clone(),
        );

        let builder = RestCatalogBuilder::default();
        let inner = builder.load("SupabaseCatalog", props).await?;
        let client = SupabaseClient::new(catalog_uri, warehouse_name, catalog_token);
        let catalog = SupabaseCatalog::new(inner, client);

        Ok(IcebergClient {
            catalog: Arc::new(catalog),
        })
    }

    /// Creates a namespace if it does not already exist.
    ///
    /// This method performs an idempotent namespace creation operation. It first
    /// checks if the namespace exists and only creates it if it's missing. The
    /// namespace string can use dot notation for hierarchical namespaces
    /// (e.g., "warehouse.schema").
    pub async fn create_namespace_if_missing(&self, namespace: &str) -> Result<(), iceberg::Error> {
        debug!("creating namespace {namespace}");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?;
        }

        Ok(())
    }

    /// Checks whether a namespace exists in the catalog.
    ///
    /// This method queries the catalog to determine if the specified namespace
    /// is present. The namespace string supports dot notation for hierarchical
    /// namespaces.
    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool, iceberg::Error> {
        debug!("checking if namespace {namespace} exists");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.namespace_exists(&namespace_ident).await
    }

    /// Creates a table if it does not already exist.
    ///
    /// This method performs an idempotent table creation operation. It checks
    /// if the table exists within the specified namespace and creates it if missing.
    /// The table schema is derived from the provided column schemas, which are
    /// automatically converted from PostgreSQL types to Iceberg schema format.
    ///
    /// The created table includes default commit properties for retry behavior
    /// and transaction management.
    pub async fn create_table_if_missing(
        &self,
        namespace: &str,
        table_name: String,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), iceberg::Error> {
        debug!("creating table {table_name} in namespace {namespace} if missing");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());
        if !self.catalog.table_exists(&table_ident).await? {
            let iceberg_schema = postgres_to_iceberg_schema(column_schemas)?;
            let creation = TableCreation::builder()
                .name(table_name)
                .schema(iceberg_schema)
                .properties(Self::get_table_properties())
                .build();
            self.catalog
                .create_table(&namespace_ident, creation)
                .await?;
        }
        Ok(())
    }

    /// Generates default table properties for commit behavior and retry configuration.
    ///
    /// This method returns a set of table properties that configure the commit
    /// behavior for Iceberg tables, including retry timeouts, retry counts, and
    /// total retry time limits. These properties help ensure reliable commits
    /// during etl operations.
    fn get_table_properties() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "100".to_string(),
        );
        props.insert(
            PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "10000".to_string(),
        );
        props.insert(PROPERTY_COMMIT_NUM_RETRIES.to_string(), "10".to_string());
        props.insert(
            PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS.to_string(),
            "1800000".to_string(),
        );
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
        debug!("checking if table {table_name} in namespace {namespace} exists");

        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.table_exists(&table_ident).await
    }

    /// Removes a table from the specified namespace if it exists.
    ///
    /// This method checks if the table exists before attempting to drop it,
    /// providing idempotent behavior. Returns `Ok(true)` if a table was dropped,
    /// `Ok(false)` if the table did not exist.
    pub async fn drop_table_if_exists(
        &self,
        namespace: &str,
        table_name: String,
    ) -> Result<bool, iceberg::Error> {
        debug!("dropping table {table_name} in namespace {namespace} if exists");

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
    /// This method permanently deletes the namespace from the catalog. The namespace
    /// must be empty (contain no tables) before it can be dropped.
    pub async fn drop_namespace(&self, namespace: &str) -> Result<(), iceberg::Error> {
        debug!("dropping namespace {namespace}");
        let namespace_ident = NamespaceIdent::from_strs(namespace.split('.'))?;
        self.catalog.drop_namespace(&namespace_ident).await
    }

    /// Loads a table from the catalog.
    ///
    /// This method retrieves the table metadata and returns a [`Table`] instance
    /// that can be used for further operations such as reading data, writing data,
    /// or inspecting the table schema and properties.
    pub async fn load_table(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<iceberg::table::Table, iceberg::Error> {
        debug!("loading table {table_name} in namespace {namespace}");
        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.load_table(&table_ident).await
    }

    /// Inserts rows into a table within the specified namespace.
    ///
    /// This method performs a batch insert operation, converting the provided
    /// table rows into Arrow RecordBatch format and writing them to the Iceberg
    /// table using Parquet format. The operation uses a fast append strategy
    /// without duplicate checking for optimal performance.
    pub async fn insert_rows(
        &self,
        namespace: String,
        table_name: String,
        table_rows: Vec<TableRow>,
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
        let record_batch =
            rows_to_record_batch(&table_rows, arrow_schema).map_err(arrow_error_to_etl_error)?;

        self.write_record_batch(&table, record_batch)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        Ok(())
    }

    /// Writes a RecordBatch to an Iceberg table using Parquet format.
    ///
    /// This method handles the low-level details of writing Arrow RecordBatch data
    /// to an Iceberg table. It creates the necessary writers, applies the data,
    /// and commits the transaction to make the data visible in the table.
    async fn write_record_batch(
        &self,
        table: &Table,
        record_batch: RecordBatch,
    ) -> Result<(), iceberg::Error> {
        // Create Parquet writer properties
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        // Create location and file name generators
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "data".to_string(),
            Some(uuid::Uuid::new_v4().to_string()), // Add unique UUID for each file
            iceberg::spec::DataFileFormat::Parquet,
        );

        // Create Parquet writer builder
        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            table.metadata().current_schema().clone(),
            None,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer with empty partition (unpartitioned table)
        let data_file_writer_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            None, // No partition value for unpartitioned tables
            table.metadata().default_partition_spec_id(),
        );

        // Build the writer
        let mut data_file_writer = data_file_writer_builder.build().await?;

        // Write the record batch using Iceberg writer
        data_file_writer.write(record_batch.clone()).await?;

        // Close writer and get data files
        let data_files = data_file_writer.close().await?;

        // Create transaction and fast append action
        let transaction = Transaction::new(table);
        let append_action = transaction
            .fast_append()
            .with_check_duplicate(false)
            .add_data_files(data_files); // Don't check duplicates for performance

        // Apply the append action to create updated transaction
        let updated_transaction = append_action.apply(transaction)?;

        // Commit the transaction to the catalog
        let _updated_table = updated_transaction.commit(&*self.catalog).await?;

        Ok(())
    }
}
