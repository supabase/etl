use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use etl::{
    error::EtlResult,
    types::{TableRow, TableSchema},
};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    arrow::arrow_schema_to_schema,
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY},
    table::Table,
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
        },
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::{basic::Compression, file::properties::WriterProperties};

use crate::iceberg::{
    encoding::rows_to_record_batch,
    error::{arrow_error_to_etl_error, iceberg_error_to_etl_error},
    schema::postgres_to_iceberg_schema,
};

/// Client for connecting to Iceberg data lakes.
#[derive(Debug, Clone)]
pub struct IcebergClient {
    catalog: Arc<dyn Catalog>,
}

impl IcebergClient {
    /// Creates a new [IcebergClient] from a REST catalog URI and a warehouse name.
    pub fn new_with_rest_catalog(
        catalog_uri: String,
        warehouse_name: String,
        props: HashMap<String, String>,
    ) -> Self {
        let catalog_config = RestCatalogConfig::builder()
            .uri(catalog_uri)
            .warehouse(warehouse_name)
            .props(props)
            .build();
        let catalog = RestCatalog::new(catalog_config);
        IcebergClient {
            catalog: Arc::new(catalog),
        }
    }

    /// Creates a new [IcebergClient] from a REST catalog URI and a warehouse name.
    pub fn new_with_s3_and_rest_catalog(
        catalog_uri: String,
        warehouse_name: String,
        s3_endpoint: String,
        s3_access_key_id: String,
        s3_secret_access_key: String,
    ) -> Self {
        let mut props: HashMap<String, String> = HashMap::new();

        props.insert(S3_ACCESS_KEY_ID.to_string(), s3_access_key_id.to_string());
        props.insert(
            S3_SECRET_ACCESS_KEY.to_string(),
            s3_secret_access_key.to_string(),
        );
        props.insert(S3_ENDPOINT.to_string(), s3_endpoint.to_string());

        let catalog_config = RestCatalogConfig::builder()
            .uri(catalog_uri)
            .warehouse(warehouse_name)
            .props(props)
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

    /// Load a table
    pub async fn load_table(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<iceberg::table::Table, iceberg::Error> {
        let namespace_ident = NamespaceIdent::new(namespace);
        let table_ident = TableIdent::new(namespace_ident, table_name);
        self.catalog.load_table(&table_ident).await
    }

    /// Insert table rows into the table in the destination
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
        let mut append_action = transaction
            .fast_append(None, None, vec![])?
            .with_check_duplicate(false); // Don't check duplicates for performance
        append_action.add_data_files(data_files)?;

        // Apply the append action to create updated transaction
        let updated_transaction = append_action.apply().await?;

        // Commit the transaction to the catalog
        let _updated_table = updated_transaction.commit(&*self.catalog).await?;

        Ok(())
    }

    pub async fn write_equality_delete_file(
        &self,
        namespace: String,
        table_name: String,
        equality_ids: Vec<i32>,
        // table_id: TableId,
    ) -> EtlResult<()> {
        // let table_schema = self
        //     .store
        //     .get_table_schema(&table_id)
        //     .await?
        //     .ok_or_else(|| {
        //         etl_error!(
        //             ErrorKind::MissingTableSchema,
        //             "Table schema not found",
        //             format!("No schema found for table {table_id}")
        //         )
        //     })?;

        // let equality_ids = table_schema
        //     .column_schemas
        //     .iter()
        //     .enumerate()
        //     .filter_map(|(id, column_schema)| {
        //         if column_schema.primary {
        //             Some(id as i32)
        //         } else {
        //             None
        //         }
        //     })
        //     .collect();

        // let table_name = self
        //     .store
        //     .get_table_mapping(&table_id)
        //     .await?
        //     .ok_or_else(|| {
        //         etl_error!(
        //             ErrorKind::MissingTableMapping,
        //             "Table mapping not found",
        //             format!("The table mapping for table id {table_id} was not found")
        //         )
        //     })?;

        let table = self
            .load_table(namespace.clone(), table_name)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        let table_schema = table.metadata().current_schema();

        let config = EqualityDeleteWriterConfig::new(
            equality_ids,
            Arc::clone(table_schema),
            None,
            0, // TODO: use correct partition spec id
        )
        .map_err(iceberg_error_to_etl_error)?;

        let delete_arrow_schema = config.projected_arrow_schema_ref().clone();
        let delete_schema = arrow_schema_to_schema(&delete_arrow_schema).unwrap();

        // Create Parquet writer properties
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        // Create location and file name generators
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(iceberg_error_to_etl_error)?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "delete".to_string(),
            Some(uuid::Uuid::new_v4().to_string()), // Add unique UUID for each file
            iceberg::spec::DataFileFormat::Parquet,
        );

        // Create Parquet writer builder
        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            Arc::new(delete_schema),
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut _equality_delete_writer =
            EqualityDeleteFileWriterBuilder::new(parquet_writer_builder, config)
                .build()
                .await
                .map_err(iceberg_error_to_etl_error)?;

        Ok(())
    }
}
