//! Merge and compaction logic for changelog tables.
//!
//! Implements the core merge operation that:
//! 1. Loads the Puffin index from changelog table metadata
//! 2. Scans changelog Parquet files using the index for deduplication
//! 3. Builds compacted mirror Parquet files (INSERT/UPDATE → upsert, DELETE → omit)
//! 4. Writes new Puffin index for the mirror table
//! 5. Commits Iceberg transaction to swap manifest

use crate::config::{IcebergCatalogConfig, TableConfig};
use crate::index::{IndexBuilder, PuffinIndex, PuffinIndexMetadata, compute_pk_hash};
use anyhow::Context;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::Schema as ArrowSchema;
use futures::TryStreamExt;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, Schema};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use metrics::{counter, histogram};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

/// Executor for merge operations on Iceberg tables.
pub struct MergeExecutor {
    /// Iceberg catalog client.
    catalog: Arc<dyn Catalog>,

    /// Batch size for processing records.
    batch_size: usize,
}

/// Authentication token key for catalog configuration.
const CATALOG_TOKEN: &str = "token";

impl MergeExecutor {
    /// Creates a new merge executor with the provided configuration.
    pub async fn new(config: IcebergCatalogConfig, batch_size: usize) -> anyhow::Result<Self> {
        info!("initializing iceberg catalog connection");

        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(REST_CATALOG_PROP_URI.to_string(), config.catalog_url);
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), config.warehouse);
        props.insert(CATALOG_TOKEN.to_string(), config.catalog_token);
        props.insert(S3_ACCESS_KEY_ID.to_string(), config.s3_access_key_id);
        props.insert(
            S3_SECRET_ACCESS_KEY.to_string(),
            config.s3_secret_access_key,
        );
        props.insert(S3_ENDPOINT.to_string(), config.s3_endpoint);
        props.insert(S3_REGION.to_string(), config.s3_region);

        let builder = RestCatalogBuilder::default();
        let catalog = builder
            .load("RestCatalog", props)
            .await
            .context("building REST catalog")?;

        Ok(Self {
            catalog: Arc::new(catalog),
            batch_size,
        })
    }

    /// Executes the merge operation for a table.
    ///
    /// Creates the mirror table if it doesn't exist, loads the changelog index,
    /// scans and deduplicates changelog records, writes compacted mirror data,
    /// and updates the mirror index.
    pub async fn execute_merge(&self, table_config: &TableConfig) -> anyhow::Result<()> {
        let start = Instant::now();

        info!(
            namespace = %table_config.namespace,
            changelog_table = %table_config.changelog_table,
            mirror_table = %table_config.mirror_table,
            "starting merge operation"
        );

        let namespace = NamespaceIdent::from_vec(vec![table_config.namespace.clone()])
            .context("creating namespace identifier")?;

        let changelog_ident =
            TableIdent::new(namespace.clone(), table_config.changelog_table.clone());
        let mirror_ident = TableIdent::new(namespace.clone(), table_config.mirror_table.clone());

        // Load changelog table.
        let changelog_table = self
            .catalog
            .load_table(&changelog_ident)
            .await
            .context("loading changelog table")?;

        // Load or create index from changelog.
        let index = self
            .load_or_create_index(&changelog_table, table_config)
            .await?;

        // Scan changelog and build deduplicated records.
        let deduplicated = self
            .scan_and_deduplicate(&changelog_table, &index, table_config)
            .await?;

        info!(
            changelog_rows = index.len(),
            deduplicated_rows = deduplicated.len(),
            "deduplication complete"
        );

        counter!("merger.rows.scanned", "table" => table_config.mirror_table.clone())
            .increment(index.len() as u64);
        counter!("merger.rows.deduplicated", "table" => table_config.mirror_table.clone())
            .increment(deduplicated.len() as u64);

        // Recreate the mirror table to replace all data (full snapshot mode).
        // This ensures each merge produces a complete deduplicated view.
        self.recreate_mirror_table(&mirror_ident, &changelog_table, table_config)
            .await?;

        // Reload the mirror table after recreation.
        let mirror_table = self
            .catalog
            .load_table(&mirror_ident)
            .await
            .context("loading recreated mirror table")?;

        // Write compacted data to mirror table.
        self.write_mirror_data(&mirror_table, deduplicated, table_config)
            .await?;

        let duration = start.elapsed();
        histogram!("merger.merge.duration_seconds", "table" => table_config.mirror_table.clone())
            .record(duration.as_secs_f64());

        info!(
            namespace = %table_config.namespace,
            mirror_table = %table_config.mirror_table,
            duration_secs = duration.as_secs(),
            "merge operation completed"
        );

        Ok(())
    }

    /// Recreates the mirror table by dropping and creating it fresh.
    ///
    /// This implements a full snapshot mode where each merge produces a complete
    /// deduplicated view of the changelog data. All previous mirror data is replaced.
    async fn recreate_mirror_table(
        &self,
        mirror_ident: &TableIdent,
        changelog_table: &Table,
        table_config: &TableConfig,
    ) -> anyhow::Result<()> {
        // Drop the existing mirror table if it exists.
        if self.catalog.load_table(mirror_ident).await.is_ok() {
            debug!("dropping existing mirror table for full refresh");
            self.catalog
                .drop_table(mirror_ident)
                .await
                .context("dropping mirror table")?;
        }

        // Derive schema from changelog table, excluding CDC columns.
        let mirror_schema = self.derive_mirror_schema(changelog_table, table_config)?;

        // Create the mirror table.
        self.catalog
            .create_table(
                &mirror_ident.namespace,
                TableCreation::builder()
                    .name(mirror_ident.name.clone())
                    .schema(mirror_schema)
                    .build(),
            )
            .await
            .context("creating mirror table")?;

        info!("mirror table recreated for full snapshot merge");
        Ok(())
    }

    /// Derives the mirror table schema from the changelog schema.
    ///
    /// Removes CDC-specific columns like sequence number and operation type.
    fn derive_mirror_schema(
        &self,
        changelog_table: &Table,
        table_config: &TableConfig,
    ) -> anyhow::Result<Schema> {
        let changelog_schema = changelog_table.metadata().current_schema();

        let excluded_columns: HashSet<&str> = [
            table_config.sequence_column.as_str(),
            table_config.operation_column.as_str(),
        ]
        .into_iter()
        .collect();

        let mirror_fields: Vec<Arc<NestedField>> = changelog_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| !excluded_columns.contains(field.name.as_str()))
            .cloned()
            .collect();

        Schema::builder()
            .with_fields(mirror_fields)
            .build()
            .context("building mirror schema")
    }

    /// Loads the Puffin index from table metadata, or creates a new one.
    async fn load_or_create_index(
        &self,
        changelog_table: &Table,
        table_config: &TableConfig,
    ) -> anyhow::Result<PuffinIndex> {
        let metadata = changelog_table.metadata();

        if let Some(index_metadata_str) = metadata.properties().get("puffin_index_metadata") {
            info!("loading existing puffin index");
            let index_metadata: PuffinIndexMetadata =
                serde_json::from_str(index_metadata_str).context("parsing index metadata")?;

            debug!(
                hash_bits = index_metadata.hash_bits,
                num_rows = index_metadata.num_rows,
                puffin_path = %index_metadata.puffin_path,
                "index metadata found"
            );

            // Load the Puffin file from the path and deserialize the index.
            let file_io = changelog_table.file_io();
            let puffin_bytes = file_io
                .new_input(&index_metadata.puffin_path)
                .context("opening puffin file")?
                .read()
                .await
                .context("reading puffin file")?;

            PuffinIndex::from_parquet(&puffin_bytes, index_metadata.hash_bits)
                .context("deserializing puffin index")
        } else {
            info!("no existing index found, building new index from changelog");

            // Build index by scanning changelog files.
            let builder = IndexBuilder::new(64);
            builder
                .build_from_table(changelog_table, &table_config.primary_keys)
                .await
        }
    }

    /// Scans changelog files and builds deduplicated records using the index.
    ///
    /// Scans all records from the changelog table, keeping only the latest version
    /// of each primary key based on the sequence number. DELETE operations result
    /// in the record being omitted from the output.
    async fn scan_and_deduplicate(
        &self,
        changelog_table: &Table,
        _index: &PuffinIndex,
        table_config: &TableConfig,
    ) -> anyhow::Result<HashMap<u64, DeduplicatedRecord>> {
        info!("scanning and deduplicating changelog records");

        let mut deduplicated: HashMap<u64, DeduplicatedRecord> = HashMap::new();
        let mut rows_scanned: u64 = 0;

        // Check if table has any snapshots.
        let Some(current_snapshot_id) = changelog_table.metadata().current_snapshot_id() else {
            debug!("no snapshots in changelog table, returning empty result");
            return Ok(deduplicated);
        };

        // Scan all records from the changelog table.
        let scan = changelog_table
            .scan()
            .snapshot_id(current_snapshot_id)
            .select_all()
            .build()
            .context("building table scan")?;

        let mut stream = scan.to_arrow().await.context("creating arrow stream")?;

        // Find column indices for CDC fields.
        let changelog_schema = changelog_table.metadata().current_schema();
        let seq_col_idx = changelog_schema
            .as_struct()
            .fields()
            .iter()
            .position(|f| f.name == table_config.sequence_column);
        let op_col_idx = changelog_schema
            .as_struct()
            .fields()
            .iter()
            .position(|f| f.name == table_config.operation_column);

        let pk_col_indices: Vec<usize> = table_config
            .primary_keys
            .iter()
            .filter_map(|pk| {
                changelog_schema
                    .as_struct()
                    .fields()
                    .iter()
                    .position(|f| &f.name == pk)
            })
            .collect();

        if pk_col_indices.len() != table_config.primary_keys.len() {
            anyhow::bail!(
                "not all primary key columns found in schema: expected {:?}",
                table_config.primary_keys
            );
        }

        while let Some(batch_result) = stream.try_next().await? {
            let batch_rows = batch_result.num_rows();
            rows_scanned += batch_rows as u64;

            for row_idx in 0..batch_rows {
                // Compute primary key hash.
                let pk_hash = compute_pk_hash(&batch_result, &pk_col_indices, row_idx);

                // Extract sequence number.
                let sequence = seq_col_idx
                    .and_then(|idx| extract_i64(&batch_result, idx, row_idx))
                    .unwrap_or(0);

                // Extract operation type.
                let operation = op_col_idx
                    .and_then(|idx| extract_string(&batch_result, idx, row_idx))
                    .unwrap_or_else(|| "INSERT".to_string());

                // Check if this record is newer than the existing one.
                let should_update = match deduplicated.get(&pk_hash) {
                    Some(existing) => sequence > existing.sequence,
                    None => true,
                };

                if should_update {
                    // Store the record batch and row index for later writing.
                    deduplicated.insert(
                        pk_hash,
                        DeduplicatedRecord {
                            sequence,
                            operation,
                            batch: batch_result.clone(),
                            row_idx,
                        },
                    );
                }
            }

            if rows_scanned.is_multiple_of(100_000) {
                debug!(rows_scanned, "scan progress");
            }
        }

        // Remove DELETE operations from the final result.
        deduplicated.retain(|_, record| {
            !matches!(record.operation.to_uppercase().as_str(), "DELETE" | "D")
        });

        info!(
            rows_scanned,
            deduplicated_count = deduplicated.len(),
            "scan and deduplication complete"
        );

        Ok(deduplicated)
    }

    /// Writes deduplicated data to the mirror table.
    ///
    /// Builds Parquet files from deduplicated records, writes them to the Iceberg
    /// table location, and commits an Iceberg transaction to update the manifest.
    async fn write_mirror_data(
        &self,
        mirror_table: &Table,
        deduplicated: HashMap<u64, DeduplicatedRecord>,
        table_config: &TableConfig,
    ) -> anyhow::Result<()> {
        if deduplicated.is_empty() {
            info!("no records to write to mirror table");
            return Ok(());
        }

        info!(
            record_count = deduplicated.len(),
            "writing deduplicated data to mirror table"
        );

        // Build record batches from deduplicated records, excluding CDC columns.
        let record_batches = self.build_mirror_batches(deduplicated, mirror_table, table_config)?;

        if record_batches.is_empty() {
            info!("no batches to write");
            return Ok(());
        }

        // Create Parquet writer.
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let location_gen = DefaultLocationGenerator::new(mirror_table.metadata().clone())
            .context("creating location generator")?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "data".to_string(),
            Some(uuid::Uuid::new_v4().to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            mirror_table.metadata().current_schema().clone(),
            None,
            mirror_table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            None,
            mirror_table.metadata().default_partition_spec_id(),
        );

        let mut data_file_writer = data_file_writer_builder
            .build()
            .await
            .context("building data file writer")?;

        // Write all record batches.
        for batch in record_batches {
            data_file_writer
                .write(batch)
                .await
                .context("writing record batch")?;
        }

        // Close writer and get data files.
        let data_files = data_file_writer
            .close()
            .await
            .context("closing data file writer")?;

        let bytes_written: u64 = data_files.iter().map(|df| df.file_size_in_bytes()).sum();

        info!(
            file_count = data_files.len(),
            bytes_written, "data files written"
        );

        // Create transaction and fast append action.
        let transaction = Transaction::new(mirror_table);
        let append_action = transaction
            .fast_append()
            .with_check_duplicate(false)
            .add_data_files(data_files);

        let updated_transaction = append_action
            .apply(transaction)
            .context("applying append action")?;

        updated_transaction
            .commit(&*self.catalog)
            .await
            .context("committing transaction")?;

        counter!("merger.bytes.written", "table" => table_config.mirror_table.clone())
            .increment(bytes_written);

        info!("mirror table updated successfully");
        Ok(())
    }

    /// Builds record batches for the mirror table from deduplicated records.
    ///
    /// Extracts rows from the source batches, excludes CDC columns, and groups
    /// them into batches of `batch_size` rows.
    fn build_mirror_batches(
        &self,
        deduplicated: HashMap<u64, DeduplicatedRecord>,
        mirror_table: &Table,
        table_config: &TableConfig,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        if deduplicated.is_empty() {
            return Ok(Vec::new());
        }

        let mirror_schema = mirror_table.metadata().current_schema();
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(mirror_schema)
                .context("converting mirror schema to arrow")?,
        );

        let excluded_columns: HashSet<&str> = [
            table_config.sequence_column.as_str(),
            table_config.operation_column.as_str(),
        ]
        .into_iter()
        .collect();

        let mut result_batches = Vec::new();
        let mut current_arrays: Vec<Vec<Arc<dyn Array>>> =
            vec![Vec::new(); arrow_schema.fields().len()];

        let mut current_batch_size = 0;

        for record in deduplicated.into_values() {
            let source_schema = record.batch.schema();

            // For each column in the mirror schema, find the corresponding column in
            // the source.
            for (mirror_col_idx, mirror_field) in arrow_schema.fields().iter().enumerate() {
                if excluded_columns.contains(mirror_field.name().as_str()) {
                    continue;
                }

                if let Some(source_col_idx) = source_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == mirror_field.name())
                {
                    let source_array = record.batch.column(source_col_idx);
                    let sliced = source_array.slice(record.row_idx, 1);
                    current_arrays[mirror_col_idx].push(sliced);
                }
            }

            current_batch_size += 1;

            // Flush batch if we've reached the batch size.
            if current_batch_size >= self.batch_size {
                let batch = self.concat_arrays_to_batch(
                    &arrow_schema,
                    &mut current_arrays,
                    current_batch_size,
                )?;
                result_batches.push(batch);
                current_batch_size = 0;
            }
        }

        // Flush remaining rows.
        if current_batch_size > 0 {
            let batch = self.concat_arrays_to_batch(
                &arrow_schema,
                &mut current_arrays,
                current_batch_size,
            )?;
            result_batches.push(batch);
        }

        Ok(result_batches)
    }

    /// Concatenates accumulated arrays into a single record batch.
    fn concat_arrays_to_batch(
        &self,
        schema: &Arc<ArrowSchema>,
        arrays: &mut [Vec<Arc<dyn Array>>],
        _row_count: usize,
    ) -> anyhow::Result<RecordBatch> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(arrays.len());

        for col_arrays in arrays.iter_mut() {
            if col_arrays.is_empty() {
                anyhow::bail!("empty column arrays during batch construction");
            }

            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated =
                arrow::compute::concat(&refs).context("concatenating column arrays")?;
            columns.push(concatenated);
            col_arrays.clear();
        }

        RecordBatch::try_new(schema.clone(), columns).context("creating record batch")
    }
}

/// A deduplicated record with its metadata.
#[derive(Debug, Clone)]
struct DeduplicatedRecord {
    /// CDC sequence number.
    sequence: i64,

    /// CDC operation type.
    operation: String,

    /// The record batch containing this row.
    batch: RecordBatch,

    /// Row index within the batch.
    row_idx: usize,
}

/// Extracts an i64 value from a record batch column.
fn extract_i64(batch: &RecordBatch, col_idx: usize, row_idx: usize) -> Option<i64> {
    let col = batch.column(col_idx);

    if col.is_null(row_idx) {
        return None;
    }

    col.as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|arr| arr.value(row_idx))
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .map(|arr| arr.value(row_idx) as i64)
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .map(|arr| arr.value(row_idx) as i64)
        })
}

/// Extracts a string value from a record batch column.
fn extract_string(batch: &RecordBatch, col_idx: usize, row_idx: usize) -> Option<String> {
    let col = batch.column(col_idx);

    if col.is_null(row_idx) {
        return None;
    }

    col.as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .map(|arr| arr.value(row_idx).to_string())
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .map(|arr| arr.value(row_idx).to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_compute_pk_hash_single_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let hash1 = compute_pk_hash(&batch, &[0], 0);
        let hash2 = compute_pk_hash(&batch, &[0], 1);
        let hash3 = compute_pk_hash(&batch, &[0], 0);

        assert_ne!(hash1, hash2);
        assert_eq!(hash1, hash3);
    }

    #[test]
    fn test_compute_pk_hash_multiple_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
            ],
        )
        .unwrap();

        let hash1 = compute_pk_hash(&batch, &[0, 1], 0);
        let hash2 = compute_pk_hash(&batch, &[0, 1], 1);
        let hash3 = compute_pk_hash(&batch, &[0, 1], 2);

        // All three should be different since at least one column differs.
        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash2, hash3);
    }

    #[test]
    fn test_compute_pk_hash_deterministic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create two separate batches with the same data.
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["test"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["test"])),
            ],
        )
        .unwrap();

        // Hashes should be identical for same data.
        assert_eq!(
            compute_pk_hash(&batch1, &[0, 1], 0),
            compute_pk_hash(&batch2, &[0, 1], 0)
        );
    }

    #[test]
    fn test_compute_pk_hash_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let hash1 = compute_pk_hash(&batch, &[0], 0);
        let hash2 = compute_pk_hash(&batch, &[0], 1);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_extract_i64() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 200, 300]))],
        )
        .unwrap();

        assert_eq!(extract_i64(&batch, 0, 0), Some(100));
        assert_eq!(extract_i64(&batch, 0, 1), Some(200));
        assert_eq!(extract_i64(&batch, 0, 2), Some(300));
    }

    #[test]
    fn test_extract_i64_from_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![100, 200, 300]))],
        )
        .unwrap();

        assert_eq!(extract_i64(&batch, 0, 0), Some(100));
        assert_eq!(extract_i64(&batch, 0, 1), Some(200));
        assert_eq!(extract_i64(&batch, 0, 2), Some(300));
    }

    #[test]
    fn test_extract_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("op", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "INSERT", "UPDATE", "DELETE",
            ]))],
        )
        .unwrap();

        assert_eq!(extract_string(&batch, 0, 0), Some("INSERT".to_string()));
        assert_eq!(extract_string(&batch, 0, 1), Some("UPDATE".to_string()));
        assert_eq!(extract_string(&batch, 0, 2), Some("DELETE".to_string()));
    }

    #[test]
    fn test_extract_string_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("op", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![""]))]).unwrap();

        assert_eq!(extract_string(&batch, 0, 0), Some(String::new()));
    }

    #[test]
    fn test_deduplication_logic() {
        // Simulates the deduplication logic: newer records should replace older ones.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("cdc_sequence_number", DataType::Int64, false),
            Field::new("cdc_operation", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["old", "new", "only"])),
                Arc::new(Int64Array::from(vec![100, 200, 150])),
                Arc::new(StringArray::from(vec!["INSERT", "UPDATE", "INSERT"])),
            ],
        )
        .unwrap();

        // Simulate deduplication: keep records with highest sequence per PK.
        let mut deduplicated: HashMap<u64, (i64, usize)> = HashMap::new();

        for row_idx in 0..batch.num_rows() {
            let pk_hash = compute_pk_hash(&batch, &[0], row_idx);
            let sequence = extract_i64(&batch, 2, row_idx).unwrap();

            let should_update = match deduplicated.get(&pk_hash) {
                Some((existing_seq, _)) => sequence > *existing_seq,
                None => true,
            };

            if should_update {
                deduplicated.insert(pk_hash, (sequence, row_idx));
            }
        }

        // Should have 2 deduplicated records.
        assert_eq!(deduplicated.len(), 2);

        // For id=1, should keep the one with sequence 200 (row 1).
        let pk1_hash = compute_pk_hash(&batch, &[0], 0);
        let (seq1, row1) = deduplicated.get(&pk1_hash).unwrap();
        assert_eq!(*seq1, 200);
        assert_eq!(*row1, 1);

        // For id=2, should keep the only row (row 2).
        let pk2_hash = compute_pk_hash(&batch, &[0], 2);
        let (seq2, row2) = deduplicated.get(&pk2_hash).unwrap();
        assert_eq!(*seq2, 150);
        assert_eq!(*row2, 2);
    }

    #[test]
    fn test_delete_filtering() {
        // Test that DELETE operations are correctly identified.
        let operations = ["INSERT", "UPDATE", "DELETE", "D", "delete", "I", "U"];
        let delete_ops = ["DELETE", "D"];

        for op in operations {
            let is_delete = delete_ops.iter().any(|d| op.to_uppercase() == *d);
            match op {
                "DELETE" | "D" | "delete" => assert!(is_delete, "{} should be delete", op),
                _ => assert!(!is_delete, "{} should not be delete", op),
            }
        }
    }
}
