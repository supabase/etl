//! End-to-end integration tests for the iceberg merger.
//!
//! These tests use a real Iceberg catalog (Lakekeeper with MinIO) to:
//! 1. Create changelog tables using IcebergClient
//! 2. Insert CDC events into the changelog tables
//! 3. Run the merger to compact changelog → mirror tables
//! 4. Verify the merged results are correct
//!
//! Prerequisites:
//! - Docker Compose with Lakekeeper and MinIO running
//! - See scripts/docker-compose.yaml for setup
//!
//! Run with: `cargo test --package etl-iceberg-merger --test iceberg_integration`

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use etl_destinations::iceberg::test_utils::{
    LAKEKEEPER_URL, LakekeeperClient, MINIO_PASSWORD, MINIO_URL, MINIO_USERNAME, get_catalog_url,
};
use etl_iceberg_merger::config::{IcebergCatalogConfig, TableConfig};
use etl_iceberg_merger::merge::MergeExecutor;
use etl_telemetry::tracing::init_test_tracing;
use futures::TryStreamExt;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Type};
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
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// CDC operation type column values.
const INSERT: &str = "INSERT";
const UPDATE: &str = "UPDATE";
const DELETE: &str = "DELETE";

/// Column names for changelog tables.
const SEQUENCE_COLUMN: &str = "sequence_number";
const OPERATION_COLUMN: &str = "cdc_operation";

/// Creates a catalog client for tests.
async fn create_test_catalog(warehouse_name: &str) -> Arc<dyn Catalog> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), get_catalog_url());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse_name.to_string(),
    );
    props.insert(S3_ACCESS_KEY_ID.to_string(), MINIO_USERNAME.to_string());
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), MINIO_PASSWORD.to_string());
    props.insert(S3_ENDPOINT.to_string(), MINIO_URL.to_string());
    props.insert(S3_REGION.to_string(), "local-01".to_string());

    let builder = RestCatalogBuilder::default();
    let catalog = builder
        .load("RestCatalog", props)
        .await
        .expect("failed to create catalog");

    Arc::new(catalog)
}

/// Creates the Iceberg catalog configuration for MergeExecutor.
fn create_iceberg_config(warehouse_name: &str) -> IcebergCatalogConfig {
    IcebergCatalogConfig {
        catalog_url: get_catalog_url(),
        catalog_token: String::new(),
        warehouse: warehouse_name.to_string(),
        s3_endpoint: MINIO_URL.to_string(),
        s3_access_key_id: MINIO_USERNAME.to_string(),
        s3_secret_access_key: MINIO_PASSWORD.to_string(),
        s3_region: "local-01".to_string(),
    }
}

/// Creates the table configuration for a test table.
fn create_table_config(namespace: &str, changelog_table: &str, mirror_table: &str) -> TableConfig {
    TableConfig {
        namespace: namespace.to_string(),
        changelog_table: changelog_table.to_string(),
        mirror_table: mirror_table.to_string(),
        primary_keys: vec!["id".to_string()],
        sequence_column: SEQUENCE_COLUMN.to_string(),
        operation_column: OPERATION_COLUMN.to_string(),
    }
}

/// Creates a changelog table with the standard CDC schema.
async fn create_changelog_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());

    // Ensure namespace exists.
    if !catalog.namespace_exists(&namespace_ident).await? {
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;
    }

    // Define changelog schema: id (PK), value, sequence_number, cdc_operation.
    let schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                2,
                "value",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                3,
                SEQUENCE_COLUMN,
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                4,
                OPERATION_COLUMN,
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()?;

    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    // Check if table already exists.
    if catalog.table_exists(&table_ident).await? {
        catalog.drop_table(&table_ident).await?;
    }

    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .build();

    catalog.create_table(&namespace_ident, creation).await?;
    Ok(())
}

/// Writes a RecordBatch to an Iceberg changelog table.
async fn write_changelog_batch(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    table_name: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

    let table = catalog.load_table(&table_ident).await?;

    // Convert batch to use Iceberg schema with field IDs.
    let iceberg_schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(iceberg_schema)?);
    let batch = RecordBatch::try_new(arrow_schema, batch.columns().to_vec())?;

    // Create Parquet writer properties.
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    // Create location and file name generators.
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        "data".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );

    // Create Parquet writer builder.
    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_props,
        table.metadata().current_schema().clone(),
        None,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    // Create data file writer.
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        None,
        table.metadata().default_partition_spec_id(),
    );

    let mut data_file_writer = data_file_writer_builder.build().await?;
    data_file_writer.write(batch).await?;

    let data_files = data_file_writer.close().await?;

    // Commit transaction.
    let transaction = iceberg::transaction::Transaction::new(&table);
    let append_action = transaction
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(data_files);

    let updated_transaction =
        iceberg::transaction::ApplyTransactionAction::apply(append_action, transaction)?;
    updated_transaction.commit(&**catalog).await?;

    Ok(())
}

/// Creates a changelog RecordBatch with the given data.
fn create_test_batch(
    ids: Vec<i64>,
    values: Vec<&str>,
    sequences: Vec<i64>,
    operations: Vec<&str>,
) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new(SEQUENCE_COLUMN, DataType::Int64, false),
        Field::new(OPERATION_COLUMN, DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int64Array::from(sequences)),
            Arc::new(StringArray::from(operations)),
        ],
    )
    .expect("failed to create record batch")
}

/// Reads all rows from an Iceberg table.
async fn read_all_rows(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    table_name: &str,
) -> Vec<(i64, String)> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

    let table = catalog.load_table(&table_ident).await.unwrap();

    let Some(current_snapshot) = table.metadata().current_snapshot_id() else {
        return vec![];
    };

    let mut stream = table
        .scan()
        .snapshot_id(current_snapshot)
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();

    let mut rows = Vec::new();

    while let Some(batch) = stream.try_next().await.unwrap() {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            rows.push((id_col.value(i), value_col.value(i).to_string()));
        }
    }

    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// Reads all rows from a changelog table (including CDC columns).
async fn read_changelog_rows(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    table_name: &str,
) -> Vec<(i64, String, i64, String)> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

    let table = catalog.load_table(&table_ident).await.unwrap();

    let Some(current_snapshot) = table.metadata().current_snapshot_id() else {
        return vec![];
    };

    let mut stream = table
        .scan()
        .snapshot_id(current_snapshot)
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();

    let mut rows = Vec::new();

    while let Some(batch) = stream.try_next().await.unwrap() {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seq_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let op_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            rows.push((
                id_col.value(i),
                value_col.value(i).to_string(),
                seq_col.value(i),
                op_col.value(i).to_string(),
            ));
        }
    }

    rows.sort_by_key(|(id, _, seq, _)| (*id, *seq));
    rows
}

/// Cleans up test resources.
async fn cleanup(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    tables: &[&str],
    lakekeeper_client: &LakekeeperClient,
    warehouse_id: uuid::Uuid,
) {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());

    for table_name in tables {
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
        let _ = catalog.drop_table(&table_ident).await;
    }

    let _ = catalog.drop_namespace(&namespace_ident).await;
    let _ = lakekeeper_client.drop_warehouse(warehouse_id).await;
}

// ============================================================================
// Test: Basic Merge Operation
// ============================================================================

/// Tests that a simple changelog is correctly merged into a mirror table.
#[tokio::test(flavor = "multi_thread")]
async fn test_basic_merge_creates_mirror_table() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_basic_merge";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    // Create changelog table.
    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Insert some CDC events.
    let batch = create_test_batch(
        vec![1, 2, 3],
        vec!["alice", "bob", "charlie"],
        vec![100, 101, 102],
        vec![INSERT, INSERT, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify mirror table was created and contains deduplicated data.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 3, "expected 3 rows in mirror table");

    let mut sorted = rows.clone();
    sorted.sort_by_key(|(id, _)| *id);
    assert_eq!(sorted[0], (1, "alice".to_string()));
    assert_eq!(sorted[1], (2, "bob".to_string()));
    assert_eq!(sorted[2], (3, "charlie".to_string()));

    // Cleanup.
    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Deduplication with Updates
// ============================================================================

/// Tests that updates to the same PK keep only the latest version.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_deduplicates_updates() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_dedup_updates";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Insert followed by updates for the same PK.
    let batch = create_test_batch(
        vec![1, 1, 1, 2],
        vec!["alice_v1", "alice_v2", "alice_v3", "bob"],
        vec![100, 101, 102, 103],
        vec![INSERT, UPDATE, UPDATE, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify only latest version is kept.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 2, "expected 2 unique rows after deduplication");

    assert_eq!(
        rows[0],
        (1, "alice_v3".to_string()),
        "should keep latest update"
    );
    assert_eq!(rows[1], (2, "bob".to_string()));

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: DELETE Operations
// ============================================================================

/// Tests that DELETE operations remove rows from the mirror table.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_handles_deletes() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_deletes";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Insert then delete one row.
    let batch = create_test_batch(
        vec![1, 2, 1],
        vec!["alice", "bob", "alice"],
        vec![100, 101, 102],
        vec![INSERT, INSERT, DELETE],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify deleted row is not in mirror.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 1, "alice should be deleted");
    assert_eq!(rows[0], (2, "bob".to_string()));

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Multiple Batches
// ============================================================================

/// Tests merging with data split across multiple Parquet files.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_multiple_batches() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_multi_batch";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Write first batch.
    let batch1 = create_test_batch(
        vec![1, 2],
        vec!["alice_v1", "bob_v1"],
        vec![100, 101],
        vec![INSERT, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch1)
        .await
        .unwrap();

    // Write second batch with updates.
    let batch2 = create_test_batch(
        vec![1, 3],
        vec!["alice_v2", "charlie"],
        vec![200, 201],
        vec![UPDATE, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch2)
        .await
        .unwrap();

    // Verify changelog has 4 rows.
    let changelog_rows = read_changelog_rows(&catalog, namespace, changelog_table).await;
    assert_eq!(
        changelog_rows.len(),
        4,
        "changelog should have 4 rows total"
    );

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify merged data has 3 rows with latest values.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        (1, "alice_v2".to_string()),
        "should have latest version"
    );
    assert_eq!(rows[1], (2, "bob_v1".to_string()));
    assert_eq!(rows[2], (3, "charlie".to_string()));

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Out of Order Sequence Numbers
// ============================================================================

/// Tests that sequence numbers determine recency, not write order.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_respects_sequence_order() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_seq_order";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Write events out of sequence order.
    // sequence 300 comes before sequence 100 in the batch, but 300 is newer.
    let batch = create_test_batch(
        vec![1, 1],
        vec!["newer_value", "older_value"],
        vec![300, 100],
        vec![UPDATE, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify the newer value (higher sequence) is kept.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        (1, "newer_value".to_string()),
        "should keep value with higher sequence number"
    );

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Empty Changelog
// ============================================================================

/// Tests that merging an empty changelog creates an empty mirror.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_empty_changelog() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_empty";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Run the merger with no data in changelog.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify mirror table is created but empty.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 0, "mirror should be empty");

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: All Rows Deleted
// ============================================================================

/// Tests that deleting all rows results in an empty mirror.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_all_deleted() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_all_deleted";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Insert and then delete all rows.
    let batch = create_test_batch(
        vec![1, 2, 1, 2],
        vec!["alice", "bob", "alice", "bob"],
        vec![100, 101, 200, 201],
        vec![INSERT, INSERT, DELETE, DELETE],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify mirror is empty.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), 0, "all rows should be deleted");

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Incremental Merge (Run Twice)
// ============================================================================

/// Tests running the merger twice with new data added between runs.
#[tokio::test(flavor = "multi_thread")]
async fn test_incremental_merge() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_incremental";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    // First batch of data.
    let batch1 = create_test_batch(
        vec![1, 2],
        vec!["alice", "bob"],
        vec![100, 101],
        vec![INSERT, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch1)
        .await
        .unwrap();

    // First merge.
    let executor = MergeExecutor::new(iceberg_config.clone(), 1000)
        .await
        .unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    let rows1 = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows1.len(), 2);

    // Add more data.
    let batch2 = create_test_batch(
        vec![1, 3],
        vec!["alice_updated", "charlie"],
        vec![200, 201],
        vec![UPDATE, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch2)
        .await
        .unwrap();

    // Second merge.
    let executor2 = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor2.execute_merge(&table_config).await.unwrap();

    let rows2 = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows2.len(), 3);
    assert_eq!(rows2[0], (1, "alice_updated".to_string()));
    assert_eq!(rows2[1], (2, "bob".to_string()));
    assert_eq!(rows2[2], (3, "charlie".to_string()));

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Large Number of Rows
// ============================================================================

/// Tests merging a large number of rows efficiently.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_large_dataset() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_large";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Create 1000 unique rows.
    let num_rows = 1000;
    let ids: Vec<i64> = (1..=num_rows).collect();
    let values: Vec<String> = (1..=num_rows).map(|i| format!("value_{}", i)).collect();
    let sequences: Vec<i64> = (1..=num_rows).collect();
    let operations: Vec<&str> = vec![INSERT; num_rows as usize];

    let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new(SEQUENCE_COLUMN, DataType::Int64, false),
        Field::new(OPERATION_COLUMN, DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values_refs)),
            Arc::new(Int64Array::from(sequences)),
            Arc::new(StringArray::from(operations)),
        ],
    )
    .unwrap();

    write_changelog_batch(&catalog, namespace, changelog_table, batch)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Verify all 1000 rows are in the mirror.
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;
    assert_eq!(rows.len(), num_rows as usize);

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}

// ============================================================================
// Test: Complex CDC Scenario
// ============================================================================

/// Tests a realistic CDC scenario with mixed operations across multiple batches.
#[tokio::test(flavor = "multi_thread")]
async fn test_complex_cdc_scenario() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();

    let catalog = create_test_catalog(&warehouse_name).await;
    let namespace = "test_complex";
    let changelog_table = "users_changelog";
    let mirror_table = "users_mirror";

    create_changelog_table(&catalog, namespace, changelog_table)
        .await
        .unwrap();

    // Batch 1: Initial inserts.
    let batch1 = create_test_batch(
        vec![1, 2, 3, 4, 5],
        vec!["alice", "bob", "charlie", "diana", "eve"],
        vec![100, 101, 102, 103, 104],
        vec![INSERT, INSERT, INSERT, INSERT, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch1)
        .await
        .unwrap();

    // Batch 2: Some updates and deletes.
    let batch2 = create_test_batch(
        vec![1, 3, 5],
        vec!["alice_v2", "charlie_v2", "eve"],
        vec![200, 201, 202],
        vec![UPDATE, UPDATE, DELETE],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch2)
        .await
        .unwrap();

    // Batch 3: More updates, a re-insert, and another delete.
    let batch3 = create_test_batch(
        vec![1, 2, 5],
        vec!["alice_v3", "bob", "eve_new"],
        vec![300, 301, 302],
        vec![UPDATE, DELETE, INSERT],
    );
    write_changelog_batch(&catalog, namespace, changelog_table, batch3)
        .await
        .unwrap();

    // Run the merger.
    let iceberg_config = create_iceberg_config(&warehouse_name);
    let table_config = create_table_config(namespace, changelog_table, mirror_table);

    let executor = MergeExecutor::new(iceberg_config, 1000).await.unwrap();
    executor.execute_merge(&table_config).await.unwrap();

    // Expected final state:
    // - id=1: alice_v3 (latest update at seq 300)
    // - id=2: deleted (seq 301)
    // - id=3: charlie_v2 (latest update at seq 201)
    // - id=4: diana (original insert, no changes)
    // - id=5: eve_new (re-inserted at seq 302)
    let rows = read_all_rows(&catalog, namespace, mirror_table).await;

    assert_eq!(rows.len(), 4, "expected 4 rows (bob deleted)");
    assert_eq!(rows[0], (1, "alice_v3".to_string()));
    assert_eq!(rows[1], (3, "charlie_v2".to_string()));
    assert_eq!(rows[2], (4, "diana".to_string()));
    assert_eq!(rows[3], (5, "eve_new".to_string()));

    cleanup(
        &catalog,
        namespace,
        &[changelog_table, mirror_table],
        &lakekeeper_client,
        warehouse_id,
    )
    .await;
}
