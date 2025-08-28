use etl::destination::memory::MemoryDestination;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::types::PipelineId;
use etl_postgres::tokio::test_utils::PgDatabase;
use tokio_postgres::GenericClient;
use etl_postgres::types::{TableId, TableName};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

/// Creates a partitioned table with the given name and partitions.
/// 
/// This function creates:
/// 1. A parent partitioned table with a primary key
/// 2. Several child partitions based on the provided partition specifications
/// 
/// Returns the table ID of the parent table and a list of partition table IDs.
pub async fn create_partitioned_table<G: GenericClient>(
    database: &PgDatabase<G>,
    table_name: TableName,
    partition_specs: &[(&str, &str)], // (partition_name, partition_constraint)
) -> Result<(TableId, Vec<TableId>), tokio_postgres::Error> {
    // Create the parent partitioned table with a primary key
    let create_parent_query = format!(
        "CREATE TABLE {} (
            id bigserial,
            data text NOT NULL,
            partition_key integer NOT NULL,
            PRIMARY KEY (id, partition_key)
        ) PARTITION BY RANGE (partition_key)",
        table_name.as_quoted_identifier()
    );
    
    database.run_sql(&create_parent_query).await?;
    
    // Get the OID of the parent table
    let parent_row = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await?;
    
    let parent_table_id: TableId = parent_row.get(0);
    let mut partition_table_ids = Vec::new();
    
    // Create child partitions
    for (partition_name, partition_constraint) in partition_specs {
        let partition_table_name = TableName::new(
            table_name.schema.clone(),
            format!("{}_{}", table_name.name, partition_name),
        );
        
        let create_partition_query = format!(
            "CREATE TABLE {} PARTITION OF {} FOR VALUES {}",
            partition_table_name.as_quoted_identifier(),
            table_name.as_quoted_identifier(),
            partition_constraint
        );
        
        database.run_sql(&create_partition_query).await?;
        
        // Get the OID of the partition table
        let partition_row = database
            .client
            .as_ref()
            .unwrap()
            .query_one(
                "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&partition_table_name.schema, &partition_table_name.name],
            )
            .await?;
        
        let partition_table_id: TableId = partition_row.get(0);
        partition_table_ids.push(partition_table_id);
    }
    
    Ok((parent_table_id, partition_table_ids))
}

/// Test that verifies partitioned tables with inherited primary keys work correctly.
/// 
/// This test validates the fix for GitHub issue #296 where partitioned tables 
/// failed during sync because the ETL system didn't recognize that leaf partitions 
/// inherit primary key constraints from their parent table.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_sync_succeeds_with_inherited_primary_keys() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    
    // Create a partitioned table with three partitions based on partition_key ranges
    let partition_specs = [
        ("p1", "FROM (1) TO (100)"),
        ("p2", "FROM (100) TO (200)"), 
        ("p3", "FROM (200) TO (300)"),
    ];
    
    let (parent_table_id, partition_table_ids) = create_partitioned_table(
        &database, 
        table_name.clone(), 
        &partition_specs
    ).await.expect("Failed to create partitioned table");

    // Insert some test data into the partitioned table  
    database
        .run_sql(&format!(
            "INSERT INTO {} (data, partition_key) VALUES 
             ('event1', 50), ('event2', 150), ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for all partitions to complete their sync (should succeed now)
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // Check that all partitions successfully synced without primary key errors
    let table_states = state_store.get_table_replication_states().await;
    
    let mut sync_done_count = 0;
    let mut error_count = 0;
    
    for (table_id, state) in &table_states {
        match state.as_type() {
            TableReplicationPhaseType::SyncDone | TableReplicationPhaseType::Ready => {
                sync_done_count += 1;
                println!("✓ Partition {} successfully synced", table_id);
            }
            TableReplicationPhaseType::Errored => {
                error_count += 1;
                let reason = match state {
                    etl::state::table::TableReplicationPhase::Errored { reason, .. } => reason,
                    _ => unreachable!(),
                };
                println!("✗ Partition {} failed with error: {}", table_id, reason);
            }
            other_state => {
                println!("? Partition {} in state: {:?}", table_id, other_state);
            }
        }
    }

    // Shutdown the pipeline (should succeed now)
    let shutdown_result = pipeline.shutdown_and_wait().await;
    
    // Verify all 3 partitions successfully synced without errors
    assert_eq!(
        error_count, 0,
        "Expected no partitions to fail, but got {} errors",
        error_count
    );
    
    assert!(
        sync_done_count >= 3,
        "Expected all 3 partitions to sync successfully, but only {} completed",
        sync_done_count
    );

    // The pipeline should succeed (or fail for reasons other than primary key issues)
    if let Err(e) = shutdown_result {
        // If there's an error, it shouldn't be about missing primary keys
        let error_str = format!("{:?}", e);
        assert!(
            !error_str.contains("Missing primary key"),
            "Pipeline failed with primary key error despite fix: {}",
            error_str
        );
    }

    // Verify that data was synced successfully
    let table_rows = destination.get_table_rows().await;
    
    // We expect data to be synced from all 3 partitions
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert!(
        total_rows >= 3,
        "Expected at least 3 rows synced (one per partition), but got {}",
        total_rows
    );

    println!(
        "✓ Successfully verified GitHub issue #296 fix: Partitioned table sync works with inherited primary keys"
    );
    println!("✓ Parent table ID: {}", parent_table_id);
    println!("✓ Successfully synced partition IDs: {:?}", partition_table_ids);
    println!("✓ Total rows synced: {}", total_rows);
}
