#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::create_partitioned_table;
use etl::types::PipelineId;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

/// Test that verifies partitioned tables with inherited primary keys work correctly.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_sync_succeeds_with_inherited_primary_keys() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs = [
        ("p1", "from (1) to (100)"),
        ("p2", "from (100) to (200)"),
        ("p3", "from (200) to (300)"),
    ];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values 
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

    let mut partition_notifications = Vec::new();
    for &partition_id in &partition_table_ids {
        let notification = state_store
            .notify_on_table_state(partition_id, TableReplicationPhaseType::SyncDone)
            .await;
        partition_notifications.push(notification);
    }

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    for notification in partition_notifications {
        notification.notified().await;
    }

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();

    assert_eq!(
        total_rows, 3,
        "Expected 3 rows synced (one per partition), but got {}",
        total_rows
    );

    let table_states = state_store.get_table_replication_states().await;

    assert_eq!(
        table_states.len(),
        partition_table_ids.len(),
        "Expected {} partition states, but found {}",
        partition_table_ids.len(),
        table_states.len()
    );

    for &partition_id in &partition_table_ids {
        let state = table_states
            .get(&partition_id)
            .unwrap_or_else(|| panic!("Partition {} should have a state", partition_id));
        assert!(
            matches!(
                state.as_type(),
                TableReplicationPhaseType::SyncDone | TableReplicationPhaseType::Ready
            ),
            "Partition {} should be in SyncDone or Ready state, but was in {:?}",
            partition_id,
            state.as_type()
        );
    }

    assert!(
        !table_states.contains_key(&parent_table_id),
        "Parent table {} should not be tracked since parent partitioned tables are excluded from processing",
        parent_table_id
    );

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(
        parent_table_rows, 0,
        "Parent table {} should have no data since it's excluded from processing and all data goes to partitions, but found {} rows",
        parent_table_id, parent_table_rows
    );
}
