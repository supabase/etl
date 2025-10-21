#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::event::group_events_by_type_and_table_id;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::create_partitioned_table;
use etl::types::EventType;
use etl::types::PipelineId;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_replicates_existing_data() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs = [
        ("p1", "from (1) to (100)"),
        ("p2", "from (100) to (200)"),
        ("p3", "from (200) to (300)"),
    ];

    let (parent_table_id, _partition_table_ids) =
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

    // Register notification for initial copy completion.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();

    assert_eq!(
        total_rows, 3,
        "Expected 3 rows synced (one per partition), but got {total_rows}"
    );

    let table_states = state_store.get_table_replication_states().await;

    assert!(
        table_states.contains_key(&parent_table_id),
        "Parent table should be tracked in state"
    );
    assert_eq!(
        table_states.len(),
        1,
        "Only the parent table should be tracked in state"
    );

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(
        parent_table_rows, 3,
        "Parent table should contain all rows when publishing via root"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_and_streams_new_data_from_new_partition() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_late");
    let initial_partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _initial_partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &initial_partition_specs)
            .await
            .expect("Failed to create initial partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_late".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Register notification for initial copy completion.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    let new_partition_name = format!("{}_{}", table_name.name, "p3");
    let new_partition_qualified_name = format!("{}.{}", table_name.schema, new_partition_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (200) to (300)",
            new_partition_qualified_name,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for CDC to deliver the new row.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;
    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert_eq!(
        total_rows, 2,
        "Expected 2 rows synced from initial copy, got {total_rows}"
    );

    let table_states = state_store.get_table_replication_states().await;
    assert!(table_states.contains_key(&parent_table_id));
    assert_eq!(table_states.len(), 1);

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 2);

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_drop_does_not_emit_delete_or_truncate() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_drop");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_drop".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    let events_before = destination.get_events().await;
    let grouped_before = group_events_by_type_and_table_id(&events_before);
    let del_before = grouped_before
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let trunc_before = grouped_before
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    // Detach and drop one child partition (DDL should not generate DML events)
    let child_p1_name = format!("{}_{}", table_name.name, "p1");
    let child_p1_qualified = format!("{}.{}", table_name.schema, child_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            child_p1_qualified
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("drop table {child_p1_qualified}"))
        .await
        .unwrap();

    let _ = pipeline.shutdown_and_wait().await;

    let events_after = destination.get_events().await;
    let grouped_after = group_events_by_type_and_table_id(&events_after);
    let del_after = grouped_after
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let trunc_after = grouped_after
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(
        del_after, del_before,
        "Partition drop must not emit DELETE events"
    );
    assert_eq!(
        trunc_after, trunc_before,
        "Partition drop must not emit TRUNCATE events"
    );
}
