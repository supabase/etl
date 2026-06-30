use std::collections::HashMap;

use etl::{
    data::TableRow,
    event::EventType,
    pipeline::PipelineId,
    schema::{TableId, TableName},
    store::TableStateType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::group_events_by_type_and_table_id,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, create_pipeline},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::create_partitioned_table,
    },
};
use etl_postgres::{below_version, version::POSTGRES_15};
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::quote_identifier;
use rand::random;
use tokio_postgres::types::Type;

use crate::support::partition::{create_nested_partition_hierarchy, partition_table_name};

fn quoted_qualified_table_name(schema: &str, table: &str) -> String {
    TableName::new(schema.to_owned(), table.to_owned()).as_quoted_identifier()
}

#[derive(Clone, Copy)]
enum PublishedPartitionTarget {
    Top,
    Middle,
    Leaf,
}

#[derive(Clone, Copy)]
enum TableCopyMode {
    Serial,
    Parallel,
}

impl TableCopyMode {
    fn max_copy_connections_per_table(self) -> u16 {
        match self {
            TableCopyMode::Serial => 1,
            TableCopyMode::Parallel => 2,
        }
    }
}

fn assert_table_row_counts(
    table_rows: &HashMap<TableId, Vec<TableRow>>,
    expected_counts: &[(TableId, usize)],
) {
    assert_eq!(table_rows.len(), expected_counts.len());
    for (table_id, expected_count) in expected_counts {
        assert_eq!(table_rows.get(table_id).map_or(0, Vec::len), *expected_count);
    }
}

async fn assert_nested_partition_pipeline_case(
    test_name: &str,
    published_partition_target: PublishedPartitionTarget,
    publish_via_partition_root: bool,
) {
    init_test_tracing();
    let database = spawn_source_database().await;

    let hierarchy = create_nested_partition_hierarchy(&database, test_table_name(test_name)).await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_year, partition_month) values
             ('initial_2025_01', 2025, 1),
             ('initial_2025_02', 2025, 2),
             ('initial_2026_01', 2026, 1),
             ('initial_2026_02', 2026, 2)",
            hierarchy.root_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_table_name = match published_partition_target {
        PublishedPartitionTarget::Top => hierarchy.root_table_name.clone(),
        PublishedPartitionTarget::Middle => hierarchy.p_2026_table_name.clone(),
        PublishedPartitionTarget::Leaf => partition_table_name(&hierarchy.p_2026_table_name, "01"),
    };
    let publication_name = format!("pub_{test_name}");
    database
        .create_publication_with_config(
            &publication_name,
            std::slice::from_ref(&publication_table_name),
            publish_via_partition_root,
        )
        .await
        .unwrap();

    let expected_copy_counts = match (published_partition_target, publish_via_partition_root) {
        (PublishedPartitionTarget::Top, true) => vec![(hierarchy.root_table_id, 4)],
        (PublishedPartitionTarget::Top, false) => vec![
            (hierarchy.p_2025_01_table_id, 1),
            (hierarchy.p_2025_02_table_id, 1),
            (hierarchy.p_2026_01_table_id, 1),
            (hierarchy.p_2026_02_table_id, 1),
        ],
        (PublishedPartitionTarget::Middle, true) => vec![(hierarchy.p_2026_table_id, 2)],
        (PublishedPartitionTarget::Middle, false) => {
            vec![(hierarchy.p_2026_01_table_id, 1), (hierarchy.p_2026_02_table_id, 1)]
        }
        (PublishedPartitionTarget::Leaf, _) => vec![(hierarchy.p_2026_01_table_id, 1)],
    };
    let expected_cdc_counts = expected_copy_counts.clone();
    let expected_cdc_total = expected_cdc_counts.iter().map(|(_, count)| count).sum::<usize>();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let mut ready_notifies = Vec::new();
    for (table_id, _) in &expected_copy_counts {
        ready_notifies
            .push(state_store.notify_on_table_state_type(*table_id, TableStateType::Ready).await);
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

    for notify in &ready_notifies {
        notify.notified().await;
    }

    let table_states = state_store.get_table_states().await;
    assert_eq!(table_states.len(), expected_copy_counts.len());
    for (table_id, _) in &expected_copy_counts {
        assert!(table_states.contains_key(table_id));
    }

    let table_rows = destination.get_table_rows().await;
    assert_table_row_counts(&table_rows, &expected_copy_counts);

    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, expected_cdc_total as u64)])
        .await;

    let cdc_insert_values = match published_partition_target {
        PublishedPartitionTarget::Top => {
            "('new_2025_01', 2025, 1),
             ('new_2025_02', 2025, 2),
             ('new_2026_01', 2026, 1),
             ('new_2026_02', 2026, 2)"
        }
        PublishedPartitionTarget::Middle => "('new_2026_01', 2026, 1), ('new_2026_02', 2026, 2)",
        PublishedPartitionTarget::Leaf => "('new_2026_01', 2026, 1)",
    };
    database
        .run_sql(&format!(
            "insert into {} (data, partition_year, partition_month) values {cdc_insert_values}",
            hierarchy.root_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    for (table_id, expected_count) in expected_cdc_counts {
        let inserts = grouped.get(&(EventType::Insert, table_id)).cloned().unwrap_or_default();
        assert_eq!(inserts.len(), expected_count);
    }

    // We check the table rows again just to validate that no new ones were added.
    let table_rows = destination.get_table_rows().await;
    assert_table_row_counts(&table_rows, &expected_copy_counts);
}

async fn assert_nested_partition_pipeline_row_filter_case(
    test_name: &str,
    published_partition_target: PublishedPartitionTarget,
    table_copy_mode: TableCopyMode,
) {
    init_test_tracing();
    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        return;
    }

    let hierarchy = create_nested_partition_hierarchy(&database, test_table_name(test_name)).await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_year, partition_month) values
             ('initial_2025_01', 2025, 1),
             ('initial_2025_02', 2025, 2),
             ('initial_2026_01', 2026, 1),
             ('initial_2026_02', 2026, 2)",
            hierarchy.root_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let (publication_table_name, expected_copy_counts) = match published_partition_target {
        PublishedPartitionTarget::Top => {
            (hierarchy.root_table_name.clone(), vec![(hierarchy.root_table_id, 2)])
        }
        PublishedPartitionTarget::Middle => {
            (hierarchy.p_2026_table_name.clone(), vec![(hierarchy.p_2026_table_id, 1)])
        }
        PublishedPartitionTarget::Leaf => (
            partition_table_name(&hierarchy.p_2026_table_name, "01"),
            vec![(hierarchy.p_2026_01_table_id, 1)],
        ),
    };
    let publication_name = format!("pub_{test_name}");
    database
        .run_sql(&format!(
            "create publication {} for table {} where (partition_month = 1) with \
             (publish_via_partition_root = true)",
            quote_identifier(&publication_name),
            publication_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let mut ready_notifies = Vec::new();
    for (table_id, _) in &expected_copy_counts {
        ready_notifies
            .push(state_store.notify_on_table_state_type(*table_id, TableStateType::Ready).await);
    }

    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name,
        state_store,
        destination.clone(),
    )
    .with_max_copy_connections_per_table(table_copy_mode.max_copy_connections_per_table())
    .build();

    pipeline.start().await.unwrap();
    for notify in &ready_notifies {
        notify.notified().await;
    }

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    assert_table_row_counts(&table_rows, &expected_copy_counts);
}

/// Tests that initial COPY replicates all rows from a partitioned table.
/// Only the parent table is tracked, not individual child partitions.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_replicates_existing_data() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs =
        [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)"), ("p3", "from (200) to (300)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150), \
             ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    // Register notification for initial copy completion.
    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_ready_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify table schema was discovered correctly.
    let table_schemas = state_store.get_latest_table_schemas().await;
    assert!(table_schemas.contains_key(&parent_table_id));

    let parent_schema = &table_schemas[&parent_table_id];
    assert_eq!(parent_schema.id, parent_table_id);
    assert_eq!(parent_schema.name, table_name);

    // Verify columns are correctly discovered.
    assert_eq!(parent_schema.column_schemas.len(), 3);

    // Check id column (added by default).
    let id_column = &parent_schema.column_schemas[0];
    assert_eq!(id_column.name, "id");
    assert_eq!(id_column.typ, Type::INT8);
    assert!(!id_column.nullable);
    assert!(id_column.primary_key());

    // Check data column.
    let data_column = &parent_schema.column_schemas[1];
    assert_eq!(data_column.name, "data");
    assert_eq!(data_column.typ, Type::TEXT);
    assert!(!data_column.nullable);
    assert!(!data_column.primary_key());

    // Check partition_key column.
    let partition_key_column = &parent_schema.column_schemas[2];
    assert_eq!(partition_key_column.name, "partition_key");
    assert_eq!(partition_key_column.typ, Type::INT4);
    assert!(!partition_key_column.nullable);
    assert!(partition_key_column.primary_key());

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(Vec::len).sum();
    assert_eq!(total_rows, 3);

    let table_states = state_store.get_table_states().await;
    assert!(table_states.contains_key(&parent_table_id));
    assert_eq!(table_states.len(), 1);

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 3);
}

/// Tests that CDC streams inserts to partitions created after pipeline startup.
/// New partitions are automatically included without publication changes.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_and_streams_new_data_from_new_partition() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_late");
    let initial_partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _initial_partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &initial_partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_late".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    // Register notification for initial copy completion.
    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_ready_notify.notified().await;

    let new_partition_name = format!("{}_{}", table_name.name, "p3");
    let new_partition_qualified_name =
        quoted_qualified_table_name(&table_name.schema, &new_partition_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (200) to (300)",
            new_partition_qualified_name,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for CDC to deliver the new row.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 2);

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let parent_inserts =
        grouped.get(&(EventType::Insert, parent_table_id)).cloned().unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);
}

/// Tests that detaching and dropping a partition does not emit DELETE or
/// TRUNCATE events. Partition management is a DDL operation, not DML, so no
/// data events should be generated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_drop_does_not_emit_delete_or_truncate() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_drop");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_drop".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    let events_before = destination.get_events().await;
    let grouped_before = group_events_by_type_and_table_id(&events_before);
    let delete_count_before =
        grouped_before.get(&(EventType::Delete, parent_table_id)).map_or(0, Vec::len);
    let truncate_count_before =
        grouped_before.get(&(EventType::Truncate, parent_table_id)).map_or(0, Vec::len);

    // Detach and drop one child partition (DDL should not generate DML events).
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();
    database.run_sql(&format!("drop table {partition_p1_qualified}")).await.unwrap();

    // Insert a row into an existing partition to ensure the pipeline is still
    // processing events.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events_after = destination.get_events().await;
    let grouped_after = group_events_by_type_and_table_id(&events_after);
    let delete_count_after =
        grouped_after.get(&(EventType::Delete, parent_table_id)).map_or(0, Vec::len);
    let truncate_count_after =
        grouped_after.get(&(EventType::Truncate, parent_table_id)).map_or(0, Vec::len);

    assert_eq!(delete_count_after, delete_count_before);
    assert_eq!(truncate_count_after, truncate_count_before);
}

/// Tests that issuing a TRUNCATE at the parent table level does emit a TRUNCATE
/// event in the replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn parent_table_truncate_does_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_ready_notify.notified().await;

    // Wait for the parent table truncate to be replicated.
    let truncate_notify = destination.wait_for_events_count(vec![(EventType::Truncate, 1)]).await;

    // We truncate the parent table.
    database
        .run_sql(&format!("truncate table {}", table_name.as_quoted_identifier(),))
        .await
        .unwrap();

    truncate_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let truncate_count =
        grouped_events.get(&(EventType::Truncate, parent_table_id)).map_or(0, Vec::len);

    assert_eq!(truncate_count, 1);
}

/// Tests that issuing a TRUNCATE at the child table level does NOT emit a
/// TRUNCATE event in the replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn child_table_truncate_does_not_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_ready_notify.notified().await;

    // We truncate the child table.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database.run_sql(&format!("truncate table {partition_p1_qualified}")).await.unwrap();

    // Insert a row into an existing partition to ensure the pipeline is still
    // processing events.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let truncate_count =
        grouped_events.get(&(EventType::Truncate, parent_table_id)).map_or(0, Vec::len);

    assert_eq!(truncate_count, 0);
}

/// Tests that detached partitions are not replicated with explicit
/// publications. Once detached, the partition becomes independent and is not in
/// the publication since only the parent table was explicitly added. Inserts to
/// detached partitions are not replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_explicit_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data into both partitions.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create explicit publication for parent table only.
    let publication_name = "test_partitioned_pub_detach".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    // Verify initial sync copied both rows.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.len(), 1);
    let parent_rows: usize = table_rows.get(&parent_table_id).map_or(0, Vec::len);
    assert_eq!(parent_rows, 2);

    // Detach partition p1 from parent.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into the detached partition (should NOT be replicated).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', \
             25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    // Insert into the parent table (should be replicated to remaining partition
    // p2).
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('parent_event', 125)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify events
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event (the insert after detachment).
    let parent_inserts =
        grouped.get(&(EventType::Insert, parent_table_id)).cloned().unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition should have NO insert events.
    let detached_inserts =
        grouped.get(&(EventType::Insert, p1_table_id)).cloned().unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests catalog state when a partition is detached with FOR ALL TABLES
/// publication. The detached partition appears in pg_publication_tables but is
/// not automatically discovered by the running pipeline. Table discovery only
/// happens at pipeline startup, not during execution.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_all_tables");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication.
    let publication_name = "test_all_tables_pub_detach".to_owned();
    database.create_publication_for_all(&publication_name, None).await.unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    // Verify the initial state. The parent table is the only table tracked.
    let table_states_before = state_store.get_table_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state. The detached partition is now a standalone table.
    let inherits_check = database
        .client
        .as_ref()
        .unwrap()
        .query("select count(*) as cnt from pg_inherits where inhrelid = $1", &[&p1_table_id.0])
        .await
        .unwrap();
    let inherits_count: i64 = inherits_check[0].get("cnt");
    assert_eq!(inherits_count, 0);

    // Check pg_publication_tables. With FOR ALL TABLES, the detached partition
    // should appear.
    let pub_tables_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_publication_tables
             where pubname = $1 and tablename = $2",
            &[&publication_name, &partition_p1_name],
        )
        .await
        .unwrap();
    let pub_tables_count: i64 = pub_tables_check[0].get("cnt");
    assert_eq!(pub_tables_count, 1);

    // Insert into detached partition.
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', \
             25)"
        ))
        .await
        .unwrap();

    // Note: The running pipeline won't automatically discover the detached
    // partition without re-scanning for new tables. This is expected behavior,
    // the table discovery happens at pipeline start or explicit refresh.

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table (not the detached
    // partition) because it hasn't re-scanned for new tables.
    let table_states_after = state_store.get_table_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // The detached partition insert should NOT be replicated in this pipeline run
    // because the pipeline hasn't discovered it as a new table.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let detached_inserts =
        grouped.get(&(EventType::Insert, p1_table_id)).cloned().unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline
/// restart. With FOR ALL TABLES publication, the detached partition is
/// re-discovered during table scanning at startup and its data is replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_replicate_detached_inserts_on_restart() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication.
    let publication_name = "test_all_tables_restart".to_owned();
    database.create_publication_for_all(&publication_name, None).await.unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    // Start pipeline and wait for initial sync.
    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    // Verify the initial state. The parent table is the only table tracked.
    let table_states_before = state_store.get_table_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into detached partition (while pipeline is stopped).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', \
             25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new
    // table.
    let detached_ready_notify =
        state_store.notify_on_table_state_type(p1_table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for the detached partition to be synced.
    detached_ready_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify the detached partition was discovered and synced.
    let table_states_after = state_store.get_table_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows.get(&parent_table_id).map_or(0, Vec::len);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows.get(&p1_table_id).map_or(0, Vec::len);
    assert_eq!(detached_rows, 2);
}

/// Tests that detached partitions are not automatically discovered with FOR
/// TABLES IN SCHEMA publication. Similar to FOR ALL TABLES, the detached
/// partition appears in pg_publication_tables but is not automatically
/// discovered by the running pipeline without restart. Requires PostgreSQL 15+
/// for FOR TABLES IN SCHEMA support.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_schema_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Skip test if PostgreSQL version is < 15 (FOR TABLES IN SCHEMA requires 15+).
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    let table_name = test_table_name("partitioned_events_schema_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_detach".to_owned();
    database.create_publication_for_all(&publication_name, Some(&table_name.schema)).await.unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    // Verify initial state.
    let table_states_before = state_store.get_table_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state. The detached partition should appear in
    // pg_publication_tables.
    let pub_tables_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_publication_tables
             where pubname = $1 and tablename = $2",
            &[&publication_name, &partition_p1_name],
        )
        .await
        .unwrap();
    let pub_tables_count: i64 = pub_tables_check[0].get("cnt");
    assert_eq!(pub_tables_count, 1);

    // Insert into detached partition.
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', \
             25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    // Insert into parent table (should be replicated).
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('parent_event', 125)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table.
    let table_states_after = state_store.get_table_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // Verify events.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event.
    let parent_inserts =
        grouped.get(&(EventType::Insert, parent_table_id)).cloned().unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition inserts should NOT be replicated without table
    // re-discovery.
    let detached_inserts =
        grouped.get(&(EventType::Insert, p1_table_id)).cloned().unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline
/// restart with FOR TABLES IN SCHEMA publication. After restart, the detached
/// partition in the same schema should be discovered and its data replicated.
/// Requires PostgreSQL 15+ for FOR TABLES IN SCHEMA support.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_schema_publication_does_replicate_detached_inserts_on_restart() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Skip test if PostgreSQL version is < 15 (FOR TABLES IN SCHEMA requires 15+).
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    let table_name = test_table_name("partitioned_events_schema_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_restart".to_owned();
    database.create_publication_for_all(&publication_name, Some(&table_name.schema)).await.unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    // Start pipeline and wait for initial sync.
    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_ready_notify.notified().await;

    // Verify initial state.
    let table_states_before = state_store.get_table_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified =
        quoted_qualified_table_name(&table_name.schema, &partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into detached partition (while pipeline is still running).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', \
             25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new
    // table.
    let detached_ready_notify =
        state_store.notify_on_table_state_type(p1_table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for the detached partition to be synced.
    detached_ready_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify the detached partition was discovered and synced.
    let table_states_after = state_store.get_table_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows.get(&parent_table_id).map_or(0, Vec::len);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows.get(&p1_table_id).map_or(0, Vec::len);
    assert_eq!(detached_rows, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_top_root_with_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_top_root_true",
        PublishedPartitionTarget::Top,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_top_root_without_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_top_root_false",
        PublishedPartitionTarget::Top,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_middle_root_with_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_middle_root_true",
        PublishedPartitionTarget::Middle,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_middle_root_without_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_middle_root_false",
        PublishedPartitionTarget::Middle,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_leaf_with_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_leaf_true",
        PublishedPartitionTarget::Leaf,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_leaf_without_partition_root() {
    assert_nested_partition_pipeline_case(
        "nested_pipeline_leaf_false",
        PublishedPartitionTarget::Leaf,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_top_root_with_partition_root_respects_row_filter_during_parallel_copy() {
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_top_root_row_filter_parallel",
        PublishedPartitionTarget::Top,
        TableCopyMode::Parallel,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_top_root_with_partition_root_respects_row_filter_during_serial_copy() {
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_top_root_row_filter_serial",
        PublishedPartitionTarget::Top,
        TableCopyMode::Serial,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_middle_root_with_partition_root_respects_row_filter_during_parallel_copy()
{
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_middle_root_row_filter_parallel",
        PublishedPartitionTarget::Middle,
        TableCopyMode::Parallel,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_middle_root_with_partition_root_respects_row_filter_during_serial_copy() {
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_middle_root_row_filter_serial",
        PublishedPartitionTarget::Middle,
        TableCopyMode::Serial,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_leaf_with_partition_root_respects_row_filter_during_parallel_copy() {
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_leaf_row_filter_parallel",
        PublishedPartitionTarget::Leaf,
        TableCopyMode::Parallel,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipeline_leaf_with_partition_root_respects_row_filter_during_serial_copy() {
    assert_nested_partition_pipeline_row_filter_case(
        "nested_pipeline_leaf_row_filter_serial",
        PublishedPartitionTarget::Leaf,
        TableCopyMode::Serial,
    )
    .await;
}
