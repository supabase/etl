#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::error::ErrorKind;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::event::group_events_by_type_and_table_id;
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::create_partitioned_table;
use etl::types::EventType;
use etl::types::PipelineId;
use etl::types::TableId;
use etl_postgres::below_version;
use etl_postgres::version::POSTGRES_15;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::types::Type;

/// Tests that initial COPY replicates all rows from a partitioned table.
/// Only the parent table is tracked, not individual child partitions.
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
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150), ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    // Register notification for initial copy completion.
    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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

    parent_ready_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify table schema was discovered correctly.
    let table_schemas = state_store.get_table_schemas().await;
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
    assert!(id_column.primary);

    // Check data column.
    let data_column = &parent_schema.column_schemas[1];
    assert_eq!(data_column.name, "data");
    assert_eq!(data_column.typ, Type::TEXT);
    assert!(!data_column.nullable);
    assert!(!data_column.primary);

    // Check partition_key column.
    let partition_key_column = &parent_schema.column_schemas[2];
    assert_eq!(partition_key_column.name, "partition_key");
    assert_eq!(partition_key_column.typ, Type::INT4);
    assert!(!partition_key_column.nullable);
    assert!(partition_key_column.primary);

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert_eq!(total_rows, 3);

    let table_states = state_store.get_table_replication_states().await;
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

    let publication_name = "test_partitioned_pub_late".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    // Register notification for initial copy completion.
    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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

    parent_ready_notify.notified().await;

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

    // Wait for CDC to deliver the new row.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

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
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);
}

/// Tests that detaching and dropping a partition does not emit DELETE or TRUNCATE events.
/// Partition management is a DDL operation, not DML, so no data events should be generated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_drop_does_not_emit_delete_or_truncate() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_drop");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_drop".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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
    parent_ready_notify.notified().await;

    let events_before = destination.get_events().await;
    let grouped_before = group_events_by_type_and_table_id(&events_before);
    let delete_count_before = grouped_before
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let truncate_count_before = grouped_before
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    // Detach and drop one child partition (DDL should not generate DML events).
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("drop table {partition_p1_qualified}"))
        .await
        .unwrap();

    // Insert a row into an existing partition to ensure the pipeline is still processing events.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

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
    let delete_count_after = grouped_after
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let truncate_count_after = grouped_after
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(delete_count_after, delete_count_before);
    assert_eq!(truncate_count_after, truncate_count_before);
}

/// Tests that issuing a TRUNCATE at the parent table level does emit a TRUNCATE event in the
/// replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn parent_table_truncate_does_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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

    parent_ready_notify.notified().await;

    // Wait for the parent table truncate to be replicated.
    let truncate_notify = destination
        .wait_for_events_count(vec![(EventType::Truncate, 1)])
        .await;

    // We truncate the parent table.
    database
        .run_sql(&format!(
            "truncate table {}",
            table_name.as_quoted_identifier(),
        ))
        .await
        .unwrap();

    truncate_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let truncate_count = grouped_events
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(truncate_count, 1);
}

/// Tests that issuing a TRUNCATE at the child table level does NOT emit a TRUNCATE event in the
/// replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn child_table_truncate_does_not_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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

    parent_ready_notify.notified().await;

    // We truncate the child table.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!("truncate table {partition_p1_qualified}"))
        .await
        .unwrap();

    // Insert a row into an existing partition to ensure the pipeline is still processing events.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

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
    let truncate_count = grouped_events
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(truncate_count, 0);
}

/// Tests that detached partitions are not replicated with explicit publications.
/// Once detached, the partition becomes independent and is not in the publication since
/// only the parent table was explicitly added. Inserts to detached partitions are not replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_explicit_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

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
    let publication_name = "test_partitioned_pub_detach".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);

    // Detach partition p1 from parent.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
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
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert into the parent table (should be replicated to remaining partition p2).
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
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition should have NO insert events.
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests catalog state when a partition is detached with FOR ALL TABLES publication.
/// The detached partition appears in pg_publication_tables but is not automatically discovered
/// by the running pipeline. Table discovery only happens at pipeline startup, not during execution.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_all_tables");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

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
    let publication_name = "test_all_tables_pub_detach".to_string();
    database
        .create_publication_for_all(&publication_name, None)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
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
        .query(
            "select count(*) as cnt from pg_inherits where inhrelid = $1",
            &[&p1_table_id.0],
        )
        .await
        .unwrap();
    let inherits_count: i64 = inherits_check[0].get("cnt");
    assert_eq!(inherits_count, 0);

    // Check pg_publication_tables. With FOR ALL TABLES, the detached partition should appear.
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
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Note: The running pipeline won't automatically discover the detached partition
    // without re-scanning for new tables. This is expected behavior, the table discovery
    // happens at pipeline start or explicit refresh.

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table (not the detached partition)
    // because it hasn't re-scanned for new tables.
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // The detached partition insert should NOT be replicated in this pipeline run
    // because the pipeline hasn't discovered it as a new table.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline restart.
/// With FOR ALL TABLES publication, the detached partition is re-discovered during table
/// scanning at startup and its data is replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_replicate_detached_inserts_on_restart() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

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
    let publication_name = "test_all_tables_restart".to_string();
    database
        .create_publication_for_all(&publication_name, None)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    // Start pipeline and wait for initial sync.
    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
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
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new table.
    let detached_ready_notify = state_store
        .notify_on_table_state_type(p1_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows
        .get(&p1_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(detached_rows, 2);
}

/// Tests that detached partitions are not automatically discovered with FOR TABLES IN SCHEMA publication.
/// Similar to FOR ALL TABLES, the detached partition appears in pg_publication_tables but is not
/// automatically discovered by the running pipeline without restart.
/// Requires PostgreSQL 15+ for FOR TABLES IN SCHEMA support.
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
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_detach".to_string();
    database
        .create_publication_for_all(&publication_name, Some(&table_name.schema))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state. The detached partition should appear in pg_publication_tables.
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
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

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
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // Verify events.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event.
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition inserts should NOT be replicated without table re-discovery.
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline restart
/// with FOR TABLES IN SCHEMA publication. After restart, the detached partition in the same
/// schema should be discovered and its data replicated.
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
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_restart".to_string();
    database
        .create_publication_for_all(&publication_name, Some(&table_name.schema))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    // Start pipeline and wait for initial sync.
    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
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
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new table.
    let detached_ready_notify = state_store
        .notify_on_table_state_type(p1_table_id, TableReplicationPhaseType::Ready)
        .await;

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
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows
        .get(&p1_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(detached_rows, 2);
}

/// Tests that nested partitions (sub-partitioned tables) work correctly.
/// Creates a two-level partition hierarchy where one partition is itself partitioned,
/// and verifies that both initial COPY and CDC streaming work correctly.
/// Only the top-level parent table should be tracked in the pipeline state.
#[tokio::test(flavor = "multi_thread")]
async fn nested_partitioned_table_copy_and_cdc() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("nested_partitioned_events");

    // Create the parent partitioned table (Level 1).
    // Primary key must include all partitioning columns used at any level.
    database
        .run_sql(&format!(
            "create table {} (
                id bigserial,
                data text NOT NULL,
                partition_key integer NOT NULL,
                sub_partition_key integer NOT NULL,
                primary key (id, partition_key, sub_partition_key)
            ) partition by range (partition_key)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Get parent table ID.
    let parent_row = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await
        .unwrap();
    let parent_table_id: TableId = parent_row.get(0);

    // Create first partition (simple leaf partition) (Level 2a).
    let p1_name = format!("{}_{}", table_name.name, "p1");
    let p1_qualified = format!("{}.{}", table_name.schema, p1_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (1) to (100)",
            p1_qualified,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create second partition that is itself partitioned (Level 2b).
    let p2_name = format!("{}_{}", table_name.name, "p2");
    let p2_qualified = format!("{}.{}", table_name.schema, p2_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (100) to (200) partition by range (sub_partition_key)",
            p2_qualified,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create sub-partitions of p2 (Level 3).
    let p2_sub1_name = format!("{}_{}", p2_name, "sub1");
    let p2_sub1_qualified = format!("{}.{}", table_name.schema, p2_sub1_name);
    database
        .run_sql(&format!(
            "create table {p2_sub1_qualified} partition of {p2_qualified} for values from (1) to (50)"
        ))
        .await
        .unwrap();

    let p2_sub2_name = format!("{}_{}", p2_name, "sub2");
    let p2_sub2_qualified = format!("{}.{}", table_name.schema, p2_sub2_name);
    database
        .run_sql(&format!(
            "create table {p2_sub2_qualified} partition of {p2_qualified} for values from (50) to (100)"
        ))
        .await
        .unwrap();

    // Create third partition that is itself partitioned (Level 2c).
    let p3_name = format!("{}_{}", table_name.name, "p3");
    let p3_qualified = format!("{}.{}", table_name.schema, p3_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (200) to (300) partition by range (sub_partition_key)",
            p3_qualified,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create sub-partitions of p3 (Level 3).
    let p3_sub1_name = format!("{}_{}", p3_name, "sub1");
    let p3_sub1_qualified = format!("{}.{}", table_name.schema, p3_sub1_name);
    database
        .run_sql(&format!(
            "create table {p3_sub1_qualified} partition of {p3_qualified} for values from (1) to (50)"
        ))
        .await
        .unwrap();

    let p3_sub2_name = format!("{}_{}", p3_name, "sub2");
    let p3_sub2_qualified = format!("{}.{}", table_name.schema, p3_sub2_name);
    database
        .run_sql(&format!(
            "create table {p3_sub2_qualified} partition of {p3_qualified} for values from (50) to (100)"
        ))
        .await
        .unwrap();

    // Create fourth partition (simple leaf partition) (Level 2d).
    let p4_name = format!("{}_{}", table_name.name, "p4");
    let p4_qualified = format!("{}.{}", table_name.schema, p4_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (300) to (400)",
            p4_qualified,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Insert initial data into all 6 leaf partitions:
    // - p1: partition_key=50
    // - p2_sub1: partition_key=150, sub_partition_key=25
    // - p2_sub2: partition_key=150, sub_partition_key=75
    // - p3_sub1: partition_key=250, sub_partition_key=25
    // - p3_sub2: partition_key=250, sub_partition_key=75
    // - p4: partition_key=350
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key, sub_partition_key) values
             ('event_p1', 50, 25),
             ('event_p2_sub1', 150, 25),
             ('event_p2_sub2', 150, 75),
             ('event_p3_sub1', 250, 25),
             ('event_p3_sub2', 250, 75),
             ('event_p4', 350, 25)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_nested_partitioned_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    // Register notification for initial copy completion.
    let parent_ready_notify = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
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

    parent_ready_notify.notified().await;

    // Verify table schema was discovered correctly for nested partitioned table.
    let table_schemas = state_store.get_table_schemas().await;
    assert!(table_schemas.contains_key(&parent_table_id));

    let parent_schema = &table_schemas[&parent_table_id];
    assert_eq!(parent_schema.id, parent_table_id);
    assert_eq!(parent_schema.name, table_name);

    // Verify columns are correctly discovered (includes sub_partition_key).
    assert_eq!(parent_schema.column_schemas.len(), 4);

    // Check id column (added by default).
    let id_column = &parent_schema.column_schemas[0];
    assert_eq!(id_column.name, "id");
    assert_eq!(id_column.typ, Type::INT8);
    assert!(!id_column.nullable);
    assert!(id_column.primary);

    // Check data column.
    let data_column = &parent_schema.column_schemas[1];
    assert_eq!(data_column.name, "data");
    assert_eq!(data_column.typ, Type::TEXT);
    assert!(!data_column.nullable);
    assert!(!data_column.primary);

    // Check partition_key column (part of primary key).
    let partition_key_column = &parent_schema.column_schemas[2];
    assert_eq!(partition_key_column.name, "partition_key");
    assert_eq!(partition_key_column.typ, Type::INT4);
    assert!(!partition_key_column.nullable);
    assert!(partition_key_column.primary);

    // Check sub_partition_key column (part of primary key for nested partitioning).
    let sub_partition_key_column = &parent_schema.column_schemas[3];
    assert_eq!(sub_partition_key_column.name, "sub_partition_key");
    assert_eq!(sub_partition_key_column.typ, Type::INT4);
    assert!(!sub_partition_key_column.nullable);
    assert!(sub_partition_key_column.primary);

    // Verify initial COPY replicated all 6 rows (one per leaf partition).
    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert_eq!(total_rows, 6);

    // Verify only the parent table is tracked (not intermediate or leaf partitions).
    let table_states = state_store.get_table_replication_states().await;
    assert!(table_states.contains_key(&parent_table_id));
    assert_eq!(table_states.len(), 1);

    // Verify all rows are attributed to the parent table.
    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 6);

    // Insert new rows into all 6 leaf partitions via CDC.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 6)])
        .await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key, sub_partition_key) values
             ('new_event_p1', 75, 30),
             ('new_event_p2_sub1', 125, 40),
             ('new_event_p2_sub2', 175, 60),
             ('new_event_p3_sub1', 225, 40),
             ('new_event_p3_sub2', 275, 60),
             ('new_event_p4', 350, 30)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify that CDC events were captured for all 6 leaf partitions.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 6);
}

/// Tests that the pipeline throws an error during startup when `publish_via_partition_root`
/// is set to `false` and the publication contains partitioned tables.
///
/// When `publish_via_partition_root = false`, logical replication messages contain child
/// partition OIDs instead of parent table OIDs. Since the pipeline's schema cache only
/// tracks parent table IDs, this configuration would cause pipeline failures when relation
/// messages arrive with unknown child OIDs.
///
/// The pipeline validates this configuration at startup and rejects it with a clear error
/// message instructing the user to enable `publish_via_partition_root`.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_with_publish_via_partition_root_false_and_partitioned_tables() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (_parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication_with_config(&publication_name, std::slice::from_ref(&table_name), false)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // The pipeline should fail to start due to invalid configuration.
    let err = pipeline.start().await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::ConfigError);
}

/// Tests that the pipeline doesn't throw an error when `publish_via_partition_root=false` and there
/// are no partitioned tables in the tables of the publication.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_with_publish_via_partition_root_false_and_no_partitioned_tables() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("non_partitioned_events");
    database
        .create_table(
            table_name.clone(),
            true,
            &[("description", "text not null")],
        )
        .await
        .unwrap();

    let publication_name = "test_non_partitioned_pub".to_string();
    database
        .create_publication_with_config(&publication_name, std::slice::from_ref(&table_name), false)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(), state_store.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // The pipeline should start and stop successfully.
    pipeline.start().await.unwrap();
    let result = pipeline.shutdown_and_wait().await;
    assert!(result.is_ok());
}
