use etl::conversions::Cell;
use etl::conversions::cdc_event::CdcEvent;
use postgres::schema::{ColumnSchema, TableId};
use postgres::tokio::test_utils::{PgDatabase, id_column_schema};
use std::ops::Range;
use telemetry::init_test_tracing;
use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination::TestDestination;
use crate::common::pipeline::{
    PipelineMode, spawn_async_pg_pipeline, spawn_pg_pipeline, test_slot_name,
};
use crate::common::table::assert_table_schema_from_destination;
use crate::common::wait_for_condition;

fn get_expected_ages_sum(num_users: usize) -> i32 {
    ((num_users * (num_users + 1)) / 2) as i32
}

async fn create_users_table<G: GenericClient>(database: &PgDatabase<G>) -> TableId {
    database
        .create_table(test_table_name("users"), &[("age", "integer")])
        .await
        .unwrap()
}

async fn create_users_table_with_publication<G: GenericClient>(
    database: &PgDatabase<G>,
    publication_name: &str,
) -> TableId {
    let table_id = create_users_table(database).await;

    database
        .create_publication(publication_name, &[test_table_name("users")])
        .await
        .unwrap();

    table_id
}

async fn fill_users<G: GenericClient>(database: &PgDatabase<G>, num_users: usize) {
    for i in 0..num_users {
        let age = i as i32 + 1;
        database
            .insert_values(test_table_name("users"), &["age"], &[&age])
            .await
            .unwrap();
    }
}

async fn double_users_ages<G: GenericClient>(database: &PgDatabase<G>) {
    database
        .update_values(test_table_name("users"), &["age"], &["age * 2"])
        .await
        .unwrap();
}

fn assert_users_table_schema(
    destination: &TestDestination,
    users_table_id: TableId,
    schema_index: usize,
) {
    let expected_columns = vec![
        id_column_schema(),
        ColumnSchema {
            name: "age".to_string(),
            typ: Type::INT4,
            modifier: -1,
            nullable: true,
            primary: false,
        },
    ];

    assert_table_schema_from_destination(
        destination,
        users_table_id,
        schema_index,
        test_table_name("users"),
        &expected_columns,
    );
}

fn get_users_age_sum_from_rows(destination: &TestDestination, users_table_id: TableId) -> i32 {
    let mut actual_sum = 0;

    let tables_rows = destination.get_tables_rows();
    let table_rows = tables_rows.get(&users_table_id).unwrap();
    for table_row in table_rows {
        if let Cell::I32(age) = &table_row.values[1] {
            actual_sum += age;
        }
    }

    actual_sum
}

fn get_users_age_sum_from_events(
    destination: &TestDestination,
    users_table_id: TableId,
    // We use a range since events are not indexed by table id but just an ordered sequence which
    // we want to slice through.
    range: Range<usize>,
) -> i32 {
    let mut actual_sum = 0;

    let mut i = 0;
    for event in destination.get_events() {
        match event.as_ref() {
            CdcEvent::Insert((table_id, table_row)) | CdcEvent::Update((table_id, table_row))
                if table_id == &users_table_id && range.contains(&i) =>
            {
                if let Cell::I32(age) = &table_row.values[1] {
                    actual_sum += age;
                }
                i += 1;
            }
            _ => {}
        }
    }

    actual_sum
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_with_insert_and_update() {
    init_test_tracing();
    let database = spawn_database().await;

    // We insert 100 rows.
    let users_table_id = create_users_table(&database).await;
    fill_users(&database, 100).await;

    // We create a pipeline that copies the users table.
    let mut pipeline = spawn_pg_pipeline(
        &database.config,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")],
        },
        TestDestination::new(),
    )
    .await;
    pipeline.start().await.unwrap();

    assert_users_table_schema(pipeline.destination(), users_table_id, 0);
    let expected_sum = get_expected_ages_sum(100);
    let actual_sum = get_users_age_sum_from_rows(pipeline.destination(), users_table_id);
    assert_eq!(actual_sum, expected_sum);
    assert_eq!(pipeline.destination().get_tables_copied(), 1);
    assert_eq!(pipeline.destination().get_tables_truncated(), 1);

    // We double the user ages.
    double_users_ages(&database).await;

    // We recreate the pipeline to copy again and see if we have the new data.
    let mut pipeline = spawn_pg_pipeline(
        &database.config,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")],
        },
        TestDestination::new(),
    )
    .await;
    pipeline.start().await.unwrap();

    assert_users_table_schema(pipeline.destination(), users_table_id, 0);
    let expected_sum = expected_sum * 2;
    let actual_sum = get_users_age_sum_from_rows(pipeline.destination(), users_table_id);
    assert_eq!(actual_sum, expected_sum);
    assert_eq!(pipeline.destination().get_tables_copied(), 1);
    assert_eq!(pipeline.destination().get_tables_truncated(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cdc_with_multiple_inserts() {
    init_test_tracing();
    let database = spawn_database().await;

    // We create the table and publication.
    let users_table_id = create_users_table_with_publication(&database, "users_publication").await;

    // We create a pipeline that subscribes to the changes of the users table.
    let destination = TestDestination::new();
    let mut pipeline = spawn_async_pg_pipeline(
        &database.config,
        PipelineMode::Cdc {
            publication: "users_publication".to_owned(),
            slot_name: test_slot_name("users_slot"),
        },
        destination.clone(),
    )
    .await;

    // We insert 100 rows.
    fill_users(&database, 100).await;

    // We run the pipeline in the background which should correctly pick up entries from the start
    // even though the pipeline was started after insertions.
    let pipeline_task_handle = pipeline.run().await;

    // Wait for all events to be processed.
    let expected_sum = get_expected_ages_sum(100);
    wait_for_condition(|| {
        let actual_sum = get_users_age_sum_from_events(&destination, users_table_id, 0..100);
        actual_sum == expected_sum
    })
    .await;

    // We stop the pipeline and wait for it to finish.
    pipeline.stop_and_wait(pipeline_task_handle).await;

    assert_users_table_schema(&destination, users_table_id, 0);
    assert_eq!(destination.get_tables_copied(), 0);
    assert_eq!(destination.get_tables_truncated(), 0);
}
