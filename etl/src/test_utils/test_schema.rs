use etl_postgres::tokio::test_utils::{PgDatabase, id_column_schema};
use etl_postgres::types::{
    ColumnSchema, ReplicatedTableSchema, ReplicationMask, TableId, TableName, TableSchema,
};
use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio_postgres::types::{PgLsn, Type};
use tokio_postgres::{Client, GenericClient};

use crate::test_utils::database::{TEST_DATABASE_SCHEMA, test_table_name};
use crate::test_utils::test_destination_wrapper::TestDestinationWrapper;
use crate::types::{Cell, Event, InsertEvent, TableRow};

/// Creates a test column schema with sensible defaults.
fn test_column(
    name: &str,
    typ: Type,
    ordinal_position: i32,
    nullable: bool,
    primary_key: bool,
) -> ColumnSchema {
    ColumnSchema::new(
        name.to_string(),
        typ,
        -1,
        ordinal_position,
        if primary_key { Some(1) } else { None },
        nullable,
    )
}

#[derive(Debug, Clone, Copy)]
pub enum TableSelection {
    Both,
    UsersOnly,
    OrdersOnly,
}

#[derive(Debug)]
pub struct TestDatabaseSchema {
    users_table_schema: Option<TableSchema>,
    orders_table_schema: Option<TableSchema>,
    publication_name: String,
}

impl TestDatabaseSchema {
    pub fn publication_name(&self) -> String {
        self.publication_name.clone()
    }

    pub fn users_schema(&self) -> TableSchema {
        self.users_table_schema
            .clone()
            .expect("Users table schema not found")
    }

    pub fn orders_schema(&self) -> TableSchema {
        self.orders_table_schema
            .clone()
            .expect("Orders table schema not found")
    }

    pub fn schema() -> &'static str {
        TEST_DATABASE_SCHEMA
    }
}

pub async fn setup_test_database_schema<G: GenericClient>(
    database: &PgDatabase<G>,
    selection: TableSelection,
) -> TestDatabaseSchema {
    let mut tables_to_publish = Vec::new();
    let mut users_table_schema = None;
    let mut orders_table_schema = None;

    if matches!(selection, TableSelection::Both | TableSelection::UsersOnly) {
        let users_table_name = test_table_name("users");
        let users_table_id = database
            .create_table(
                users_table_name.clone(),
                true,
                &[("name", "text not null"), ("age", "integer not null")],
            )
            .await
            .expect("Failed to create users table");

        tables_to_publish.push(users_table_name.clone());

        users_table_schema = Some(TableSchema::new(
            users_table_id,
            users_table_name,
            vec![
                id_column_schema(),
                test_column("name", Type::TEXT, 2, false, false),
                test_column("age", Type::INT4, 3, false, false),
            ],
        ));
    }

    if matches!(selection, TableSelection::Both | TableSelection::OrdersOnly) {
        let orders_table_name = test_table_name("orders");
        let orders_table_id = database
            .create_table(
                orders_table_name.clone(),
                true,
                &[("description", "text not null")],
            )
            .await
            .expect("Failed to create orders table");

        tables_to_publish.push(orders_table_name.clone());

        orders_table_schema = Some(TableSchema::new(
            orders_table_id,
            orders_table_name,
            vec![
                id_column_schema(),
                test_column("description", Type::TEXT, 2, false, false),
            ],
        ));
    }

    // Create publication for selected tables
    let publication_name = "test_pub";
    database
        .create_publication(publication_name, &tables_to_publish)
        .await
        .expect("Failed to create publication");

    TestDatabaseSchema {
        users_table_schema,
        orders_table_schema,
        publication_name: publication_name.to_owned(),
    }
}

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
    let create_parent_query = format!(
        "create table {} (
            id bigserial,
            data text NOT NULL,
            partition_key integer NOT NULL,
            primary key (id, partition_key)
        ) partition by range (partition_key)",
        table_name.as_quoted_identifier()
    );

    database.run_sql(&create_parent_query).await?;

    let parent_row = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await?;

    let parent_table_id: TableId = parent_row.get(0);
    let mut partition_table_ids = Vec::new();

    for (partition_name, partition_constraint) in partition_specs {
        let partition_table_name = TableName::new(
            table_name.schema.clone(),
            format!("{}_{}", table_name.name, partition_name),
        );

        let create_partition_query = format!(
            "create table {} partition of {} for values {}",
            partition_table_name.as_quoted_identifier(),
            table_name.as_quoted_identifier(),
            partition_constraint
        );

        database.run_sql(&create_partition_query).await?;

        let partition_row = database
            .client
            .as_ref()
            .unwrap()
            .query_one(
                "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace
                 where n.nspname = $1 and c.relname = $2",
                &[&partition_table_name.schema, &partition_table_name.name],
            )
            .await?;

        let partition_table_id: TableId = partition_row.get(0);
        partition_table_ids.push(partition_table_id);
    }

    Ok((parent_table_id, partition_table_ids))
}

/// Inserts users data into the database for testing purposes.
pub async fn insert_users_data<G: GenericClient>(
    client: &mut PgDatabase<G>,
    users_table_name: &TableName,
    range: RangeInclusive<usize>,
) {
    for i in range {
        client
            .insert_values(
                users_table_name.clone(),
                &["name", "age"],
                &[&format!("user_{i}"), &(i as i32)],
            )
            .await
            .expect("Failed to insert users");
    }
}

/// Inserts orders data into the database for testing purposes.
pub async fn insert_orders_data<G: GenericClient>(
    client: &mut PgDatabase<G>,
    orders_table_name: &TableName,
    range: RangeInclusive<usize>,
) {
    for i in range {
        client
            .insert_values(
                orders_table_name.clone(),
                &["description"],
                &[&format!("description_{i}")],
            )
            .await
            .expect("Failed to insert orders");
    }
}

pub async fn insert_mock_data(
    database: &mut PgDatabase<Client>,
    users_table_name: &TableName,
    orders_table_name: &TableName,
    range: RangeInclusive<usize>,
    use_transaction: bool,
) {
    if use_transaction {
        let mut transaction = database.begin_transaction().await;

        insert_users_data(&mut transaction, users_table_name, range.clone()).await;
        insert_orders_data(&mut transaction, orders_table_name, range).await;

        transaction.commit_transaction().await;
    } else {
        insert_users_data(database, users_table_name, range.clone()).await;
        insert_orders_data(database, orders_table_name, range).await;
    }
}

pub async fn get_users_age_sum_from_rows<D>(
    destination: &TestDestinationWrapper<D>,
    table_id: TableId,
) -> i32 {
    let mut actual_sum = 0;

    let tables_rows = destination.get_table_rows().await;
    let table_rows = tables_rows.get(&table_id).unwrap();
    for table_row in table_rows {
        if let Cell::I32(age) = &table_row.values[2] {
            actual_sum += age;
        }
    }

    actual_sum
}

pub fn get_n_integers_sum(n: usize) -> i32 {
    ((n * (n + 1)) / 2) as i32
}

pub fn assert_events_equal(left: &[Event], right: &[Event]) {
    assert_eq!(left.len(), right.len());

    for (left, right) in left.iter().zip(right.iter()) {
        assert!(events_equal_excluding_fields(left, right));
    }
}

pub fn events_equal_excluding_fields(left: &Event, right: &Event) -> bool {
    match (left, right) {
        (Event::Begin(left), Event::Begin(right)) => {
            left.commit_lsn == right.commit_lsn
                && left.timestamp == right.timestamp
                && left.xid == right.xid
        }
        (Event::Commit(left), Event::Commit(right)) => {
            left.flags == right.flags
                && left.commit_lsn == right.commit_lsn
                && left.end_lsn == right.end_lsn
                && left.timestamp == right.timestamp
        }
        (Event::Insert(left), Event::Insert(right)) => {
            left.replicated_table_schema.id() == right.replicated_table_schema.id()
                && left.table_row == right.table_row
        }
        (Event::Update(left), Event::Update(right)) => {
            left.replicated_table_schema.id() == right.replicated_table_schema.id()
                && left.table_row == right.table_row
                && left.old_table_row == right.old_table_row
        }
        (Event::Delete(left), Event::Delete(right)) => {
            left.replicated_table_schema.id() == right.replicated_table_schema.id()
                && left.old_table_row == right.old_table_row
        }
        (Event::Truncate(left), Event::Truncate(right)) => {
            if left.options != right.options
                || left.truncated_tables.len() != right.truncated_tables.len()
            {
                return false;
            }
            // Compare table IDs of truncated tables
            let left_ids: Vec<_> = left.truncated_tables.iter().map(|s| s.id()).collect();
            let right_ids: Vec<_> = right.truncated_tables.iter().map(|s| s.id()).collect();
            left_ids == right_ids
        }
        (Event::Unsupported, Event::Unsupported) => true,
        _ => false, // Different event types
    }
}

pub fn build_expected_users_inserts(
    mut starting_id: i64,
    users_table_schema: &TableSchema,
    expected_rows: Vec<(&str, i32)>,
) -> Vec<Event> {
    let mut events = Vec::new();

    // We build the replicated table schema with a mask for all columns.
    let users_table_column_names = users_table_schema
        .column_schemas
        .iter()
        .map(|c| c.name.clone())
        .collect::<HashSet<_>>();
    let replicated_table_schema = ReplicatedTableSchema::from_mask(
        Arc::new(users_table_schema.clone()),
        ReplicationMask::build_or_all(users_table_schema, &users_table_column_names),
    );

    for (name, age) in expected_rows {
        events.push(Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(0),
            replicated_table_schema: replicated_table_schema.clone(),
            table_row: TableRow {
                values: vec![
                    Cell::I64(starting_id),
                    Cell::String(name.to_owned()),
                    Cell::I32(age),
                ],
            },
        }));

        starting_id += 1;
    }

    events
}

pub fn build_expected_orders_inserts(
    mut starting_id: i64,
    orders_table_schema: &TableSchema,
    expected_rows: Vec<&str>,
) -> Vec<Event> {
    let mut events = Vec::new();

    // We build the replicated table schema with a mask for all columns.
    let orders_table_column_names = orders_table_schema
        .column_schemas
        .iter()
        .map(|c| c.name.clone())
        .collect::<HashSet<_>>();
    let replicated_table_schema = ReplicatedTableSchema::from_mask(
        Arc::new(orders_table_schema.clone()),
        ReplicationMask::build_or_all(orders_table_schema, &orders_table_column_names),
    );

    for name in expected_rows {
        events.push(Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(0),
            replicated_table_schema: replicated_table_schema.clone(),
            table_row: TableRow {
                values: vec![Cell::I64(starting_id), Cell::String(name.to_owned())],
            },
        }));

        starting_id += 1;
    }

    events
}
