use etl_postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use etl_postgres::tokio::test_utils::{PgDatabase, id_column_schema};
use std::ops::RangeInclusive;
use tokio_postgres::types::{PgLsn, Type};
use tokio_postgres::{Client, GenericClient};

use crate::conversions::Cell;
use crate::conversions::event::{Event, InsertEvent};
use crate::conversions::table_row::TableRow;
use crate::test_utils::database::test_table_name;
use crate::test_utils::test_destination_wrapper::TestDestinationWrapper;

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
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    typ: Type::INT4,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
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
                ColumnSchema {
                    name: "description".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
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
            left.table_id == right.table_id && left.table_row == right.table_row
        }
        (Event::Update(left), Event::Update(right)) => {
            left.table_id == right.table_id
                && left.table_row == right.table_row
                && left.old_table_row == right.old_table_row
        }
        (Event::Delete(left), Event::Delete(right)) => {
            left.table_id == right.table_id && left.old_table_row == right.old_table_row
        }
        (Event::Relation(left), Event::Relation(right)) => left.table_schema == right.table_schema,
        (Event::Truncate(left), Event::Truncate(right)) => {
            left.options == right.options && left.rel_ids == right.rel_ids
        }
        (Event::Unsupported, Event::Unsupported) => true,
        _ => false, // Different event types
    }
}

pub fn build_expected_users_inserts(
    mut starting_id: i64,
    users_table_id: TableId,
    expected_rows: Vec<(&str, i32)>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for (name, age) in expected_rows {
        events.push(Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(0),
            table_id: users_table_id,
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
    orders_table_id: TableId,
    expected_rows: Vec<&str>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for name in expected_rows {
        events.push(Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(0),
            table_id: orders_table_id,
            table_row: TableRow {
                values: vec![Cell::I64(starting_id), Cell::String(name.to_owned())],
            },
        }));

        starting_id += 1;
    }

    events
}
