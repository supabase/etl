use etl::conversions::Cell;
use etl::conversions::event::{Event, InsertEvent};
use etl::conversions::table_row::TableRow;
use postgres::schema::{ColumnSchema, Oid, TableName, TableSchema};
use postgres::tokio::test_utils::{PgDatabase, id_column_schema};
use std::ops::RangeInclusive;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use crate::common::database::test_table_name;
use crate::common::test_destination_wrapper::TestDestinationWrapper;

#[derive(Debug, Clone, Copy)]
pub enum TableSelection {
    Both,
    #[allow(dead_code)]
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

pub async fn insert_mock_data(
    database: &mut PgDatabase<Client>,
    users_table_name: &TableName,
    orders_table_name: &TableName,
    range: RangeInclusive<usize>,
    use_transaction: bool,
) {
    if use_transaction {
        let mut transaction = database.begin_transaction().await;

        // Insert users with deterministic data.
        for i in range.clone() {
            transaction
                .insert_values(
                    users_table_name.clone(),
                    &["name", "age"],
                    &[&format!("user_{i}"), &(i as i32)],
                )
                .await
                .expect("Failed to insert users");
        }

        // Insert orders with deterministic data.
        for i in range {
            transaction
                .insert_values(
                    orders_table_name.clone(),
                    &["description"],
                    &[&format!("description_{i}")],
                )
                .await
                .expect("Failed to insert orders");
        }

        // Commit the transaction.
        transaction.commit_transaction().await;
    } else {
        // Insert users with deterministic data.
        for i in range.clone() {
            database
                .insert_values(
                    users_table_name.clone(),
                    &["name", "age"],
                    &[&format!("user_{i}"), &(i as i32)],
                )
                .await
                .expect("Failed to insert users");
        }

        // Insert orders with deterministic data.
        for i in range {
            database
                .insert_values(
                    orders_table_name.clone(),
                    &["description"],
                    &[&format!("description_{i}")],
                )
                .await
                .expect("Failed to insert orders");
        }
    }
}

pub async fn get_users_age_sum_from_rows<D>(
    destination: &TestDestinationWrapper<D>,
    table_id: Oid,
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

pub fn build_expected_users_inserts(
    mut starting_id: i64,
    users_table_id: Oid,
    expected_rows: Vec<(&str, i32)>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for (name, age) in expected_rows {
        events.push(Event::Insert(InsertEvent {
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
    orders_table_id: Oid,
    expected_rows: Vec<&str>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for name in expected_rows {
        events.push(Event::Insert(InsertEvent {
            table_id: orders_table_id,
            table_row: TableRow {
                values: vec![Cell::I64(starting_id), Cell::String(name.to_owned())],
            },
        }));

        starting_id += 1;
    }

    events
}

#[cfg(feature = "bigquery")]
pub mod bigquery {
    use gcp_bigquery_client::model::table_cell::TableCell;
    use gcp_bigquery_client::model::table_row::TableRow;
    use std::fmt;
    use std::str::FromStr;

    pub fn parse_table_cell<O>(table_cell: TableCell) -> O
    where
        O: FromStr,
        <O as FromStr>::Err: fmt::Debug,
    {
        table_cell.value.unwrap().as_str().unwrap().parse().unwrap()
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    pub struct BigQueryUser {
        id: i32,
        name: String,
        age: i32,
    }

    impl BigQueryUser {
        pub fn new(id: i32, name: &str, age: i32) -> Self {
            Self {
                id,
                name: name.to_owned(),
                age,
            }
        }
    }

    impl From<TableRow> for BigQueryUser {
        fn from(value: TableRow) -> Self {
            let columns = value.columns.unwrap();

            BigQueryUser {
                id: parse_table_cell(columns[0].clone()),
                name: parse_table_cell(columns[1].clone()),
                age: parse_table_cell(columns[2].clone()),
            }
        }
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    pub struct BigQueryOrder {
        id: i32,
        description: String,
    }

    impl BigQueryOrder {
        pub fn new(id: i32, description: &str) -> Self {
            Self {
                id,
                description: description.to_owned(),
            }
        }
    }

    impl From<TableRow> for BigQueryOrder {
        fn from(value: TableRow) -> Self {
            let columns = value.columns.unwrap();

            BigQueryOrder {
                id: parse_table_cell(columns[0].clone()),
                description: parse_table_cell(columns[1].clone()),
            }
        }
    }

    pub fn parse_bigquery_table_rows<T>(table_rows: Vec<TableRow>) -> Vec<T>
    where
        T: Ord,
        T: From<TableRow>,
    {
        let mut parsed_table_rows = Vec::with_capacity(table_rows.len());

        for table_row in table_rows {
            parsed_table_rows.push(table_row.into());
        }
        parsed_table_rows.sort();

        parsed_table_rows
    }
}
