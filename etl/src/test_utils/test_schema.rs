use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::tokio::test_utils::{PgDatabase, id_column_schema};
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

#[cfg(feature = "bigquery")]
pub mod bigquery {
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use gcp_bigquery_client::model::table_cell::TableCell;
    use gcp_bigquery_client::model::table_row::TableRow;
    use std::fmt;
    use std::str::FromStr;

    use crate::conversions::numeric::PgNumeric;

    pub fn parse_table_cell<O>(table_cell: TableCell) -> Option<O>
    where
        O: FromStr,
        <O as FromStr>::Err: fmt::Debug,
    {
        table_cell
            .value
            .map(|value| value.as_str().unwrap().parse().unwrap())
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
                id: parse_table_cell(columns[0].clone()).unwrap(),
                name: parse_table_cell(columns[1].clone()).unwrap(),
                age: parse_table_cell(columns[2].clone()).unwrap(),
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
                id: parse_table_cell(columns[0].clone()).unwrap(),
                description: parse_table_cell(columns[1].clone()).unwrap(),
            }
        }
    }

    #[derive(Debug)]
    pub struct NullableColsScalar {
        id: i32,
        b: Option<bool>,
        t: Option<String>,
        i2: Option<i16>,
        i4: Option<i32>,
        i8: Option<i64>,
        f4: Option<f32>,
        f8: Option<f64>,
        n: Option<PgNumeric>,
        by: Option<Vec<u8>>,
        d: Option<NaiveDate>,
        ti: Option<NaiveTime>,
        ts: Option<NaiveDateTime>,
        tstz: Option<DateTime<Utc>>,
        u: Option<uuid::Uuid>,
        j: Option<serde_json::Value>,
        jb: Option<serde_json::Value>,
        o: Option<u32>,
    }

    impl NullableColsScalar {
        pub fn all_nulls(id: i32) -> Self {
            Self {
                id,
                b: None,
                t: None,
                i2: None,
                i4: None,
                i8: None,
                f4: None,
                f8: None,
                n: None,
                by: None,
                d: None,
                ti: None,
                ts: None,
                tstz: None,
                u: None,
                j: None,
                jb: None,
                o: None,
            }
        }

        #[expect(clippy::too_many_arguments)]
        pub fn with_non_null_values(
            id: i32,
            b: bool,
            t: String,
            i2: i16,
            i4: i32,
            i8: i64,
            f4: f32,
            f8: f64,
            n: PgNumeric,
            by: Vec<u8>,
            d: NaiveDate,
            ti: NaiveTime,
            ts: NaiveDateTime,
            tstz: DateTime<Utc>,
            u: uuid::Uuid,
            j: serde_json::Value,
            jb: serde_json::Value,
            o: u32,
        ) -> Self {
            Self {
                id,
                b: Some(b),
                t: Some(t),
                i2: Some(i2),
                i4: Some(i4),
                i8: Some(i8),
                f4: Some(f4),
                f8: Some(f8),
                n: Some(n),
                by: Some(by),
                d: Some(d),
                ti: Some(ti),
                ts: Some(ts),
                tstz: Some(tstz),
                u: Some(u),
                j: Some(j),
                jb: Some(jb),
                o: Some(o),
            }
        }
    }

    impl From<TableRow> for NullableColsScalar {
        fn from(value: TableRow) -> Self {
            let columns = value.columns.unwrap();

            // Helper function to parse JSON values
            fn parse_json_value(table_cell: TableCell) -> Option<serde_json::Value> {
                table_cell
                    .value
                    .and_then(|v| v.as_str().and_then(|s| serde_json::from_str(s).ok()))
            }

            // Helper function to parse byte arrays (decode from base64)
            fn parse_bytes(table_cell: TableCell) -> Option<Vec<u8>> {
                table_cell
                    .value
                    .and_then(|v| v.as_str().and_then(|s| BASE64_STANDARD.decode(s).ok()))
            }

            // Helper function to parse Unix timestamp from TableCell to DateTime<Utc>
            fn parse_unix_timestamp_from_cell(table_cell: TableCell) -> Option<DateTime<Utc>> {
                table_cell
                    .value
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .and_then(|s| s.parse::<f64>().ok())
                    .and_then(|timestamp| {
                        let secs = timestamp.trunc() as i64;
                        let nanos = ((timestamp.fract()) * 1_000_000_000.0).round() as u32;
                        DateTime::from_timestamp(secs, nanos)
                    })
            }

            // Helper function to parse Unix timestamp to NaiveDateTime
            fn parse_unix_timestamp_naive(table_cell: TableCell) -> Option<NaiveDateTime> {
                parse_unix_timestamp_from_cell(table_cell).map(|dt| dt.naive_utc())
            }

            // Helper function to parse Unix timestamp to DateTime<Utc>
            fn parse_unix_timestamp_utc(table_cell: TableCell) -> Option<DateTime<Utc>> {
                parse_unix_timestamp_from_cell(table_cell)
            }

            NullableColsScalar {
                id: parse_table_cell(columns[0].clone()).unwrap(),
                b: parse_table_cell(columns[1].clone()),
                t: parse_table_cell(columns[2].clone()),
                i2: parse_table_cell(columns[3].clone()),
                i4: parse_table_cell(columns[4].clone()),
                i8: parse_table_cell(columns[5].clone()),
                f4: parse_table_cell(columns[6].clone()),
                f8: parse_table_cell(columns[7].clone()),
                n: parse_table_cell(columns[8].clone()),
                by: parse_bytes(columns[9].clone()),
                d: parse_table_cell(columns[10].clone()),
                ti: parse_table_cell(columns[11].clone()),
                ts: parse_unix_timestamp_naive(columns[12].clone()),
                tstz: parse_unix_timestamp_utc(columns[13].clone()),
                u: parse_table_cell(columns[14].clone()),
                j: parse_json_value(columns[15].clone()),
                jb: parse_json_value(columns[16].clone()),
                o: parse_table_cell(columns[17].clone()),
            }
        }
    }

    impl PartialEq for NullableColsScalar {
        fn eq(&self, other: &Self) -> bool {
            // Helper function for float comparison with epsilon
            fn float_eq(a: Option<f32>, b: Option<f32>) -> bool {
                match (a, b) {
                    (Some(x), Some(y)) => (x - y).abs() < f32::EPSILON,
                    (None, None) => true,
                    _ => false,
                }
            }

            fn double_eq(a: Option<f64>, b: Option<f64>) -> bool {
                match (a, b) {
                    (Some(x), Some(y)) => (x - y).abs() < f64::EPSILON,
                    (None, None) => true,
                    _ => false,
                }
            }

            self.id == other.id
                && self.b == other.b
                && self.t == other.t
                && self.i2 == other.i2
                && self.i4 == other.i4
                && self.i8 == other.i8
                && float_eq(self.f4, other.f4)
                && double_eq(self.f8, other.f8)
                && self.n == other.n
                && self.by == other.by
                && self.d == other.d
                && self.ti == other.ti
                && self.ts == other.ts
                && self.tstz == other.tstz
                && self.u == other.u
                && self.j == other.j
                && self.jb == other.jb
                && self.o == other.o
        }
    }

    impl Eq for NullableColsScalar {}

    impl PartialOrd for NullableColsScalar {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for NullableColsScalar {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.id.cmp(&other.id)
        }
    }

    #[derive(Debug)]
    pub struct NullableColsArray {
        id: i32,
        b_arr: Option<Vec<bool>>,
        t_arr: Option<Vec<String>>,
        i2_arr: Option<Vec<i16>>,
        i4_arr: Option<Vec<i32>>,
        i8_arr: Option<Vec<i64>>,
        f4_arr: Option<Vec<f32>>,
        f8_arr: Option<Vec<f64>>,
        n_arr: Option<Vec<PgNumeric>>,
        by_arr: Option<Vec<Vec<u8>>>,
        d_arr: Option<Vec<NaiveDate>>,
        ti_arr: Option<Vec<NaiveTime>>,
        ts_arr: Option<Vec<NaiveDateTime>>,
        tstz_arr: Option<Vec<DateTime<Utc>>>,
        u_arr: Option<Vec<uuid::Uuid>>,
        j_arr: Option<Vec<serde_json::Value>>,
        jb_arr: Option<Vec<serde_json::Value>>,
        o_arr: Option<Vec<u32>>,
    }

    impl NullableColsArray {
        pub fn all_empty(id: i32) -> Self {
            Self {
                id,
                b_arr: Some(vec![]),
                t_arr: Some(vec![]),
                i2_arr: Some(vec![]),
                i4_arr: Some(vec![]),
                i8_arr: Some(vec![]),
                f4_arr: Some(vec![]),
                f8_arr: Some(vec![]),
                n_arr: Some(vec![]),
                by_arr: Some(vec![]),
                d_arr: Some(vec![]),
                ti_arr: Some(vec![]),
                ts_arr: Some(vec![]),
                tstz_arr: Some(vec![]),
                u_arr: Some(vec![]),
                j_arr: Some(vec![]),
                jb_arr: Some(vec![]),
                o_arr: Some(vec![]),
            }
        }

        #[expect(clippy::too_many_arguments)]
        pub fn with_non_null_values(
            id: i32,
            b_arr: Vec<bool>,
            t_arr: Vec<String>,
            i2_arr: Vec<i16>,
            i4_arr: Vec<i32>,
            i8_arr: Vec<i64>,
            f4_arr: Vec<f32>,
            f8_arr: Vec<f64>,
            n_arr: Vec<PgNumeric>,
            by_arr: Vec<Vec<u8>>,
            d_arr: Vec<NaiveDate>,
            ti_arr: Vec<NaiveTime>,
            ts_arr: Vec<NaiveDateTime>,
            tstz_arr: Vec<DateTime<Utc>>,
            u_arr: Vec<uuid::Uuid>,
            j_arr: Vec<serde_json::Value>,
            jb_arr: Vec<serde_json::Value>,
            o_arr: Vec<u32>,
        ) -> Self {
            Self {
                id,
                b_arr: Some(b_arr),
                t_arr: Some(t_arr),
                i2_arr: Some(i2_arr),
                i4_arr: Some(i4_arr),
                i8_arr: Some(i8_arr),
                f4_arr: Some(f4_arr),
                f8_arr: Some(f8_arr),
                n_arr: Some(n_arr),
                by_arr: Some(by_arr),
                d_arr: Some(d_arr),
                ti_arr: Some(ti_arr),
                ts_arr: Some(ts_arr),
                tstz_arr: Some(tstz_arr),
                u_arr: Some(u_arr),
                j_arr: Some(j_arr),
                jb_arr: Some(jb_arr),
                o_arr: Some(o_arr),
            }
        }
    }

    impl From<TableRow> for NullableColsArray {
        fn from(value: TableRow) -> Self {
            let columns = value.columns.unwrap();

            // Helper function to parse array values from BigQuery
            fn parse_array_cell<T>(table_cell: TableCell) -> Option<Vec<T>>
            where
                T: FromStr,
                <T as FromStr>::Err: fmt::Debug,
            {
                table_cell.value.and_then(|v| {
                    v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                item.as_object()
                                    .and_then(|obj| obj.get("v"))
                                    .and_then(|val| val.as_str())
                                    .and_then(|s| s.parse::<T>().ok())
                            })
                            .collect()
                    })
                })
            }

            // Helper function to parse array of JSON values
            fn parse_json_array(table_cell: TableCell) -> Option<Vec<serde_json::Value>> {
                table_cell.value.and_then(|v| {
                    v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                item.as_object()
                                    .and_then(|obj| obj.get("v"))
                                    .and_then(|val| val.as_str())
                                    .and_then(|s| serde_json::from_str(s).ok())
                            })
                            .collect()
                    })
                })
            }

            // Helper function to parse array of timestamps
            fn parse_timestamp_array(table_cell: TableCell) -> Option<Vec<DateTime<Utc>>> {
                table_cell.value.and_then(|v| {
                    v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                item.as_object()
                                    .and_then(|obj| obj.get("v"))
                                    .and_then(|val| val.as_str())
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .and_then(|timestamp| {
                                        let secs = timestamp.trunc() as i64;
                                        let nanos =
                                            ((timestamp.fract()) * 1_000_000_000.0).round() as u32;
                                        DateTime::from_timestamp(secs, nanos)
                                    })
                            })
                            .collect()
                    })
                })
            }

            // Helper function to parse array of naive datetimes
            fn parse_naive_datetime_array(table_cell: TableCell) -> Option<Vec<NaiveDateTime>> {
                parse_timestamp_array(table_cell)
                    .map(|timestamps| timestamps.into_iter().map(|dt| dt.naive_utc()).collect())
            }

            // Helper function to parse array of byte arrays (decode from base64)
            fn parse_bytes_array(table_cell: TableCell) -> Option<Vec<Vec<u8>>> {
                table_cell.value.and_then(|v| {
                    v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                item.as_object()
                                    .and_then(|obj| obj.get("v"))
                                    .and_then(|val| val.as_str())
                                    .and_then(|s| BASE64_STANDARD.decode(s).ok())
                            })
                            .collect()
                    })
                })
            }

            NullableColsArray {
                id: parse_table_cell(columns[0].clone()).unwrap(),
                b_arr: parse_array_cell(columns[1].clone()),
                t_arr: parse_array_cell(columns[2].clone()),
                i2_arr: parse_array_cell(columns[3].clone()),
                i4_arr: parse_array_cell(columns[4].clone()),
                i8_arr: parse_array_cell(columns[5].clone()),
                f4_arr: parse_array_cell(columns[6].clone()),
                f8_arr: parse_array_cell(columns[7].clone()),
                n_arr: parse_array_cell(columns[8].clone()),
                by_arr: parse_bytes_array(columns[9].clone()),
                d_arr: parse_array_cell(columns[10].clone()),
                ti_arr: parse_array_cell(columns[11].clone()),
                ts_arr: parse_naive_datetime_array(columns[12].clone()),
                tstz_arr: parse_timestamp_array(columns[13].clone()),
                u_arr: parse_array_cell(columns[14].clone()),
                j_arr: parse_json_array(columns[15].clone()),
                jb_arr: parse_json_array(columns[16].clone()),
                o_arr: parse_array_cell(columns[17].clone()),
            }
        }
    }

    impl PartialEq for NullableColsArray {
        fn eq(&self, other: &Self) -> bool {
            // Helper function for float array comparison with epsilon
            fn float_array_eq(a: &Option<Vec<f32>>, b: &Option<Vec<f32>>) -> bool {
                match (a, b) {
                    (Some(a_vec), Some(b_vec)) => {
                        a_vec.len() == b_vec.len()
                            && a_vec
                                .iter()
                                .zip(b_vec.iter())
                                .all(|(x, y)| (x - y).abs() < f32::EPSILON)
                    }
                    (None, None) => true,
                    _ => false,
                }
            }

            fn double_array_eq(a: &Option<Vec<f64>>, b: &Option<Vec<f64>>) -> bool {
                match (a, b) {
                    (Some(a_vec), Some(b_vec)) => {
                        a_vec.len() == b_vec.len()
                            && a_vec
                                .iter()
                                .zip(b_vec.iter())
                                .all(|(x, y)| (x - y).abs() < f64::EPSILON)
                    }
                    (None, None) => true,
                    _ => false,
                }
            }

            self.id == other.id
                && self.b_arr == other.b_arr
                && self.t_arr == other.t_arr
                && self.i2_arr == other.i2_arr
                && self.i4_arr == other.i4_arr
                && self.i8_arr == other.i8_arr
                && float_array_eq(&self.f4_arr, &other.f4_arr)
                && double_array_eq(&self.f8_arr, &other.f8_arr)
                && self.n_arr == other.n_arr
                && self.by_arr == other.by_arr
                && self.d_arr == other.d_arr
                && self.ti_arr == other.ti_arr
                && self.ts_arr == other.ts_arr
                && self.tstz_arr == other.tstz_arr
                && self.u_arr == other.u_arr
                && self.j_arr == other.j_arr
                && self.jb_arr == other.jb_arr
                && self.o_arr == other.o_arr
        }
    }

    impl Eq for NullableColsArray {}

    impl PartialOrd for NullableColsArray {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for NullableColsArray {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.id.cmp(&other.id)
        }
    }

    #[derive(Debug)]
    pub struct NonNullableColsScalar {
        id: i32,
        b: bool,
        t: String,
        i2: i16,
        i4: i32,
        i8: i64,
        f4: f32,
        f8: f64,
        n: PgNumeric,
        by: Vec<u8>,
        d: NaiveDate,
        ti: NaiveTime,
        ts: NaiveDateTime,
        tstz: DateTime<Utc>,
        u: uuid::Uuid,
        j: serde_json::Value,
        jb: serde_json::Value,
        o: u32,
    }

    impl NonNullableColsScalar {
        #[expect(clippy::too_many_arguments)]
        pub fn new(
            id: i32,
            b: bool,
            t: String,
            i2: i16,
            i4: i32,
            i8: i64,
            f4: f32,
            f8: f64,
            n: PgNumeric,
            by: Vec<u8>,
            d: NaiveDate,
            ti: NaiveTime,
            ts: NaiveDateTime,
            tstz: DateTime<Utc>,
            u: uuid::Uuid,
            j: serde_json::Value,
            jb: serde_json::Value,
            o: u32,
        ) -> Self {
            Self {
                id,
                b,
                t,
                i2,
                i4,
                i8,
                f4,
                f8,
                n,
                by,
                d,
                ti,
                ts,
                tstz,
                u,
                j,
                jb,
                o,
            }
        }
    }

    impl From<TableRow> for NonNullableColsScalar {
        fn from(value: TableRow) -> Self {
            let columns = value.columns.unwrap();

            // Helper function to parse JSON values
            fn parse_json_value(table_cell: TableCell) -> serde_json::Value {
                table_cell
                    .value
                    .and_then(|v| v.as_str().and_then(|s| serde_json::from_str(s).ok()))
                    .unwrap()
            }

            // Helper function to parse byte arrays (decode from base64)
            fn parse_bytes(table_cell: TableCell) -> Vec<u8> {
                table_cell
                    .value
                    .and_then(|v| v.as_str().and_then(|s| BASE64_STANDARD.decode(s).ok()))
                    .unwrap()
            }

            // Helper function to parse Unix timestamp from TableCell to DateTime<Utc>
            fn parse_unix_timestamp_from_cell(table_cell: TableCell) -> DateTime<Utc> {
                table_cell
                    .value
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .and_then(|s| s.parse::<f64>().ok())
                    .and_then(|timestamp| {
                        let secs = timestamp.trunc() as i64;
                        let nanos = ((timestamp.fract()) * 1_000_000_000.0).round() as u32;
                        DateTime::from_timestamp(secs, nanos)
                    })
                    .unwrap()
            }

            // Helper function to parse Unix timestamp to NaiveDateTime
            fn parse_unix_timestamp_naive(table_cell: TableCell) -> NaiveDateTime {
                parse_unix_timestamp_from_cell(table_cell).naive_utc()
            }

            NonNullableColsScalar {
                id: parse_table_cell(columns[0].clone()).unwrap(),
                b: parse_table_cell(columns[1].clone()).unwrap(),
                t: parse_table_cell(columns[2].clone()).unwrap(),
                i2: parse_table_cell(columns[3].clone()).unwrap(),
                i4: parse_table_cell(columns[4].clone()).unwrap(),
                i8: parse_table_cell(columns[5].clone()).unwrap(),
                f4: parse_table_cell(columns[6].clone()).unwrap(),
                f8: parse_table_cell(columns[7].clone()).unwrap(),
                n: parse_table_cell(columns[8].clone()).unwrap(),
                by: parse_bytes(columns[9].clone()),
                d: parse_table_cell(columns[10].clone()).unwrap(),
                ti: parse_table_cell(columns[11].clone()).unwrap(),
                ts: parse_unix_timestamp_naive(columns[12].clone()),
                tstz: parse_unix_timestamp_from_cell(columns[13].clone()),
                u: parse_table_cell(columns[14].clone()).unwrap(),
                j: parse_json_value(columns[15].clone()),
                jb: parse_json_value(columns[16].clone()),
                o: parse_table_cell(columns[17].clone()).unwrap(),
            }
        }
    }

    impl PartialEq for NonNullableColsScalar {
        fn eq(&self, other: &Self) -> bool {
            // Helper function for float comparison with epsilon
            fn float_eq(a: f32, b: f32) -> bool {
                (a - b).abs() < f32::EPSILON
            }

            fn double_eq(a: f64, b: f64) -> bool {
                (a - b).abs() < f64::EPSILON
            }

            self.id == other.id
                && self.b == other.b
                && self.t == other.t
                && self.i2 == other.i2
                && self.i4 == other.i4
                && self.i8 == other.i8
                && float_eq(self.f4, other.f4)
                && double_eq(self.f8, other.f8)
                && self.n == other.n
                && self.by == other.by
                && self.d == other.d
                && self.ti == other.ti
                && self.ts == other.ts
                && self.tstz == other.tstz
                && self.u == other.u
                && self.j == other.j
                && self.jb == other.jb
                && self.o == other.o
        }
    }

    impl Eq for NonNullableColsScalar {}

    impl PartialOrd for NonNullableColsScalar {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for NonNullableColsScalar {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.id.cmp(&other.id)
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
