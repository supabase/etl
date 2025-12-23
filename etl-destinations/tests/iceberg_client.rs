#![cfg(feature = "iceberg")]

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{ArrayCell, Cell, ColumnSchema, TableRow, Type};
use etl_destinations::iceberg::IcebergClient;
use etl_telemetry::tracing::init_test_tracing;
use uuid::Uuid;

/// Creates a test column schema with common defaults.
///
/// This helper simplifies column schema creation in tests by providing sensible
/// defaults for fields that are typically not relevant to the test logic.
fn test_column(
    name: &str,
    typ: Type,
    ordinal_position: i32,
    nullable: bool,
    primary_key_ordinal_position: Option<i32>,
) -> ColumnSchema {
    ColumnSchema::new(
        name.to_string(),
        typ,
        -1,
        ordinal_position,
        primary_key_ordinal_position,
        nullable,
    )
}

use crate::support::{
    iceberg::{LAKEKEEPER_URL, create_props, get_catalog_url, read_all_rows},
    lakekeeper::LakekeeperClient,
};

mod support;

#[tokio::test]
async fn create_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    let namespace = "test_namespace";

    // namespace doesn't exist yet
    assert!(!client.namespace_exists(namespace).await.unwrap());

    // create namespace for the first time
    client.create_namespace_if_missing(namespace).await.unwrap();
    // namespace should exist now
    assert!(client.namespace_exists(namespace).await.unwrap());

    // trying to create an existing namespace is a no-op
    client.create_namespace_if_missing(namespace).await.unwrap();
    // namespace still exists
    assert!(client.namespace_exists(namespace).await.unwrap());

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_hierarchical_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    let root_namespace = "root_namespace";

    // create namespace for the first time
    client
        .create_namespace_if_missing(root_namespace)
        .await
        .unwrap();
    // root namespace should exist now
    assert!(client.namespace_exists(root_namespace).await.unwrap());

    let child_namespace = &format!("{root_namespace}.child_namespace");

    client
        .create_namespace_if_missing(child_namespace)
        .await
        .unwrap();
    // child namespace should exist now
    assert!(client.namespace_exists(child_namespace).await.unwrap());

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_namespace(child_namespace).await.unwrap();
    client.drop_namespace(root_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_table_if_missing() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types
    let table_name = "test_table".to_string();
    let column_schemas = vec![
        // Primary key
        test_column("id", Type::INT4, 1, false, Some(1)),
        // Boolean types
        test_column("bool_col", Type::BOOL, 2, true, None),
        // String types
        test_column("char_col", Type::CHAR, 3, true, None),
        test_column("bpchar_col", Type::BPCHAR, 4, true, None),
        test_column("varchar_col", Type::VARCHAR, 5, true, None),
        test_column("name_col", Type::NAME, 6, true, None),
        test_column("text_col", Type::TEXT, 7, true, None),
        // Integer types
        test_column("int2_col", Type::INT2, 8, true, None),
        test_column("int4_col", Type::INT4, 9, true, None),
        test_column("int8_col", Type::INT8, 10, true, None),
        // Float types
        test_column("float4_col", Type::FLOAT4, 11, true, None),
        test_column("float8_col", Type::FLOAT8, 12, true, None),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, 13, true, None),
        // Date/Time types
        test_column("date_col", Type::DATE, 14, true, None),
        test_column("time_col", Type::TIME, 15, true, None),
        test_column("timestamp_col", Type::TIMESTAMP, 16, true, None),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, 17, true, None),
        // UUID type
        test_column("uuid_col", Type::UUID, 18, true, None),
        // JSON types
        test_column("json_col", Type::JSON, 19, true, None),
        test_column("jsonb_col", Type::JSONB, 20, true, None),
        // OID type
        test_column("oid_col", Type::OID, 21, true, None),
        // Binary type
        test_column("bytea_col", Type::BYTEA, 22, true, None),
        // Array types
        test_column("bool_array_col", Type::BOOL_ARRAY, 23, true, None),
        test_column("char_array_col", Type::CHAR_ARRAY, 24, true, None),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, 25, true, None),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, 26, true, None),
        test_column("name_array_col", Type::NAME_ARRAY, 27, true, None),
        test_column("text_array_col", Type::TEXT_ARRAY, 28, true, None),
        test_column("int2_array_col", Type::INT2_ARRAY, 29, true, None),
        test_column("int4_array_col", Type::INT4_ARRAY, 30, true, None),
        test_column("int8_array_col", Type::INT8_ARRAY, 31, true, None),
        test_column("float4_array_col", Type::FLOAT4_ARRAY, 32, true, None),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, 33, true, None),
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, 34, true, None),
        test_column("date_array_col", Type::DATE_ARRAY, 35, true, None),
        test_column("time_array_col", Type::TIME_ARRAY, 36, true, None),
        test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, 37, true, None),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            38,
            true,
            None,
        ),
        test_column("uuid_array_col", Type::UUID_ARRAY, 39, true, None),
        test_column("json_array_col", Type::JSON_ARRAY, 40, true, None),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, 41, true, None),
        test_column("oid_array_col", Type::OID_ARRAY, 42, true, None),
        test_column("bytea_array_col", Type::BYTEA_ARRAY, 43, true, None),
    ];

    // table doesn't exist yet
    assert!(
        !client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Create table for the first time
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // table should exist now
    assert!(
        client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Verify identifier fields are set correctly
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();
    let identifier_field_ids: Vec<i32> = table
        .metadata()
        .current_schema()
        .identifier_field_ids()
        .collect();
    // The "id" column is the primary key and should be the only identifier field (field_id = 1)
    assert_eq!(identifier_field_ids, vec![1]);

    // Creating the same table again should be a no-op (no error)
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // table should still exist
    assert!(
        client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_table_if_exists_is_idempotent() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a simple table schema
    let table_name = "test_table".to_string();
    let column_schemas = vec![test_column("id", Type::INT4, 1, false, Some(1))];

    // Create table
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // Table should exist
    assert!(
        client
            .table_exists(namespace, table_name.clone())
            .await
            .unwrap()
    );

    // First drop should succeed and return true (table was dropped)
    let dropped = client
        .drop_table_if_exists(namespace, table_name.clone())
        .await
        .unwrap();
    assert!(dropped);

    // Table should no longer exist
    assert!(
        !client
            .table_exists(namespace, table_name.clone())
            .await
            .unwrap()
    );

    // Second drop should succeed and return false (table didn't exist)
    let dropped = client
        .drop_table_if_exists(namespace, table_name.clone())
        .await
        .unwrap();
    assert!(!dropped);

    // Dropping a table that never existed should also succeed and return false
    let dropped = client
        .drop_table_if_exists(namespace, "nonexistent_table".to_string())
        .await
        .unwrap();
    assert!(!dropped);

    // Manual cleanup
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_nullable_scalars() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types
    let table_name = "test_table".to_string();
    let column_schemas = vec![
        // Primary key
        test_column("id", Type::INT4, 1, false, Some(1)),
        // Boolean types
        test_column("bool_col", Type::BOOL, 2, true, None),
        // String types
        test_column("char_col", Type::CHAR, 3, true, None),
        test_column("bpchar_col", Type::BPCHAR, 4, true, None),
        test_column("varchar_col", Type::VARCHAR, 5, true, None),
        test_column("name_col", Type::NAME, 6, true, None),
        test_column("text_col", Type::TEXT, 7, true, None),
        // Integer types
        test_column("int2_col", Type::INT2, 8, true, None),
        test_column("int4_col", Type::INT4, 9, true, None),
        test_column("int8_col", Type::INT8, 10, true, None),
        // Float types
        test_column("float4_col", Type::FLOAT4, 11, true, None),
        test_column("float8_col", Type::FLOAT8, 12, true, None),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, 13, true, None),
        // Date/Time types
        test_column("date_col", Type::DATE, 14, true, None),
        test_column("time_col", Type::TIME, 15, true, None),
        test_column("timestamp_col", Type::TIMESTAMP, 16, true, None),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, 17, true, None),
        // UUID type
        test_column("uuid_col", Type::UUID, 18, true, None),
        // JSON types
        test_column("json_col", Type::JSON, 19, true, None),
        test_column("jsonb_col", Type::JSONB, 20, true, None),
        // OID type
        test_column("oid_col", Type::OID, 21, true, None),
        // Binary type
        test_column("bytea_col", Type::BYTEA, 22, true, None),
    ];

    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    let mut table_rows = vec![
        TableRow {
            values: vec![
                Cell::I32(42),                                              // id
                Cell::Bool(true),                                           // bool_col
                Cell::String("A".to_string()),                              // char_col
                Cell::String("fixed".to_string()),                          // bpchar_col
                Cell::String("variable".to_string()),                       // varchar_col
                Cell::String("name_value".to_string()),                     // name_col
                Cell::String("test string".to_string()),                    // text_col
                Cell::I16(123), // int2_col (maps to Int in Iceberg, comes back as I32) TODO:fix this
                Cell::I32(456), // int4_col
                Cell::I64(9876543210), // int8_col
                Cell::F32(std::f32::consts::PI), // float4_col
                Cell::F64(std::f64::consts::E), // float8_col
                Cell::String("123.456".to_string()), // numeric_col (maps to String in Iceberg)
                Cell::Date(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap()), // date_col
                Cell::Time(NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap()),
                Cell::Timestamp(NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 12, 25).unwrap(),
                    NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap(),
                )),
                Cell::TimestampTz(DateTime::<Utc>::from_naive_utc_and_offset(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2023, 12, 25).unwrap(),
                        NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap(),
                    ),
                    Utc,
                )),
                Cell::Uuid(Uuid::new_v4()),
                Cell::String(r#"{"key": "value"}"#.to_string()), // json_col (maps to String in Iceberg)
                Cell::String(r#"{"key": "value"}"#.to_string()), // jsonb_col (maps to String in Iceberg)
                Cell::U32(12345),                                // oid_col (maps to Int in Iceberg)
                Cell::Bytes(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]), // bytea_col (Hello in bytes)
            ],
        },
        TableRow {
            values: vec![
                Cell::I32(0),
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        },
    ];
    client
        .insert_rows(
            namespace.to_string(),
            table_name.clone(),
            table_rows.clone(),
        )
        .await
        .unwrap();

    // Change the expected type due to roundtrip issues
    // * Cell::I16 rountrips to Cell::I32,
    // * Cell::U32 rountrips to Cell::I64,
    let row = &mut table_rows[0];
    row.values[7] = Cell::I32(123);
    row.values[20] = Cell::I64(12345);

    let read_rows = read_all_rows(&client, namespace.to_string(), table_name.clone()).await;

    // Compare the actual values in the read_rows with inserted table_rows
    assert_eq!(read_rows, table_rows);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_non_nullable_scalars() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types as non-nullable
    let table_name = "test_table".to_string();
    let column_schemas = vec![
        // Primary key
        test_column("id", Type::INT4, 1, false, Some(1)),
        // Boolean types
        test_column("bool_col", Type::BOOL, 2, false, None),
        // String types
        test_column("char_col", Type::CHAR, 3, false, None),
        test_column("bpchar_col", Type::BPCHAR, 4, false, None),
        test_column("varchar_col", Type::VARCHAR, 5, false, None),
        test_column("name_col", Type::NAME, 6, false, None),
        test_column("text_col", Type::TEXT, 7, false, None),
        // Integer types
        test_column("int2_col", Type::INT2, 8, false, None),
        test_column("int4_col", Type::INT4, 9, false, None),
        test_column("int8_col", Type::INT8, 10, false, None),
        // Float types
        test_column("float4_col", Type::FLOAT4, 11, false, None),
        test_column("float8_col", Type::FLOAT8, 12, false, None),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, 13, false, None),
        // Date/Time types
        test_column("date_col", Type::DATE, 14, false, None),
        test_column("time_col", Type::TIME, 15, false, None),
        test_column("timestamp_col", Type::TIMESTAMP, 16, false, None),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, 17, false, None),
        // UUID type
        test_column("uuid_col", Type::UUID, 18, false, None),
        // JSON types
        test_column("json_col", Type::JSON, 19, false, None),
        test_column("jsonb_col", Type::JSONB, 20, false, None),
        // OID type
        test_column("oid_col", Type::OID, 21, false, None),
        // Binary type
        test_column("bytea_col", Type::BYTEA, 22, false, None),
    ];

    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    let mut table_rows = vec![TableRow {
        values: vec![
            Cell::I32(42),                                              // id
            Cell::Bool(true),                                           // bool_col
            Cell::String("A".to_string()),                              // char_col
            Cell::String("fixed".to_string()),                          // bpchar_col
            Cell::String("variable".to_string()),                       // varchar_col
            Cell::String("name_value".to_string()),                     // name_col
            Cell::String("test string".to_string()),                    // text_col
            Cell::I16(123), // int2_col (maps to Int in Iceberg, comes back as I32) TODO:fix this
            Cell::I32(456), // int4_col
            Cell::I64(9876543210), // int8_col
            Cell::F32(std::f32::consts::PI), // float4_col
            Cell::F64(std::f64::consts::E), // float8_col
            Cell::String("123.456".to_string()), // numeric_col (maps to String in Iceberg)
            Cell::Date(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap()), // date_col
            Cell::Time(NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap()),
            Cell::Timestamp(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 12, 25).unwrap(),
                NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap(),
            )),
            Cell::TimestampTz(DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 12, 25).unwrap(),
                    NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap(),
                ),
                Utc,
            )),
            Cell::Uuid(Uuid::new_v4()),
            Cell::String(r#"{"key": "value"}"#.to_string()), // json_col (maps to String in Iceberg)
            Cell::String(r#"{"key": "value"}"#.to_string()), // jsonb_col (maps to String in Iceberg)
            Cell::U32(12345),                                // oid_col (maps to Int in Iceberg)
            Cell::Bytes(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]), // bytea_col (Hello in bytes)
        ],
    }];
    client
        .insert_rows(
            namespace.to_string(),
            table_name.clone(),
            table_rows.clone(),
        )
        .await
        .unwrap();

    // Change the expected type due to roundtrip issues
    // * Cell::I16 rountrips to Cell::I32,
    // * Cell::U32 rountrips to Cell::I64,
    let row = &mut table_rows[0];
    row.values[7] = Cell::I32(123);
    row.values[20] = Cell::I64(12345);

    let read_rows = read_all_rows(&client, namespace.to_string(), table_name.clone()).await;

    assert_eq!(read_rows.len(), 1);

    // Compare the actual values in the read_rows with inserted table_rows
    assert_eq!(read_rows, table_rows);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_nullable_array() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with array types for all supported types
    let table_name = "test_array_table".to_string();
    let column_schemas = vec![
        // Primary key
        test_column("id", Type::INT4, 1, false, Some(1)),
        // Boolean array type
        test_column("bool_array_col", Type::BOOL_ARRAY, 2, true, None),
        // String array types
        test_column("char_array_col", Type::CHAR_ARRAY, 3, true, None),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, 4, true, None),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, 5, true, None),
        test_column("name_array_col", Type::NAME_ARRAY, 6, true, None),
        test_column("text_array_col", Type::TEXT_ARRAY, 7, true, None),
        // Integer array types
        test_column("int2_array_col", Type::INT2_ARRAY, 8, true, None),
        test_column("int4_array_col", Type::INT4_ARRAY, 9, true, None),
        test_column("int8_array_col", Type::INT8_ARRAY, 10, true, None),
        // Float array types
        test_column("float4_array_col", Type::FLOAT4_ARRAY, 11, true, None),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, 12, true, None),
        // Numeric array type
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, 13, true, None),
        // Date/Time array types
        test_column("date_array_col", Type::DATE_ARRAY, 14, true, None),
        test_column("time_array_col", Type::TIME_ARRAY, 15, true, None),
        test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, 16, true, None),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            17,
            true,
            None,
        ),
        // UUID array type
        test_column("uuid_array_col", Type::UUID_ARRAY, 18, true, None),
        // JSON array types
        test_column("json_array_col", Type::JSON_ARRAY, 19, true, None),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, 20, true, None),
        // OID array type
        test_column("oid_array_col", Type::OID_ARRAY, 21, true, None),
        // Binary array type
        test_column("bytea_array_col", Type::BYTEA_ARRAY, 22, true, None),
    ];

    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    let table_rows = vec![
        TableRow {
            values: vec![
                Cell::I32(1),                                                            // id
                Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false), Some(true)])), // bool_array_col
                Cell::Array(ArrayCell::String(vec![
                    Some("A".to_string()),
                    Some("B".to_string()),
                ])), // char_array_col
                Cell::Array(ArrayCell::String(vec![
                    Some("fix1".to_string()),
                    Some("fix2".to_string()),
                ])), // bpchar_array_col
                Cell::Array(ArrayCell::String(vec![
                    Some("var1".to_string()),
                    Some("var2".to_string()),
                ])), // varchar_array_col
                Cell::Array(ArrayCell::String(vec![
                    Some("name1".to_string()),
                    Some("name2".to_string()),
                ])), // name_array_col
                Cell::Array(ArrayCell::String(vec![
                    Some("text1".to_string()),
                    Some("text2".to_string()),
                ])), // text_array_col
                Cell::Array(ArrayCell::I16(vec![Some(1), Some(2), Some(3)])), // int2_array_col
                Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), Some(30)])), // int4_array_col
                Cell::Array(ArrayCell::I64(vec![Some(100), Some(200), Some(300)])), // int8_array_col
                Cell::Array(ArrayCell::F32(vec![Some(1.5), Some(2.5), Some(3.5)])), // float4_array_col
                Cell::Array(ArrayCell::F64(vec![Some(10.5), Some(20.5), Some(30.5)])), // float8_array_col
                Cell::Array(ArrayCell::Numeric(vec![
                    Some("123.45".parse().unwrap()),
                    Some("678.90".parse().unwrap()),
                ])), // numeric_array_col
                Cell::Array(ArrayCell::Date(vec![
                    Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
                    Some(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()),
                ])), // date_array_col
                Cell::Array(ArrayCell::Time(vec![
                    Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
                    Some(NaiveTime::from_hms_opt(17, 30, 0).unwrap()),
                ])), // time_array_col
                Cell::Array(ArrayCell::Timestamp(vec![
                    Some(NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
                        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                    )),
                    Some(NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2023, 6, 16).unwrap(),
                        NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
                    )),
                ])), // timestamp_array_col
                Cell::Array(ArrayCell::TimestampTz(vec![
                    Some(DateTime::<Utc>::from_naive_utc_and_offset(
                        NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
                            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                        ),
                        Utc,
                    )),
                    Some(DateTime::<Utc>::from_naive_utc_and_offset(
                        NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(2023, 6, 16).unwrap(),
                            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
                        ),
                        Utc,
                    )),
                ])), // timestamptz_array_col
                Cell::Array(ArrayCell::Uuid(vec![
                    Some(Uuid::new_v4()),
                    Some(Uuid::new_v4()),
                ])), // uuid_array_col
                Cell::Array(ArrayCell::Json(vec![
                    Some(serde_json::json!({"key1": "value1"})),
                    Some(serde_json::json!({"key2": "value2"})),
                ])), // json_array_col
                Cell::Array(ArrayCell::Json(vec![
                    Some(serde_json::json!({"keyb1": "valueb1"})),
                    Some(serde_json::json!({"keyb2": "valueb2"})),
                ])), // jsonb_array_col
                Cell::Array(ArrayCell::U32(vec![Some(1001), Some(1002)])), // oid_array_col
                Cell::Array(ArrayCell::Bytes(vec![
                    Some(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]),
                    Some(vec![0x57, 0x6f, 0x72, 0x6c, 0x64]),
                ])), // bytea_array_col
            ],
        },
        TableRow {
            values: vec![
                Cell::I32(2), // id
                Cell::Null,   // All other columns are null
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        },
    ];

    client
        .insert_rows(
            namespace.to_string(),
            table_name.clone(),
            table_rows.clone(),
        )
        .await
        .unwrap();

    // Create expected rows with proper type conversions for roundtrip
    let mut expected_rows = table_rows.clone();

    // Convert array types that round-trip through different representations
    let TableRow { values } = &mut expected_rows[0];

    // int2_array_col (index 7): Convert I16 to I32
    if let Cell::Array(ArrayCell::I16(vec)) = &values[7] {
        let converted: Vec<Option<i32>> = vec.iter().map(|&opt| opt.map(|v| v as i32)).collect();
        values[7] = Cell::Array(ArrayCell::I32(converted));
    }

    // numeric_array_col (index 12): NUMERIC_ARRAY maps to String in Iceberg
    if let Cell::Array(ArrayCell::Numeric(vec)) = &values[12] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|n| n.to_string()))
            .collect();
        values[12] = Cell::Array(ArrayCell::String(converted));
    }

    // json_array_col (index 18) and jsonb_array_col (index 19): JSON arrays map to String in Iceberg
    if let Cell::Array(ArrayCell::Json(vec)) = &values[18] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|j| j.to_string()))
            .collect();
        values[18] = Cell::Array(ArrayCell::String(converted));
    }
    if let Cell::Array(ArrayCell::Json(vec)) = &values[19] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|j| j.to_string()))
            .collect();
        values[19] = Cell::Array(ArrayCell::String(converted));
    }

    // oid_array_col (index 20): Convert U32 to I64
    if let Cell::Array(ArrayCell::U32(vec)) = &values[20] {
        let converted: Vec<Option<i64>> = vec.iter().map(|&opt| opt.map(|v| v as i64)).collect();
        values[20] = Cell::Array(ArrayCell::I64(converted));
    }

    let read_rows = read_all_rows(&client, namespace.to_string(), table_name.clone()).await;

    // Compare the actual values in the read_rows with expected table_rows
    assert_eq!(read_rows, expected_rows);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_non_nullable_array() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props())
            .await
            .unwrap();

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with non-nullable array types for all supported types
    let table_name = "test_non_nullable_array_table".to_string();
    let column_schemas = vec![
        // Primary key
        test_column("id", Type::INT4, 1, false, Some(1)),
        // Boolean array type
        test_column("bool_array_col", Type::BOOL_ARRAY, 2, false, None),
        // String array types
        test_column("char_array_col", Type::CHAR_ARRAY, 3, false, None),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, 4, false, None),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, 5, false, None),
        test_column("name_array_col", Type::NAME_ARRAY, 6, false, None),
        test_column("text_array_col", Type::TEXT_ARRAY, 7, false, None),
        // Integer array types
        test_column("int2_array_col", Type::INT2_ARRAY, 8, false, None),
        test_column("int4_array_col", Type::INT4_ARRAY, 9, false, None),
        test_column("int8_array_col", Type::INT8_ARRAY, 10, false, None),
        // Float array types
        test_column("float4_array_col", Type::FLOAT4_ARRAY, 11, false, None),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, 12, false, None),
        // Numeric array type
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, 13, false, None),
        // Date/Time array types
        test_column("date_array_col", Type::DATE_ARRAY, 14, false, None),
        test_column("time_array_col", Type::TIME_ARRAY, 15, false, None),
        test_column(
            "timestamp_array_col",
            Type::TIMESTAMP_ARRAY,
            16,
            false,
            None,
        ),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            17,
            false,
            None,
        ),
        // UUID array type
        test_column("uuid_array_col", Type::UUID_ARRAY, 18, false, None),
        // JSON array types
        test_column("json_array_col", Type::JSON_ARRAY, 19, false, None),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, 20, false, None),
        // OID array type
        test_column("oid_array_col", Type::OID_ARRAY, 21, false, None),
        // Binary array type
        test_column("bytea_array_col", Type::BYTEA_ARRAY, 22, false, None),
    ];

    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    let table_rows = vec![TableRow {
        values: vec![
            Cell::I32(1),                                                            // id
            Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false), Some(true)])), // bool_array_col
            Cell::Array(ArrayCell::String(vec![
                Some("A".to_string()),
                Some("B".to_string()),
            ])), // char_array_col
            Cell::Array(ArrayCell::String(vec![
                Some("fix1".to_string()),
                Some("fix2".to_string()),
            ])), // bpchar_array_col
            Cell::Array(ArrayCell::String(vec![
                Some("var1".to_string()),
                Some("var2".to_string()),
            ])), // varchar_array_col
            Cell::Array(ArrayCell::String(vec![
                Some("name1".to_string()),
                Some("name2".to_string()),
            ])), // name_array_col
            Cell::Array(ArrayCell::String(vec![
                Some("text1".to_string()),
                Some("text2".to_string()),
            ])), // text_array_col
            Cell::Array(ArrayCell::I16(vec![Some(1), Some(2), Some(3)])), // int2_array_col
            Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), Some(30)])), // int4_array_col
            Cell::Array(ArrayCell::I64(vec![Some(100), Some(200), Some(300)])), // int8_array_col
            Cell::Array(ArrayCell::F32(vec![Some(1.5), Some(2.5), Some(3.5)])), // float4_array_col
            Cell::Array(ArrayCell::F64(vec![Some(10.5), Some(20.5), Some(30.5)])), // float8_array_col
            Cell::Array(ArrayCell::Numeric(vec![
                Some("123.45".parse().unwrap()),
                Some("678.90".parse().unwrap()),
            ])), // numeric_array_col
            Cell::Array(ArrayCell::Date(vec![
                Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
                Some(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()),
            ])), // date_array_col
            Cell::Array(ArrayCell::Time(vec![
                Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
                Some(NaiveTime::from_hms_opt(17, 30, 0).unwrap()),
            ])), // time_array_col
            Cell::Array(ArrayCell::Timestamp(vec![
                Some(NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
                    NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                )),
                Some(NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 6, 16).unwrap(),
                    NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
                )),
            ])), // timestamp_array_col
            Cell::Array(ArrayCell::TimestampTz(vec![
                Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
                        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                    ),
                    Utc,
                )),
                Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2023, 6, 16).unwrap(),
                        NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
                    ),
                    Utc,
                )),
            ])), // timestamptz_array_col
            Cell::Array(ArrayCell::Uuid(vec![
                Some(Uuid::new_v4()),
                Some(Uuid::new_v4()),
            ])), // uuid_array_col
            Cell::Array(ArrayCell::Json(vec![
                Some(serde_json::json!({"key1": "value1"})),
                Some(serde_json::json!({"key2": "value2"})),
            ])), // json_array_col
            Cell::Array(ArrayCell::Json(vec![
                Some(serde_json::json!({"keyb1": "valueb1"})),
                Some(serde_json::json!({"keyb2": "valueb2"})),
            ])), // jsonb_array_col
            Cell::Array(ArrayCell::U32(vec![Some(1001), Some(1002)])),             // oid_array_col
            Cell::Array(ArrayCell::Bytes(vec![
                Some(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]),
                Some(vec![0x57, 0x6f, 0x72, 0x6c, 0x64]),
            ])), // bytea_array_col
        ],
    }];

    client
        .insert_rows(
            namespace.to_string(),
            table_name.clone(),
            table_rows.clone(),
        )
        .await
        .unwrap();

    // Create expected rows with proper type conversions for roundtrip
    let mut expected_rows = table_rows.clone();

    // Convert array types that round-trip through different representations
    let TableRow { values } = &mut expected_rows[0];

    // int2_array_col (index 7): Convert I16 to I32
    if let Cell::Array(ArrayCell::I16(vec)) = &values[7] {
        let converted: Vec<Option<i32>> = vec.iter().map(|&opt| opt.map(|v| v as i32)).collect();
        values[7] = Cell::Array(ArrayCell::I32(converted));
    }

    // numeric_array_col (index 12): NUMERIC_ARRAY maps to String in Iceberg
    if let Cell::Array(ArrayCell::Numeric(vec)) = &values[12] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|n| n.to_string()))
            .collect();
        values[12] = Cell::Array(ArrayCell::String(converted));
    }

    // json_array_col (index 18) and jsonb_array_col (index 19): JSON arrays map to String in Iceberg
    if let Cell::Array(ArrayCell::Json(vec)) = &values[18] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|j| j.to_string()))
            .collect();
        values[18] = Cell::Array(ArrayCell::String(converted));
    }
    if let Cell::Array(ArrayCell::Json(vec)) = &values[19] {
        let converted: Vec<Option<String>> = vec
            .iter()
            .map(|opt| opt.as_ref().map(|j| j.to_string()))
            .collect();
        values[19] = Cell::Array(ArrayCell::String(converted));
    }

    // oid_array_col (index 20): Convert U32 to I64
    if let Cell::Array(ArrayCell::U32(vec)) = &values[20] {
        let converted: Vec<Option<i64>> = vec.iter().map(|&opt| opt.map(|v| v as i64)).collect();
        values[20] = Cell::Array(ArrayCell::I64(converted));
    }

    let read_rows = read_all_rows(&client, namespace.to_string(), table_name.clone()).await;

    assert_eq!(read_rows.len(), 1);

    // Compare the actual values in the read_rows with expected table_rows
    assert_eq!(read_rows, expected_rows);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
