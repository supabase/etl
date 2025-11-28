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
fn test_column(name: &str, typ: Type, nullable: bool, primary_key: bool) -> ColumnSchema {
    ColumnSchema::new(
        name.to_string(),
        typ,
        -1,
        0,
        if primary_key { Some(1) } else { None },
        nullable,
        true,
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
        test_column("id", Type::INT4, false, true),
        // Boolean types
        test_column("bool_col", Type::BOOL, true, false),
        // String types
        test_column("char_col", Type::CHAR, true, false),
        test_column("bpchar_col", Type::BPCHAR, true, false),
        test_column("varchar_col", Type::VARCHAR, true, false),
        test_column("name_col", Type::NAME, true, false),
        test_column("text_col", Type::TEXT, true, false),
        // Integer types
        test_column("int2_col", Type::INT2, true, false),
        test_column("int4_col", Type::INT4, true, false),
        test_column("int8_col", Type::INT8, true, false),
        // Float types
        test_column("float4_col", Type::FLOAT4, true, false),
        test_column("float8_col", Type::FLOAT8, true, false),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, true, false),
        // Date/Time types
        test_column("date_col", Type::DATE, true, false),
        test_column("time_col", Type::TIME, true, false),
        test_column("timestamp_col", Type::TIMESTAMP, true, false),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, true, false),
        // UUID type
        test_column("uuid_col", Type::UUID, true, false),
        // JSON types
        test_column("json_col", Type::JSON, true, false),
        test_column("jsonb_col", Type::JSONB, true, false),
        // OID type
        test_column("oid_col", Type::OID, true, false),
        // Binary type
        test_column("bytea_col", Type::BYTEA, true, false),
        // Array types
        test_column("bool_array_col", Type::BOOL_ARRAY, true, false),
        test_column("char_array_col", Type::CHAR_ARRAY, true, false),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, true, false),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, true, false),
        test_column("name_array_col", Type::NAME_ARRAY, true, false),
        test_column("text_array_col", Type::TEXT_ARRAY, true, false),
        test_column("int2_array_col", Type::INT2_ARRAY, true, false),
        test_column("int4_array_col", Type::INT4_ARRAY, true, false),
        test_column("int8_array_col", Type::INT8_ARRAY, true, false),
        test_column("float4_array_col", Type::FLOAT4_ARRAY, true, false),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, true, false),
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, true, false),
        test_column("date_array_col", Type::DATE_ARRAY, true, false),
        test_column("time_array_col", Type::TIME_ARRAY, true, false),
        test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, true, false),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            true,
            false,
        ),
        test_column("uuid_array_col", Type::UUID_ARRAY, true, false),
        test_column("json_array_col", Type::JSON_ARRAY, true, false),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, true, false),
        test_column("oid_array_col", Type::OID_ARRAY, true, false),
        test_column("bytea_array_col", Type::BYTEA_ARRAY, true, false),
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
    let column_schemas = vec![test_column("id", Type::INT4, false, true)];

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
        test_column("id", Type::INT4, false, true),
        // Boolean types
        test_column("bool_col", Type::BOOL, true, false),
        // String types
        test_column("char_col", Type::CHAR, true, false),
        test_column("bpchar_col", Type::BPCHAR, true, false),
        test_column("varchar_col", Type::VARCHAR, true, false),
        test_column("name_col", Type::NAME, true, false),
        test_column("text_col", Type::TEXT, true, false),
        // Integer types
        test_column("int2_col", Type::INT2, true, false),
        test_column("int4_col", Type::INT4, true, false),
        test_column("int8_col", Type::INT8, true, false),
        // Float types
        test_column("float4_col", Type::FLOAT4, true, false),
        test_column("float8_col", Type::FLOAT8, true, false),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, true, false),
        // Date/Time types
        test_column("date_col", Type::DATE, true, false),
        test_column("time_col", Type::TIME, true, false),
        test_column("timestamp_col", Type::TIMESTAMP, true, false),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, true, false),
        // UUID type
        test_column("uuid_col", Type::UUID, true, false),
        // JSON types
        test_column("json_col", Type::JSON, true, false),
        test_column("jsonb_col", Type::JSONB, true, false),
        // OID type
        test_column("oid_col", Type::OID, true, false),
        // Binary type
        test_column("bytea_col", Type::BYTEA, true, false),
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
        test_column("id", Type::INT4, false, true),
        // Boolean types
        test_column("bool_col", Type::BOOL, false, false),
        // String types
        test_column("char_col", Type::CHAR, false, false),
        test_column("bpchar_col", Type::BPCHAR, false, false),
        test_column("varchar_col", Type::VARCHAR, false, false),
        test_column("name_col", Type::NAME, false, false),
        test_column("text_col", Type::TEXT, false, false),
        // Integer types
        test_column("int2_col", Type::INT2, false, false),
        test_column("int4_col", Type::INT4, false, false),
        test_column("int8_col", Type::INT8, false, false),
        // Float types
        test_column("float4_col", Type::FLOAT4, false, false),
        test_column("float8_col", Type::FLOAT8, false, false),
        // Numeric type
        test_column("numeric_col", Type::NUMERIC, false, false),
        // Date/Time types
        test_column("date_col", Type::DATE, false, false),
        test_column("time_col", Type::TIME, false, false),
        test_column("timestamp_col", Type::TIMESTAMP, false, false),
        test_column("timestamptz_col", Type::TIMESTAMPTZ, false, false),
        // UUID type
        test_column("uuid_col", Type::UUID, false, false),
        // JSON types
        test_column("json_col", Type::JSON, false, false),
        test_column("jsonb_col", Type::JSONB, false, false),
        // OID type
        test_column("oid_col", Type::OID, false, false),
        // Binary type
        test_column("bytea_col", Type::BYTEA, false, false),
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
        test_column("id", Type::INT4, false, true),
        // Boolean array type
        test_column("bool_array_col", Type::BOOL_ARRAY, true, false),
        // String array types
        test_column("char_array_col", Type::CHAR_ARRAY, true, false),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, true, false),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, true, false),
        test_column("name_array_col", Type::NAME_ARRAY, true, false),
        test_column("text_array_col", Type::TEXT_ARRAY, true, false),
        // Integer array types
        test_column("int2_array_col", Type::INT2_ARRAY, true, false),
        test_column("int4_array_col", Type::INT4_ARRAY, true, false),
        test_column("int8_array_col", Type::INT8_ARRAY, true, false),
        // Float array types
        test_column("float4_array_col", Type::FLOAT4_ARRAY, true, false),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, true, false),
        // Numeric array type
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, true, false),
        // Date/Time array types
        test_column("date_array_col", Type::DATE_ARRAY, true, false),
        test_column("time_array_col", Type::TIME_ARRAY, true, false),
        test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, true, false),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            true,
            false,
        ),
        // UUID array type
        test_column("uuid_array_col", Type::UUID_ARRAY, true, false),
        // JSON array types
        test_column("json_array_col", Type::JSON_ARRAY, true, false),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, true, false),
        // OID array type
        test_column("oid_array_col", Type::OID_ARRAY, true, false),
        // Binary array type
        test_column("bytea_array_col", Type::BYTEA_ARRAY, true, false),
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
        test_column("id", Type::INT4, false, true),
        // Boolean array type
        test_column("bool_array_col", Type::BOOL_ARRAY, false, false),
        // String array types
        test_column("char_array_col", Type::CHAR_ARRAY, false, false),
        test_column("bpchar_array_col", Type::BPCHAR_ARRAY, false, false),
        test_column("varchar_array_col", Type::VARCHAR_ARRAY, false, false),
        test_column("name_array_col", Type::NAME_ARRAY, false, false),
        test_column("text_array_col", Type::TEXT_ARRAY, false, false),
        // Integer array types
        test_column("int2_array_col", Type::INT2_ARRAY, false, false),
        test_column("int4_array_col", Type::INT4_ARRAY, false, false),
        test_column("int8_array_col", Type::INT8_ARRAY, false, false),
        // Float array types
        test_column("float4_array_col", Type::FLOAT4_ARRAY, false, false),
        test_column("float8_array_col", Type::FLOAT8_ARRAY, false, false),
        // Numeric array type
        test_column("numeric_array_col", Type::NUMERIC_ARRAY, false, false),
        // Date/Time array types
        test_column("date_array_col", Type::DATE_ARRAY, false, false),
        test_column("time_array_col", Type::TIME_ARRAY, false, false),
        test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, false, false),
        test_column(
            "timestamptz_array_col",
            Type::TIMESTAMPTZ_ARRAY,
            false,
            false,
        ),
        // UUID array type
        test_column("uuid_array_col", Type::UUID_ARRAY, false, false),
        // JSON array types
        test_column("json_array_col", Type::JSON_ARRAY, false, false),
        test_column("jsonb_array_col", Type::JSONB_ARRAY, false, false),
        // OID array type
        test_column("oid_array_col", Type::OID_ARRAY, false, false),
        // Binary array type
        test_column("bytea_array_col", Type::BYTEA_ARRAY, false, false),
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
