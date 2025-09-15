#![cfg(feature = "iceberg")]

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{Cell, ColumnSchema, TableId, TableName, TableRow, TableSchema, Type};
use etl_destinations::iceberg::IcebergClient;
use etl_telemetry::tracing::init_test_tracing;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
use uuid::Uuid;

use crate::support::{iceberg::read_all_rows, lakekeeper::LakekeeperClient};

mod support;

const LAKEKEEPER_URL: &str = "http://localhost:8182";
const MINIO_URL: &str = "http://localhost:9010";
const MINIO_USERNAME: &str = "minio-admin";
const MINIO_PASSWORD: &str = "minio-admin-password";

fn get_catalog_url() -> String {
    format!("{LAKEKEEPER_URL}/catalog")
}

fn create_props() -> HashMap<String, String> {
    let mut props: HashMap<String, String> = HashMap::new();

    props.insert(S3_ACCESS_KEY_ID.to_string(), MINIO_USERNAME.to_string());
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), MINIO_PASSWORD.to_string());
    props.insert(S3_ENDPOINT.to_string(), MINIO_URL.to_string());

    props
}

#[tokio::test]
async fn create_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

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
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

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
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types
    let table_name = "test_table".to_string();
    let table_id = TableId::new(12345);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, true, false),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, true, false),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, true, false),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, true, false),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, true, false),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, true, false),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, true, false),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, true, false),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, true, false),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, true, false),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, true, false),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true, false),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true, false),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true, false),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            true,
            false,
        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true, false),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true, false),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, true, false),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, true, false),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true, false),
        // Array types
        ColumnSchema::new(
            "bool_array_col".to_string(),
            Type::BOOL_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "char_array_col".to_string(),
            Type::CHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "bpchar_array_col".to_string(),
            Type::BPCHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "varchar_array_col".to_string(),
            Type::VARCHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "name_array_col".to_string(),
            Type::NAME_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "text_array_col".to_string(),
            Type::TEXT_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int2_array_col".to_string(),
            Type::INT2_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int4_array_col".to_string(),
            Type::INT4_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int8_array_col".to_string(),
            Type::INT8_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "float4_array_col".to_string(),
            Type::FLOAT4_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "float8_array_col".to_string(),
            Type::FLOAT8_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "numeric_array_col".to_string(),
            Type::NUMERIC_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "date_array_col".to_string(),
            Type::DATE_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "time_array_col".to_string(),
            Type::TIME_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamp_array_col".to_string(),
            Type::TIMESTAMP_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_array_col".to_string(),
            Type::TIMESTAMPTZ_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "uuid_array_col".to_string(),
            Type::UUID_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "json_array_col".to_string(),
            Type::JSON_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "jsonb_array_col".to_string(),
            Type::JSONB_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "oid_array_col".to_string(),
            Type::OID_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "bytea_array_col".to_string(),
            Type::BYTEA_ARRAY,
            -1,
            true,
            false,
        ),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    // table doesn't exist yet
    assert!(
        !client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Create table for the first time
    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
    client.drop_table(namespace, table_name).await.unwrap();
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
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types
    let table_name = "test_table".to_string();
    let table_id = TableId::new(12345);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, true, false),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, true, false),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, true, false),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, true, false),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, true, false),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, true, false),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, true, false),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, true, false),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, true, false),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, true, false),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, true, false),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true, false),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true, false),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true, false),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            true,
            false,
        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true, false),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true, false),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, true, false),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, true, false),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true, false),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
        .insert_rows(namespace.to_string(), table_name.clone(), &table_rows)
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
    client.drop_table(namespace, table_name).await.unwrap();
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
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types as non-nullable
    let table_name = "test_table".to_string();
    let table_id = TableId::new(12345);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, false, false),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, false, false),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, false, false),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, false, false),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, false, false),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, false, false),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, false, false),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, false, false),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, false, false),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, false, false),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, false, false),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, false, false),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, false, false),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, false, false),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            false,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            false,
            false,
        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, false, false),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, false, false),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, false, false),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, false, false),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, false, false),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
        .insert_rows(namespace.to_string(), table_name.clone(), &table_rows)
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
    client.drop_table(namespace, table_name).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
