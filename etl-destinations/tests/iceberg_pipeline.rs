#![cfg(feature = "iceberg")]

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{ArrayCell, Cell, ColumnSchema, TableId, TableName, TableRow, TableSchema, Type};
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
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, true),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, true),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, true),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, true),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, true),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, true),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, true),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, true),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, true),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, true),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, true),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            true,

        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            true,

        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, true),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, true),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true),
        // Array types
        ColumnSchema::new(
            "bool_array_col".to_string(),
            Type::BOOL_ARRAY,
            -1,
            true,

        ),
        ColumnSchema::new(
            "char_array_col".to_string(),
            Type::CHAR_ARRAY,
            -1,
            true,

        ),
        ColumnSchema::new(
            "bpchar_array_col".to_string(),
            Type::BPCHAR_ARRAY,
            -1,
            true,

        ),
        ColumnSchema::new(
            "varchar_array_col".to_string(),
            Type::VARCHAR_ARRAY,
            -1,
            true,

        ),
        ColumnSchema::new(
            "name_array_col".to_string(),
            Type::NAME_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "text_array_col".to_string(),
            Type::TEXT_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "int2_array_col".to_string(),
            Type::INT2_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "int4_array_col".to_string(),
            Type::INT4_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "int8_array_col".to_string(),
            Type::INT8_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "float4_array_col".to_string(),
            Type::FLOAT4_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "float8_array_col".to_string(),
            Type::FLOAT8_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "numeric_array_col".to_string(),
            Type::NUMERIC_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "date_array_col".to_string(),
            Type::DATE_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "time_array_col".to_string(),
            Type::TIME_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "timestamp_array_col".to_string(),
            Type::TIMESTAMP_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "timestamptz_array_col".to_string(),
            Type::TIMESTAMPTZ_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "uuid_array_col".to_string(),
            Type::UUID_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "json_array_col".to_string(),
            Type::JSON_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "jsonb_array_col".to_string(),
            Type::JSONB_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "oid_array_col".to_string(),
            Type::OID_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "bytea_array_col".to_string(),
            Type::BYTEA_ARRAY,
            -1,
            true,
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
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, true),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, true),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, true),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, true),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, true),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, true),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, true),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, true),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, true),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, true),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, true),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            true,

        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            true,

        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, true),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, true),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true),
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
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, false),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, false),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, false),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, false),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, false),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, false),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, false),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, false),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, false),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, false,),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, false),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, false),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, false),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, false),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            false,
        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, false),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, false),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, false),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, false),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, false),
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

#[tokio::test]
async fn insert_nullable_array() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with array types for all supported types
    let table_name = "test_array_table".to_string();
    let table_id = TableId::new(12346);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false),
        // Boolean array type
        ColumnSchema::new(
            "bool_array_col".to_string(),
            Type::BOOL_ARRAY,
            -1,
            true,
        ),
        // String array types
        ColumnSchema::new(
            "char_array_col".to_string(),
            Type::CHAR_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "bpchar_array_col".to_string(),
            Type::BPCHAR_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "varchar_array_col".to_string(),
            Type::VARCHAR_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "name_array_col".to_string(),
            Type::NAME_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "text_array_col".to_string(),
            Type::TEXT_ARRAY,
            -1,
            true,
        ),
        // Integer array types
        ColumnSchema::new(
            "int2_array_col".to_string(),
            Type::INT2_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "int4_array_col".to_string(),
            Type::INT4_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "int8_array_col".to_string(),
            Type::INT8_ARRAY,
            -1,
            true,
        ),
        // Float array types
        ColumnSchema::new(
            "float4_array_col".to_string(),
            Type::FLOAT4_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "float8_array_col".to_string(),
            Type::FLOAT8_ARRAY,
            -1,
            true,
        ),
        // Numeric array type
        ColumnSchema::new(
            "numeric_array_col".to_string(),
            Type::NUMERIC_ARRAY,
            -1,
            true,
        ),
        // Date/Time array types
        ColumnSchema::new(
            "date_array_col".to_string(),
            Type::DATE_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "time_array_col".to_string(),
            Type::TIME_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "timestamp_array_col".to_string(),
            Type::TIMESTAMP_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "timestamptz_array_col".to_string(),
            Type::TIMESTAMPTZ_ARRAY,
            -1,
            true,
        ),
        // UUID array type
        ColumnSchema::new(
            "uuid_array_col".to_string(),
            Type::UUID_ARRAY,
            -1,
            true,
        ),
        // JSON array types
        ColumnSchema::new(
            "json_array_col".to_string(),
            Type::JSON_ARRAY,
            -1,
            true,
        ),
        ColumnSchema::new(
            "jsonb_array_col".to_string(),
            Type::JSONB_ARRAY,
            -1,
            true,
        ),
        // OID array type
        ColumnSchema::new(
            "oid_array_col".to_string(),
            Type::OID_ARRAY,
            -1,
            true,
        ),
        // Binary array type
        ColumnSchema::new(
            "bytea_array_col".to_string(),
            Type::BYTEA_ARRAY,
            -1,
            true,
        ),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
        .insert_rows(namespace.to_string(), table_name.clone(), &table_rows)
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
    client.drop_table(namespace, table_name).await.unwrap();
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
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with non-nullable array types for all supported types
    let table_name = "test_non_nullable_array_table".to_string();
    let table_id = TableId::new(12347);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false),
        // Boolean array type
        ColumnSchema::new(
            "bool_array_col".to_string(),
            Type::BOOL_ARRAY,
            -1,
            false,
        ),
        // String array types
        ColumnSchema::new(
            "char_array_col".to_string(),
            Type::CHAR_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "bpchar_array_col".to_string(),
            Type::BPCHAR_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "varchar_array_col".to_string(),
            Type::VARCHAR_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "name_array_col".to_string(),
            Type::NAME_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "text_array_col".to_string(),
            Type::TEXT_ARRAY,
            -1,
            false,
        ),
        // Integer array types
        ColumnSchema::new(
            "int2_array_col".to_string(),
            Type::INT2_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "int4_array_col".to_string(),
            Type::INT4_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "int8_array_col".to_string(),
            Type::INT8_ARRAY,
            -1,
            false,
        ),
        // Float array types
        ColumnSchema::new(
            "float4_array_col".to_string(),
            Type::FLOAT4_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "float8_array_col".to_string(),
            Type::FLOAT8_ARRAY,
            -1,
            false,
        ),
        // Numeric array type
        ColumnSchema::new(
            "numeric_array_col".to_string(),
            Type::NUMERIC_ARRAY,
            -1,
            false,
        ),
        // Date/Time array types
        ColumnSchema::new(
            "date_array_col".to_string(),
            Type::DATE_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "time_array_col".to_string(),
            Type::TIME_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "timestamp_array_col".to_string(),
            Type::TIMESTAMP_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_array_col".to_string(),
            Type::TIMESTAMPTZ_ARRAY,
            -1,
            false,
        ),
        // UUID array type
        ColumnSchema::new(
            "uuid_array_col".to_string(),
            Type::UUID_ARRAY,
            -1,
            false,
        ),
        // JSON array types
        ColumnSchema::new(
            "json_array_col".to_string(),
            Type::JSON_ARRAY,
            -1,
            false,
        ),
        ColumnSchema::new(
            "jsonb_array_col".to_string(),
            Type::JSONB_ARRAY,
            -1,
            false,
        ),
        // OID array type
        ColumnSchema::new(
            "oid_array_col".to_string(),
            Type::OID_ARRAY,
            -1,
            false,
        ),
        // Binary array type
        ColumnSchema::new(
            "bytea_array_col".to_string(),
            Type::BYTEA_ARRAY,
            -1,
            false,
        ),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
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
        .insert_rows(namespace.to_string(), table_name.clone(), &table_rows)
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
    client.drop_table(namespace, table_name).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
