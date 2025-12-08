#![cfg(feature = "test-utils")]

use etl_mysql::types::{ColumnSchema, TableId, TableName, TableSchema};
use etl_mysql::version::{MYSQL_8_0, meets_version};
use etl_telemetry::tracing::init_test_tracing;
use std::num::NonZeroI32;

fn create_sample_table_schema() -> TableSchema {
    let table_id = TableId::new(12345);
    let table_name = TableName::new("test_db".to_string(), "test_table".to_string());
    let columns = vec![
        ColumnSchema::new("id".to_string(), "INT".to_string(), -1, false, true),
        ColumnSchema::new("name".to_string(), "VARCHAR".to_string(), 255, true, false),
        ColumnSchema::new(
            "created_at".to_string(),
            "TIMESTAMP".to_string(),
            -1,
            false,
            false,
        ),
    ];

    TableSchema::new(table_id, table_name, columns)
}

fn create_another_table_schema() -> TableSchema {
    let table_id = TableId::new(67890);
    let table_name = TableName::new("test_db".to_string(), "another_table".to_string());
    let columns = vec![
        ColumnSchema::new("id".to_string(), "BIGINT".to_string(), -1, false, true),
        ColumnSchema::new(
            "description".to_string(),
            "TEXT".to_string(),
            -1,
            true,
            false,
        ),
    ];

    TableSchema::new(table_id, table_name, columns)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_creation() {
    init_test_tracing();

    let table_schema = create_sample_table_schema();

    assert_eq!(table_schema.id.into_inner(), 12345);
    assert_eq!(table_schema.name.schema, "test_db");
    assert_eq!(table_schema.name.name, "test_table");
    assert_eq!(table_schema.num_columns(), 3);

    let id_column = table_schema
        .column_schemas
        .iter()
        .find(|c| c.name == "id");
    assert!(id_column.is_some());
    let id_column = id_column.unwrap();
    assert_eq!(id_column.typ, "INT");
    assert!(id_column.primary);
    assert!(!id_column.nullable);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_name_formatting() {
    init_test_tracing();

    let table_name = TableName::new("my_database".to_string(), "users".to_string());

    assert_eq!(table_name.to_string(), "my_database.users");
    assert_eq!(table_name.as_quoted_identifier(), "`my_database`.`users`");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_id_operations() {
    init_test_tracing();

    let id1 = TableId::new(100);
    let id2 = TableId::new(200);
    let id3 = TableId::new(100);

    assert_eq!(id1, id3);
    assert_ne!(id1, id2);
    assert!(id1 < id2);

    assert_eq!(id1.into_inner(), 100);
    assert_eq!(id1.to_string(), "100");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_column_schema_comparison() {
    init_test_tracing();

    let col1 = ColumnSchema::new("id".to_string(), "INT".to_string(), -1, false, true);
    let col2 = ColumnSchema::new("id".to_string(), "INT".to_string(), -1, true, true);

    assert_ne!(col1, col2);

    assert!(col1.partial_eq(&col2));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_comparison() {
    init_test_tracing();

    let schema1 = create_sample_table_schema();
    let mut schema2 = create_sample_table_schema();

    assert_eq!(schema1, schema2);

    schema2.column_schemas[0].nullable = true;
    assert_ne!(schema1, schema2);

    assert!(schema1.partial_eq(&schema2));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_tables_ordering() {
    init_test_tracing();

    let schema1 = create_sample_table_schema();
    let schema2 = create_another_table_schema();

    assert!(schema1.id < schema2.id);
    assert!(schema1 < schema2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mysql_version_checking() {
    init_test_tracing();

    let version_8_0 = NonZeroI32::new(80035);
    assert!(meets_version(version_8_0, MYSQL_8_0));

    let version_5_7 = NonZeroI32::new(50744);
    assert!(!meets_version(version_5_7, MYSQL_8_0));

    assert!(!meets_version(None, MYSQL_8_0));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_slots() {
    init_test_tracing();

    use etl_mysql::replication::slots::{EtlReplicationSlot, APPLY_WORKER_PREFIX};

    let pipeline_id = 1;
    let slot = EtlReplicationSlot::for_apply_worker(pipeline_id);

    let slot_name: String = slot.try_into().unwrap();
    assert!(slot_name.starts_with(APPLY_WORKER_PREFIX));
    assert_eq!(slot_name, "supabase_etl_apply_1");

    let parsed_slot: EtlReplicationSlot = slot_name.as_str().try_into().unwrap();
    assert_eq!(parsed_slot, EtlReplicationSlot::for_apply_worker(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_sync_slots() {
    init_test_tracing();

    use etl_mysql::replication::slots::{EtlReplicationSlot, TABLE_SYNC_WORKER_PREFIX};

    let pipeline_id = 1;
    let table_id = TableId::new(12345);
    let slot = EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id);

    let slot_name: String = slot.try_into().unwrap();
    assert!(slot_name.starts_with(TABLE_SYNC_WORKER_PREFIX));
    assert_eq!(slot_name, "supabase_etl_table_sync_1_12345");

    let parsed_slot: EtlReplicationSlot = slot_name.as_str().try_into().unwrap();
    assert_eq!(
        parsed_slot,
        EtlReplicationSlot::for_table_sync_worker(1, TableId::new(12345))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_mappings() {
    init_test_tracing();

    use etl_mysql::replication::table_mappings::TableMappings;

    let mut mappings = TableMappings::new();
    assert!(mappings.is_empty());

    let table_id = TableId::new(123);
    let table_name = TableName::new("test_db".to_string(), "users".to_string());

    mappings.add_mapping(table_id, table_name.clone());
    assert_eq!(mappings.len(), 1);
    assert_eq!(mappings.get_table_name(&table_id), Some(&table_name));

    let all_mappings = mappings.all_mappings();
    assert_eq!(all_mappings.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_worker_config() {
    init_test_tracing();

    use etl_mysql::replication::worker::{WorkerConfig, WorkerState, WorkerType};

    let mut config = WorkerConfig::new(1, WorkerType::Apply);
    assert_eq!(config.state, WorkerState::Initializing);
    assert!(!config.is_running());
    assert!(!config.is_errored());

    config.set_state(WorkerState::Running);
    assert!(config.is_running());
    assert!(!config.is_errored());

    config.set_state(WorkerState::Errored {
        reason: "Test error".to_string(),
    });
    assert!(config.is_errored());
    assert!(!config.is_running());

    config.set_state(WorkerState::Stopped);
    assert!(!config.is_running());
    assert!(!config.is_errored());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_binlog_position() {
    init_test_tracing();

    use etl_mysql::replication::lag::BinlogPosition;

    let pos1 = BinlogPosition::new(1, 1000);
    let pos2 = BinlogPosition::new(1, 2000);

    assert_eq!(pos1.lag_bytes(&pos2), 1000);

    let pos3 = BinlogPosition::new(1, 1000);
    let pos4 = BinlogPosition::new(2, 500);
    assert!(pos3.lag_bytes(&pos4) > 0);

    let pos5 = BinlogPosition::new(2, 2000);
    let pos6 = BinlogPosition::new(1, 1000);
    assert!(pos5.lag_bytes(&pos6) < 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sequence_number_generation() {
    init_test_tracing();

    use etl_mysql::types::generate_sequence_number;

    let start = 1000u64;
    let commit = 2000u64;
    let seq = generate_sequence_number(start, commit);
    assert_eq!(seq, "00000000000007d0/00000000000003e8");

    let seq1 = generate_sequence_number(100, 1000);
    let seq2 = generate_sequence_number(200, 1000);
    let seq3 = generate_sequence_number(100, 2000);

    assert!(seq2 > seq1);
    assert!(seq3 > seq1);
    assert!(seq3 > seq2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_state() {
    init_test_tracing();

    use etl_mysql::replication::state::ReplicationState;

    let state = ReplicationState::new("mysql-bin.000123".to_string(), 4567);
    assert_eq!(state.binlog_file, "mysql-bin.000123");
    assert_eq!(state.binlog_position, 4567);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_version_extraction() {
    init_test_tracing();

    use etl_mysql::replication::extract_server_version;

    assert_eq!(
        extract_server_version("8.0.35"),
        NonZeroI32::new(80035)
    );
    assert_eq!(
        extract_server_version("5.7.44"),
        NonZeroI32::new(50744)
    );
    assert_eq!(
        extract_server_version("8.0.35-log"),
        NonZeroI32::new(80035)
    );
    assert_eq!(
        extract_server_version("5.7.44-0ubuntu0.18.04.1"),
        NonZeroI32::new(50744)
    );

    assert_eq!(extract_server_version(""), None);
    assert_eq!(extract_server_version("invalid"), None);
    assert_eq!(extract_server_version("0.0.0"), None);
}
