//! Comprehensive integration tests for Iceberg destination
//! 
//! These tests require Docker Compose infrastructure to be running.
//! Run with: docker-compose -f docker-compose.test.yml up --abort-on-container-exit

#![cfg(all(feature = "iceberg", feature = "integration-tests"))]

use std::collections::HashMap;
use std::env;
use std::time::Duration;

use etl::types::{CdcEvent, ChangeType, TableName, TableRow, TableSchema};
use etl_destinations::iceberg::{IcebergDestination, IcebergConfig};
use etl_postgres::tokio::test_utils::PgDatabase;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
use chrono::Utc;
use tracing::{info, debug};

/// Test environment configuration
struct TestEnvironment {
    source_client: Client,
    iceberg_destination: IcebergDestination,
    test_id: String,
}

impl TestEnvironment {
    /// Initialize test environment with connections to all services
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize tracing for debugging
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .init();

        info!("Initializing test environment");

        // Connect to source PostgreSQL
        let source_client = Self::connect_postgres().await?;
        
        // Create Iceberg destination
        let iceberg_destination = Self::create_iceberg_destination().await?;
        
        // Generate unique test ID to avoid conflicts
        let test_id = format!("test_{}", Uuid::new_v4().simple());
        
        Ok(Self {
            source_client,
            iceberg_destination,
            test_id,
        })
    }

    async fn connect_postgres() -> Result<Client, Box<dyn std::error::Error>> {
        let host = env::var("SOURCE_POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("SOURCE_POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
        let db = env::var("SOURCE_POSTGRES_DB").unwrap_or_else(|_| "source_db".to_string());
        let user = env::var("SOURCE_POSTGRES_USER").unwrap_or_else(|_| "replication_user".to_string());
        let password = env::var("SOURCE_POSTGRES_PASSWORD").unwrap_or_else(|_| "replication_pass".to_string());
        
        let conn_str = format!(
            "host={} port={} dbname={} user={} password={}",
            host, port, db, user, password
        );
        
        info!("Connecting to PostgreSQL at {}", conn_str);
        
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        
        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        
        // Wait for connection to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        Ok(client)
    }

    async fn create_iceberg_destination() -> Result<IcebergDestination, Box<dyn std::error::Error>> {
        let catalog_uri = env::var("ICEBERG_CATALOG_URI")
            .unwrap_or_else(|_| "http://localhost:8181".to_string());
        let warehouse = env::var("ICEBERG_WAREHOUSE")
            .unwrap_or_else(|_| "s3://iceberg-warehouse/".to_string());
        let namespace = env::var("ICEBERG_NAMESPACE")
            .unwrap_or_else(|_| "test_namespace".to_string());
        
        info!("Creating Iceberg destination with catalog: {}", catalog_uri);
        
        let config = IcebergConfig {
            catalog_type: "rest".to_string(),
            catalog_uri,
            warehouse,
            namespace,
            file_format: Some("parquet".to_string()),
            compression: Some("zstd".to_string()),
            writer_config: None,
            cdc_config: None,
            aws_config: Some(HashMap::from([
                ("region".to_string(), env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string())),
                ("endpoint".to_string(), env::var("AWS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string())),
                ("access_key_id".to_string(), env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string())),
                ("secret_access_key".to_string(), env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())),
                ("allow_http".to_string(), "true".to_string()),
                ("path_style".to_string(), "true".to_string()),
            ])),
        };
        
        // Wait for Iceberg REST catalog to be ready
        for attempt in 1..=30 {
            match IcebergDestination::new(config.clone()).await {
                Ok(dest) => {
                    info!("Successfully connected to Iceberg catalog");
                    return Ok(dest);
                }
                Err(e) if attempt < 30 => {
                    debug!("Attempt {} failed, retrying: {}", attempt, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        
        Err("Failed to connect to Iceberg catalog after 30 attempts".into())
    }

    /// Create a test table in both source and destination
    async fn create_test_table(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let full_table_name = format!("{}_{}", table_name, self.test_id);
        
        // Create table in source PostgreSQL
        let create_sql = format!(
            r#"
            CREATE TABLE test_schema.{} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER,
                active BOOLEAN,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            )
            "#,
            full_table_name
        );
        
        self.source_client.execute(&create_sql, &[]).await?;
        info!("Created test table: {}", full_table_name);
        
        // Create corresponding table in Iceberg
        let table_schema = self.get_table_schema(&full_table_name).await?;
        self.iceberg_destination.create_table(
            &TableName::new("test_schema".to_string(), full_table_name.clone()),
            &table_schema
        ).await?;
        
        Ok(())
    }

    /// Get table schema from PostgreSQL
    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, Box<dyn std::error::Error>> {
        // Query PostgreSQL information_schema to build TableSchema
        let query = r#"
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'test_schema' AND table_name = $1
            ORDER BY ordinal_position
        "#;
        
        let rows = self.source_client.query(query, &[&table_name]).await?;
        
        // Build TableSchema from query results
        // This is a simplified version - real implementation would parse types properly
        let columns = rows.iter().map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let nullable: String = row.get(2);
            
            etl::types::ColumnSchema {
                name,
                typ: Self::parse_postgres_type(&data_type),
                nullable: nullable == "YES",
                modifier: 0,
                primary: false,
            }
        }).collect();
        
        Ok(TableSchema {
            id: etl_postgres::schema::TableId::new(1),
            name: TableName::new("test_schema".to_string(), table_name.to_string()),
            column_schemas: columns,
        })
    }

    fn parse_postgres_type(type_name: &str) -> tokio_postgres::types::Type {
        // Simplified type parsing - real implementation would be more comprehensive
        match type_name.to_lowercase().as_str() {
            "integer" | "int" => tokio_postgres::types::Type::INT4,
            "bigint" => tokio_postgres::types::Type::INT8,
            "text" | "character varying" | "varchar" => tokio_postgres::types::Type::TEXT,
            "boolean" | "bool" => tokio_postgres::types::Type::BOOL,
            "timestamp with time zone" | "timestamptz" => tokio_postgres::types::Type::TIMESTAMPTZ,
            "jsonb" => tokio_postgres::types::Type::JSONB,
            _ => tokio_postgres::types::Type::TEXT,
        }
    }

    /// Clean up test resources
    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Drop all test tables
        let query = "SELECT table_name FROM information_schema.tables 
                     WHERE table_schema = 'test_schema' 
                     AND table_name LIKE $1";
        
        let pattern = format!("%_{}", self.test_id);
        let rows = self.source_client.query(query, &[&pattern]).await?;
        
        for row in rows {
            let table_name: String = row.get(0);
            let drop_sql = format!("DROP TABLE IF EXISTS test_schema.{}", table_name);
            self.source_client.execute(&drop_sql, &[]).await?;
            info!("Dropped test table: {}", table_name);
        }
        
        Ok(())
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_end_to_end_replication() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create test table
    env.create_test_table("replication_test").await?;
    let table_name = format!("replication_test_{}", env.test_id);
    
    // Insert test data
    let insert_sql = format!(
        r#"
        INSERT INTO test_schema.{} (name, value, active, metadata) 
        VALUES 
            ('Alice', 100, true, '{{"role": "admin"}}'),
            ('Bob', 200, false, '{{"role": "user"}}'),
            ('Charlie', 300, true, null)
        "#,
        table_name
    );
    env.source_client.execute(&insert_sql, &[]).await?;
    
    // Simulate CDC events
    let events = vec![
        CdcEvent {
            table_name: TableName::new("test_schema".to_string(), table_name.clone()),
            change_type: ChangeType::Insert,
            table_row: TableRow {
                values: vec![
                    Some("1".to_string()),
                    Some("Alice".to_string()),
                    Some("100".to_string()),
                    Some("true".to_string()),
                    Some(Utc::now().to_rfc3339()),
                    Some(r#"{"role": "admin"}"#.to_string()),
                ],
            },
            lsn: Some(1000),
            sequence_number: Some(1),
        },
        CdcEvent {
            table_name: TableName::new("test_schema".to_string(), table_name.clone()),
            change_type: ChangeType::Update,
            table_row: TableRow {
                values: vec![
                    Some("1".to_string()),
                    Some("Alice Updated".to_string()),
                    Some("150".to_string()),
                    Some("true".to_string()),
                    Some(Utc::now().to_rfc3339()),
                    Some(r#"{"role": "admin", "updated": true}"#.to_string()),
                ],
            },
            lsn: Some(1001),
            sequence_number: Some(2),
        },
        CdcEvent {
            table_name: TableName::new("test_schema".to_string(), table_name.clone()),
            change_type: ChangeType::Delete,
            table_row: TableRow {
                values: vec![
                    Some("2".to_string()),
                    None, None, None, None, None,
                ],
            },
            lsn: Some(1002),
            sequence_number: Some(3),
        },
    ];
    
    // Process CDC events
    for event in events {
        env.iceberg_destination.write_cdc_event(event).await?;
    }
    
    // Verify data in Iceberg (read operations)
    let table_name_obj = TableName::new("test_schema".to_string(), table_name.clone());
    let results = env.iceberg_destination.read_table(&table_name_obj).await?;
    
    // Validate results
    assert!(results.len() > 0, "Should have data in Iceberg table");
    
    // Clean up
    env.cleanup().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_schema_evolution() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create initial table
    env.create_test_table("schema_evolution").await?;
    let table_name = format!("schema_evolution_{}", env.test_id);
    
    // Add column to source table
    let alter_sql = format!(
        "ALTER TABLE test_schema.{} ADD COLUMN new_field VARCHAR(50)",
        table_name
    );
    env.source_client.execute(&alter_sql, &[]).await?;
    
    // Add column to Iceberg table
    let table_name_obj = TableName::new("test_schema".to_string(), table_name.clone());
    env.iceberg_destination.add_column(
        &table_name_obj.table,
        "new_field",
        &tokio_postgres::types::Type::VARCHAR,
        true
    ).await?;
    
    // Insert data with new schema
    let insert_sql = format!(
        r#"
        INSERT INTO test_schema.{} (name, value, active, new_field) 
        VALUES ('Test', 999, true, 'new_value')
        "#,
        table_name
    );
    env.source_client.execute(&insert_sql, &[]).await?;
    
    // Process as CDC event
    let event = CdcEvent {
        table_name: table_name_obj.clone(),
        change_type: ChangeType::Insert,
        table_row: TableRow {
            values: vec![
                Some("10".to_string()),
                Some("Test".to_string()),
                Some("999".to_string()),
                Some("true".to_string()),
                Some(Utc::now().to_rfc3339()),
                None,
                Some("new_value".to_string()),
            ],
        },
        lsn: Some(2000),
        sequence_number: Some(1),
    };
    
    env.iceberg_destination.write_cdc_event(event).await?;
    
    // Drop column
    let alter_sql = format!(
        "ALTER TABLE test_schema.{} DROP COLUMN new_field",
        table_name
    );
    env.source_client.execute(&alter_sql, &[]).await?;
    
    env.iceberg_destination.drop_column(
        &table_name_obj.table,
        "new_field"
    ).await?;
    
    // Clean up
    env.cleanup().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_bulk_load_performance() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create test table
    env.create_test_table("bulk_load").await?;
    let table_name = format!("bulk_load_{}", env.test_id);
    
    // Generate large batch of test data
    let mut events = Vec::new();
    let batch_size = 10000;
    
    for i in 0..batch_size {
        events.push(CdcEvent {
            table_name: TableName::new("test_schema".to_string(), table_name.clone()),
            change_type: ChangeType::Insert,
            table_row: TableRow {
                values: vec![
                    Some(i.to_string()),
                    Some(format!("Name_{}", i)),
                    Some((i * 10).to_string()),
                    Some((i % 2 == 0).to_string()),
                    Some(Utc::now().to_rfc3339()),
                    Some(format!(r#"{{"index": {}}}"#, i)),
                ],
            },
            lsn: Some(3000 + i as i64),
            sequence_number: Some(i as i64),
        });
    }
    
    // Measure bulk load performance
    let start = std::time::Instant::now();
    
    for event in events {
        env.iceberg_destination.write_cdc_event(event).await?;
    }
    
    let duration = start.elapsed();
    let rows_per_sec = batch_size as f64 / duration.as_secs_f64();
    
    info!("Bulk load performance: {} rows/sec", rows_per_sec);
    assert!(rows_per_sec > 100.0, "Bulk load should achieve >100 rows/sec");
    
    // Clean up
    env.cleanup().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling_and_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Try to write to non-existent table
    let invalid_event = CdcEvent {
        table_name: TableName::new("test_schema".to_string(), "non_existent_table".to_string()),
        change_type: ChangeType::Insert,
        table_row: TableRow {
            values: vec![Some("1".to_string())],
        },
        lsn: Some(4000),
        sequence_number: Some(1),
    };
    
    let result = env.iceberg_destination.write_cdc_event(invalid_event).await;
    assert!(result.is_err(), "Should fail for non-existent table");
    
    // Create table and retry
    env.create_test_table("error_recovery").await?;
    let table_name = format!("error_recovery_{}", env.test_id);
    
    // Send malformed data (wrong number of columns)
    let malformed_event = CdcEvent {
        table_name: TableName::new("test_schema".to_string(), table_name.clone()),
        change_type: ChangeType::Insert,
        table_row: TableRow {
            values: vec![Some("1".to_string()), Some("Too few columns".to_string())],
        },
        lsn: Some(4001),
        sequence_number: Some(2),
    };
    
    let result = env.iceberg_destination.write_cdc_event(malformed_event).await;
    assert!(result.is_err(), "Should fail for malformed data");
    
    // Send valid data to verify recovery
    let valid_event = CdcEvent {
        table_name: TableName::new("test_schema".to_string(), table_name.clone()),
        change_type: ChangeType::Insert,
        table_row: TableRow {
            values: vec![
                Some("1".to_string()),
                Some("Valid".to_string()),
                Some("100".to_string()),
                Some("true".to_string()),
                Some(Utc::now().to_rfc3339()),
                None,
            ],
        },
        lsn: Some(4002),
        sequence_number: Some(3),
    };
    
    env.iceberg_destination.write_cdc_event(valid_event).await?;
    
    // Clean up
    env.cleanup().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create test table
    env.create_test_table("concurrent").await?;
    let table_name = format!("concurrent_{}", env.test_id);
    
    // Spawn multiple concurrent write tasks
    let mut handles = Vec::new();
    let num_tasks = 10;
    let writes_per_task = 100;
    
    for task_id in 0..num_tasks {
        let dest = env.iceberg_destination.clone();
        let table_name_clone = table_name.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_task {
                let event = CdcEvent {
                    table_name: TableName::new("test_schema".to_string(), table_name_clone.clone()),
                    change_type: ChangeType::Insert,
                    table_row: TableRow {
                        values: vec![
                            Some(format!("{}_{}", task_id, i)),
                            Some(format!("Task_{}", task_id)),
                            Some(i.to_string()),
                            Some("true".to_string()),
                            Some(Utc::now().to_rfc3339()),
                            None,
                        ],
                    },
                    lsn: Some(5000 + (task_id * 1000 + i) as i64),
                    sequence_number: Some((task_id * 1000 + i) as i64),
                };
                
                dest.write_cdc_event(event).await.unwrap();
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }
    
    info!("Successfully wrote {} concurrent events", num_tasks * writes_per_task);
    
    // Clean up
    env.cleanup().await?;
    
    Ok(())
}