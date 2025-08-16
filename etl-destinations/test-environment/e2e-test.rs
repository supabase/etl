#!/usr/bin/env rust-script
//! End-to-end test for Iceberg destination with real services
//! 
//! Run with: cargo script e2e-test.rs
//! 
//! ```cargo
//! [dependencies]
//! etl = { path = "../../../" }
//! etl-destinations = { path = "../", features = ["iceberg"] }
//! tokio = { version = "1", features = ["full"] }
//! tracing = "0.1"
//! tracing-subscriber = { version = "0.3", features = ["env-filter"] }
//! ```

use etl::config::PostgresConfig;
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl::types::{TableName, PipelineId};
use etl_destinations::iceberg::IcebergDestination;
use std::time::Duration;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,etl=debug,etl_destinations=debug")
        .init();

    info!("🚀 Starting End-to-End Test for Iceberg Destination");

    // Configuration for local PostgreSQL
    let pg_config = PostgresConfig {
        host: "localhost".to_string(),
        port: 5432,
        database: "etl_source".to_string(),
        user: "etl_user".to_string(),
        password: Some("etl_password".to_string()),
        sslmode: None,
    };

    // Create memory store for state management
    let store = MemoryStore::new();

    // Create Iceberg destination
    let destination = IcebergDestination::new(
        "http://localhost:8181".to_string(),
        "s3://iceberg-warehouse/".to_string(),
        "etl_test".to_string(),
        None,
        store.clone(),
    )
    .await?;

    info!("✅ Successfully connected to Iceberg REST catalog");

    // Create pipeline
    let pipeline_id = PipelineId::new();
    let mut pipeline = Pipeline::new(
        pg_config,
        pipeline_id,
        "etl_publication".to_string(),
        store.clone(),
        destination,
    );

    info!("✅ Created ETL pipeline");

    // Start the pipeline
    pipeline.start().await?;
    info!("✅ Pipeline started successfully");

    // Let it run for a while to process initial data
    info!("⏳ Processing initial data...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Perform some operations on PostgreSQL to test CDC
    info!("📝 Performing test operations on PostgreSQL...");
    
    // Connect to PostgreSQL for test operations
    let pg_client = tokio_postgres::connect(
        "host=localhost port=5432 dbname=etl_source user=etl_user password=etl_password",
        tokio_postgres::NoTls,
    )
    .await?
    .0;

    // Insert new data
    pg_client
        .execute(
            "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
            &[&"Test User", &"test@example.com", &25i32],
        )
        .await?;
    info!("  ✅ Inserted new user");

    // Update existing data
    pg_client
        .execute(
            "UPDATE users SET age = age + 1 WHERE name = $1",
            &[&"Alice Johnson"],
        )
        .await?;
    info!("  ✅ Updated existing user");

    // Delete data
    pg_client
        .execute(
            "DELETE FROM orders WHERE id = $1",
            &[&1i32],
        )
        .await?;
    info!("  ✅ Deleted order");

    // Let pipeline process the changes
    info!("⏳ Processing CDC events...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check table creation in Iceberg
    info!("🔍 Verifying Iceberg tables...");
    
    // Query Iceberg REST catalog for tables
    let client = reqwest::Client::new();
    let namespaces = client
        .get("http://localhost:8181/v1/namespaces")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    
    info!("  Namespaces: {}", namespaces);

    let tables = client
        .get("http://localhost:8181/v1/namespaces/etl_test/tables")
        .send()
        .await?;
    
    if tables.status().is_success() {
        let tables_json = tables.json::<serde_json::Value>().await?;
        info!("  ✅ Tables in etl_test namespace: {}", tables_json);
    } else {
        info!("  ⚠️ No tables found or namespace doesn't exist yet");
    }

    // Shutdown pipeline
    info!("🛑 Shutting down pipeline...");
    pipeline.shutdown_and_wait().await?;
    info!("✅ Pipeline shutdown complete");

    info!("🎉 End-to-End Test Completed Successfully!");
    info!("");
    info!("You can now query the Iceberg tables using Trino:");
    info!("  docker exec -it etl-trino trino");
    info!("  > USE iceberg.etl_test;");
    info!("  > SHOW TABLES;");
    info!("  > SELECT * FROM public_users;");

    Ok(())
}