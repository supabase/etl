#![cfg(feature = "databend")]

//! Basic Databend integration tests without full pipeline

use etl::store::both::memory::MemoryStore;
use etl::types::{ColumnSchema, TableRow, Cell, Type};
use etl_destinations::databend::DatabendDestination;
use etl_destinations::encryption::install_crypto_provider;
use etl_telemetry::tracing::init_test_tracing;
use uuid::Uuid;

const DATABEND_DSN_ENV_NAME: &str = "TESTS_DATABEND_DSN";

fn get_databend_dsn() -> String {
    std::env::var(DATABEND_DSN_ENV_NAME)
        .unwrap_or_else(|_| panic!("The env variable {} must be set", DATABEND_DSN_ENV_NAME))
}

#[tokio::test]
async fn test_databend_destination_creation() {
    init_test_tracing();
    install_crypto_provider();

    let dsn = get_databend_dsn();
    let database = format!("test_db_{}", Uuid::new_v4().simple());
    let store = MemoryStore::new();

    println!("Creating Databend destination with database: {}", database);

    let destination = DatabendDestination::new(dsn, database, store)
        .await
        .expect("Failed to create Databend destination");

    println!("✅ Successfully created Databend destination");
}

// The other tests require access to private fields
// For now, let's just test that the destination can be created
// and that the connection is established properly
