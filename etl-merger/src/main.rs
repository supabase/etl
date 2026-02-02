//! CLI binary for the etl-merger.

use std::collections::HashMap;

use clap::Parser;
use etl_destinations::iceberg::IcebergClient;
use etl_merger::{Merger, MergerConfig};
use etl_postgres::types::ColumnSchema;
use tokio_postgres::types::Type;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

/// ETL Merger - Merges CDC changelog tables into mirror tables.
#[derive(Parser, Debug)]
#[command(name = "etl-merger")]
#[command(about = "Merges CDC changelog tables into mirror tables")]
struct Args {
    /// Iceberg catalog URL
    #[arg(long)]
    catalog_url: String,

    /// Warehouse name
    #[arg(long)]
    warehouse: String,

    /// Changelog table name
    #[arg(long)]
    changelog_table: String,

    /// Mirror table name
    #[arg(long)]
    mirror_table: String,

    /// Changelog namespace (default: "default")
    #[arg(long, default_value = "default")]
    changelog_namespace: String,

    /// Mirror namespace (default: "mirror")
    #[arg(long, default_value = "mirror")]
    mirror_namespace: String,

    /// Start from this sequence number (optional)
    #[arg(long)]
    checkpoint: Option<String>,

    /// Batch size for processing
    #[arg(long, default_value = "10000")]
    batch_size: usize,

    /// Primary key column indices (comma-separated, 0-based)
    #[arg(long, default_value = "0")]
    pk_columns: String,

    /// S3 access key ID (optional, for MinIO/S3)
    #[arg(long)]
    s3_access_key_id: Option<String>,

    /// S3 secret access key (optional, for MinIO/S3)
    #[arg(long)]
    s3_secret_access_key: Option<String>,

    /// S3 endpoint (optional, for MinIO)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 region (optional)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    if let Err(e) = run().await {
        error!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    info!(
        catalog = %args.catalog_url,
        warehouse = %args.warehouse,
        changelog = %args.changelog_table,
        mirror = %args.mirror_table,
        "Starting ETL Merger"
    );

    // Build S3 properties if provided
    let mut props = HashMap::new();
    if let Some(access_key) = &args.s3_access_key_id {
        props.insert("s3.access-key-id".to_string(), access_key.clone());
    }
    if let Some(secret_key) = &args.s3_secret_access_key {
        props.insert("s3.secret-access-key".to_string(), secret_key.clone());
    }
    if let Some(endpoint) = &args.s3_endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
    }
    props.insert("s3.region".to_string(), args.s3_region.clone());

    // Create Iceberg client
    let client = IcebergClient::new_with_rest_catalog(
        args.catalog_url.clone(),
        args.warehouse.clone(),
        props,
    )
    .await?;

    // Parse PK column indices
    let pk_columns: Vec<usize> = args
        .pk_columns
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;

    // Create merger config
    let config = MergerConfig::new(
        args.changelog_namespace.clone(),
        args.mirror_namespace.clone(),
    )
    .with_batch_size(args.batch_size)
    .with_checkpoint(args.checkpoint);

    // Load changelog table to infer schema
    info!("Loading changelog table to infer schema...");
    let changelog_table = client
        .load_table(
            args.changelog_namespace.clone(),
            args.changelog_table.clone(),
        )
        .await?;

    let schema = changelog_table.metadata().current_schema();
    let fields: Vec<_> = schema.as_struct().fields().iter().collect();

    // Build mirror schema from changelog schema (excluding CDC columns)
    let mut mirror_schema = Vec::new();
    for (idx, field) in fields.iter().enumerate() {
        // Skip CDC columns (last 2 columns)
        if idx >= fields.len() - 2 {
            break;
        }

        // Map Iceberg types to Postgres types (simplified)
        let pg_type = iceberg_type_to_postgres_type(&field.field_type);
        mirror_schema.push(ColumnSchema::new(
            field.name.clone(),
            pg_type,
            -1,
            !field.required,
            pk_columns.contains(&idx),
        ));
    }

    info!(columns = mirror_schema.len(), "Inferred mirror schema");

    // Create merger
    let mut merger = Merger::new(
        client.clone(),
        &args.changelog_table,
        &args.mirror_table,
        mirror_schema,
        pk_columns,
        config,
    )
    .await?;

    // Build index from existing mirror table
    info!("Building index from existing mirror table...");
    let index_size = merger.build_index_from_mirror(&client).await?;
    info!(index_entries = index_size, "Index built");

    // Run merge
    info!("Starting merge process...");
    let summary = merger.merge_until_caught_up().await?;

    info!(
        events = summary.events_processed,
        batches = summary.batches_processed,
        "Merge completed successfully"
    );

    Ok(())
}

/// Maps Iceberg primitive types to Postgres types (simplified).
fn iceberg_type_to_postgres_type(iceberg_type: &iceberg::spec::Type) -> Type {
    use iceberg::spec::PrimitiveType;
    use iceberg::spec::Type as IcebergType;

    match iceberg_type {
        IcebergType::Primitive(prim) => match prim {
            PrimitiveType::Boolean => Type::BOOL,
            PrimitiveType::Int => Type::INT4,
            PrimitiveType::Long => Type::INT8,
            PrimitiveType::Float => Type::FLOAT4,
            PrimitiveType::Double => Type::FLOAT8,
            PrimitiveType::String => Type::TEXT,
            PrimitiveType::Binary => Type::BYTEA,
            PrimitiveType::Date => Type::DATE,
            PrimitiveType::Time => Type::TIME,
            PrimitiveType::Timestamp => Type::TIMESTAMP,
            PrimitiveType::Timestamptz => Type::TIMESTAMPTZ,
            PrimitiveType::Uuid => Type::UUID,
            _ => Type::TEXT, // Fallback for unsupported types
        },
        IcebergType::List(_) => Type::TEXT, // Arrays need special handling
        IcebergType::Map(_) => Type::JSONB,
        IcebergType::Struct(_) => Type::JSONB,
    }
}
