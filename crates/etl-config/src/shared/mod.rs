mod base;
mod connection;
mod destination;
mod pipeline;
mod replicator;
mod sentry;
mod supabase;
mod validators;

pub use base::{Validate, ValidationError};
pub use connection::{
    IntoConnectOptions, PgConnectionConfig, PgConnectionConfigWithoutSecrets, PgConnectionOptions,
    PgConnectionOptionsBuilder, TcpKeepaliveConfig, TlsConfig,
};
pub use destination::{
    BigQueryPartitionBy, BigQueryTableOptions, BigQueryTableOptionsConfig,
    BigQueryTimePartitionGranularity, ClickHouseEngine, DestinationConfig,
    DestinationConfigWithoutSecrets, DestinationKind, DuckLakeCopyBufferConfig,
    DuckLakeMaintenanceMode, DuckLakeSortBy, DuckLakeSortColumn, DuckLakeSortDirection,
    DuckLakeSortNulls, DuckLakeTableSortConfig, DuckLakeTableSortingConfig, IcebergConfig,
    IcebergConfigWithoutSecrets,
};
pub use pipeline::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PipelineConfig,
    PipelineConfigWithoutSecrets, TableSyncCopyConfig,
};
pub use replicator::{ReplicatorConfig, ReplicatorConfigWithoutSecrets};
pub use sentry::SentryConfig;
pub use supabase::{SupabaseConfig, SupabaseConfigWithoutSecrets};
pub use validators::{validate_snowflake_account_id, validate_supabase_project_ref};
