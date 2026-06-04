mod base;
mod connection;
mod destination;
mod pipeline;
mod replicator;
mod sentry;
mod supabase;
mod validators;

pub use base::ValidationError;
pub use connection::{
    ETL_API_OPTIONS, ETL_MIGRATION_OPTIONS, ETL_REPLICATION_OPTIONS, ETL_STATE_MANAGEMENT_OPTIONS,
    IntoConnectOptions, PgConnectionConfig, PgConnectionConfigWithoutSecrets, PgConnectionOptions,
    TcpKeepaliveConfig, TlsConfig,
};
pub use destination::{
    ClickHouseEngine, DestinationConfig, DestinationConfigWithoutSecrets, DestinationKind,
    DuckLakeMaintenanceMode, IcebergConfig, IcebergConfigWithoutSecrets,
};
pub use pipeline::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PipelineConfig,
    PipelineConfigWithoutSecrets, TableSyncCopyConfig,
};
pub use replicator::{ReplicatorConfig, ReplicatorConfigWithoutSecrets};
pub use sentry::SentryConfig;
pub use supabase::{SupabaseConfig, SupabaseConfigWithoutSecrets};
pub use validators::{validate_snowflake_account_id, validate_supabase_project_ref};
