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
    IntoConnectOptions, PgConnectionConfig, PgConnectionConfigWithoutSecrets, PgConnectionOptions,
    PgConnectionOptionsBuilder, TcpKeepaliveConfig, TlsConfig,
};
pub use destination::{
    ClickHouseEngine, DestinationConfig, DestinationConfigWithoutSecrets, DestinationKind,
    DuckLakeMaintenanceMode, IcebergConfig, IcebergConfigWithoutSecrets,
};
pub use pipeline::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PipelineConfig,
    PipelineConfigWithoutSecrets, PublicationChangesMode, TableSyncCopyConfig,
};
pub use replicator::{ReplicatorConfig, ReplicatorConfigWithoutSecrets};
pub use sentry::SentryConfig;
pub use supabase::{SupabaseConfig, SupabaseConfigWithoutSecrets};
pub use validators::{validate_snowflake_account_id, validate_supabase_project_ref};
