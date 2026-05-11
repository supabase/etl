//! Configuration objects for ETL pipelines.
//!
//! Re-exports configuration types and utilities required for pipeline setup and
//! operation.

pub use etl_config::{
    Config, Environment, LoadConfigError, ParseDucklakeUrlError, SerializableSecretString,
    load_config, parse_ducklake_url,
    shared::{
        BatchConfig, DestinationConfig, DestinationConfigWithoutSecrets, ETL_API_OPTIONS,
        ETL_MIGRATION_OPTIONS, ETL_REPLICATION_OPTIONS, ETL_STATE_MANAGEMENT_OPTIONS,
        IcebergConfig, IcebergConfigWithoutSecrets, IntoConnectOptions, InvalidatedSlotBehavior,
        MemoryBackpressureConfig, PgConnectionConfig, PgConnectionConfigWithoutSecrets,
        PgConnectionOptions, PipelineConfig, PipelineConfigWithoutSecrets, ReplicatorConfig,
        ReplicatorConfigWithoutSecrets, SentryConfig, SupabaseConfig, SupabaseConfigWithoutSecrets,
        TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig, ValidationError,
    },
};
