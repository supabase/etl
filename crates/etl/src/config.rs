//! Configuration objects for ETL pipelines.
//!
//! Re-exports configuration types and utilities required for pipeline setup and
//! operation.

pub use etl_config::{
    Config, Environment, LoadConfigError, ParseDucklakeUrlError, SerializableSecretString,
    libpq_tcp_host, load_config, parse_ducklake_s3_data_path, parse_ducklake_url,
    shared::{
        BatchConfig, DestinationConfig, DestinationConfigWithoutSecrets, IcebergConfig,
        IcebergConfigWithoutSecrets, IntoConnectOptions, InvalidatedSlotBehavior,
        MemoryBackpressureConfig, PgConnectionConfig, PgConnectionConfigWithoutSecrets,
        PgConnectionOptions, PgConnectionOptionsBuilder, PipelineConfig,
        PipelineConfigWithoutSecrets, ReplicatorConfig, ReplicatorConfigWithoutSecrets,
        SentryConfig, SupabaseConfig, SupabaseConfigWithoutSecrets, TableSyncCopyConfig,
        TcpKeepaliveConfig, TlsConfig, ValidationError,
    },
};
