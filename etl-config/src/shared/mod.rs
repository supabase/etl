//! Shared configuration types for etl pipelines.

mod batch;
mod connection;
mod heartbeat;
mod pipeline;

pub use batch::BatchConfig;
pub use connection::{
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError,
};
pub use heartbeat::{HeartbeatConfig, ETL_HEARTBEAT_OPTIONS};
pub use pipeline::{
    PipelineConfig, PipelineConfigWithoutSecrets, TableSyncCopyConfig,
};
